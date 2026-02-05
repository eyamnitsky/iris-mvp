import os
import json
import boto3
import email
from email import policy
from email.utils import getaddresses
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import uuid
from typing import Optional
from botocore.exceptions import ClientError

# --- NEW: AI parser import (module, not a Lambda) ---
from iris_ai_parser import parse_email


# -----------------------------
# AWS clients
# -----------------------------
AWS_REGION = os.environ.get("AWS_REGION")  # Lambda sets this automatically
s3 = boto3.client("s3", region_name=AWS_REGION)
ses = boto3.client("ses", region_name=AWS_REGION)

# Force DynamoDB to the Lambda region to avoid cross-region table mismatch
ddb = boto3.resource("dynamodb", region_name=AWS_REGION)
ddb_client = boto3.client("dynamodb", region_name=AWS_REGION)


# -----------------------------
# Env vars (existing)
# -----------------------------
BUCKET_NAME = os.environ["BUCKET_NAME"]
TABLE_NAME = os.environ["TABLE_NAME"]
IRIS_EMAIL = os.environ.get("IRIS_EMAIL", "iris@liazon.cc").lower()
TIMEZONE = os.environ.get("TIMEZONE", "America/New_York")
DEFAULT_START_HOUR = int(os.environ.get("DEFAULT_START_HOUR", "13"))
DEFAULT_DURATION_MINUTES = int(os.environ.get("DEFAULT_DURATION_MINUTES", "30"))

# Optional if your table has a sort key: what fixed value to use for message rows
DDB_SK_VALUE = os.environ.get("DDB_SK_VALUE", "STATE")


# -----------------------------
# DynamoDB table + schema discovery
# -----------------------------
table = ddb.Table(TABLE_NAME)

# Cache schema across warm invocations
PK_ATTR = None
SK_ATTR = None
SK_TYPE = None  # 'S' | 'N' | 'B' or None

def _ensure_ddb_schema_loaded():
    """
    Always safe to call. On first call per warm container, it loads and prints the
    table key schema so we can debug key-mismatch issues deterministically.
    """
    global PK_ATTR, SK_ATTR, SK_TYPE
    if PK_ATTR is not None:
        return

    desc = ddb_client.describe_table(TableName=TABLE_NAME)["Table"]
    key_schema = desc.get("KeySchema", [])
    attr_defs = desc.get("AttributeDefinitions", [])

    print("[ddb] region=", AWS_REGION)
    print("[ddb] table=", TABLE_NAME)
    print("[ddb] KeySchema=", key_schema)
    print("[ddb] AttrDefs=", attr_defs)

    PK_ATTR = next((k["AttributeName"] for k in key_schema if k["KeyType"] == "HASH"), None)
    SK_ATTR = next((k["AttributeName"] for k in key_schema if k["KeyType"] == "RANGE"), None)

    if SK_ATTR:
        SK_TYPE = next((a.get("AttributeType") for a in attr_defs if a.get("AttributeName") == SK_ATTR), None)

    if not PK_ATTR:
        raise RuntimeError(f"Could not determine PK attribute for table {TABLE_NAME}")


def _ddb_key_for_message(message_id: str) -> dict:
    """
    Build the correct DynamoDB primary key for this table at runtime.
    Supports PK-only or PK+SK tables.
    """
    _ensure_ddb_schema_loaded()
    key = {PK_ATTR: f"msg#{message_id}"}
    if SK_ATTR:
        key[SK_ATTR] = DDB_SK_VALUE
    return key


# -----------------------------
# Helpers (existing)
# -----------------------------
def _flatten_emails(header_value: Optional[str]) -> list[str]:
    if not header_value:
        return []
    return [addr.lower() for _, addr in getaddresses([header_value]) if addr]


def _dedupe(seq: list[str]) -> list[str]:
    seen = set()
    out = []
    for x in seq:
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out


def _next_day_at_default_time(local_tz: ZoneInfo):
    now_local = datetime.now(tz=local_tz)
    next_day = (now_local + timedelta(days=1)).date()
    start = datetime(
        next_day.year,
        next_day.month,
        next_day.day,
        DEFAULT_START_HOUR,
        0,
        0,
        tzinfo=local_tz,
    )
    end = start + timedelta(minutes=DEFAULT_DURATION_MINUTES)
    return start, end


def _build_ics(subject: str, start: datetime, end: datetime, organizer: str, attendees: list[str], uid: str) -> str:
    dtstamp = datetime.now(tz=ZoneInfo("UTC")).strftime("%Y%m%dT%H%M%SZ")

    def fmt(dt: datetime) -> str:
        return dt.strftime("%Y%m%dT%H%M%S")

    lines = [
        "BEGIN:VCALENDAR",
        "PRODID:-//Iris MVP//EN",
        "VERSION:2.0",
        "CALSCALE:GREGORIAN",
        "METHOD:REQUEST",
        "BEGIN:VEVENT",
        f"UID:{uid}",
        f"DTSTAMP:{dtstamp}",
        f"SUMMARY:{subject}",
        f"DTSTART;TZID={TIMEZONE}:{fmt(start)}",
        f"DTEND;TZID={TIMEZONE}:{fmt(end)}",
        f"ORGANIZER:mailto:{organizer}",
    ]

    for a in attendees:
        lines.append(f"ATTENDEE;CN={a};RSVP=TRUE:mailto:{a}")

    lines += ["END:VEVENT", "END:VCALENDAR", ""]
    return "\r\n".join(lines)


def _build_raw_mime_reply(
    subject: str,
    text_body: str,
    ics_body: str,
    from_addr: str,
    to_addrs: list[str],
    in_reply_to: Optional[str],
    references: Optional[str],
) -> bytes:
    msg = email.message.EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = ", ".join(to_addrs)
    if in_reply_to:
        msg["In-Reply-To"] = in_reply_to
    if references:
        msg["References"] = references

    msg.set_content(text_body)

    msg.add_attachment(
        ics_body.encode("utf-8"),
        maintype="text",
        subtype="calendar",
        filename="invite.ics",
        params={"method": "REQUEST"},
    )

    return msg.as_bytes(policy=policy.SMTP)


def _safe_json(obj) -> str:
    try:
        return json.dumps(obj, default=str)
    except Exception:
        return "<unserializable>"


def _load_email_bytes_from_s3(bucket: str, message_id: str, receipt: dict) -> tuple[bytes, str]:
    candidate_keys: list[str] = []

    action = (receipt or {}).get("action", {}) or {}
    if isinstance(action, dict):
        if action.get("objectKey"):
            candidate_keys.append(action["objectKey"])
        if action.get("key"):
            candidate_keys.append(action["key"])

    candidate_keys.append(f"raw/{message_id}")
    candidate_keys.append(message_id)

    last_err: Exception | None = None
    for k in _dedupe(candidate_keys):
        try:
            print(f"[s3] trying key={k}")
            resp = s3.get_object(Bucket=bucket, Key=k)
            data = resp["Body"].read()
            print(f"[s3] loaded key={k} bytes={len(data)}")
            return data, k
        except Exception as e:
            last_err = e

    raise last_err if last_err else RuntimeError("Failed to load email from S3")


# -----------------------------
# Lambda handler
# -----------------------------
def lambda_handler(event, context):
    print("DEPLOY_MARKER_AI_002")
    print("[event] records=", len(event.get("Records", [])))

    try:
        record = event["Records"][0]
        ses_payload = record.get("ses", {}) or {}
        receipt = ses_payload.get("receipt", {}) or {}
        mail = ses_payload.get("mail", {}) or {}

        message_id = mail.get("messageId") or str(uuid.uuid4())
        print(f"[ses] messageId={message_id}")

        # --- NEW: log DDB key details before GetItem ---
        _ensure_ddb_schema_loaded()
        ddb_key = _ddb_key_for_message(message_id)
        print("[ddb] PK_ATTR=", PK_ATTR, "SK_ATTR=", SK_ATTR, "SK_TYPE=", SK_TYPE)
        print("[ddb] GetItem Key=", ddb_key)
        if SK_ATTR and SK_TYPE and SK_TYPE != "S":
            print("[ddb] WARNING: sort key type is not String. Current DDB_SK_VALUE is a string:", DDB_SK_VALUE)

        # --- DDB idempotency check (FIXED to match table schema) ---
        existing = table.get_item(Key=ddb_key).get("Item")
        if existing and existing.get("invite_sent_at"):
            print(f"[ddb] idempotent skip message_id={message_id}")
            return {"statusCode": 200, "body": json.dumps({"ok": True, "skipped": True})}

        raw_bytes, used_key = _load_email_bytes_from_s3(BUCKET_NAME, message_id, receipt)
        eml = email.message_from_bytes(raw_bytes, policy=policy.default)

        subject = eml.get("Subject", "(no subject)")
        from_email_list = _flatten_emails(eml.get("From"))[:1]
        if not from_email_list:
            return {"statusCode": 400, "body": json.dumps({"error": "missing From"})}
        from_email = from_email_list[0]

        to_emails = _flatten_emails(eml.get("To"))
        cc_emails = _flatten_emails(eml.get("Cc"))

        # Only respond when Iris is CC'd (existing behavior)
        if IRIS_EMAIL not in [e.lower() for e in cc_emails]:
            return {"statusCode": 200, "body": json.dumps({"ok": True, "ignored": "iris_not_cc"})}

        # Extract text body (plain text)
        body_text = ""
        if eml.is_multipart():
            for part in eml.walk():
                if part.get_content_type() == "text/plain":
                    body_text = part.get_content()
                    break
        else:
            body_text = eml.get_content()

        # --- NEW: invoke AI parser and log result (NO behavior change yet) ---
        try:
            ai_result = parse_email(
                {
                    "thread_id": f"thread#{message_id}",
                    "message_id": message_id,
                    "body_text": body_text,
                    "timezone_default": TIMEZONE,
                }
            )
            print("[ai] result=", _safe_json(ai_result))
        except Exception as e:
            # Do not break scheduling if AI fails (for now)
            print("[ai] ERROR (ignored for now):", repr(e))
            ai_result = None

        # ---- EXISTING scheduling behavior remains unchanged ----
        tz = ZoneInfo(TIMEZONE)
        start, end = _next_day_at_default_time(tz)

        event_uid = f"{uuid.uuid4()}@{IRIS_EMAIL.split('@', 1)[1]}"
        ics = _build_ics(
            subject=subject,
            start=start,
            end=end,
            organizer=IRIS_EMAIL,
            attendees=_dedupe([from_email] + to_emails),
            uid=event_uid,
        )

        text_body = "I scheduled a meeting for 1:00 PM tomorrow.\n"

        raw_mime = _build_raw_mime_reply(
            subject=f"Re: {subject}",
            text_body=text_body,
            ics_body=ics,
            from_addr=IRIS_EMAIL,
            to_addrs=_dedupe([from_email] + to_emails + cc_emails),
            in_reply_to=eml.get("Message-Id"),
            references=eml.get("References"),
        )

        ses.send_raw_email(
            Source=IRIS_EMAIL,
            Destinations=_dedupe([from_email] + to_emails + cc_emails),
            RawMessage={"Data": raw_mime},
        )

        # --- DDB write (FIXED key schema) ---
        item = _ddb_key_for_message(message_id)
        item.update(
            {
                "subject": subject,
                "from_email": from_email,
                "to_emails": set(to_emails),
                "cc_emails": set(cc_emails),
                "s3_key": used_key,
                "received_at": datetime.utcnow().isoformat() + "Z",
                "event_uid": event_uid,
                "invite_sent_at": datetime.utcnow().isoformat() + "Z",
            }
        )
        table.put_item(Item=item)

        return {"statusCode": 200, "body": json.dumps({"ok": True})}

    except ClientError as e:
        print("[error] ClientError", repr(e))
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
    except Exception as e:
        print("[error]", repr(e))
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}