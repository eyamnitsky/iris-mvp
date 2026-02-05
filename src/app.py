import os
import json
import re
import boto3
import email
from email import policy
from email.utils import getaddresses
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import uuid
from typing import Optional
from botocore.exceptions import ClientError

# AI parser module (same folder as app.py in the Lambda package)
from iris_ai_parser import parse_email


# -----------------------------
# AWS clients
# -----------------------------
AWS_REGION = os.environ.get("AWS_REGION")  # Lambda sets this automatically
s3 = boto3.client("s3", region_name=AWS_REGION)
ses = boto3.client("ses", region_name=AWS_REGION)

ddb = boto3.resource("dynamodb", region_name=AWS_REGION)
ddb_client = boto3.client("dynamodb", region_name=AWS_REGION)


# -----------------------------
# Env vars
# -----------------------------
BUCKET_NAME = os.environ["BUCKET_NAME"]
TABLE_NAME = os.environ["TABLE_NAME"]
IRIS_EMAIL = os.environ.get("IRIS_EMAIL", "iris@liazon.cc").lower()
TIMEZONE = os.environ.get("TIMEZONE", "America/New_York")

DEFAULT_START_HOUR = int(os.environ.get("DEFAULT_START_HOUR", "13"))
DEFAULT_DURATION_MINUTES = int(os.environ.get("DEFAULT_DURATION_MINUTES", "30"))

# If your DDB table has a sort key, this is the constant value we use for message rows.
DDB_SK_VALUE = os.environ.get("DDB_SK_VALUE", "STATE")


# -----------------------------
# DynamoDB table + schema discovery (lazy, but cached per warm container)
# -----------------------------
table = ddb.Table(TABLE_NAME)

PK_ATTR = None
SK_ATTR = None
SK_TYPE = None  # 'S' | 'N' | 'B' or None


def _ensure_ddb_schema_loaded():
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
    _ensure_ddb_schema_loaded()
    key = {PK_ATTR: f"msg#{message_id}"}
    if SK_ATTR:
        key[SK_ATTR] = DDB_SK_VALUE
    return key


# -----------------------------
# Email helpers
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


def _extract_plaintext_body(eml: email.message.EmailMessage) -> str:
    body_text = ""
    if eml.is_multipart():
        for part in eml.walk():
            if part.get_content_type() == "text/plain":
                body_text = part.get_content()
                break
    else:
        body_text = eml.get_content()
    return body_text or ""


# -----------------------------
# Scheduling helpers
# -----------------------------
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


# 3-letter day-of-week keys only
_DOW = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}


def _next_weekday_date(today_local: datetime, target_wd: int) -> datetime:
    days_ahead = (target_wd - today_local.weekday()) % 7
    return today_local + timedelta(days=days_ahead)


def _parse_time_12h(s: str) -> tuple[int, int]:
    s = s.strip().lower()
    m = re.search(r"\b(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\b", s)
    if not m:
        raise ValueError(f"Could not parse time from: {s}")
    hour = int(m.group(1))
    minute = int(m.group(2) or "0")
    ampm = m.group(3)

    if ampm:
        if hour == 12:
            hour = 0
        if ampm == "pm":
            hour += 12

    return hour, minute


def _candidate_to_datetimes(candidate: dict, tz: ZoneInfo) -> tuple[datetime, datetime]:
    """
    Convert candidate like:
      start_local: 'Saturday 3:00 PM'
      end_local:   'Saturday 3:30 PM' OR '3:30 PM'
    into timezone-aware datetimes.
    """
    start_local = (candidate.get("start_local") or "").strip()
    end_local = (candidate.get("end_local") or "").strip()
    if not start_local or not end_local:
        raise ValueError("Missing start_local/end_local")

    # FIX: match full day names too (Saturday/Sunday/etc.)
    mday = re.search(
        r"\b(mon(?:day)?|tue(?:sday)?|wed(?:nesday)?|thu(?:rsday)?|fri(?:day)?|sat(?:urday)?|sun(?:day)?)\b",
        start_local,
        re.IGNORECASE,
    )
    if not mday:
        raise ValueError(f"No weekday found in start_local: {start_local}")

    wd = mday.group(1).lower()  # e.g. 'saturday' or 'sat'
    # normalize to 3-letter key
    if wd.startswith("tue"):
        wd_key = "tue"
    elif wd.startswith("thu"):
        wd_key = "thu"
    else:
        wd_key = wd[:3]

    target_wd = _DOW[wd_key]

    now_local = datetime.now(tz=tz)
    base = _next_weekday_date(now_local, target_wd)

    sh, sm = _parse_time_12h(start_local)
    eh, em = _parse_time_12h(end_local)

    start_dt = datetime(base.year, base.month, base.day, sh, sm, tzinfo=tz)
    end_dt = datetime(base.year, base.month, base.day, eh, em, tzinfo=tz)

    # If it landed on "today" but time is already past, push to next week
    if start_dt <= now_local and base.date() == now_local.date():
        start_dt = start_dt + timedelta(days=7)
        end_dt = end_dt + timedelta(days=7)

    if end_dt <= start_dt:
        end_dt = end_dt + timedelta(days=1)

    return start_dt, end_dt


# -----------------------------
# MIME building (text-only and text+ICS)
# -----------------------------
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


def _build_raw_mime_text_reply(
    subject: str,
    text_body: str,
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
    return msg.as_bytes(policy=policy.SMTP)


def _build_raw_mime_reply_with_ics(
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


# -----------------------------
# Lambda handler
# -----------------------------
def lambda_handler(event, context):
    print("DEPLOY_MARKER_AI_SCHEDULE_002")
    print("[event] records=", len(event.get("Records", [])))

    try:
        record = event["Records"][0]
        ses_payload = record.get("ses", {}) or {}
        receipt = ses_payload.get("receipt", {}) or {}
        mail = ses_payload.get("mail", {}) or {}

        message_id = mail.get("messageId") or str(uuid.uuid4())
        print(f"[ses] messageId={message_id}")

        # ---- DDB idempotency ----
        ddb_key = _ddb_key_for_message(message_id)
        existing = table.get_item(Key=ddb_key).get("Item")

        if existing and (existing.get("invite_sent_at") or existing.get("clarification_sent_at")):
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

        # Only respond when Iris is CC'd
        if IRIS_EMAIL not in [e.lower() for e in cc_emails]:
            return {"statusCode": 200, "body": json.dumps({"ok": True, "ignored": "iris_not_cc"})}

        body_text = _extract_plaintext_body(eml)

        # ---- AI parse ----
        ai_result = parse_email(
            {
                "thread_id": f"thread#{message_id}",
                "message_id": message_id,
                "body_text": body_text,
                "timezone_default": TIMEZONE,
            }
        )
        print("[ai] result=", _safe_json(ai_result))

        ai_parsed = (ai_result.get("parsed") or {}) if ai_result.get("ok") else None

        reply_recipients = _dedupe([from_email] + to_emails + cc_emails)

        # ---- Clarification path: email question only, no ICS ----
        if ai_parsed and ai_parsed.get("needs_clarification"):
            clar_q = (ai_parsed.get("clarifying_question") or "What day and time works for you?").strip()
            text_body_reply = clar_q + "\n"

            raw_mime = _build_raw_mime_text_reply(
                subject=f"Re: {subject}",
                text_body=text_body_reply,
                from_addr=IRIS_EMAIL,
                to_addrs=reply_recipients,
                in_reply_to=eml.get("Message-Id"),
                references=eml.get("References"),
            )

            ses.send_raw_email(
                Source=IRIS_EMAIL,
                Destinations=reply_recipients,
                RawMessage={"Data": raw_mime},
            )

            item = _ddb_key_for_message(message_id)
            item.update(
                {
                    "subject": subject,
                    "from_email": from_email,
                    "to_emails": set(to_emails),
                    "cc_emails": set(cc_emails),
                    "s3_key": used_key,
                    "received_at": datetime.utcnow().isoformat() + "Z",
                    "clarification_sent_at": datetime.utcnow().isoformat() + "Z",
                    "ai_json": json.dumps(ai_parsed),
                }
            )
            table.put_item(Item=item)

            return {"statusCode": 200, "body": json.dumps({"ok": True, "action": "clarify"})}

        # ---- Scheduling path: use AI candidate if present; else fallback ----
        tz = ZoneInfo(TIMEZONE)
        start, end = _next_day_at_default_time(tz)

        if ai_parsed and not ai_parsed.get("needs_clarification"):
            candidates = ai_parsed.get("candidates") or []
            if candidates:
                try:
                    start, end = _candidate_to_datetimes(candidates[0], tz)
                    print("[decision] scheduling from AI candidate", start, end)
                except Exception as e:
                    print("[decision] AI candidate parse failed; falling back:", repr(e))

        event_uid = f"{uuid.uuid4()}@{IRIS_EMAIL.split('@', 1)[1]}"
        ics = _build_ics(
            subject=subject,
            start=start,
            end=end,
            organizer=IRIS_EMAIL,
            attendees=_dedupe([from_email] + to_emails),
            uid=event_uid,
        )

        try:
            pretty_when = start.strftime("%A %I:%M %p").lstrip("0")
        except Exception:
            pretty_when = "the requested time"
        text_body_reply = f"I scheduled a meeting for {pretty_when}.\n"

        raw_mime = _build_raw_mime_reply_with_ics(
            subject=f"Re: {subject}",
            text_body=text_body_reply,
            ics_body=ics,
            from_addr=IRIS_EMAIL,
            to_addrs=reply_recipients,
            in_reply_to=eml.get("Message-Id"),
            references=eml.get("References"),
        )

        ses.send_raw_email(
            Source=IRIS_EMAIL,
            Destinations=reply_recipients,
            RawMessage={"Data": raw_mime},
        )

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
                "ai_json": json.dumps(ai_parsed) if ai_parsed else "{}",
                "scheduled_start": start.isoformat(),
                "scheduled_end": end.isoformat(),
            }
        )
        table.put_item(Item=item)

        return {"statusCode": 200, "body": json.dumps({"ok": True, "action": "scheduled"})}

    except ClientError as e:
        print("[error] ClientError", repr(e))
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
    except Exception as e:
        print("[error]", repr(e))
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}