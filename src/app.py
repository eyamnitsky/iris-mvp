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

s3 = boto3.client("s3")
ses = boto3.client("ses")
ddb = boto3.resource("dynamodb")

BUCKET_NAME = os.environ["BUCKET_NAME"]
TABLE_NAME = os.environ["TABLE_NAME"]
IRIS_EMAIL = os.environ.get("IRIS_EMAIL", "iris@liazon.cc").lower()
TIMEZONE = os.environ.get("TIMEZONE", "America/New_York")
DEFAULT_START_HOUR = int(os.environ.get("DEFAULT_START_HOUR", "13"))
DEFAULT_DURATION_MINUTES = int(os.environ.get("DEFAULT_DURATION_MINUTES", "30"))

table = ddb.Table(TABLE_NAME)


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
        ics_body,
        maintype="text",
        subtype="calendar",
        filename="invite.ics",
        params={"method": "REQUEST", "charset": "UTF-8"},
    )

    return msg.as_bytes(policy=policy.SMTP)


def _safe_json(obj) -> str:
    try:
        return json.dumps(obj, default=str)
    except Exception:
        return "<unserializable>"


def _load_email_bytes_from_s3(bucket: str, message_id: str, receipt: dict) -> tuple[bytes, str]:
    """
    SES -> S3 -> Lambda often does NOT include the S3 object key in the Lambda invocation payload.
    Since we configured the S3 action prefix as 'raw/', SES stores objects under raw/<messageId>.
    Try a few candidate keys deterministically.
    """
    candidate_keys: list[str] = []

    action = (receipt or {}).get("action", {}) or {}
    # Sometimes action includes objectKey/key; often it doesn't (it may refer to the Lambda action).
    if isinstance(action, dict):
        if action.get("objectKey"):
            candidate_keys.append(action["objectKey"])
        if action.get("key"):
            candidate_keys.append(action["key"])

    # Our S3 action uses prefix raw/ (and sometimes SES uses messageId as the key)
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

    print(f"[s3] FAILED to load email. bucket={bucket} candidates={candidate_keys} err={repr(last_err)}")
    raise last_err if last_err else RuntimeError("Failed to load email from S3")


def lambda_handler(event, context):
    # Log enough to debug, but avoid dumping huge payloads.
    print("[event] records=", len(event.get("Records", [])))
    print("[event] head=", _safe_json(event)[:2000])

    try:
        record = event["Records"][0]
        ses_payload = record.get("ses", {}) or {}
        receipt = ses_payload.get("receipt", {}) or {}
        mail = ses_payload.get("mail", {}) or {}

        message_id = mail.get("messageId") or str(uuid.uuid4())
        print(f"[ses] messageId={message_id}")

        # Idempotency
        pk = f"msg#{message_id}"
        existing = table.get_item(Key={"pk": pk}).get("Item")
        if existing and existing.get("invite_sent_at"):
            print(f"[ddb] idempotent skip pk={pk}")
            return {"statusCode": 200, "body": json.dumps({"ok": True, "skipped": True})}

        # Load raw email bytes from S3
        raw_bytes, used_key = _load_email_bytes_from_s3(BUCKET_NAME, message_id, receipt)
        eml = email.message_from_bytes(raw_bytes, policy=policy.default)

        subject = eml.get("Subject", "(no subject)")
        from_email_list = _flatten_emails(eml.get("From"))[:1]
        if not from_email_list:
            print("[parse] missing From header")
            return {"statusCode": 400, "body": json.dumps({"error": "missing From"})}
        from_email = from_email_list[0]

        to_emails = _flatten_emails(eml.get("To"))
        cc_emails = _flatten_emails(eml.get("Cc"))

        print(f"[parse] subject={subject!r} from={from_email} to={to_emails} cc={cc_emails}")

        # Process only if Iris was CC'd in *headers*
        if IRIS_EMAIL not in [e.lower() for e in cc_emails]:
            print(f"[logic] ignoring: iris not in Cc header. IRIS={IRIS_EMAIL} cc={cc_emails}")
            return {"statusCode": 200, "body": json.dumps({"ok": True, "ignored": "iris_not_cc"})}

        # Reply-all recipients: From + To + Cc, minus Iris
        recipients = _dedupe([from_email] + to_emails + cc_emails)
        recipients = [r for r in recipients if r.lower() != IRIS_EMAIL]
        print(f"[logic] reply-all recipients={recipients}")

        # Threading headers
        orig_msg_id = eml.get("Message-Id")
        references = eml.get("References")
        if references and orig_msg_id and orig_msg_id not in references:
            references = references.strip() + " " + orig_msg_id
        elif not references and orig_msg_id:
            references = orig_msg_id

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
            to_addrs=recipients,
            in_reply_to=orig_msg_id,
            references=references,
        )

        # Send invite email
        print("[ses] sending invite...")
        ses.send_raw_email(
            Source=IRIS_EMAIL,
            Destinations=recipients,
            RawMessage={"Data": raw_mime},
        )
        print("[ses] invite sent")

        # Write Dynamo record
        print(f"[ddb] writing item pk={pk} s3key={used_key}")
        table.put_item(
            Item={
                "pk": pk,
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

        return {"statusCode": 200, "body": json.dumps({"ok": True, "sent_to": recipients})}

    except Exception as e:
        print("[error]", repr(e))
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
