import os
import json
import boto3
import email
from email import policy
from email.utils import getaddresses
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import uuid

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

def _flatten_emails(header_value: str) -> list[str]:
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

def _next_day_at_1pm(local_tz: ZoneInfo):
    now_local = datetime.now(tz=local_tz)
    next_day = (now_local + timedelta(days=1)).date()
    start = datetime(next_day.year, next_day.month, next_day.day, DEFAULT_START_HOUR, 0, 0, tzinfo=local_tz)
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

def _build_raw_mime_reply(subject: str, text_body: str, ics_body: str, from_addr: str, to_addrs: list[str],
                         in_reply_to: str | None, references: str | None) -> bytes:
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

def lambda_handler(event, context):
    try:
        record = event["Records"][0]
        ses_payload = record.get("ses", {})
        receipt = ses_payload.get("receipt", {})
        mail = ses_payload.get("mail", {})

        message_id = mail.get("messageId") or str(uuid.uuid4())

        # SES S3 action fields can vary; we support common shapes.
        action = receipt.get("action", {}) or {}
        object_key = action.get("objectKey") or action.get("key") or message_id

        # Idempotency
        pk = f"msg#{message_id}"
        existing = table.get_item(Key={"pk": pk}).get("Item")
        if existing and existing.get("invite_sent_at"):
            return {"statusCode": 200, "body": json.dumps({"ok": True, "skipped": True})}

        raw_obj = s3.get_object(Bucket=BUCKET_NAME, Key=object_key)
        raw_bytes = raw_obj["Body"].read()
        eml = email.message_from_bytes(raw_bytes, policy=policy.default)

        subject = eml.get("Subject", "(no subject)")
        from_email_list = _flatten_emails(eml.get("From"))[:1]
        if not from_email_list:
            return {"statusCode": 400, "body": json.dumps({"error": "missing From"})}
        from_email = from_email_list[0]

        to_emails = _flatten_emails(eml.get("To"))
        cc_emails = _flatten_emails(eml.get("Cc"))

        # Process only if Iris was CC'd
        if IRIS_EMAIL not in [e.lower() for e in cc_emails]:
            return {"statusCode": 200, "body": json.dumps({"ok": True, "ignored": "iris_not_cc"})}

        # Reply-all recipients: From + To + Cc, minus Iris
        recipients = _dedupe([from_email] + to_emails + cc_emails)
        recipients = [r for r in recipients if r.lower() != IRIS_EMAIL]

        orig_msg_id = eml.get("Message-Id")
        references = eml.get("References")
        if references and orig_msg_id and orig_msg_id not in references:
            references = references.strip() + " " + orig_msg_id
        elif not references and orig_msg_id:
            references = orig_msg_id

        tz = ZoneInfo(TIMEZONE)
        start, end = _next_day_at_1pm(tz)

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

        ses.send_raw_email(
            Source=IRIS_EMAIL,
            Destinations=recipients,
            RawMessage={"Data": raw_mime},
        )

        table.put_item(
            Item={
                "pk": pk,
                "subject": subject,
                "from_email": from_email,
                "to_emails": set(to_emails),
                "cc_emails": set(cc_emails),
                "received_at": datetime.utcnow().isoformat() + "Z",
                "event_uid": event_uid,
                "invite_sent_at": datetime.utcnow().isoformat() + "Z",
            }
        )

        return {"statusCode": 200, "body": json.dumps({"ok": True, "sent_to": recipients})}

    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
