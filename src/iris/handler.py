from __future__ import annotations

import json
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo
from botocore.exceptions import ClientError

from .config import BUCKET_NAME, IRIS_EMAIL, TIMEZONE, require_env
from .aws_clients import table as _table, ses as _ses
from .ddb import key_for_message
from .email_utils import flatten_emails, dedupe, safe_json, extract_plaintext_body, parse_eml
from .s3_loader import load_email_bytes_from_s3
from .scheduling import next_day_at_default_time, candidate_to_datetimes
from .mime_builder import build_ics, build_raw_mime_text_reply, build_raw_mime_reply_with_ics

# Backwards-compatible import (root-level shim also exists)
from iris_ai_parser import parse_email


def handle_ses_event(event: dict) -> dict:
    print("DEPLOY_MARKER_AI_SCHEDULE_002")
    print("[event] records=", len(event.get("Records", [])))

    record = event["Records"][0]
    ses_payload = record.get("ses", {}) or {}
    receipt = ses_payload.get("receipt", {}) or {}
    mail = ses_payload.get("mail", {}) or {}

    message_id = mail.get("messageId") or str(uuid.uuid4())
    print(f"[ses] messageId={message_id}")

    # ---- DDB idempotency ----
    ddb_key = key_for_message(message_id)
    existing = _table().get_item(Key=ddb_key).get("Item")

    if existing and (existing.get("invite_sent_at") or existing.get("clarification_sent_at")):
        print(f"[ddb] idempotent skip message_id={message_id}")
        return {"statusCode": 200, "body": json.dumps({"ok": True, "skipped": True})}

    raw_bytes, used_key = load_email_bytes_from_s3(BUCKET_NAME, message_id, receipt)
    eml = parse_eml(raw_bytes)

    subject = eml.get("Subject", "(no subject)")
    from_email_list = flatten_emails(eml.get("From"))[:1]
    if not from_email_list:
        return {"statusCode": 400, "body": json.dumps({"error": "missing From"})}
    from_email = from_email_list[0]

    to_emails = flatten_emails(eml.get("To"))
    cc_emails = flatten_emails(eml.get("Cc"))

    to_set = {e.lower() for e in to_emails}
    cc_set = {e.lower() for e in cc_emails}

    # Ignore messages sent BY Iris (avoid loops)
    if from_email == IRIS_EMAIL:
        return {"statusCode": 200, "body": json.dumps({"ok": True, "ignored": "from_iris"})}

    # Process if Iris is in To or Cc
    if IRIS_EMAIL not in to_set and IRIS_EMAIL not in cc_set:
        return {"statusCode": 200, "body": json.dumps({"ok": True, "ignored": "iris_not_recipient"})}

    body_text = extract_plaintext_body(eml)

    # ---- AI parse ----
    ai_result = parse_email(
        {
            "thread_id": f"thread#{message_id}",
            "message_id": message_id,
            "body_text": body_text,
            "timezone_default": TIMEZONE,
        }
    )

    print("[ai] result=", safe_json(ai_result))

    ai_parsed = (ai_result.get("parsed") or {}) if ai_result.get("ok") else None

    reply_recipients = dedupe([from_email] + to_emails + cc_emails)

    # ---- Clarification path: email question only, no ICS ----
    if ai_parsed and ai_parsed.get("needs_clarification"):
        clar_q = (ai_parsed.get("clarifying_question") or "What day and time works for you?").strip()
        text_body_reply = clar_q + "\n"

        raw_mime = build_raw_mime_text_reply(
            subject=f"Re: {subject}",
            text_body=text_body_reply,
            from_addr=IRIS_EMAIL,
            to_addrs=reply_recipients,
            in_reply_to=eml.get("Message-Id"),
            references=eml.get("References"),
        )

        _ses().send_raw_email(
            Source=IRIS_EMAIL,
            Destinations=reply_recipients,
            RawMessage={"Data": raw_mime},
        )

        item = key_for_message(message_id)
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
        _table().put_item(Item=item)

        return {"statusCode": 200, "body": json.dumps({"ok": True, "action": "clarify"})}

    # ---- Scheduling path: use AI candidate if present; else fallback ----
    tz = ZoneInfo(TIMEZONE)
    start, end = next_day_at_default_time(tz)

    if ai_parsed and not ai_parsed.get("needs_clarification"):
        candidates = ai_parsed.get("candidates") or []
        if candidates:
            try:
                start, end = candidate_to_datetimes(candidates[0], tz)
                print("[decision] scheduling from AI candidate", start, end)
            except Exception as e:
                print("[decision] AI candidate parse failed; falling back:", repr(e))

    event_uid = f"{uuid.uuid4()}@{IRIS_EMAIL.split('@', 1)[1]}"
    ics = build_ics(
        subject=subject,
        start=start,
        end=end,
        organizer=IRIS_EMAIL,
        attendees=dedupe([from_email] + to_emails),
        uid=event_uid,
    )

    try:
        pretty_when = start.strftime("%A %I:%M %p").lstrip("0")
    except Exception:
        pretty_when = "the requested time"
    text_body_reply = f"I scheduled a meeting for {pretty_when}.\n"

    raw_mime = build_raw_mime_reply_with_ics(
        subject=f"Re: {subject}",
        text_body=text_body_reply,
        ics_body=ics,
        from_addr=IRIS_EMAIL,
        to_addrs=reply_recipients,
        in_reply_to=eml.get("Message-Id"),
        references=eml.get("References"),
    )

    _ses().send_raw_email(
        Source=IRIS_EMAIL,
        Destinations=reply_recipients,
        RawMessage={"Data": raw_mime},
    )

    item = key_for_message(message_id)
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
    _table().put_item(Item=item)

    return {"statusCode": 200, "body": json.dumps({"ok": True, "action": "scheduled"})}


def lambda_handler(event, context):
    try:
        return handle_ses_event(event)
    except ClientError as e:
        print("[error] ClientError", repr(e))
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
    except Exception as e:
        print("[error]", repr(e))
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
