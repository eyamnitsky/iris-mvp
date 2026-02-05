from __future__ import annotations

import json
import uuid
import re
from datetime import datetime
from zoneinfo import ZoneInfo
from botocore.exceptions import ClientError

from ..infra.config import BUCKET_NAME, IRIS_EMAIL, TIMEZONE, require_env
from ..infra.aws_clients import table as _table, ses as _ses
from ..infra.ddb import key_for_message
from ..email.email_utils import flatten_emails, dedupe, safe_json, extract_plaintext_body, parse_eml
from ..infra.s3_loader import load_email_bytes_from_s3
from ..scheduling.scheduling import next_day_at_default_time, candidate_to_datetimes
from ..email.mime_builder import build_ics, build_raw_mime_text_reply, build_raw_mime_reply_with_ics
from ..conversation.engine import process_incoming_email
from ..conversation.guardrails import apply_input_guardrail

# Backwards-compatible import (root-level shim also exists)
from iris_ai_parser import parse_email

from decimal import Decimal

def ddb_sanitize(obj):
    if isinstance(obj, float):
        return Decimal(str(obj))
    if isinstance(obj, dict):
        return {k: ddb_sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [ddb_sanitize(v) for v in obj]
    if isinstance(obj, tuple):
        return [ddb_sanitize(v) for v in obj]
    return obj

def _extract_thread_root_id(eml: dict, fallback_message_id: str) -> str:
    """Best-effort thread identifier using References/In-Reply-To, else message_id."""
    def _first_msgid(value: str) -> str:
        if not value:
            return ""
        # Message-Ids typically look like <abc@domain>; References may contain several.
        ids = re.findall(r"<([^>]+)>", value)
        if ids:
            return ids[0]
        return value.strip().strip("<>").split()[0]

    refs = eml.get("References") or ""
    root = _first_msgid(str(refs))
    if not root:
        irt = eml.get("In-Reply-To") or ""
        root = _first_msgid(str(irt))
    if not root:
        root = fallback_message_id
    # Keep it stable and DynamoDB-safe
    root = root.replace("\n", "").replace("\r", "").strip()
    return root

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

    if existing and (
        existing.get("invite_sent_at")
        or existing.get("clarification_sent_at")
        or existing.get("guardrail_blocked_at")
    ):
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

    # Who should receive Iris' replies
    reply_recipients = dedupe([from_email] + to_emails + cc_emails)

    to_set = {e.lower() for e in to_emails}
    cc_set = {e.lower() for e in cc_emails}

    # Ignore messages sent BY Iris (avoid loops)
    if from_email == IRIS_EMAIL:
        return {"statusCode": 200, "body": json.dumps({"ok": True, "ignored": "from_iris"})}

    # Process if Iris is in To or Cc
    if IRIS_EMAIL not in to_set and IRIS_EMAIL not in cc_set:
        return {"statusCode": 200, "body": json.dumps({"ok": True, "ignored": "iris_not_recipient"})}

    body_text = extract_plaintext_body(eml)

    # ---- Bedrock Guardrails (INPUT) ----
    allowed, block_msg, guardrail_resp = apply_input_guardrail(body_text)
    if not allowed:
        text_body_reply = (block_msg or "").strip() + "\n"

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
                "record_type": "MESSAGE",
                "subject": subject,
                "from_email": from_email,
                "to_emails": set(to_emails),
                "cc_emails": set(cc_emails),
                "s3_key": used_key,
                "received_at": datetime.utcnow().isoformat() + "Z",
                "guardrail_blocked_at": datetime.utcnow().isoformat() + "Z",
                "guardrail_json": json.dumps(guardrail_resp) if guardrail_resp else "{}",
            }
        )
        _table().put_item(Item=ddb_sanitize(item))

        return {"statusCode": 200, "body": json.dumps({"ok": True, "action": "guardrail_blocked"})}

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

    # ---- Conversation engine (thread-scoped) ----
    thread_root = _extract_thread_root_id(eml, message_id)
    thread_id = f"thread#{thread_root}"

    thread_state, decision = process_incoming_email(
        table=_table(),
        thread_id=thread_id,
        message_id=message_id,
        body_text=body_text,
        timezone_default=TIMEZONE,
        ai_parsed=ai_parsed,
    )

    if decision.action == "ignore":
        return {"statusCode": 200, "body": json.dumps({"ok": True, "skipped": True})}

    # ---- Clarification path: email question only, no ICS ----
    if decision.action == "clarify":
        text_body_reply = (decision.reply_text or "What day and time would you like to schedule this meeting for?").strip() + "\n"

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
                "record_type": "MESSAGE",
                "thread_id": thread_id,
                "subject": subject,
                "from_email": from_email,
                "to_emails": set(to_emails),
                "cc_emails": set(cc_emails),
                "s3_key": used_key,
                "received_at": datetime.utcnow().isoformat() + "Z",
                "clarification_sent_at": datetime.utcnow().isoformat() + "Z",
                "ai_json": json.dumps(ai_parsed) if ai_parsed else "{}",
                "conv_state": thread_state.state,
                "conv_intent": thread_state.intent,
                "conv_question": decision.reply_text,
            }
        )
        _table().put_item(Item=ddb_sanitize(item))

        return {"statusCode": 200, "body": json.dumps({"ok": True, "action": "clarify"})}

    # ---- Scheduling path ----
    tz = ZoneInfo(thread_state.timezone or TIMEZONE)
    start, end = next_day_at_default_time(tz)

    if decision.time_kind == "candidate" and decision.chosen_candidate:
        try:
            start, end = candidate_to_datetimes(decision.chosen_candidate, tz)
            print("[decision] scheduling from candidate", start, end)
        except Exception as e:
            print("[decision] candidate parse failed; falling back:", repr(e))

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
            "record_type": "MESSAGE",
            "thread_id": thread_id,
            "subject": subject,
            "from_email": from_email,
            "to_emails": set(to_emails),
            "cc_emails": set(cc_emails),
            "s3_key": used_key,
            "received_at": datetime.utcnow().isoformat() + "Z",
            "event_uid": event_uid,
            "invite_sent_at": datetime.utcnow().isoformat() + "Z",
            "ai_json": json.dumps(ai_parsed) if ai_parsed else "{}",
            "conv_state": thread_state.state,
            "conv_intent": thread_state.intent,
            "scheduled_start": start.isoformat(),
            "scheduled_end": end.isoformat(),
        }
    )
    _table().put_item(Item=ddb_sanitize(item))

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
