from __future__ import annotations

import json
import uuid
import re
from datetime import datetime, date
from zoneinfo import ZoneInfo
from decimal import Decimal
from botocore.exceptions import ClientError

from ..infra.config import BUCKET_NAME, IRIS_EMAIL, TIMEZONE
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


# -------------------------
# DynamoDB float hardening
# -------------------------

def deep_decimalize(x):
    """Convert floats to Decimal recursively (DynamoDB-safe)."""
    if isinstance(x, float):
        return Decimal(str(x))
    if isinstance(x, dict):
        return {k: deep_decimalize(v) for k, v in x.items()}
    if isinstance(x, list):
        return [deep_decimalize(v) for v in x]
    if isinstance(x, tuple):
        return [deep_decimalize(v) for v in x]
    return x


def ddb_sanitize(item: dict) -> dict:
    """Ensure no floats make it into DynamoDB items."""
    return deep_decimalize(item)


# -------------------------
# Thread identification
# -------------------------

def _extract_thread_root_id(eml: dict, fallback_message_id: str) -> str:
    """Best-effort thread identifier using References/In-Reply-To, else message_id."""
    def _first_msgid(value: str) -> str:
        if not value:
            return ""
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

    return root.replace("\n", "").replace("\r", "").strip()


# -------------------------
# Coordination state store
# (DDB keyed by threadId)
# -------------------------

class _DdbCoordinationStore:
    """
    Stores a coordination thread as a single item:
      threadId (PK) = thread_id
      record_type = "COORDINATION_THREAD"
      coordination_json = JSON blob
    """
    def __init__(self, table):
        self._table = table

    def get(self, thread_id: str):
        resp = self._table.get_item(Key={"threadId": thread_id})
        item = resp.get("Item")
        if not item:
            return None
        if item.get("record_type") != "COORDINATION_THREAD":
            return None

        # Lazy import to avoid circular imports at cold start
        from ..coordination.models import MeetingThread, Participant, TimeWindow

        data = json.loads(item.get("coordination_json", "{}") or "{}")

        participants: dict[str, Participant] = {}
        for email, pd in (data.get("participants") or {}).items():
            windows = []
            for w in (pd.get("parsed_windows") or []):
                # w["day"] is ISO date like "2026-02-11"
                windows.append(
                    TimeWindow(
                        day=date.fromisoformat(w["day"]),
                        start_minute=int(w["start_minute"]),
                        end_minute=int(w["end_minute"]),
                    )
                )

            participants[email] = Participant(
                email=email,
                has_responded=bool(pd.get("has_responded")),
                raw_response_text=pd.get("raw_response_text"),
                parsed_windows=windows,
                needs_clarification=bool(pd.get("needs_clarification")),
                clarification_question=pd.get("clarification_question"),
                responded_at=datetime.fromisoformat(pd["responded_at"]) if pd.get("responded_at") else None,
            )

        thread = MeetingThread(
            thread_id=data["thread_id"],
            organizer_email=data["organizer_email"],
            participants=participants,
            timezone=data["timezone"],
            meeting_duration_minutes=int(data.get("meeting_duration_minutes", 30)),
            subject=data.get("subject", "Meeting"),
        )

        thread.status = data.get("status", thread.status)
        thread.availability_requests_sent_at = (
            datetime.fromisoformat(data["availability_requests_sent_at"])
            if data.get("availability_requests_sent_at") else None
        )
        thread.deadline_at = datetime.fromisoformat(data["deadline_at"]) if data.get("deadline_at") else None
        thread.scheduled_start = datetime.fromisoformat(data["scheduled_start"]) if data.get("scheduled_start") else None
        thread.scheduled_end = datetime.fromisoformat(data["scheduled_end"]) if data.get("scheduled_end") else None
        thread.scheduling_rationale = data.get("scheduling_rationale")
        return thread

    def put(self, thread) -> None:
        # Lazy import to avoid circular imports at cold start
        from ..coordination.models import TimeWindow

        def tw_to_dict(tw: TimeWindow) -> dict:
            return {"day": tw.day.isoformat(), "start_minute": tw.start_minute, "end_minute": tw.end_minute}

        def p_to_dict(p) -> dict:
            return {
                "email": p.email,
                "has_responded": p.has_responded,
                "raw_response_text": p.raw_response_text,
                "parsed_windows": [tw_to_dict(w) for w in (p.parsed_windows or [])],
                "needs_clarification": p.needs_clarification,
                "clarification_question": p.clarification_question,
                "responded_at": p.responded_at.isoformat() if p.responded_at else None,
            }

        data = {
            "thread_id": thread.thread_id,
            "organizer_email": thread.organizer_email,
            "participants": {e: p_to_dict(p) for e, p in (thread.participants or {}).items()},
            "timezone": thread.timezone,
            "meeting_duration_minutes": thread.meeting_duration_minutes,
            "subject": thread.subject,
            "status": thread.status,
            "availability_requests_sent_at": thread.availability_requests_sent_at.isoformat()
            if thread.availability_requests_sent_at else None,
            "deadline_at": thread.deadline_at.isoformat() if thread.deadline_at else None,
            "scheduled_start": thread.scheduled_start.isoformat() if thread.scheduled_start else None,
            "scheduled_end": thread.scheduled_end.isoformat() if thread.scheduled_end else None,
            "scheduling_rationale": thread.scheduling_rationale,
        }

        self._table.put_item(
            Item=ddb_sanitize({
                "threadId": thread.thread_id,
                "record_type": "COORDINATION_THREAD",
                "updated_at": datetime.utcnow().isoformat() + "Z",
                "coordination_json": json.dumps(data),
            })
        )


# -------------------------
# Main handler
# -------------------------

def handle_ses_event(event: dict) -> dict:
    print("DEPLOY_MARKER_ENTRYPOINT_REWRITE_001")

    record = event["Records"][0]
    ses_payload = record.get("ses", {}) or {}
    receipt = ses_payload.get("receipt", {}) or {}
    mail = ses_payload.get("mail", {}) or {}

    message_id = mail.get("messageId") or str(uuid.uuid4())
    print(f"[ses] messageId={message_id}")

    # Idempotency (skip if already handled)
    ddb_key = key_for_message(message_id)
    existing = _table().get_item(Key=ddb_key).get("Item")
    if existing and (existing.get("invite_sent_at") or existing.get("clarification_sent_at") or existing.get("guardrail_blocked_at")):
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

    reply_recipients = dedupe([from_email] + to_emails + cc_emails)

    # Avoid loops
    if from_email.lower() == IRIS_EMAIL.lower():
        return {"statusCode": 200, "body": json.dumps({"ok": True, "ignored": "from_iris"})}

    # Only handle if Iris is in To or Cc
    to_set = {e.lower() for e in to_emails}
    cc_set = {e.lower() for e in cc_emails}
    if IRIS_EMAIL.lower() not in to_set and IRIS_EMAIL.lower() not in cc_set:
        return {"statusCode": 200, "body": json.dumps({"ok": True, "ignored": "iris_not_recipient"})}

    body_text = extract_plaintext_body(eml)

    # Thread id early
    thread_root = _extract_thread_root_id(eml, message_id)
    thread_id = f"thread#{thread_root}"

    # Guardrails
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
        _ses().send_raw_email(Source=IRIS_EMAIL, Destinations=reply_recipients, RawMessage={"Data": raw_mime})

        item = key_for_message(message_id)
        item.update({
            "record_type": "MESSAGE",
            "thread_id": thread_id,
            "subject": subject,
            "from_email": from_email,
            "to_emails": set(to_emails),
            "cc_emails": set(cc_emails),
            "s3_key": used_key,
            "received_at": datetime.utcnow().isoformat() + "Z",
            "guardrail_blocked_at": datetime.utcnow().isoformat() + "Z",
            "guardrail_json": json.dumps(guardrail_resp) if guardrail_resp else "{}",
        })
        _table().put_item(Item=ddb_sanitize(item))
        return {"statusCode": 200, "body": json.dumps({"ok": True, "action": "guardrail_blocked"})}

    # AI parse
    ai_result = parse_email({
        "thread_id": thread_id,
        "message_id": message_id,
        "body_text": body_text,
        "timezone_default": TIMEZONE,
    })
    print("[ai] result=", safe_json(ai_result))

    ai_parsed_raw = (ai_result.get("parsed") or {}) if ai_result.get("ok") else None
    # Critical: decimalize before it ever hits your engine or DDB
    ai_parsed = deep_decimalize(ai_parsed_raw) if ai_parsed_raw else None

    # Participants for routing (include sender; exclude Iris)
    participants_all = dedupe([from_email] + to_emails + cc_emails)
    participants_all = [e for e in participants_all if e and e.lower() != IRIS_EMAIL.lower()]
    is_multi = len(participants_all) >= 2
    print("[coord] participants_all=", participants_all)
    print("[coord] is_multi=", is_multi)

    # -------------------------
    # Multi-participant flow
    # -------------------------
    if is_multi:
        # Lazy imports to avoid circular imports at cold start
        from ..coordination.handler import IrisCoordinationHandler, InboundEmail
        from ..coordination.models import MeetingThread, Participant

        store = _DdbCoordinationStore(_table())
        coord_thread = store.get(thread_id)

        is_new = coord_thread is None
        if is_new:
            coord_thread = MeetingThread(
                thread_id=thread_id,
                organizer_email=from_email,
                participants={e: Participant(email=e) for e in participants_all},
                timezone=TIMEZONE,
                subject=subject,
            )
            store.put(coord_thread)

        handler = IrisCoordinationHandler(store)
        inbound = InboundEmail(
            thread_id=thread_id,
            from_email=from_email,
            to_emails=to_emails,
            cc_emails=cc_emails,
            subject=subject,
            body_text=body_text,
            is_new_request=is_new,
        )

        outbound_msgs, schedule_plan = handler.handle(inbound)

        for m in outbound_msgs:
            raw_mime = build_raw_mime_text_reply(
                subject=m.subject,
                text_body=(m.body or "").rstrip() + "\n",
                from_addr=IRIS_EMAIL,
                to_addrs=m.to,
                in_reply_to=eml.get("Message-Id"),
                references=eml.get("References"),
            )
            _ses().send_raw_email(Source=IRIS_EMAIL, Destinations=m.to, RawMessage={"Data": raw_mime})

        if schedule_plan:
            event_uid = f"{uuid.uuid4()}@{IRIS_EMAIL.split('@', 1)[1]}"
            attendees = [e for e in participants_all if e.lower() != IRIS_EMAIL.lower()]

            ics = build_ics(
                subject=subject,
                start=schedule_plan.start,
                end=schedule_plan.end,
                organizer=IRIS_EMAIL,
                attendees=attendees,
                uid=event_uid,
            )

            pretty_when = schedule_plan.start.strftime("%A %I:%M %p").lstrip("0")
            text_body_reply = f"I scheduled a meeting for {pretty_when}.\n"

            raw_mime = build_raw_mime_reply_with_ics(
                subject=f"Re: {subject}",
                text_body=text_body_reply,
                ics_body=ics,
                from_addr=IRIS_EMAIL,
                to_addrs=attendees,
                in_reply_to=eml.get("Message-Id"),
                references=eml.get("References"),
            )
            _ses().send_raw_email(Source=IRIS_EMAIL, Destinations=attendees, RawMessage={"Data": raw_mime})

        # Message record (store AI as string to avoid future DDB type issues)
        item = key_for_message(message_id)
        item.update({
            "record_type": "MESSAGE",
            "thread_id": thread_id,
            "subject": subject,
            "from_email": from_email,
            "to_emails": set(to_emails),
            "cc_emails": set(cc_emails),
            "s3_key": used_key,
            "received_at": datetime.utcnow().isoformat() + "Z",
            "ai_json": json.dumps(ai_parsed_raw or {}) if ai_result.get("ok") else "{}",
            "coord_action": "handled_multi",
        })
        _table().put_item(Item=ddb_sanitize(item))

        return {"statusCode": 200, "body": json.dumps({"ok": True, "action": "coordination"})}

    # -------------------------
    # Single-participant flow
    # -------------------------
    thread_state, decision = process_incoming_email(
        table=_table(),
        thread_id=thread_id,
        message_id=message_id,
        body_text=body_text,
        timezone_default=TIMEZONE,
        ai_parsed=ai_parsed,  # already decimalized
    )

    if decision.action == "ignore":
        return {"statusCode": 200, "body": json.dumps({"ok": True, "skipped": True})}

    if decision.action == "clarify":
        # Better clarification prompt with examples + flexible constraints
        default_prompt = (
            "Sure — what time should I schedule this?\n\n"
            "You can reply in either of these ways:\n\n"
            "A) Specific time slot (preferred)\n"
            "MM/DD: start–end (timezone)\n"
            "Examples:\n"
            "02/11: 1pm–2pm ET\n"
            "02/12: 09:30–10:00 PT\n\n"
            "B) Flexible constraints (also OK)\n"
            "- “Any afternoon Mon–Tue next week”\n"
            "- “Any time after 3pm on Wednesday”\n"
            "- “Any 30 min slot Tue–Thu between 10am–4pm PT”\n"
        )
        if decision.reply_text and decision.reply_text.strip():
            text_body_reply = decision.reply_text.strip() + "\n\n" + default_prompt
        else:
            text_body_reply = default_prompt

        raw_mime = build_raw_mime_text_reply(
            subject=f"Re: {subject}",
            text_body=text_body_reply.rstrip() + "\n",
            from_addr=IRIS_EMAIL,
            to_addrs=reply_recipients,
            in_reply_to=eml.get("Message-Id"),
            references=eml.get("References"),
        )
        _ses().send_raw_email(Source=IRIS_EMAIL, Destinations=reply_recipients, RawMessage={"Data": raw_mime})

        item = key_for_message(message_id)
        item.update({
            "record_type": "MESSAGE",
            "thread_id": thread_id,
            "subject": subject,
            "from_email": from_email,
            "to_emails": set(to_emails),
            "cc_emails": set(cc_emails),
            "s3_key": used_key,
            "received_at": datetime.utcnow().isoformat() + "Z",
            "clarification_sent_at": datetime.utcnow().isoformat() + "Z",
            "ai_json": json.dumps(ai_parsed_raw or {}) if ai_result.get("ok") else "{}",
            "conv_state": thread_state.state,
            "conv_intent": thread_state.intent,
            "conv_question": decision.reply_text,
        })
        _table().put_item(Item=ddb_sanitize(item))
        return {"statusCode": 200, "body": json.dumps({"ok": True, "action": "clarify"})}

    tz = ZoneInfo(thread_state.timezone or TIMEZONE)
    start, end = next_day_at_default_time(tz)

    if decision.time_kind == "candidate" and decision.chosen_candidate:
        try:
            start, end = candidate_to_datetimes(decision.chosen_candidate, tz)
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

    pretty_when = start.strftime("%A %I:%M %p").lstrip("0")
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
    _ses().send_raw_email(Source=IRIS_EMAIL, Destinations=reply_recipients, RawMessage={"Data": raw_mime})

    item = key_for_message(message_id)
    item.update({
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
        "ai_json": json.dumps(ai_parsed_raw or {}) if ai_result.get("ok") else "{}",
        "conv_state": thread_state.state,
        "conv_intent": thread_state.intent,
        "scheduled_start": start.isoformat(),
        "scheduled_end": end.isoformat(),
    })
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