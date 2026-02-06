from __future__ import annotations

import json
import uuid
import re
from datetime import datetime, date
from zoneinfo import ZoneInfo
from botocore.exceptions import ClientError

from ..infra.config import BUCKET_NAME, IRIS_EMAIL, TIMEZONE, require_env
from ..infra.aws_clients import table as _table, ses as _ses
from ..infra.ddb import key_for_message
from ..infra.serialization import ddb_clean, ddb_sanitize, to_json_safe
from ..infra.threading import extract_message_ids, resolve_thread_id, upsert_thread_aliases
from ..email.email_utils import flatten_emails, dedupe, safe_json, extract_plaintext_body, parse_eml
from ..infra.s3_loader import load_email_bytes_from_s3
from ..scheduling.scheduling import next_day_at_default_time, candidate_to_datetimes
from ..email.mime_builder import build_ics, build_raw_mime_text_reply, build_raw_mime_reply_with_ics
from ..infra.google_calendar import create_meet_event
from ..conversation.engine import process_incoming_email
from ..conversation.guardrails import apply_input_guardrail

# Backwards-compatible import (root-level shim also exists)
from iris_ai_parser import parse_email


# -------------------------
# Thread identification
# -------------------------

def _extract_thread_root_id(eml: dict, fallback_message_id: str) -> str:
    """
    Deprecated: use infra.threading.resolve_thread_id instead.
    Retained for backward compatibility in case of external imports.
    """
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
        mid = eml.get("Message-Id") or eml.get("Message-ID") or ""
        root = _first_msgid(str(mid))
    if not root:
        root = fallback_message_id
    return root.replace("\n", "").replace("\r", "").strip()


# -------------------------
# Coordination state storage
# (uses existing DDB key generator safely)
# -------------------------

def _coord_key(thread_id: str) -> dict:
    # Store coordination thread state under a synthetic message id so we don't
    # depend on table key schema details.
    return key_for_message(f"coord::{thread_id}")


def _coord_get(thread_id: str) -> dict | None:
    resp = _table().get_item(Key=_coord_key(thread_id))
    item = resp.get("Item")
    if not item:
        return None
    if item.get("record_type") != "COORDINATION_THREAD":
        return None
    return item


def _coord_put(thread_id: str, coordination_json: str) -> None:
    item = _coord_key(thread_id)
    item.update({
        "record_type": "COORDINATION_THREAD",
        "thread_id": thread_id,
        "updated_at": datetime.utcnow().isoformat() + "Z",
        "coordination_json": coordination_json,
    })
    _table().put_item(Item=ddb_clean(ddb_sanitize(item)))


# -------------------------
# Main handler
# -------------------------

def _append_line(existing: str | None, line: str) -> str:
    if existing and existing.strip():
        return existing.rstrip() + "\n" + line
    return line


def _timezone_name_from_dt(dt: datetime) -> str:
    tzinfo = dt.tzinfo
    if hasattr(tzinfo, "key") and tzinfo.key:
        return tzinfo.key
    return TIMEZONE


def handle_ses_event(event: dict) -> dict:
    print("DEPLOY_MARKER_ENTRYPOINT_REWRITE_002")
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
    if from_email.lower() == IRIS_EMAIL.lower():
        return {"statusCode": 200, "body": json.dumps({"ok": True, "ignored": "from_iris"})}

    # Process if Iris is in To or Cc (Iris might be in either)
    if IRIS_EMAIL.lower() not in to_set and IRIS_EMAIL.lower() not in cc_set:
        return {"statusCode": 200, "body": json.dumps({"ok": True, "ignored": "iris_not_recipient"})}

    body_text = extract_plaintext_body(eml)

    # Compute canonical thread id early and use it everywhere
    candidates = extract_message_ids(eml)
    if message_id and message_id not in candidates:
        candidates.append(message_id)

    thread_id = resolve_thread_id(eml, message_id, _table())
    print("[thread] resolved thread_id=", thread_id, " candidates=", candidates)

    # Upsert aliases for all candidate IDs
    upsert_thread_aliases(_table(), candidates, thread_id)

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
        item.update({
            "record_type": "MESSAGE",
            "thread_id": thread_id,
            "subject": subject,
            "from_email": from_email,
            "to_emails": list(to_emails),
            "cc_emails": list(cc_emails),
            "s3_key": used_key,
            "received_at": datetime.utcnow().isoformat() + "Z",
            "guardrail_blocked_at": datetime.utcnow().isoformat() + "Z",
            "guardrail_json": json.dumps(to_json_safe(guardrail_resp)) if guardrail_resp else "{}",
        })
        _table().put_item(Item=ddb_clean(ddb_sanitize(item)))

        return {"statusCode": 200, "body": json.dumps({"ok": True, "action": "guardrail_blocked"})}

    # ---- AI parse (use real thread_id, not thread#message_id) ----
    ai_result = parse_email({
        "thread_id": thread_id,
        "message_id": message_id,
        "body_text": body_text,
        "timezone_default": TIMEZONE,
    })
    print("[ai] result=", safe_json(ai_result))

    ai_parsed_raw = (ai_result.get("parsed") or {}) if ai_result.get("ok") else None
    ai_parsed = ddb_sanitize(ai_parsed_raw) if ai_parsed_raw else None  # critical for DDB + engine safety

    # ---- Multi participant routing (deterministic) ----
    existing_coord_item = _coord_get(thread_id)
    existing_participants = []
    if existing_coord_item:
        try:
            data = json.loads(existing_coord_item.get("coordination_json") or "{}")
            existing_participants = list((data.get("participants") or {}).keys())
        except Exception:
            existing_participants = []

    if existing_coord_item:
        participants_all = existing_participants
        is_multi = True
    else:
        participants_all = dedupe([from_email] + to_emails + cc_emails)
        participants_all = [e for e in participants_all if e and e.lower() != IRIS_EMAIL.lower()]
        is_multi = len(participants_all) >= 2

    print("[coord] participants_all=", participants_all)
    print("[coord] is_multi=", is_multi)

    # -------------------------
    # Multi-participant flow
    # -------------------------
    if is_multi:
        # Lazy imports to avoid circular import on cold start
        from ..coordination.handler import IrisCoordinationHandler, InboundEmail
        from ..coordination.models import MeetingThread, Participant
        from ..coordination.store import ThreadStore  # only for typing; safe
        # We'll adapt our DDB storage to match the coordination handler expected store API:

        class _StoreAdapter:
            def get(self, tid: str):
                item = _coord_get(tid)
                if not item:
                    return None
                data = json.loads(item.get("coordination_json") or "{}")
                # Rehydrate MeetingThread minimally using models
                participants = {}
                for email, pd in (data.get("participants") or {}).items():
                    p = Participant(email=email)
                    p.has_responded = bool(pd.get("has_responded"))
                    p.raw_response_text = pd.get("raw_response_text")
                    p.needs_clarification = bool(pd.get("needs_clarification"))
                    p.clarification_question = pd.get("clarification_question")
                    # parsed_windows are reconstructed by the coordination module in coordinator;
                    # store them as-is if present.
                    p.parsed_windows = []
                    for w in (pd.get("parsed_windows") or []):
                        # TimeWindow is in models; import lazily
                        from ..coordination.models import TimeWindow
                        p.parsed_windows.append(TimeWindow(
                            day=date.fromisoformat(w["day"]),
                            start_minute=int(w["start_minute"]),
                            end_minute=int(w["end_minute"]),
                        ))
                    participants[email] = p

                t = MeetingThread(
                    thread_id=data["thread_id"],
                    organizer_email=data["organizer_email"],
                    participants=participants,
                    timezone=data["timezone"],
                    meeting_duration_minutes=int(data.get("meeting_duration_minutes", 30)),
                    subject=data.get("subject", subject),
                )
                t.status = data.get("status", t.status)
                if data.get("deadline_at"):
                    t.deadline_at = datetime.fromisoformat(data["deadline_at"])
                if data.get("availability_requests_sent_at"):
                    t.availability_requests_sent_at = datetime.fromisoformat(data["availability_requests_sent_at"])
                if data.get("scheduled_start"):
                    t.scheduled_start = datetime.fromisoformat(data["scheduled_start"])
                if data.get("scheduled_end"):
                    t.scheduled_end = datetime.fromisoformat(data["scheduled_end"])
                t.scheduling_rationale = data.get("scheduling_rationale")
                t.pending_candidate = data.get("pending_candidate")
                return t

            def put(self, thread):
                def tw_to_dict(tw):
                    return {"day": tw.day.isoformat(), "start_minute": tw.start_minute, "end_minute": tw.end_minute}
                def p_to_dict(p):
                    return {
                        "email": p.email,
                        "has_responded": p.has_responded,
                        "raw_response_text": p.raw_response_text,
                        "parsed_windows": [tw_to_dict(w) for w in (p.parsed_windows or [])],
                        "needs_clarification": p.needs_clarification,
                        "clarification_question": p.clarification_question,
                        "responded_at": p.responded_at.isoformat() if getattr(p, "responded_at", None) else None,
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
                    "pending_candidate": thread.pending_candidate,
                }
                _coord_put(thread.thread_id, json.dumps(to_json_safe(data)))

        store = _StoreAdapter()
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
            ai_parsed=ai_parsed,
        )

        outbound_msgs, schedule_plan = handler.handle(inbound)

        if schedule_plan:
            # Avoid double-notifying: we will send ICS invite below.
            outbound_msgs = [m for m in outbound_msgs if " — scheduled" not in (m.subject or "")]

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
            attendees = participants_all[:]  # already excludes Iris

            meet_url = None
            try:
                print("[meet] create start", schedule_plan.start, schedule_plan.end, attendees)
                meet = create_meet_event(
                    summary=subject,
                    start_rfc3339=schedule_plan.start.isoformat(),
                    end_rfc3339=schedule_plan.end.isoformat(),
                    attendees=attendees,
                    timezone=_timezone_name_from_dt(schedule_plan.start),
                )
                meet_url = meet.get("meet_url")
                print("[meet] create success", meet_url)
            except Exception as e:
                print("[meet] create failed", repr(e))

            description = None
            location = None
            url = None
            if meet_url:
                description = _append_line(description, f"Google Meet: {meet_url}")
                location = _append_line(location, meet_url)
                url = _append_line(url, meet_url)

            ics = build_ics(
                subject=subject,
                start=schedule_plan.start,
                end=schedule_plan.end,
                organizer=IRIS_EMAIL,
                attendees=attendees,
                uid=event_uid,
                description=description,
                location=location,
                url=url,
            )

            pretty_when = schedule_plan.start.strftime("%A %I:%M %p").lstrip("0")
            if meet_url:
                text_body_reply = f"Google Meet: {meet_url}\n\nI scheduled a meeting for {pretty_when}.\n"
            else:
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

        # Message record (store ai_raw as string; don't store floats directly)
        item = key_for_message(message_id)
        item.update({
            "record_type": "MESSAGE",
            "thread_id": thread_id,
            "subject": subject,
            "from_email": from_email,
            "to_emails": list(to_emails),
            "cc_emails": list(cc_emails),
            "s3_key": used_key,
            "received_at": datetime.utcnow().isoformat() + "Z",
            "ai_raw": ai_result.get("raw") if isinstance(ai_result, dict) else None,
            "coord_action": "handled_multi",
        })
        _table().put_item(Item=ddb_clean(ddb_sanitize(item)))

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
        ai_parsed=ai_parsed,  # decimalized (no floats)
    )

    if decision.action == "ignore":
        return {"statusCode": 200, "body": json.dumps({"ok": True, "skipped": True})}

    # ---- Clarification path: email question only, no ICS ----
    if decision.action == "clarify":
        default_prompt = (
            "You can reply in either of these ways:\n\n"
            "A) Specific time slot (preferred)\n"
            "Day, MM/DD: start–end (timezone)\n"
            "Examples:\n"
            "Tue, 02/11: 1pm–2pm ET\n"
            "Wed, 02/12: 09:30–10:00 PT\n\n"
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

        _ses().send_raw_email(
            Source=IRIS_EMAIL,
            Destinations=reply_recipients,
            RawMessage={"Data": raw_mime},
        )

        item = key_for_message(message_id)
        item.update({
            "record_type": "MESSAGE",
            "thread_id": thread_id,
            "subject": subject,
            "from_email": from_email,
            "to_emails": list(to_emails),
            "cc_emails": list(cc_emails),
            "s3_key": used_key,
            "received_at": datetime.utcnow().isoformat() + "Z",
            "clarification_sent_at": datetime.utcnow().isoformat() + "Z",
            "ai_raw": ai_result.get("raw") if isinstance(ai_result, dict) else None,
            "conv_state": thread_state.state,
            "conv_intent": thread_state.intent,
            "conv_question": decision.reply_text,
        })
        _table().put_item(Item=ddb_clean(ddb_sanitize(item)))

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
    attendees = dedupe([from_email] + to_emails)

    meet_url = None
    try:
        print("[meet] create start", start, end, attendees)
        meet = create_meet_event(
            summary=subject,
            start_rfc3339=start.isoformat(),
            end_rfc3339=end.isoformat(),
            attendees=attendees,
            timezone=_timezone_name_from_dt(start),
        )
        meet_url = meet.get("meet_url")
        print("[meet] create success", meet_url)
    except Exception as e:
        print("[meet] create failed", repr(e))

    description = None
    location = None
    url = None
    if meet_url:
        description = _append_line(description, f"Google Meet: {meet_url}")
        location = _append_line(location, meet_url)
        url = _append_line(url, meet_url)

    ics = build_ics(
        subject=subject,
        start=start,
        end=end,
        organizer=IRIS_EMAIL,
        attendees=attendees,
        uid=event_uid,
        description=description,
        location=location,
        url=url,
    )

    try:
        pretty_when = start.strftime("%A %I:%M %p").lstrip("0")
    except Exception:
        pretty_when = "the requested time"
    if meet_url:
        text_body_reply = f"Google Meet: {meet_url}\n\nI scheduled a meeting for {pretty_when}.\n"
    else:
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
    item.update({
        "record_type": "MESSAGE",
        "thread_id": thread_id,
        "subject": subject,
        "from_email": from_email,
        "to_emails": list(to_emails),
        "cc_emails": list(cc_emails),
        "s3_key": used_key,
        "received_at": datetime.utcnow().isoformat() + "Z",
        "event_uid": event_uid,
        "invite_sent_at": datetime.utcnow().isoformat() + "Z",
        "ai_raw": ai_result.get("raw") if isinstance(ai_result, dict) else None,
        "conv_state": thread_state.state,
        "conv_intent": thread_state.intent,
        "scheduled_start": start.isoformat(),
        "scheduled_end": end.isoformat(),
    })
    _table().put_item(Item=ddb_clean(ddb_sanitize(item)))

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
