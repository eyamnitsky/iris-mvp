from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Optional

from botocore.exceptions import ClientError

from ..coordination.models import ThreadStatus
from ..coordination.templates import reminder_email
from ..email.mime_builder import build_raw_mime_text_reply
from ..infra.aws_clients import ses as _ses, table as _table
from ..infra.config import IRIS_EMAIL, REMINDER_DELAY_SECONDS
from ..infra.coordination_store import CoordinationStore


def _extract_thread_id(event) -> Optional[str]:
    if isinstance(event, str):
        try:
            event = json.loads(event)
        except Exception:
            return None

    if not isinstance(event, dict):
        return None

    for key in ("thread_id", "threadId"):
        if key in event:
            return event.get(key)

    detail = event.get("detail") if isinstance(event.get("detail"), dict) else None
    if detail:
        for key in ("thread_id", "threadId"):
            if key in detail:
                return detail.get(key)

        detail_input = detail.get("input")
        if detail_input:
            try:
                parsed = json.loads(detail_input) if isinstance(detail_input, str) else detail_input
                if isinstance(parsed, dict):
                    return parsed.get("thread_id") or parsed.get("threadId")
            except Exception:
                return None

    raw_input = event.get("input")
    if raw_input:
        try:
            parsed = json.loads(raw_input) if isinstance(raw_input, str) else raw_input
            if isinstance(parsed, dict):
                return parsed.get("thread_id") or parsed.get("threadId")
        except Exception:
            return None

    body = event.get("body")
    if body:
        try:
            parsed = json.loads(body) if isinstance(body, str) else body
            if isinstance(parsed, dict):
                return parsed.get("thread_id") or parsed.get("threadId")
        except Exception:
            return None

    return None


def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def lambda_handler(event, context):
    thread_id = _extract_thread_id(event)
    print(f"[reminder] start thread_id={thread_id}")
    if not thread_id:
        return {"statusCode": 200, "body": json.dumps({"ok": True, "skipped": "missing_thread_id"})}

    store = CoordinationStore(_table())
    thread = store.get(thread_id)
    if not thread:
        print(f"[reminder] thread not found thread_id={thread_id}")
        return {"statusCode": 200, "body": json.dumps({"ok": True, "skipped": "missing_thread"})}

    status_val = thread.status.value if isinstance(thread.status, ThreadStatus) else str(thread.status)
    reminder_status = (thread.reminder_status or "").upper()
    if status_val == ThreadStatus.SCHEDULED.value or reminder_status in ("SCHEDULED", "CLOSED"):
        print(f"[reminder] thread already scheduled/closed thread_id={thread_id}")
        return {"statusCode": 200, "body": json.dumps({"ok": True, "skipped": "already_scheduled"})}

    now = datetime.now(timezone.utc)
    delay = timedelta(seconds=REMINDER_DELAY_SECONDS)
    reminded = 0

    for p in thread.participants.values():
        status = (p.status or "").upper()
        if status != "PENDING":
            continue
        if p.last_reminded_at:
            continue

        requested_at = p.requested_at or thread.availability_requests_sent_at
        if not requested_at:
            continue

        if _to_utc(requested_at) + delay > now:
            continue

        subject = f"{thread.subject} — availability reminder"
        body = reminder_email()
        raw_mime = build_raw_mime_text_reply(
            subject=subject,
            text_body=body,
            from_addr=IRIS_EMAIL,
            to_addrs=[p.email],
            in_reply_to=None,
            references=None,
        )

        try:
            _ses().send_raw_email(
                Source=IRIS_EMAIL,
                Destinations=[p.email],
                RawMessage={"Data": raw_mime},
            )
            p.last_reminded_at = now
            reminded += 1
        except ClientError as e:
            print(f"[reminder] send failed email={p.email} err={repr(e)}")
        except Exception as e:
            print(f"[reminder] send failed email={p.email} err={repr(e)}")

    if reminded:
        store.put(thread)

    print(f"[reminder] reminded_count={reminded} thread_id={thread_id}")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "reminded": reminded})}
