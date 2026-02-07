from __future__ import annotations

import json
from datetime import datetime, date
from typing import Optional

from ..coordination.models import MeetingThread, Participant, TimeWindow, ThreadStatus
from ..infra.ddb import key_for_message
from ..infra.serialization import ddb_clean, ddb_sanitize, to_json_safe


def _coord_key(thread_id: str) -> dict:
    # Store coordination thread state under a synthetic message id so we don't
    # depend on table key schema details.
    return key_for_message(f"coord::{thread_id}")


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def _parse_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except Exception:
        return None


class CoordinationStore:
    def __init__(self, table):
        self._table = table

    def get(self, thread_id: str) -> Optional[MeetingThread]:
        resp = self._table.get_item(Key=_coord_key(thread_id))
        item = resp.get("Item")
        if not item:
            return None
        if item.get("record_type") != "COORDINATION_THREAD":
            return None

        data = json.loads(item.get("coordination_json") or "{}")
        participants = {}
        for email, pd in (data.get("participants") or {}).items():
            email_norm = (email or "").lower()
            p = Participant(email=email_norm)
            p.has_responded = bool(pd.get("has_responded"))
            p.raw_response_text = pd.get("raw_response_text")
            p.needs_clarification = bool(pd.get("needs_clarification"))
            p.clarification_question = pd.get("clarification_question")
            p.responded_at = _parse_iso(pd.get("responded_at"))
            p.status = pd.get("status") or ("RESPONDED" if p.has_responded else "PENDING")
            p.requested_at = _parse_iso(pd.get("requested_at"))
            p.last_reminded_at = _parse_iso(pd.get("last_reminded_at"))

            p.parsed_windows = []
            for w in (pd.get("parsed_windows") or []):
                parsed_day = _parse_date(w.get("day"))
                if parsed_day is None:
                    continue
                try:
                    p.parsed_windows.append(TimeWindow(
                        day=parsed_day,
                        start_minute=int(w["start_minute"]),
                        end_minute=int(w["end_minute"]),
                    ))
                except Exception:
                    continue

            participants[email_norm] = p

        thread = MeetingThread(
            thread_id=data.get("thread_id", thread_id),
            organizer_email=data.get("organizer_email", ""),
            participants=participants,
            timezone=data.get("timezone") or "UTC",
            meeting_duration_minutes=int(data.get("meeting_duration_minutes", 30)),
            subject=data.get("subject", "Meeting"),
        )

        raw_status = data.get("status")
        if raw_status in [s.value for s in ThreadStatus]:
            thread.status = ThreadStatus(raw_status)

        created_at = _parse_iso(data.get("created_at"))
        if created_at:
            thread.created_at = created_at

        thread.availability_requests_sent_at = _parse_iso(data.get("availability_requests_sent_at"))
        thread.deadline_at = _parse_iso(data.get("deadline_at"))
        thread.scheduled_start = _parse_iso(data.get("scheduled_start"))
        thread.scheduled_end = _parse_iso(data.get("scheduled_end"))
        thread.scheduling_rationale = data.get("scheduling_rationale")
        thread.pending_candidate = data.get("pending_candidate")
        thread.reminder_status = data.get("reminder_status") or thread.reminder_status
        thread.reminder_schedule_name = data.get("reminder_schedule_name")

        return thread

    def put(self, thread: MeetingThread) -> None:
        def tw_to_dict(tw: TimeWindow) -> dict:
            return {
                "day": tw.day.isoformat(),
                "start_minute": tw.start_minute,
                "end_minute": tw.end_minute,
            }

        def participant_to_dict(p: Participant) -> dict:
            return {
                "email": p.email,
                "has_responded": p.has_responded,
                "raw_response_text": p.raw_response_text,
                "parsed_windows": [tw_to_dict(w) for w in (p.parsed_windows or [])],
                "needs_clarification": p.needs_clarification,
                "clarification_question": p.clarification_question,
                "responded_at": p.responded_at.isoformat() if p.responded_at else None,
                "status": p.status,
                "requested_at": p.requested_at.isoformat() if p.requested_at else None,
                "last_reminded_at": p.last_reminded_at.isoformat() if p.last_reminded_at else None,
            }

        data = {
            "thread_id": thread.thread_id,
            "organizer_email": thread.organizer_email,
            "participants": {e: participant_to_dict(p) for e, p in (thread.participants or {}).items()},
            "timezone": thread.timezone,
            "meeting_duration_minutes": thread.meeting_duration_minutes,
            "subject": thread.subject,
            "status": thread.status.value if isinstance(thread.status, ThreadStatus) else thread.status,
            "availability_requests_sent_at": thread.availability_requests_sent_at.isoformat()
                if thread.availability_requests_sent_at else None,
            "deadline_at": thread.deadline_at.isoformat() if thread.deadline_at else None,
            "scheduled_start": thread.scheduled_start.isoformat() if thread.scheduled_start else None,
            "scheduled_end": thread.scheduled_end.isoformat() if thread.scheduled_end else None,
            "scheduling_rationale": thread.scheduling_rationale,
            "pending_candidate": thread.pending_candidate,
            "created_at": thread.created_at.isoformat() if thread.created_at else None,
            "reminder_status": thread.reminder_status,
            "reminder_schedule_name": thread.reminder_schedule_name,
        }

        item = _coord_key(thread.thread_id)
        item.update({
            "record_type": "COORDINATION_THREAD",
            "thread_id": thread.thread_id,
            "updated_at": datetime.utcnow().isoformat() + "Z",
            "coordination_json": json.dumps(to_json_safe(data)),
        })
        self._table.put_item(Item=ddb_clean(ddb_sanitize(item)))
