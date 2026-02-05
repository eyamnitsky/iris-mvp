from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime
from typing import Optional

from .models import MeetingThread, Participant, TimeWindow
from ..infra.serialization import to_ddb_safe, to_json_safe


def _tw_to_dict(tw: TimeWindow) -> dict:
    return {
        "day": tw.day.isoformat(),
        "start_minute": tw.start_minute,
        "end_minute": tw.end_minute,
    }


def _tw_from_dict(d: dict) -> TimeWindow:
    from datetime import date
    return TimeWindow(
        day=date.fromisoformat(d["day"]),
        start_minute=int(d["start_minute"]),
        end_minute=int(d["end_minute"]),
    )


class DdbThreadStore:
    """
    Stores threads in the SAME table you already use.

    Assumes you can store an item keyed by:
      pk = thread_id
      sk = "COORDINATION"

    If your table key schema is different, adjust _key().
    """
    def __init__(self, table):
        self._table = table

    def _key(self, thread_id: str) -> dict:
        # Adjust if your DDB schema differs
        return {"pk": thread_id, "sk": "COORDINATION"}

    def get(self, thread_id: str) -> Optional[MeetingThread]:
        resp = self._table.get_item(Key=self._key(thread_id))
        item = resp.get("Item")
        if not item:
            return None

        data = json.loads(item["json"])

        participants = {}
        for email, pd in (data["participants"] or {}).items():
            p = Participant(
                email=email,
                has_responded=bool(pd.get("has_responded")),
                raw_response_text=pd.get("raw_response_text"),
                parsed_windows=[_tw_from_dict(x) for x in (pd.get("parsed_windows") or [])],
                needs_clarification=bool(pd.get("needs_clarification")),
                clarification_question=pd.get("clarification_question"),
                responded_at=datetime.fromisoformat(pd["responded_at"]) if pd.get("responded_at") else None,
            )
            participants[email] = p

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
            if data.get("availability_requests_sent_at")
            else None
        )
        thread.deadline_at = datetime.fromisoformat(data["deadline_at"]) if data.get("deadline_at") else None

        thread.scheduled_start = datetime.fromisoformat(data["scheduled_start"]) if data.get("scheduled_start") else None
        thread.scheduled_end = datetime.fromisoformat(data["scheduled_end"]) if data.get("scheduled_end") else None
        thread.scheduling_rationale = data.get("scheduling_rationale")

        return thread

    def put(self, thread: MeetingThread) -> None:
        def participant_to_dict(p: Participant) -> dict:
            return {
                "email": p.email,
                "has_responded": p.has_responded,
                "raw_response_text": p.raw_response_text,
                "parsed_windows": [_tw_to_dict(x) for x in p.parsed_windows],
                "needs_clarification": p.needs_clarification,
                "clarification_question": p.clarification_question,
                "responded_at": p.responded_at.isoformat() if p.responded_at else None,
            }

        data = {
            "thread_id": thread.thread_id,
            "organizer_email": thread.organizer_email,
            "participants": {e: participant_to_dict(p) for e, p in thread.participants.items()},
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
            Item=to_ddb_safe({
                **self._key(thread.thread_id),
                "record_type": "COORDINATION_THREAD",
                "updated_at": datetime.utcnow().isoformat() + "Z",
                "json": json.dumps(to_json_safe(data)),
            })
        )
