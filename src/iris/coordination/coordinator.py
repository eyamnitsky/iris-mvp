from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, date
from typing import List, Optional
from zoneinfo import ZoneInfo

from .availability_parser import parse_availability
from .models import MeetingThread, Participant, ThreadStatus, TimeWindow
from .reconciler import find_earliest_overlap
from .templates import (
    availability_request_email,
    clarification_email,
    scheduled_email,
    no_overlap_email,
)

from .constraint_parser import parse_constraints
from ..ai.text_normalize import clean_email_text

@dataclass(frozen=True)
class OutboundMessage:
    to: List[str]
    subject: str
    body: str


@dataclass(frozen=True)
class SchedulePlan:
    start: datetime
    end: datetime
    rationale: str


class IrisCoordinator:
    def __init__(self, default_deadline_hours: int = 48) -> None:
        self.default_deadline_hours = default_deadline_hours

    def _accept_all_response(self, text: str) -> bool:
        t = (text or "").lower()
        if any(kw in t for kw in ["can't", "cannot", "doesn't work", "does not work", "not work", "except"]):
            return False
        return any(
            kw in t
            for kw in [
                "these times work",
                "those times work",
                "these times work for me",
                "those times work for me",
                "any of those work",
                "all of those work",
                "works for me",
                "fine for me",
                "sounds good",
                "i'm flexible",
                "i am flexible",
                "whatever works",
                "whatever time works",
                "any time works",
            ]
        )

    def _full_availability_windows(self, tz_name: str, days: int = 14) -> List[TimeWindow]:
        tz = ZoneInfo(tz_name or "UTC")
        today = datetime.now(tz=tz).date()
        windows: List[TimeWindow] = []
        for i in range(days):
            d = date(today.year, today.month, today.day) + timedelta(days=i)
            windows.append(TimeWindow(day=d, start_minute=0, end_minute=24 * 60))
        return windows

    def start_thread(self, thread: MeetingThread) -> OutboundMessage:
        """
        Send initial availability request to all participants.
        """
        if thread.deadline_at is None:
            thread.deadline_at = datetime.utcnow() + timedelta(hours=self.default_deadline_hours)

        thread.availability_requests_sent_at = datetime.utcnow()
        thread.status = ThreadStatus.WAITING
        thread.reminder_status = "COLLECTING_AVAILABILITY"
        for p in thread.participants.values():
            p.status = "PENDING"
            p.requested_at = thread.availability_requests_sent_at
            p.last_reminded_at = None

        body = availability_request_email(
            participant_emails=list(thread.participants.keys()),
            deadline=thread.deadline_at,
            tz_name=thread.timezone,
        )
        return OutboundMessage(
            to=list(thread.participants.keys()),
            subject=f"{thread.subject} — availability",
            body=body,
        )

    def ingest_participant_reply(self, thread: MeetingThread, participant_email: str, body_text: str) -> List[OutboundMessage]:
        """
        Parse a participant reply, update thread state, and return any outbound messages (clarifications).
        """
        p = thread.participants.get(participant_email)
        if p is None:
            # Unknown sender; ignore for now (could also add them dynamically).
            return []

        body_text = clean_email_text(body_text)
        p.raw_response_text = body_text
        p.responded_at = datetime.utcnow()
        p.status = "RESPONDED"

        result = parse_availability(body_text, tz_name=thread.timezone)
        p.parsed_windows = result.windows
        p.has_responded = True

        # If they didn't follow the structured format, try natural-language constraints
        if not p.parsed_windows and not result.needs_clarification:
            windows, clar_q = parse_constraints(body_text, tz=thread.timezone)
            if windows:
                p.parsed_windows = windows
            elif clar_q:
                p.needs_clarification = True
                p.clarification_question = clar_q
                thread.status = ThreadStatus.NEEDS_CLARIFICATION
                return [OutboundMessage(
                    to=[participant_email],
                    subject=f"{thread.subject} — quick clarification",
                    body=clarification_email(clar_q),
                )]
            elif self._accept_all_response(body_text):
                # Treat "these times work for me" as full flexibility.
                p.parsed_windows = self._full_availability_windows(thread.timezone)

        if result.needs_clarification:
            p.needs_clarification = True
            p.clarification_question = result.clarification_question
            thread.status = ThreadStatus.NEEDS_CLARIFICATION
            return [
                OutboundMessage(
                    to=[participant_email],
                    subject=f"{thread.subject} — quick clarification",
                    body=clarification_email(result.clarification_question or "Could you clarify?"),
                )
            ]

        p.needs_clarification = False
        p.clarification_question = None

        # Update thread status
        if thread.any_needs_clarification():
            thread.status = ThreadStatus.NEEDS_CLARIFICATION
        elif thread.all_responded():
            thread.status = ThreadStatus.READY_TO_SCHEDULE
        else:
            thread.status = ThreadStatus.WAITING

        return []

    def try_schedule(self, thread: MeetingThread) -> tuple[Optional[SchedulePlan], List[OutboundMessage]]:
        """
        Only schedules when ALL participants responded and no clarifications are pending.
        Returns a SchedulePlan and outbound messages (e.g., scheduled confirmation or no-overlap request).
        """
        if thread.status == ThreadStatus.SCHEDULED:
            return None, []

        if not thread.all_responded():
            return None, []

        if thread.any_needs_clarification():
            thread.status = ThreadStatus.NEEDS_CLARIFICATION
            return None, []

        if any(not p.parsed_windows for p in thread.participants.values()):
            thread.status = ThreadStatus.WAITING
            return None, [
                OutboundMessage(
                    to=list(thread.participants.keys()),
                    subject=f"{thread.subject} — need more availability",
                    body=no_overlap_email(),
                )
            ]
        
        duration = thread.duration_minutes or thread.meeting_duration_minutes  # defaults to 30
        
        slot = find_earliest_overlap(thread)
        if slot is None:
            thread.status = ThreadStatus.WAITING
            # Reset responses so everyone must provide new availability
            for p in thread.participants.values():
                p.has_responded = False
                p.parsed_windows = []
            # Ask for more availability from everyone
            return None, [
                OutboundMessage(
                    to=list(thread.participants.keys()),
                    subject=f"{thread.subject} — need more availability",
                    body=no_overlap_email(),
                )
            ]

        thread.scheduled_start = slot.start
        thread.scheduled_end = slot.end
        thread.scheduling_rationale = slot.rationale
        thread.status = ThreadStatus.SCHEDULED
        thread.reminder_status = "SCHEDULED"

        start_str = slot.start.strftime("%a %m/%d %I:%M%p")
        end_str = slot.end.strftime("%I:%M%p")

        return (
            SchedulePlan(start=slot.start, end=slot.end, rationale=slot.rationale),
            [
                OutboundMessage(
                    to=list(thread.participants.keys()),
                    subject=f"{thread.subject} — scheduled",
                    body=scheduled_email(start_str, end_str, thread.timezone, slot.rationale),
                )
            ],
        )
