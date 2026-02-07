from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, date
from enum import Enum
from typing import List, Optional, Dict


class ThreadStatus(str, Enum):
    WAITING = "WAITING"
    READY_TO_SCHEDULE = "READY_TO_SCHEDULE"
    SCHEDULED = "SCHEDULED"
    NEEDS_CLARIFICATION = "NEEDS_CLARIFICATION"


@dataclass(frozen=True)
class TimeWindow:
    """A single availability window on a specific date, in minutes since midnight local to thread tz."""
    day: date
    start_minute: int  # inclusive
    end_minute: int    # exclusive

    def is_valid(self) -> bool:
        return 0 <= self.start_minute < self.end_minute <= 24 * 60


@dataclass
class Participant:
    email: str
    has_responded: bool = False
    raw_response_text: Optional[str] = None
    parsed_windows: List[TimeWindow] = field(default_factory=list)
    needs_clarification: bool = False
    clarification_question: Optional[str] = None
    responded_at: Optional[datetime] = None
    status: str = "PENDING"  # PENDING | RESPONDED
    requested_at: Optional[datetime] = None
    last_reminded_at: Optional[datetime] = None


@dataclass
class MeetingThread:
    thread_id: str
    organizer_email: str
    participants: Dict[str, Participant]  # keyed by email
    timezone: str  # IANA name, e.g. "America/New_York"
    meeting_duration_minutes: int = 30
    duration_minutes: int | None = None
    subject: str = "Meeting"
    created_at: datetime = field(default_factory=datetime.utcnow)
    availability_requests_sent_at: Optional[datetime] = None
    deadline_at: Optional[datetime] = None
    status: ThreadStatus = ThreadStatus.WAITING
    reminder_status: str = "COLLECTING_AVAILABILITY"
    reminder_schedule_name: Optional[str] = None

    scheduled_start: Optional[datetime] = None
    scheduled_end: Optional[datetime] = None
    scheduling_rationale: Optional[str] = None
    pending_candidate: Optional[dict] = None

    def pending_participants(self) -> List[Participant]:
        return [p for p in self.participants.values() if not p.has_responded]

    def all_responded(self) -> bool:
        return all(p.has_responded for p in self.participants.values())

    def any_needs_clarification(self) -> bool:
        return any(p.needs_clarification for p in self.participants.values())
