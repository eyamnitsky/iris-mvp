from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple
import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from .models import MeetingThread, Participant, ThreadStatus
from .coordinator import IrisCoordinator
from .types import OutboundMessage, SchedulePlan

from .duration_parser import parse_duration_minutes
from .normalization import DAY_ALIASES
from .templates import clarification_email

_DAY_ORDER = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
_TIME_CONTEXT_RE = re.compile(
    r"(?:around|about|at|@|approx(?:imately)?|by|after|before)\s*"
    r"(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\s*(?:ish)?\b",
    re.IGNORECASE,
)
_TIME_WITH_AMPM_RE = re.compile(r"\b(\d{1,2})(?::(\d{2}))?\s*(am|pm)\b", re.IGNORECASE)


def _parse_explicit_day_time(text: str, tz_name: str) -> Optional[datetime]:
    if not text:
        return None

    day_match = None
    time_match = None

    for m in re.finditer(r"\b([A-Za-z]{3,9})\b", text):
        token = m.group(1).lower()
        canon = DAY_ALIASES.get(token)
        if canon in _DAY_ORDER:
            day_match = canon
            break

    m = re.search(r"\b(\d{1,2})(?::(\d{2}))?\s*(am|pm)\b", text, re.IGNORECASE)
    if m:
        time_match = (int(m.group(1)), int(m.group(2) or "0"), m.group(3).lower())

    if not day_match or not time_match:
        return None

    target_wd = _DAY_ORDER.index(day_match)
    tz = ZoneInfo(tz_name)
    now_local = datetime.now(tz=tz)
    days_ahead = (target_wd - now_local.weekday()) % 7
    base = now_local + timedelta(days=days_ahead)

    hour, minute, ap = time_match
    if ap == "am" and hour == 12:
        hour = 0
    if ap == "pm" and hour != 12:
        hour += 12

    start_dt = datetime(base.year, base.month, base.day, hour, minute, tzinfo=tz)
    if start_dt <= now_local and days_ahead == 0:
        start_dt = start_dt + timedelta(days=7)
    return start_dt


def _extract_explicit_request(text: str, tz_name: str) -> tuple[Optional[datetime], Optional[str]]:
    """
    Returns (start_dt, clarification_question).
    Clarification is returned when a time is mentioned without AM/PM.
    """
    if not text:
        return None, None

    t = text.lower()
    tz = ZoneInfo(tz_name)
    now_local = datetime.now(tz=tz)

    day_label: Optional[str] = None
    target_date = None

    if re.search(r"\btomorrow\b", t):
        day_label = "tomorrow"
        target_date = (now_local + timedelta(days=1)).date()
    elif re.search(r"\btoday\b", t):
        day_label = "today"
        target_date = now_local.date()
    else:
        day_match = None
        for m in re.finditer(r"\b([A-Za-z]{3,9})\b", text):
            token = m.group(1).lower()
            canon = DAY_ALIASES.get(token)
            if canon in _DAY_ORDER:
                day_match = canon
                break
        if day_match:
            day_label = day_match
            target_wd = _DAY_ORDER.index(day_match)
            days_ahead = (target_wd - now_local.weekday()) % 7
            base = now_local + timedelta(days=days_ahead)
            target_date = base.date()

    if target_date is None:
        return None, None

    m = _TIME_CONTEXT_RE.search(text)
    if not m:
        m = _TIME_WITH_AMPM_RE.search(text)
    if not m:
        return None, None

    hour = int(m.group(1))
    minute = int(m.group(2) or "0")
    ap = (m.group(3) or "").lower()

    if ap == "" and hour <= 12:
        time_label = f"{hour}:{minute:02d}" if minute else f"{hour}"
        day_phrase = day_label.capitalize() if day_label else "that day"
        return None, (
            f"You mentioned {day_phrase} around {time_label}. "
            f"Did you mean {time_label}am or {time_label}pm in {tz_name}?"
        )

    if ap == "am" and hour == 12:
        hour = 0
    if ap == "pm" and hour != 12:
        hour += 12

    start_dt = datetime(
        target_date.year,
        target_date.month,
        target_date.day,
        hour,
        minute,
        tzinfo=tz,
    )

    if day_label == "today" and start_dt <= now_local:
        start_dt = start_dt + timedelta(days=1)

    if day_label in _DAY_ORDER and start_dt <= now_local:
        start_dt = start_dt + timedelta(days=7)

    return start_dt, None


@dataclass(frozen=True)
class InboundEmail:
    thread_id: str
    from_email: str
    to_emails: List[str]
    cc_emails: List[str]
    subject: str
    body_text: str
    is_new_request: bool


class IrisCoordinationHandler:
    """
    Pure domain orchestrator.
    NO AWS, NO SES, NO DynamoDB, NO entrypoints imports.
    """

    def __init__(self, store):
        # store must implement get(thread_id) and put(thread)
        self.store = store
        self.coordinator = IrisCoordinator()

    def handle(
        self, inbound: InboundEmail
    ) -> Tuple[List[OutboundMessage], Optional[SchedulePlan]]:

        outbound: List[OutboundMessage] = []
        schedule_plan: Optional[SchedulePlan] = None

        thread = self.store.get(inbound.thread_id)

        # --- New coordination request ---
        if inbound.is_new_request:
            if thread is None:
                raise RuntimeError(
                    "Coordination handler called with is_new_request=True but no thread exists"
                )

            if thread.duration_minutes is None:
                dur = parse_duration_minutes(inbound.body_text)
                if dur:
                    thread.duration_minutes = dur

            start_dt, clar_q = _extract_explicit_request(inbound.body_text, thread.timezone)
            if clar_q:
                thread.status = ThreadStatus.NEEDS_CLARIFICATION
                self.store.put(thread)
                return [
                    OutboundMessage(
                        to=[thread.organizer_email],
                        subject=f"{thread.subject} — quick clarification",
                        body=clarification_email(clar_q),
                    )
                ], None

            # If the request specifies an explicit day/time, schedule immediately.
            if start_dt is None:
                start_dt = _parse_explicit_day_time(inbound.body_text, thread.timezone)
            if start_dt:
                duration = thread.duration_minutes or thread.meeting_duration_minutes
                end_dt = start_dt + timedelta(minutes=duration)
                thread.scheduled_start = start_dt
                thread.scheduled_end = end_dt
                thread.scheduling_rationale = "Explicit time requested by organizer."
                thread.status = ThreadStatus.SCHEDULED
                self.store.put(thread)
                return [], SchedulePlan(start=start_dt, end=end_dt, rationale=thread.scheduling_rationale)

            outbound.append(
                self.coordinator.start_thread(thread)
            )
            self.store.put(thread)
            return outbound, None

        # --- Existing thread reply ---
        if thread is None:
            # Defensive: nothing to do
            return [], None

        if (
            inbound.from_email == thread.organizer_email
            and thread.status == ThreadStatus.NEEDS_CLARIFICATION
            and thread.availability_requests_sent_at is None
        ):
            start_dt, clar_q = _extract_explicit_request(inbound.body_text, thread.timezone)
            if clar_q:
                self.store.put(thread)
                return [
                    OutboundMessage(
                        to=[thread.organizer_email],
                        subject=f"{thread.subject} — quick clarification",
                        body=clarification_email(clar_q),
                    )
                ], None
            if start_dt:
                duration = thread.duration_minutes or thread.meeting_duration_minutes
                end_dt = start_dt + timedelta(minutes=duration)
                thread.scheduled_start = start_dt
                thread.scheduled_end = end_dt
                thread.scheduling_rationale = "Explicit time requested by organizer."
                thread.status = ThreadStatus.SCHEDULED
                self.store.put(thread)
                return [], SchedulePlan(start=start_dt, end=end_dt, rationale=thread.scheduling_rationale)

            outbound.append(self.coordinator.start_thread(thread))
            self.store.put(thread)
            return outbound, None

        # Ingest participant response
        outbound.extend(
            self.coordinator.ingest_participant_reply(
                thread,
                inbound.from_email,
                inbound.body_text,
            )
        )

        # Attempt scheduling
        plan, followups = self.coordinator.try_schedule(thread)
        outbound.extend(followups)

        if plan is not None:
            schedule_plan = plan

        self.store.put(thread)
        return outbound, schedule_plan
