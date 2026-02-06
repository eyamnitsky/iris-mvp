from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from .models import MeetingThread, Participant, ThreadStatus
from .coordinator import IrisCoordinator
from .types import OutboundMessage, SchedulePlan

from .duration_parser import parse_duration_minutes
from .normalization import DAY_ALIASES
from .templates import clarification_email
from ..scheduling.scheduling import candidate_to_datetimes

_DAY_ORDER = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]

_WEEKDAY_RE = re.compile(
    r"\b(mon(?:day)?|tue(?:sday)?|wed(?:nesday)?|thu(?:rsday)?|fri(?:day)?|sat(?:urday)?|sun(?:day)?)\b",
    re.IGNORECASE,
)
_TIME_RE = re.compile(r"\b(\d{1,2})(?::(\d{2}))?\s*(am|pm)\b", re.IGNORECASE)

_WEEKDAY_CANON = {
    "mon": "Monday",
    "tue": "Tuesday",
    "wed": "Wednesday",
    "thu": "Thursday",
    "fri": "Friday",
    "sat": "Saturday",
    "sun": "Sunday",
}


def _candidate_has_weekday(candidate: Dict[str, Any]) -> bool:
    start_local = (candidate.get("start_local") or "").strip()
    return bool(_WEEKDAY_RE.search(start_local))


def _weekday_from_candidate(candidate: Dict[str, Any]) -> Optional[str]:
    start_local = (candidate.get("start_local") or "").strip()
    m = _WEEKDAY_RE.search(start_local)
    if not m:
        return None
    token = m.group(1).lower()
    if token.startswith("tue"):
        key = "tue"
    elif token.startswith("thu"):
        key = "thu"
    else:
        key = token[:3]
    return _WEEKDAY_CANON.get(key)


def _extract_time_minutes(text: str) -> Optional[int]:
    m = _TIME_RE.search(text or "")
    if not m:
        return None
    hour = int(m.group(1))
    minute = int(m.group(2) or "0")
    ampm = m.group(3).lower()
    if ampm == "am":
        if hour == 12:
            hour = 0
    elif ampm == "pm":
        if hour != 12:
            hour += 12
    return hour * 60 + minute


def _format_time_12h(minutes: int) -> str:
    h = minutes // 60
    m = minutes % 60
    ampm = "AM"
    if h == 0:
        h = 12
        ampm = "AM"
    elif h == 12:
        ampm = "PM"
    elif h > 12:
        h -= 12
        ampm = "PM"
    return f"{h}:{m:02d} {ampm}"


def _candidate_from_time_only(
    time_minutes: int,
    weekday: str,
    duration_minutes: int,
    source_text: str,
) -> Dict[str, Any]:
    start_minutes = time_minutes
    end_minutes = (time_minutes + duration_minutes) % (24 * 60)
    start_local = f"{weekday} {_format_time_12h(start_minutes)}"
    end_local = f"{weekday} {_format_time_12h(end_minutes)}"
    return {
        "start_local": start_local,
        "end_local": end_local,
        "confidence": 0.9,
        "source_text": source_text[:200],
    }

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


@dataclass(frozen=True)
class InboundEmail:
    thread_id: str
    from_email: str
    to_emails: List[str]
    cc_emails: List[str]
    subject: str
    body_text: str
    is_new_request: bool
    ai_parsed: Optional[Dict[str, Any]] = None


def _ai_intent(ai_parsed: Optional[Dict[str, Any]]) -> Optional[str]:
    if not isinstance(ai_parsed, dict):
        return None
    intent = ai_parsed.get("intent")
    return intent if isinstance(intent, str) else None


def _ai_needs_clarification(ai_parsed: Optional[Dict[str, Any]]) -> bool:
    if not isinstance(ai_parsed, dict):
        return False
    return bool(ai_parsed.get("needs_clarification") is True)


def _ai_clarifying_question(ai_parsed: Optional[Dict[str, Any]]) -> Optional[str]:
    if not isinstance(ai_parsed, dict):
        return None
    q = ai_parsed.get("clarifying_question")
    return q if isinstance(q, str) and q.strip() else None


def _ai_candidates(ai_parsed: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not isinstance(ai_parsed, dict):
        return []
    cands = ai_parsed.get("candidates")
    return cands if isinstance(cands, list) else []


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

            ai_intent = _ai_intent(inbound.ai_parsed)
            ai_needs = _ai_needs_clarification(inbound.ai_parsed)
            ai_cands = _ai_candidates(inbound.ai_parsed)
            ai_clar_q = _ai_clarifying_question(inbound.ai_parsed)

            if ai_intent == "NEW_REQUEST":
                if ai_needs and ai_cands:
                    thread.pending_candidate = ai_cands[0] if isinstance(ai_cands[0], dict) else None
                    clar_q = ai_clar_q or "Could you clarify the exact time (including AM/PM and timezone)?"
                    thread.status = ThreadStatus.NEEDS_CLARIFICATION
                    self.store.put(thread)
                    return [
                        OutboundMessage(
                            to=[thread.organizer_email],
                            subject=f"{thread.subject} — quick clarification",
                            body=clarification_email(clar_q),
                        )
                    ], None

                if not ai_needs and ai_cands:
                    tz = ZoneInfo(thread.timezone)
                    start_dt = None
                    end_dt = None
                    try:
                        start_dt, end_dt = candidate_to_datetimes(ai_cands[0], tz)
                    except Exception:
                        start_dt = None

                    if start_dt and end_dt:
                        thread.scheduled_start = start_dt
                        thread.scheduled_end = end_dt
                        thread.scheduling_rationale = "Explicit time requested by organizer."
                        thread.status = ThreadStatus.SCHEDULED
                        thread.pending_candidate = None
                        self.store.put(thread)
                        return [], SchedulePlan(start=start_dt, end=end_dt, rationale=thread.scheduling_rationale)

            # If the request specifies an explicit day/time, schedule immediately.
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
            ai_intent = _ai_intent(inbound.ai_parsed)
            ai_needs = _ai_needs_clarification(inbound.ai_parsed)
            ai_cands = _ai_candidates(inbound.ai_parsed)
            ai_clar_q = _ai_clarifying_question(inbound.ai_parsed)

            if ai_intent in ("NEW_REQUEST", "CONFIRMATION"):
                if ai_needs and ai_cands:
                    thread.pending_candidate = ai_cands[0] if isinstance(ai_cands[0], dict) else thread.pending_candidate
                    clar_q = ai_clar_q or "Could you clarify the exact time (including AM/PM and timezone)?"
                    self.store.put(thread)
                    return [
                        OutboundMessage(
                            to=[thread.organizer_email],
                            subject=f"{thread.subject} — quick clarification",
                            body=clarification_email(clar_q),
                        )
                    ], None

                if not ai_needs and ai_cands:
                    tz = ZoneInfo(thread.timezone)
                    start_dt = None
                    end_dt = None
                    try:
                        start_dt, end_dt = candidate_to_datetimes(ai_cands[0], tz)
                    except Exception:
                        start_dt = None
                        end_dt = None

                    if (not start_dt or not end_dt) and thread.pending_candidate:
                        pending_weekday = _weekday_from_candidate(thread.pending_candidate)
                        time_minutes = _extract_time_minutes(inbound.body_text)
                        if pending_weekday and time_minutes is not None:
                            duration = thread.duration_minutes or thread.meeting_duration_minutes
                            derived = _candidate_from_time_only(
                                time_minutes=time_minutes,
                                weekday=pending_weekday,
                                duration_minutes=duration,
                                source_text=inbound.body_text,
                            )
                            try:
                                start_dt, end_dt = candidate_to_datetimes(derived, tz)
                            except Exception:
                                start_dt = None
                                end_dt = None

                    if start_dt and end_dt:
                        thread.scheduled_start = start_dt
                        thread.scheduled_end = end_dt
                        thread.scheduling_rationale = "Explicit time requested by organizer."
                        thread.status = ThreadStatus.SCHEDULED
                        thread.pending_candidate = None
                        self.store.put(thread)
                        return [], SchedulePlan(start=start_dt, end=end_dt, rationale=thread.scheduling_rationale)

            if thread.pending_candidate:
                pending_weekday = _weekday_from_candidate(thread.pending_candidate)
                time_minutes = _extract_time_minutes(inbound.body_text)
                if pending_weekday and time_minutes is not None:
                    duration = thread.duration_minutes or thread.meeting_duration_minutes
                    derived = _candidate_from_time_only(
                        time_minutes=time_minutes,
                        weekday=pending_weekday,
                        duration_minutes=duration,
                        source_text=inbound.body_text,
                    )
                    tz = ZoneInfo(thread.timezone)
                    try:
                        start_dt, end_dt = candidate_to_datetimes(derived, tz)
                    except Exception:
                        start_dt = None
                        end_dt = None
                    if start_dt and end_dt:
                        thread.scheduled_start = start_dt
                        thread.scheduled_end = end_dt
                        thread.scheduling_rationale = "Explicit time requested by organizer."
                        thread.status = ThreadStatus.SCHEDULED
                        thread.pending_candidate = None
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
