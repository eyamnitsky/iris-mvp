from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional, Tuple
import re

from ..infra import ddb as ddb_mod
from ..infra.config import DDB_SK_VALUE, DEFAULT_DURATION_MINUTES
from ..infra.serialization import ddb_clean, ddb_sanitize, to_json_safe

from .context import IrisContext, ConversationState, Intent
from ..coordination.availability_parser import parse_availability
from ..coordination.constraint_parser import parse_constraints
from ..coordination.models import TimeWindow
from .parsing import infer_intent
from .rules import missing_fields, is_ready
from .formatting import ask_for_missing, confirm_summary


@dataclass
class ThreadState:
    """Lightweight thread state returned to the Lambda handler."""
    thread_id: str
    state: str
    intent: str
    timezone: str
    last_candidate: Optional[dict] = None


@dataclass
class Decision:
    """Decision returned to the Lambda handler."""
    action: str  # "ignore" | "clarify" | "schedule"
    reply_text: Optional[str] = None
    time_kind: Optional[str] = None  # "candidate" | None
    chosen_candidate: Optional[dict] = None


def handle_incoming_message(ctx: IrisContext, text: str):
    """Pure in-memory state machine (kept for unit tests / future evolution)."""
    if ctx.state == ConversationState.INTENT_DETECTION:
        ctx.memory.intent = infer_intent(text)
        ctx.state = ConversationState.INFO_GATHERING

    missing = missing_fields(ctx)
    if missing:
        return ask_for_missing(missing[0]), ctx, None

    if is_ready(ctx):
        ctx.state = ConversationState.CONFIRMATION_CHECK
        return confirm_summary(ctx), ctx, "READY"

    return "Can you clarify?", ctx, None


def _key_for_thread(thread_id: str) -> Dict[str, Any]:
    """Create a DynamoDB key for a thread item using the table's PK/SK schema."""
    ddb_mod.ensure_schema_loaded()
    pk_attr = ddb_mod.PK_ATTR
    sk_attr = ddb_mod.SK_ATTR
    if not pk_attr:
        raise RuntimeError("DDB schema not loaded (missing PK_ATTR)")
    key: Dict[str, Any] = {pk_attr: thread_id}
    if sk_attr:
        key[sk_attr] = DDB_SK_VALUE
    return key


def _now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def _ai_to_thread_intent(ai_intent: Optional[str]) -> str:
    if not ai_intent:
        return "OTHER"
    up = str(ai_intent).upper()
    if up in {"NEW_REQUEST", "AVAILABILITY", "CONFIRMATION", "DECLINE", "OTHER"}:
        return up
    return "OTHER"


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


_WEEKDAY_RE = re.compile(
    r"\b(mon(?:day)?|tue(?:sday)?|wed(?:nesday)?|thu(?:rsday)?|fri(?:day)?|sat(?:urday)?|sun(?:day)?)\b",
    re.IGNORECASE,
)
_RELATIVE_DAY_RE = re.compile(r"\b(today|tomorrow)\b", re.IGNORECASE)
_TIME_RE = re.compile(r"\b(\d{1,2})(?::(\d{2}))?\s*(am|pm)\b", re.IGNORECASE)


def _candidate_has_weekday(candidate: Dict[str, Any]) -> bool:
    start_local = (candidate.get("start_local") or "").strip()
    return bool(_WEEKDAY_RE.search(start_local))


_WEEKDAY_CANON = {
    "mon": "Monday",
    "tue": "Tuesday",
    "wed": "Wednesday",
    "thu": "Thursday",
    "fri": "Friday",
    "sat": "Saturday",
    "sun": "Sunday",
}


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


def _window_to_candidate(w: TimeWindow, source_text: str) -> Dict[str, Any]:
    day_name = w.day.strftime("%A")
    start_local = f"{day_name} {_format_time_12h(w.start_minute)}"
    end_local = f"{day_name} {_format_time_12h(w.end_minute)}"
    return {
        "start_local": start_local,
        "end_local": end_local,
        "confidence": 1.0,
        "source_text": source_text[:200],
    }


def process_incoming_email(
    *,
    table,
    thread_id: str,
    message_id: str,
    body_text: str,
    timezone_default: str,
    ai_parsed: Optional[dict],
) -> Tuple[ThreadState, Decision]:
    """
    Thread-scoped wrapper used by src/iris/entrypoints/handler.py.

    Returns (thread_state, decision).

    - Stores thread state in IrisThreadsTable using PK=thread_id (e.g., "thread#<root>")
      and SK=DDB_SK_VALUE if the table has a sort key.
    - Minimal logic:
        * If candidates exist -> schedule using best-by-confidence
        * Else -> clarify (use AI's clarifying_question)
    """
    key = _key_for_thread(thread_id)
    existing = table.get_item(Key=key).get("Item") or {}

    tz = (
        (ai_parsed or {}).get("timezone")
        or existing.get("timezone")
        or timezone_default
        or "America/New_York"
    )

    ai_intent = _ai_to_thread_intent((ai_parsed or {}).get("intent"))
    state = existing.get("state") or ConversationState.INFO_GATHERING.value

    candidates = []
    if isinstance((ai_parsed or {}).get("candidates"), list):
        candidates = (ai_parsed or {}).get("candidates") or []
    had_ai_candidates = bool(candidates)
    candidates = [c for c in candidates if isinstance(c, dict) and _candidate_has_weekday(c)]
    force_day_clarify = had_ai_candidates and not candidates

    clar_q = (ai_parsed or {}).get("clarifying_question") if isinstance(ai_parsed, dict) else None
    if not isinstance(clar_q, str) or not clar_q.strip():
        clar_q = "What day and time would you like to schedule this meeting for?"

    last_candidate = None
    if isinstance(existing.get("last_candidate"), dict):
        last_candidate = existing.get("last_candidate")
        if not isinstance(last_candidate, dict):
            last_candidate = None

    if candidates:
        # Choose best candidate by confidence (fallback to first)
        best = None
        best_conf = -1.0
        for c in candidates:
            if not isinstance(c, dict):
                continue
            try:
                conf_f = float(c.get("confidence", 0.0))
            except Exception:
                conf_f = 0.0
            if conf_f > best_conf:
                best_conf = conf_f
                best = c

        chosen = best or candidates[0]
        last_candidate = chosen if isinstance(chosen, dict) else None

        decision = Decision(
            action="schedule",
            time_kind="candidate",
            chosen_candidate=last_candidate,
        )
        state = ConversationState.EXECUTION.value
    else:
        # Fallback: if body text clearly specifies a day/time, schedule without asking again.
        parsed = parse_availability(body_text, tz_name=tz)
        windows = parsed.windows
        if not windows:
            windows, _ = parse_constraints(body_text, tz=tz)

        if windows:
            windows.sort(key=lambda w: (w.day, w.start_minute))
            last_candidate = _window_to_candidate(windows[0], body_text)
            decision = Decision(
                action="schedule",
                time_kind="candidate",
                chosen_candidate=last_candidate,
            )
            state = ConversationState.EXECUTION.value
        else:
            if last_candidate and _TIME_RE.search(body_text) and not _WEEKDAY_RE.search(body_text):
                weekday = _weekday_from_candidate(last_candidate)
                time_minutes = _extract_time_minutes(body_text)
                if weekday and time_minutes is not None:
                    last_candidate = _candidate_from_time_only(
                        time_minutes=time_minutes,
                        weekday=weekday,
                        duration_minutes=DEFAULT_DURATION_MINUTES,
                        source_text=body_text,
                    )
                    decision = Decision(
                        action="schedule",
                        time_kind="candidate",
                        chosen_candidate=last_candidate,
                    )
                    state = ConversationState.EXECUTION.value
                    # Persist thread state below
                    thread_item = dict(key)
                    thread_item.update(
                        {
                            "record_type": "THREAD",
                            "updated_at": _now_iso(),
                            "state": state,
                            "intent": ai_intent,
                            "timezone": tz,
                            "last_message_id": message_id,
                            "last_candidate": last_candidate or {},
                            "last_ai_json": json.dumps(to_json_safe(ai_parsed)) if ai_parsed else "{}",
                        }
                    )
                    table.put_item(Item=ddb_clean(ddb_sanitize(thread_item)))
                    thread_state = ThreadState(
                        thread_id=thread_id,
                        state=state,
                        intent=ai_intent,
                        timezone=tz,
                        last_candidate=last_candidate,
                    )
                    return thread_state, decision
            if force_day_clarify or (
                _TIME_RE.search(body_text)
                and not (_WEEKDAY_RE.search(body_text) or _RELATIVE_DAY_RE.search(body_text))
            ):
                clar_q = "Which day should I schedule that time for?"
            decision = Decision(action="clarify", reply_text=clar_q)
            state = ConversationState.CLARIFICATION_LOOP.value

    # Persist thread state
    thread_item = dict(key)
    thread_item.update(
        {
            "record_type": "THREAD",
            "updated_at": _now_iso(),
            "state": state,
            "intent": ai_intent,
            "timezone": tz,
            "last_message_id": message_id,
            "last_candidate": last_candidate or {},
            "last_ai_json": json.dumps(to_json_safe(ai_parsed)) if ai_parsed else "{}",
        }
    )
    table.put_item(Item=ddb_clean(ddb_sanitize(thread_item)))

    thread_state = ThreadState(
        thread_id=thread_id,
        state=state,
        intent=ai_intent,
        timezone=tz,
        last_candidate=last_candidate,
    )
    return thread_state, decision
