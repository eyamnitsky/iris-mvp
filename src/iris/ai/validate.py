import re
from typing import Any, Dict

def validate_result(obj: Any, tz_default: str) -> Dict[str, Any]:
    safe = {
        "intent": "OTHER",
        "needs_clarification": True,
        "clarifying_question": "What day and time would you like to schedule this meeting for?",
        "timezone": tz_default,
        "candidates": []
    }

    if not isinstance(obj, dict):
        return safe

    if obj.get("intent") not in {"NEW_REQUEST", "AVAILABILITY", "CONFIRMATION", "DECLINE", "OTHER"}:
        obj["intent"] = "OTHER"

    if not isinstance(obj.get("needs_clarification"), bool):
        obj["needs_clarification"] = True

    if not isinstance(obj.get("clarifying_question"), str):
        obj["clarifying_question"] = safe["clarifying_question"]

    if not isinstance(obj.get("timezone"), str) or not obj.get("timezone"):
        obj["timezone"] = tz_default

    candidates = obj.get("candidates")
    if not isinstance(candidates, list):
        candidates = []

    normalized = []
    for c in candidates:
        if not isinstance(c, dict):
            continue
        if "start_local" not in c or "end_local" not in c:
            continue

        start_local = c.get("start_local")
        end_local = c.get("end_local")
        source_text = c.get("source_text", "")
        conf = c.get("confidence", 0.0)

        if not isinstance(start_local, str) or not isinstance(end_local, str):
            continue

        try:
            conf_f = float(conf)
        except Exception:
            conf_f = 0.0

        normalized.append({
            "start_local": start_local[:80],
            "end_local": end_local[:80],
            "confidence": max(0.0, min(1.0, conf_f)),
            "source_text": str(source_text)[:200]
        })

    obj["candidates"] = normalized

    # deterministic override: if we have a concrete day + time, do not ask "what time" again.
    day_re = re.compile(r"\b(Mon|Tues|Tue|Wed|Thu|Thurs|Fri|Sat|Sun)(day)?\b", re.IGNORECASE)
    hour_re = re.compile(r"\b([1-9]|1[0-2])(?:\:\d{2})?\b")
    ampm_re = re.compile(r"\b(am|pm)\b", re.IGNORECASE)
    aroundish_re = re.compile(r"\b(around|about|approx|~|ish)\b", re.IGNORECASE)

    has_day_and_time = False
    for c in obj["candidates"]:
        start_local = c.get("start_local", "")
        src = c.get("source_text", "")
        text = f"{start_local} {src}"

        if not day_re.search(start_local):
            continue

        if ampm_re.search(text) and hour_re.search(text):
            has_day_and_time = True
            break

        if aroundish_re.search(text) and hour_re.search(text):
            has_day_and_time = True
            break

    if has_day_and_time:
        obj["needs_clarification"] = False
        obj["clarifying_question"] = ""
    else:
        if obj["needs_clarification"] is False:
            obj["clarifying_question"] = obj.get("clarifying_question", "") or ""

        if not obj["candidates"] and obj["needs_clarification"] is False:
            obj["needs_clarification"] = True
            obj["clarifying_question"] = safe["clarifying_question"]

    return obj
