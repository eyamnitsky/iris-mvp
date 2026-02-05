from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import List, Optional, Tuple
from zoneinfo import ZoneInfo

from .models import TimeWindow

DOW = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}
DOW_RE = r"(mon|tue|tues|wed|thu|thur|thurs|fri|sat|sun)"
RANGE_DOW_RE = re.compile(rf"\b{DOW_RE}\s*-\s*{DOW_RE}\b", re.IGNORECASE)
LIST_DOW_RE = re.compile(rf"\b{DOW_RE}(?:\s*/\s*{DOW_RE})+\b", re.IGNORECASE)

def _now_date(tz: str) -> date:
    return datetime.now(tz=ZoneInfo(tz)).date()

def _start_of_next_week(d: date) -> date:
    # next Monday (not "this Monday")
    days_until_mon = (7 - d.weekday()) % 7
    if days_until_mon == 0:
        days_until_mon = 7
    return d + timedelta(days=days_until_mon)

def _minutes(h: int, m: int = 0) -> int:
    return h * 60 + m

def _time_window_for_part_of_day(part: str) -> Tuple[int, int]:
    # Opinionated defaults (you can tune these)
    part = part.lower()
    if part == "morning":
        return _minutes(9, 0), _minutes(12, 0)
    if part == "afternoon":
        return _minutes(12, 0), _minutes(17, 0)
    if part == "evening":
        return _minutes(17, 0), _minutes(21, 0)
    return _minutes(9, 0), _minutes(17, 0)  # "anytime" fallback

TIME_BETWEEN_RE = re.compile(r"\bbetween\s+(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\s+and\s+(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\b", re.IGNORECASE)
AFTER_RE = re.compile(r"\bafter\s+(\d{1,2})(?::(\d{2}))?\s*(am|pm)\b", re.IGNORECASE)
BEFORE_RE = re.compile(r"\bbefore\s+(\d{1,2})(?::(\d{2}))?\s*(am|pm)\b", re.IGNORECASE)

def _to_24h(h: int, m: int, ap: str) -> Tuple[int, int]:
    ap = ap.lower()
    if ap == "am":
        if h == 12:
            h = 0
    elif ap == "pm":
        if h != 12:
            h += 12
    return h, m

def _parse_time_bounds(text: str) -> Optional[Tuple[int, int]]:
    t = text.lower()

    m = TIME_BETWEEN_RE.search(t)
    if m:
        h1, m1, ap1, h2, m2, ap2 = m.groups()
        h1 = int(h1); m1 = int(m1 or 0); ap1 = ap1 or ""
        h2 = int(h2); m2 = int(m2 or 0); ap2 = ap2 or ""
        # If am/pm missing, treat as ambiguous -> None
        if not ap1 or not ap2:
            return None
        h1, m1 = _to_24h(h1, m1, ap1)
        h2, m2 = _to_24h(h2, m2, ap2)
        return _minutes(h1, m1), _minutes(h2, m2)

    m = AFTER_RE.search(t)
    if m:
        h, mm, ap = m.groups()
        h = int(h); mm = int(mm or 0)
        h, mm = _to_24h(h, mm, ap)
        return _minutes(h, mm), _minutes(21, 0)

    m = BEFORE_RE.search(t)
    if m:
        h, mm, ap = m.groups()
        h = int(h); mm = int(mm or 0)
        h, mm = _to_24h(h, mm, ap)
        return _minutes(9, 0), _minutes(h, mm)

    return None

def _extract_days(text: str, tz: str) -> Optional[List[date]]:
    t = text.lower()
    today = _now_date(tz)

    base = today
    if "next week" in t:
        base = _start_of_next_week(today)

    # Range like Mon-Tue
    m = RANGE_DOW_RE.search(t)
    if m:
        a, b = m.group(0).split("-")
        a = a.strip()[:3].lower().replace("tues", "tue")
        b = b.strip()[:3].lower().replace("tues", "tue")
        if a not in DOW or b not in DOW:
            return None
        start = DOW[a]
        end = DOW[b]
        # build list within that week (base week)
        days = []
        for i in range(7):
            d = base + timedelta(days=i)
            if start <= end:
                if start <= d.weekday() <= end:
                    days.append(d)
            else:
                # wrap (e.g. Fri-Mon)
                if d.weekday() >= start or d.weekday() <= end:
                    days.append(d)
        return days

    # List like Tue/Thu
    m = LIST_DOW_RE.search(t)
    if m:
        parts = re.split(r"\s*/\s*", m.group(0))
        wanted = set()
        for p in parts:
            key = p.strip()[:3].lower().replace("tues", "tue")
            if key in DOW:
                wanted.add(DOW[key])
        if not wanted:
            return None
        days = [base + timedelta(days=i) for i in range(7) if (base + timedelta(days=i)).weekday() in wanted]
        return days

    # Single day mention
    for k, idx in DOW.items():
        if re.search(rf"\b{k}\b", t):
            # choose that day in base week
            for i in range(7):
                d = base + timedelta(days=i)
                if d.weekday() == idx:
                    return [d]
    return None

def parse_constraints(text: str, tz: str) -> Tuple[List[TimeWindow], Optional[str]]:
    """
    Returns (windows, clarification_question).
    If unclear (e.g., "between 1 and 3" with no am/pm), returns clarification_question.
    """
    t = (text or "").strip()
    if not t:
        return [], None

    days = _extract_days(t, tz)
    if not days:
        return [], None

    # part of day
    part = None
    if re.search(r"\bmorning\b", t, re.IGNORECASE): part = "morning"
    if re.search(r"\bafternoon\b", t, re.IGNORECASE): part = "afternoon"
    if re.search(r"\bevening\b", t, re.IGNORECASE): part = "evening"

    start_min, end_min = _time_window_for_part_of_day(part or "anytime")

    # explicit time bounds override part-of-day
    bounds = _parse_time_bounds(t)
    if bounds is None and ("between" in t.lower()):
        return [], "For “between … and …”, could you include AM/PM (e.g., 1pm–3pm) and your timezone?"
    if bounds:
        start_min, end_min = bounds

    windows = [TimeWindow(day=d, start_minute=start_min, end_minute=end_min) for d in days]
    windows = [w for w in windows if w.is_valid()]

    # common ambiguity: “afternoon” without timezone (we’ll default, but you can ask)
    return windows, None