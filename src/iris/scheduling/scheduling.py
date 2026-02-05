from __future__ import annotations

import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Tuple, Dict

from ..infra.config import DEFAULT_START_HOUR, DEFAULT_DURATION_MINUTES

def next_day_at_default_time(local_tz: ZoneInfo):
    now_local = datetime.now(tz=local_tz)
    next_day = (now_local + timedelta(days=1)).date()
    start = datetime(
        next_day.year,
        next_day.month,
        next_day.day,
        DEFAULT_START_HOUR,
        0,
        0,
        tzinfo=local_tz,
    )
    end = start + timedelta(minutes=DEFAULT_DURATION_MINUTES)
    return start, end


_DOW = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}


def _next_weekday_date(today_local: datetime, target_wd: int) -> datetime:
    days_ahead = (target_wd - today_local.weekday()) % 7
    return today_local + timedelta(days=days_ahead)


def _parse_time_12h(s: str) -> Tuple[int, int]:
    s = s.strip().lower()
    m = re.search(r"\b(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\b", s)
    if not m:
        raise ValueError(f"Could not parse time from: {s}")
    hour = int(m.group(1))
    minute = int(m.group(2) or "0")
    ampm = m.group(3)

    if ampm:
        if hour == 12:
            hour = 0
        if ampm == "pm":
            hour += 12

    return hour, minute


def candidate_to_datetimes(candidate: Dict, tz: ZoneInfo) -> Tuple[datetime, datetime]:
    """
    Convert candidate like:
      start_local: 'Saturday 3:00 PM'
      end_local:   'Saturday 3:30 PM' OR '3:30 PM'
    into timezone-aware datetimes.
    """
    start_local = (candidate.get("start_local") or "").strip()
    end_local = (candidate.get("end_local") or "").strip()
    if not start_local or not end_local:
        raise ValueError("Missing start_local/end_local")

    mday = re.search(
        r"\b(mon(?:day)?|tue(?:sday)?|wed(?:nesday)?|thu(?:rsday)?|fri(?:day)?|sat(?:urday)?|sun(?:day)?)\b",
        start_local,
        re.IGNORECASE,
    )
    if not mday:
        raise ValueError(f"No weekday found in start_local: {start_local}")

    wd = mday.group(1).lower()
    if wd.startswith("tue"):
        wd_key = "tue"
    elif wd.startswith("thu"):
        wd_key = "thu"
    else:
        wd_key = wd[:3]

    target_wd = _DOW[wd_key]

    now_local = datetime.now(tz=tz)
    base = _next_weekday_date(now_local, target_wd)

    sh, sm = _parse_time_12h(start_local)
    eh, em = _parse_time_12h(end_local)

    start_dt = datetime(base.year, base.month, base.day, sh, sm, tzinfo=tz)
    end_dt = datetime(base.year, base.month, base.day, eh, em, tzinfo=tz)

    if start_dt <= now_local and base.date() == now_local.date():
        start_dt = start_dt + timedelta(days=7)
        end_dt = end_dt + timedelta(days=7)

    if end_dt <= start_dt:
        end_dt = end_dt + timedelta(days=1)

    return start_dt, end_dt
