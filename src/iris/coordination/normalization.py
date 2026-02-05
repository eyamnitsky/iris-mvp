from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from zoneinfo import ZoneInfo


DAY_ALIASES = {
    "mon": "mon", "monday": "mon",
    "tue": "tue", "tues": "tue", "tuesday": "tue",
    "wed": "wed", "weds": "wed", "wednesday": "wed",
    "thu": "thu", "thur": "thu", "thurs": "thu", "thursday": "thu",
    "fri": "fri", "friday": "fri",
    "sat": "sat", "saturday": "sat",
    "sun": "sun", "sunday": "sun",
}

TIME_RANGE_SPLIT = re.compile(r"\s*(?:–|—|-|to)\s*", re.IGNORECASE)


@dataclass(frozen=True)
class ParsedTime:
    hour: int
    minute: int
    ampm: str  # "am" | "pm" | ""


def now_in_tz(tz_name: str) -> datetime:
    return datetime.now(tz=ZoneInfo(tz_name))


def infer_year_for_mmdd(mm: int, dd: int, tz_name: str) -> int:
    """Infer year; if date already passed in current year, roll forward one year."""
    today = now_in_tz(tz_name).date()
    candidate = date(today.year, mm, dd)
    if candidate < today:
        return today.year + 1
    return today.year


def clamp_minutes(m: int) -> int:
    return max(0, min(24 * 60, m))


def to_minutes(pt: ParsedTime) -> int:
    h = pt.hour
    if pt.ampm == "am":
        if h == 12:
            h = 0
    elif pt.ampm == "pm":
        if h != 12:
            h += 12
    return clamp_minutes(h * 60 + pt.minute)


def normalize_dash(s: str) -> str:
    return s.replace("—", "–").replace("-", "–")


def split_time_range(s: str) -> tuple[str, str] | None:
    parts = TIME_RANGE_SPLIT.split(s.strip())
    if len(parts) != 2:
        return None
    return parts[0].strip(), parts[1].strip()