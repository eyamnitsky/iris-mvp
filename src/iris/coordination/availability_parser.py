from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import List, Optional, Tuple

from .models import TimeWindow
from .normalization import (
    DAY_ALIASES,
    ParsedTime,
    infer_year_for_mmdd,
    normalize_dash,
    split_time_range,
    to_minutes,
)

LINE_RE = re.compile(
    r"""
    ^\s*
    (?:(?P<dayname>[A-Za-z]{3,9})\s*,\s*)?
    (?P<mm>\d{1,2})\/(?P<dd>\d{1,2})
    \s*:\s*
    (?P<slots>.+?)
    \s*$
    """,
    re.VERBOSE,
)

# Matches times like: 4, 4pm, 4:30pm, 16:00, 9am
TIME_RE = re.compile(
    r"""
    ^\s*
    (?P<h>\d{1,2})
    (?:
      :(?P<m>\d{2})
    )?
    \s*
    (?P<ampm>am|pm)?
    \s*$
    """,
    re.IGNORECASE | re.VERBOSE,
)

SLOT_SPLIT_RE = re.compile(r"\s*,\s*")


@dataclass
class ParseResult:
    windows: List[TimeWindow]
    needs_clarification: bool = False
    clarification_question: Optional[str] = None


def _parse_time(token: str) -> Optional[ParsedTime]:
    m = TIME_RE.match(token)
    if not m:
        return None
    h = int(m.group("h"))
    minute = int(m.group("m") or "0")
    ampm = (m.group("ampm") or "").lower()

    # 24h clock support if no am/pm and hour >= 13
    if ampm == "" and h >= 13:
        return ParsedTime(hour=h, minute=minute, ampm="")  # treat as 24h

    if h < 1 or h > 12:
        # could be invalid 24h like 24:00; reject
        if not (ampm == "" and 0 <= h <= 23):
            return None

    return ParsedTime(hour=h, minute=minute, ampm=ampm)


def _coerce_ampm(start: ParsedTime, end: ParsedTime) -> Tuple[ParsedTime, ParsedTime] | None:
    """
    Graceful rule:
    - If end has am/pm and start doesn't, copy end am/pm to start.
    - If start has am/pm and end doesn't, copy start am/pm to end.
    - If both empty: ambiguous unless hours look like 24h format.
    """
    s_amp, e_amp = start.ampm, end.ampm

    # 24h format: treat as non-ambiguous if both lack am/pm and either hour >= 13
    if s_amp == "" and e_amp == "" and (start.hour >= 13 or end.hour >= 13):
        return start, end

    if s_amp == "" and e_amp in ("am", "pm"):
        return ParsedTime(start.hour, start.minute, e_amp), end
    if e_amp == "" and s_amp in ("am", "pm"):
        return start, ParsedTime(end.hour, end.minute, s_amp)
    if s_amp in ("am", "pm") and e_amp in ("am", "pm"):
        return start, end

    # both empty and not 24h -> ambiguous (e.g. 1-3)
    return None


def parse_availability(text: str, tz_name: str) -> ParseResult:
    """
    Parses lines like:
      Tue, 02/11: 1pm–3pm, 4:30pm–5pm
      02/12: 9–11am
    Returns TimeWindow list (minutes since midnight).
    """
    windows: List[TimeWindow] = []
    ambiguous_examples: List[str] = []

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        m = LINE_RE.match(line)
        if not m:
            continue  # ignore non-conforming lines

        mm = int(m.group("mm"))
        dd = int(m.group("dd"))
        year = infer_year_for_mmdd(mm, dd, tz_name)
        day = date(year, mm, dd)

        slots_str = normalize_dash(m.group("slots"))
        slot_tokens = SLOT_SPLIT_RE.split(slots_str)

        for slot in slot_tokens:
            tr = split_time_range(slot)
            if not tr:
                continue
            s_tok, e_tok = tr

            ps = _parse_time(s_tok)
            pe = _parse_time(e_tok)
            if not ps or not pe:
                continue

            coerced = _coerce_ampm(ps, pe)
            if coerced is None:
                ambiguous_examples.append(f"{s_tok}–{e_tok} on {mm:02d}/{dd:02d}")
                continue

            ps2, pe2 = coerced

            # Convert; 24h handled by to_minutes only when ampm provided, so special-case 24h.
            if ps2.ampm == "" and pe2.ampm == "" and (ps2.hour >= 13 or pe2.hour >= 13):
                start_min = ps2.hour * 60 + ps2.minute
                end_min = pe2.hour * 60 + pe2.minute
            else:
                start_min = to_minutes(ps2)
                end_min = to_minutes(pe2)

            tw = TimeWindow(day=day, start_minute=start_min, end_minute=end_min)
            if tw.is_valid():
                windows.append(tw)

    if ambiguous_examples:
        ex = ambiguous_examples[0]
        return ParseResult(
            windows=windows,
            needs_clarification=True,
            clarification_question=(
                f"I can’t confidently interpret `{ex}`. "
                f"Did you mean AM or PM (e.g., `1pm–3pm`)?"
            ),
        )

    return ParseResult(windows=windows)