from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, time
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from .models import MeetingThread, TimeWindow


@dataclass(frozen=True)
class ScheduledSlot:
    start: datetime
    end: datetime
    rationale: str


def _intersect_two(a: List[TimeWindow], b: List[TimeWindow]) -> List[TimeWindow]:
    # Group by date for efficiency
    a_by = defaultdict(list)
    b_by = defaultdict(list)
    for w in a:
        a_by[w.day].append(w)
    for w in b:
        b_by[w.day].append(w)

    out: List[TimeWindow] = []
    for d in set(a_by.keys()) & set(b_by.keys()):
        for wa in a_by[d]:
            for wb in b_by[d]:
                s = max(wa.start_minute, wb.start_minute)
                e = min(wa.end_minute, wb.end_minute)
                if s < e:
                    out.append(TimeWindow(day=d, start_minute=s, end_minute=e))
    return out


def find_earliest_overlap(thread: MeetingThread) -> Optional[ScheduledSlot]:
    """
    Intersect all participants’ parsed windows and pick earliest window that can fit duration.
    """
    tz = ZoneInfo(thread.timezone)
    duration = thread.meeting_duration_minutes

    participants = list(thread.participants.values())
    if not participants:
        return None

    # Start with first participant windows, intersect progressively
    current = participants[0].parsed_windows[:]
    for p in participants[1:]:
        current = _intersect_two(current, p.parsed_windows)
        if not current:
            return None

    # Sort by date then start time
    current.sort(key=lambda w: (w.day, w.start_minute))

    for w in current:
        if (w.end_minute - w.start_minute) < duration:
            continue
        start_dt = datetime.combine(w.day, time(hour=w.start_minute // 60, minute=w.start_minute % 60), tzinfo=tz)
        end_min = w.start_minute + duration
        end_dt = datetime.combine(w.day, time(hour=end_min // 60, minute=end_min % 60), tzinfo=tz)

        rationale = f"Earliest overlap across {len(participants)} participants on {w.day.isoformat()}."
        return ScheduledSlot(start=start_dt, end=end_dt, rationale=rationale)

    return None