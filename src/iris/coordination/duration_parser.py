import re
from typing import Optional

MIN_RE = re.compile(r"\b(\d{1,3})\s*(min|mins|minute|minutes)\b", re.IGNORECASE)
HOUR_RE = re.compile(r"\b(\d{1,2})\s*(h|hr|hrs|hour|hours)\b", re.IGNORECASE)
HALF_HOUR_RE = re.compile(r"\bhalf\s*hour\b", re.IGNORECASE)

def parse_duration_minutes(text: str) -> Optional[int]:
    if not text:
        return None

    m = MIN_RE.search(text)
    if m:
        v = int(m.group(1))
        return v if 1 <= v <= 480 else None

    m = HOUR_RE.search(text)
    if m:
        v = int(m.group(1)) * 60
        return v if 1 <= v <= 480 else None

    if HALF_HOUR_RE.search(text):
        return 30

    return None