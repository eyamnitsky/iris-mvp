from __future__ import annotations

from datetime import datetime
from typing import List

from .types import OutboundMessage

def availability_request_email(participant_emails: List[str], deadline: datetime | None, tz_name: str) -> str:
    deadline_str = f"{deadline.strftime('%a %m/%d %I:%M%p')} {tz_name}" if deadline else "soon"
    return (
        "Hi everyone — I’ll coordinate this meeting.\n\n"
        "Please reply with your availability using this format (one or more lines):\n\n"
        "Day, MM/DD: start–end, start–end\n\n"
        "Examples:\n"
        "Tue, 02/11: 1pm–3pm, 4:30pm–5pm\n"
        "Wed, 02/12: 9–11am\n\n"
        "Notes:\n"
        "- You can write `4–5pm` or `4pm–5pm` — I’ll interpret both.\n"
        "- You can include multiple days.\n\n"
        f"Please reply by {deadline_str} so I can schedule promptly.\n"
    )


def clarification_email(question: str) -> str:
    return (
        "Quick clarification so I don’t schedule the wrong time:\n\n"
        f"{question}\n\n"
        "You can reply in either way:\n\n"
        "A) Specific time slot (preferred)\n"
        "MM/DD: start–end (timezone)\n"
        "Example: 02/11: 1pm–2pm ET\n\n"
        "B) Flexible constraints (also OK)\n"
        "- “Any afternoon Mon–Tue next week”\n"
        "- “Any time after 3pm on Wednesday”\n"
        "- “Any 30 min slot Tue–Thu between 10am–4pm PT”\n"
    )


def scheduled_email(start_str: str, end_str: str, tz_name: str, rationale: str) -> str:
    return (
        "Thanks everyone — I’ve scheduled the meeting for:\n\n"
        f"{start_str} – {end_str} ({tz_name})\n\n"
        f"Rationale: {rationale}\n"
    )


def no_overlap_email() -> str:
    return (
        "I couldn’t find any overlapping availability across everyone’s replies.\n\n"
        "Could each of you share a few additional time windows (same format as before), "
        "and I’ll try again?\n"
    )