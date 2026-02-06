from __future__ import annotations

import email
from email import policy
from typing import Optional, List
from datetime import datetime
from zoneinfo import ZoneInfo
from email.utils import formataddr

from ..infra.config import TIMEZONE

DISPLAY_NAME = "Iris (Liazon)"

def build_ics(
    subject: str,
    start: datetime,
    end: datetime,
    organizer: str,
    attendees: List[str],
    uid: str,
    description: Optional[str] = None,
    location: Optional[str] = None,
    url: Optional[str] = None,
) -> str:
    dtstamp = datetime.now(tz=ZoneInfo("UTC")).strftime("%Y%m%dT%H%M%SZ")

    def fmt(dt: datetime) -> str:
        return dt.strftime("%Y%m%dT%H%M%S")

    lines = [
        "BEGIN:VCALENDAR",
        "PRODID:-//Iris MVP//EN",
        "VERSION:2.0",
        "CALSCALE:GREGORIAN",
        "METHOD:REQUEST",
        "BEGIN:VEVENT",
        f"UID:{uid}",
        f"DTSTAMP:{dtstamp}",
        f"SUMMARY:{subject}",
        f"DTSTART;TZID={TIMEZONE}:{fmt(start)}",
        f"DTEND;TZID={TIMEZONE}:{fmt(end)}",
        f"ORGANIZER:mailto:{organizer}",
    ]

    for a in attendees:
        lines.append(f"ATTENDEE;CN={a};RSVP=TRUE:mailto:{a}")

    if description:
        lines.append(f"DESCRIPTION:{description}")
    if location:
        lines.append(f"LOCATION:{location}")
    if url:
        lines.append(f"URL:{url}")

    lines += ["END:VEVENT", "END:VCALENDAR", ""]
    return "\r\n".join(lines)


def build_raw_mime_text_reply(
    subject: str,
    text_body: str,
    from_addr: str,
    to_addrs: List[str],
    in_reply_to: Optional[str],
    references: Optional[str],
) -> bytes:
    msg = email.message.EmailMessage()
    msg["Subject"] = subject
    msg["From"] = formataddr((DISPLAY_NAME, from_addr))
    msg["To"] = ", ".join(to_addrs)
    if in_reply_to:
        msg["In-Reply-To"] = in_reply_to
    if references:
        msg["References"] = references
    msg.set_content(text_body)
    return msg.as_bytes(policy=policy.SMTP)


def build_raw_mime_reply_with_ics(
    subject: str,
    text_body: str,
    ics_body: str,
    from_addr: str,
    to_addrs: List[str],
    in_reply_to: Optional[str],
    references: Optional[str],
) -> bytes:
    msg = email.message.EmailMessage()
    msg["Subject"] = subject
    msg["From"] = formataddr((DISPLAY_NAME, from_addr))
    msg["To"] = ", ".join(to_addrs)
    if in_reply_to:
        msg["In-Reply-To"] = in_reply_to
    if references:
        msg["References"] = references

    msg.set_content(text_body)
    msg.add_attachment(
        ics_body.encode("utf-8"),
        maintype="text",
        subtype="calendar",
        filename="invite.ics",
        params={"method": "REQUEST"},
    )
    return msg.as_bytes(policy=policy.SMTP)
