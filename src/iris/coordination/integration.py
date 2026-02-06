from __future__ import annotations

from typing import List, Dict
from dataclasses import dataclass

from .handler import IrisCoordinationHandler, InboundEmail
from .models import MeetingThread, Participant


def build_participants(from_email: str, to_emails: List[str], cc_emails: List[str], iris_email: str) -> Dict[str, Participant]:
    # Include sender + all recipients, minus Iris
    all_people = []
    all_people.extend([from_email])
    all_people.extend(to_emails)
    all_people.extend(cc_emails)

    uniq = []
    seen = set()
    for e in all_people:
        el = (e or "").strip().lower()
        if not el or el == iris_email.lower():
            continue
        if el not in seen:
            seen.add(el)
            uniq.append(e)

    return {e: Participant(email=e) for e in uniq}


def looks_like_coordination_request(ai_parsed: dict | None, body_text: str, participant_count: int) -> bool:
    # If your parser emits intent, prefer that. Otherwise fallback to heuristics.
    if ai_parsed and ai_parsed.get("intent") in ("COORDINATE_MEETING", "MULTI_PARTICIPANT_SCHEDULING", "NEW_REQUEST"):
        # NEW_REQUEST is broad; require multiple participants to avoid hijacking 1:1 scheduling.
        return participant_count >= 2

    lowered = (body_text or "").lower()
    keywords = ("coordinate", "find a time", "schedule us", "schedule a time", "availability")
    return participant_count >= 2 and any(k in lowered for k in keywords)


def handle_coordination(
    store,
    thread_id: str,
    message_id: str,
    from_email: str,
    to_emails: List[str],
    cc_emails: List[str],
    subject: str,
    body_text: str,
    timezone: str,
    ai_parsed: dict | None,
) -> tuple[bool, list, object | None]:
    """
    Returns: (handled, outbound_messages, schedule_plan)
    """
    existing = store.get(thread_id)

    participants = build_participants(from_email, to_emails, cc_emails, iris_email="")  # iris filtered earlier in caller
    participant_count = len(participants)

    is_new_request = existing is None and looks_like_coordination_request(ai_parsed, body_text, participant_count)

    if is_new_request:
        thread = MeetingThread(
            thread_id=thread_id,
            organizer_email=from_email,
            participants=participants,
            timezone=timezone,
            subject=subject,
        )
        store.put(thread)
    else:
        thread = existing

    if thread is None:
        return False, [], None

    handler = IrisCoordinationHandler(store)

    inbound = InboundEmail(
        thread_id=thread_id,
        from_email=from_email,
        to_emails=to_emails,
        cc_emails=cc_emails,
        subject=subject,
        body_text=body_text,
        is_new_request=is_new_request,
        ai_parsed=ai_parsed,
    )

    outbound, plan = handler.handle(inbound)
    return True, outbound, plan
