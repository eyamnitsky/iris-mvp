from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple

from .models import MeetingThread, Participant
from .coordinator import IrisCoordinator
from .types import OutboundMessage, SchedulePlan


@dataclass(frozen=True)
class InboundEmail:
    thread_id: str
    from_email: str
    subject: str
    body_text: str
    is_new_request: bool


class IrisCoordinationHandler:
    """
    Pure domain orchestrator.
    NO AWS, NO SES, NO DynamoDB, NO entrypoints imports.
    """

    def __init__(self, store):
        # store must implement get(thread_id) and put(thread)
        self.store = store
        self.coordinator = IrisCoordinator()

    def handle(
        self, inbound: InboundEmail
    ) -> Tuple[List[OutboundMessage], Optional[SchedulePlan]]:

        outbound: List[OutboundMessage] = []
        schedule_plan: Optional[SchedulePlan] = None

        thread = self.store.get(inbound.thread_id)

        # --- New coordination request ---
        if inbound.is_new_request:
            if thread is None:
                raise RuntimeError(
                    "Coordination handler called with is_new_request=True but no thread exists"
                )

            outbound.append(
                self.coordinator.start_thread(thread)
            )
            self.store.put(thread)
            return outbound, None

        # --- Existing thread reply ---
        if thread is None:
            # Defensive: nothing to do
            return [], None

        # Ingest participant response
        outbound.extend(
            self.coordinator.ingest_participant_reply(
                thread,
                inbound.from_email,
                inbound.body_text,
            )
        )

        # Attempt scheduling
        plan, followups = self.coordinator.try_schedule(thread)
        outbound.extend(followups)

        if plan is not None:
            schedule_plan = plan

        self.store.put(thread)
        return outbound, schedule_plan