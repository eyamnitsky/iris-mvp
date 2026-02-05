from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional


@dataclass(frozen=True)
class OutboundMessage:
    to: List[str]
    subject: str
    body: str


@dataclass(frozen=True)
class SchedulePlan:
    start: datetime
    end: datetime
    rationale: Optional[str] = None