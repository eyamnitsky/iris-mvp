from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional

class ConversationState(str, Enum):
    INTENT_DETECTION = "INTENT_DETECTION"
    INFO_GATHERING = "INFO_GATHERING"
    CLARIFICATION_LOOP = "CLARIFICATION_LOOP"
    CONFIRMATION_CHECK = "CONFIRMATION_CHECK"
    EXECUTION = "EXECUTION"

class Intent(str, Enum):
    UNKNOWN = "unknown"
    SCHEDULE = "schedule"
    RESCHEDULE = "reschedule"

@dataclass
class TimeSpec:
    value: str
    timezone: str

@dataclass
class WorkingMemory:
    intent: Intent = Intent.UNKNOWN
    participants: List[str] = field(default_factory=list)
    time: Optional[TimeSpec] = None
    duration_minutes: int = 30
    subject: Optional[str] = None

@dataclass
class IrisContext:
    state: ConversationState = ConversationState.INTENT_DETECTION
    memory: WorkingMemory = field(default_factory=WorkingMemory)
