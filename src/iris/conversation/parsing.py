from .context import Intent

def infer_intent(text: str) -> Intent:
    t = text.lower()
    if "reschedule" in t:
        return Intent.RESCHEDULE
    if "schedule" in t or "meeting" in t:
        return Intent.SCHEDULE
    return Intent.UNKNOWN
