from .context import IrisContext

def ask_for_missing(field: str) -> str:
    if field == "participants":
        return "Who should be in the meeting?"
    if field == "time":
        return "When should it be scheduled?"
    return "Can you provide more details?"

def confirm_summary(ctx: IrisContext) -> str:
    return f"I am ready to schedule the meeting with {', '.join(ctx.memory.participants)}."
