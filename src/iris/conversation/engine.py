from .context import IrisContext, ConversationState, Intent
from .parsing import infer_intent
from .rules import missing_fields, is_ready
from .formatting import ask_for_missing, confirm_summary

def handle_incoming_message(ctx: IrisContext, text: str):
    if ctx.state == ConversationState.INTENT_DETECTION:
        ctx.memory.intent = infer_intent(text)
        ctx.state = ConversationState.INFO_GATHERING

    missing = missing_fields(ctx)
    if missing:
        return ask_for_missing(missing[0]), ctx, None

    if is_ready(ctx):
        ctx.state = ConversationState.CONFIRMATION_CHECK
        return confirm_summary(ctx), ctx, "READY"

    return "Can you clarify?", ctx, None
