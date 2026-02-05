from .context import IrisContext

def missing_fields(ctx: IrisContext):
    missing = []
    if not ctx.memory.participants:
        missing.append("participants")
    if not ctx.memory.time:
        missing.append("time")
    return missing

def is_ready(ctx: IrisContext) -> bool:
    return not missing_fields(ctx)
