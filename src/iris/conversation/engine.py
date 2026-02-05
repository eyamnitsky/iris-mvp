from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from ..infra import ddb as ddb_mod
from ..infra.config import DDB_SK_VALUE
from ..infra.serialization import ddb_clean, ddb_sanitize, to_json_safe

from .context import IrisContext, ConversationState, Intent
from .parsing import infer_intent
from .rules import missing_fields, is_ready
from .formatting import ask_for_missing, confirm_summary


@dataclass
class ThreadState:
    """Lightweight thread state returned to the Lambda handler."""
    thread_id: str
    state: str
    intent: str
    timezone: str
    last_candidate: Optional[dict] = None


@dataclass
class Decision:
    """Decision returned to the Lambda handler."""
    action: str  # "ignore" | "clarify" | "schedule"
    reply_text: Optional[str] = None
    time_kind: Optional[str] = None  # "candidate" | None
    chosen_candidate: Optional[dict] = None


def handle_incoming_message(ctx: IrisContext, text: str):
    """Pure in-memory state machine (kept for unit tests / future evolution)."""
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


def _key_for_thread(thread_id: str) -> Dict[str, Any]:
    """Create a DynamoDB key for a thread item using the table's PK/SK schema."""
    ddb_mod.ensure_schema_loaded()
    pk_attr = ddb_mod.PK_ATTR
    sk_attr = ddb_mod.SK_ATTR
    if not pk_attr:
        raise RuntimeError("DDB schema not loaded (missing PK_ATTR)")
    key: Dict[str, Any] = {pk_attr: thread_id}
    if sk_attr:
        key[sk_attr] = DDB_SK_VALUE
    return key


def _now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def _ai_to_thread_intent(ai_intent: Optional[str]) -> str:
    if not ai_intent:
        return "OTHER"
    up = str(ai_intent).upper()
    if up in {"NEW_REQUEST", "AVAILABILITY", "CONFIRMATION", "DECLINE", "OTHER"}:
        return up
    return "OTHER"


def process_incoming_email(
    *,
    table,
    thread_id: str,
    message_id: str,
    body_text: str,
    timezone_default: str,
    ai_parsed: Optional[dict],
) -> Tuple[ThreadState, Decision]:
    """
    Thread-scoped wrapper used by src/iris/entrypoints/handler.py.

    Returns (thread_state, decision).

    - Stores thread state in IrisThreadsTable using PK=thread_id (e.g., "thread#<root>")
      and SK=DDB_SK_VALUE if the table has a sort key.
    - Minimal logic:
        * If candidates exist -> schedule using best-by-confidence
        * Else -> clarify (use AI's clarifying_question)
    """
    key = _key_for_thread(thread_id)
    existing = table.get_item(Key=key).get("Item") or {}

    tz = (
        (ai_parsed or {}).get("timezone")
        or existing.get("timezone")
        or timezone_default
        or "America/New_York"
    )

    ai_intent = _ai_to_thread_intent((ai_parsed or {}).get("intent"))
    state = existing.get("state") or ConversationState.INFO_GATHERING.value

    candidates = []
    if isinstance((ai_parsed or {}).get("candidates"), list):
        candidates = (ai_parsed or {}).get("candidates") or []

    clar_q = (ai_parsed or {}).get("clarifying_question") if isinstance(ai_parsed, dict) else None
    if not isinstance(clar_q, str) or not clar_q.strip():
        clar_q = "What day and time would you like to schedule this meeting for?"

    last_candidate = None

    if candidates:
        # Choose best candidate by confidence (fallback to first)
        best = None
        best_conf = -1.0
        for c in candidates:
            if not isinstance(c, dict):
                continue
            try:
                conf_f = float(c.get("confidence", 0.0))
            except Exception:
                conf_f = 0.0
            if conf_f > best_conf:
                best_conf = conf_f
                best = c

        chosen = best or candidates[0]
        last_candidate = chosen if isinstance(chosen, dict) else None

        decision = Decision(
            action="schedule",
            time_kind="candidate",
            chosen_candidate=last_candidate,
        )
        state = ConversationState.EXECUTION.value
    else:
        decision = Decision(action="clarify", reply_text=clar_q)
        state = ConversationState.CLARIFICATION_LOOP.value

    # Persist thread state
    thread_item = dict(key)
    thread_item.update(
        {
            "record_type": "THREAD",
            "updated_at": _now_iso(),
            "state": state,
            "intent": ai_intent,
            "timezone": tz,
            "last_message_id": message_id,
            "last_candidate": last_candidate or {},
            "last_ai_json": json.dumps(to_json_safe(ai_parsed)) if ai_parsed else "{}",
        }
    )
    table.put_item(Item=ddb_clean(ddb_sanitize(thread_item)))

    thread_state = ThreadState(
        thread_id=thread_id,
        state=state,
        intent=ai_intent,
        timezone=tz,
        last_candidate=last_candidate,
    )
    return thread_state, decision
