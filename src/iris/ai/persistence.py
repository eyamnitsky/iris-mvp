from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from botocore.exceptions import ClientError

from .config import PK_NAME, SK_NAME, STATE_VALUE
from ..infra.serialization import ddb_clean, ddb_sanitize

def ddb_key(thread_id: str) -> Dict[str, Any]:
    key = {PK_NAME: thread_id}
    if SK_NAME:
        key[SK_NAME] = "CASE"
    return key


def ddb_get_case(table, thread_id: str) -> Optional[Dict[str, Any]]:
    try:
        resp = table.get_item(Key=ddb_key(thread_id), ConsistentRead=True)
        return resp.get("Item")
    except ClientError as e:
        print("DDB_GET_ERROR:", str(e))
        return None


def ddb_upsert_case(table, thread_id: str, message_id: str, parsed: Dict[str, Any]) -> None:
    now_iso = datetime.now(timezone.utc).isoformat()

    existing = ddb_get_case(table, thread_id)
    if existing and existing.get("last_processed_message_id") == message_id:
        print("IDEMPOTENT_SKIP:", message_id)
        return

    def _is_empty_value(value: Any) -> bool:
        if value is None:
            return True
        if isinstance(value, (list, dict, tuple, set)) and len(value) == 0:
            return True
        return False

    updates = [
        ("#st", ":st", "state", STATE_VALUE),
        ("#lpm", ":lpm", "last_processed_message_id", message_id),
        ("#ua", ":ua", "updated_at", now_iso),
        ("#intent", ":intent", "intent", parsed["intent"]),
        ("#needs", ":needs", "needs_clarification", parsed["needs_clarification"]),
        ("#cq", ":cq", "clarifying_question", parsed["clarifying_question"]),
        ("#tz", ":tz", "timezone", parsed["timezone"]),
        ("#cands", ":cands", "candidates", parsed["candidates"]),
    ]

    expr = ["SET"]
    names = {}
    values = {}
    for name_key, value_key, attr_name, value in updates:
        if _is_empty_value(value):
            continue
        names[name_key] = attr_name
        values[value_key] = value
        expr.append(f"{name_key} = {value_key}")

    table.update_item(
        Key=ddb_key(thread_id),
        UpdateExpression=" ".join(expr),
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=ddb_clean(ddb_sanitize(values)),
    )
    print("DDB_UPSERT_OK:", thread_id, message_id)
