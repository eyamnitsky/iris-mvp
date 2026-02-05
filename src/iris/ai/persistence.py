from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from botocore.exceptions import ClientError

from .config import PK_NAME, SK_NAME, STATE_VALUE
from ..infra.serialization import to_ddb_safe

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

    expr = [
        "SET #st = :st",
        "#lpm = :lpm",
        "#ua = :ua",
        "#intent = :intent",
        "#needs = :needs",
        "#cq = :cq",
        "#tz = :tz",
        "#cands = :cands",
    ]
    names = {
        "#st": "state",
        "#lpm": "last_processed_message_id",
        "#ua": "updated_at",
        "#intent": "intent",
        "#needs": "needs_clarification",
        "#cq": "clarifying_question",
        "#tz": "timezone",
        "#cands": "candidates",
    }
    values = {
        ":st": STATE_VALUE,
        ":lpm": message_id,
        ":ua": now_iso,
        ":intent": parsed["intent"],
        ":needs": parsed["needs_clarification"],
        ":cq": parsed["clarifying_question"],
        ":tz": parsed["timezone"],
        ":cands": parsed["candidates"],
    }

    table.update_item(
        Key=ddb_key(thread_id),
        UpdateExpression=" ".join(expr),
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=to_ddb_safe(values),
    )
    print("DDB_UPSERT_OK:", thread_id, message_id)
