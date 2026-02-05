from __future__ import annotations

import re
from typing import Any, Dict, List

from .ddb import ensure_schema_loaded, PK_ATTR, SK_ATTR
from .config import DDB_SK_VALUE
from .serialization import ddb_clean, ddb_sanitize


def _normalize_message_id(value: str) -> str:
    return value.replace("\n", "").replace("\r", "").strip().strip("<>").strip()


def extract_message_ids(eml: dict) -> List[str]:
    """
    Return candidate message IDs in this order:
      1) Message-Id
      2) In-Reply-To
      3) All Message-Ids in References (every <...>)
    """
    ids: List[str] = []

    def _add(value: str) -> None:
        norm = _normalize_message_id(value)
        if not norm:
            return
        if norm not in ids:
            ids.append(norm)

    mid = eml.get("Message-Id") or eml.get("Message-ID") or ""
    if mid:
        _add(str(mid))

    irt = eml.get("In-Reply-To") or ""
    if irt:
        _add(str(irt))

    refs = str(eml.get("References") or "")
    for m in re.findall(r"<([^>]+)>", refs):
        _add(m)

    return ids


def _alias_key(message_id: str) -> Dict[str, Any]:
    ensure_schema_loaded()
    key: Dict[str, Any] = {PK_ATTR: f"alias::{message_id}"}  # type: ignore[index]
    if SK_ATTR:
        key[SK_ATTR] = DDB_SK_VALUE
    return key


def resolve_thread_id(eml: dict, ses_message_id: str, table) -> str:
    """
    Resolve a canonical thread_id by checking alias records for any candidate IDs.
    If no alias exists, use Message-Id (if present) else SES message_id.
    """
    candidates = extract_message_ids(eml)
    if ses_message_id:
        ses_norm = _normalize_message_id(str(ses_message_id))
        if ses_norm and ses_norm not in candidates:
            candidates.append(ses_norm)

    for mid in candidates:
        item = table.get_item(Key=_alias_key(mid)).get("Item")
        if not item:
            continue
        if item.get("record_type") == "THREAD_ALIAS" and item.get("thread_id"):
            return item["thread_id"]

    canonical_id = candidates[0] if candidates else _normalize_message_id(str(ses_message_id))
    return f"thread#{canonical_id}"


def upsert_thread_aliases(table, candidates: List[str], thread_id: str) -> None:
    """
    Store alias records for all candidate message IDs.
    """
    for mid in candidates:
        item = _alias_key(mid)
        item.update(
            {
                "record_type": "THREAD_ALIAS",
                "alias": mid,
                "thread_id": thread_id,
            }
        )
        table.put_item(Item=ddb_clean(ddb_sanitize(item)))
