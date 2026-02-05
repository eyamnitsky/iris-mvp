from __future__ import annotations

from typing import Optional, Dict, Any

from .aws_clients import ddb_client as _ddb_client
from .config import TABLE_NAME, DDB_SK_VALUE

PK_ATTR: Optional[str] = None
SK_ATTR: Optional[str] = None
SK_TYPE: Optional[str] = None  # 'S' | 'N' | 'B' or None


def ensure_schema_loaded() -> None:
    global PK_ATTR, SK_ATTR, SK_TYPE
    if PK_ATTR is not None:
        return
    if not TABLE_NAME:
        raise RuntimeError("TABLE_NAME is not set")

    desc = _ddb_client().describe_table(TableName=TABLE_NAME)["Table"]
    key_schema = desc.get("KeySchema", [])
    attr_defs = desc.get("AttributeDefinitions", [])

    print("[ddb] table=", TABLE_NAME)
    print("[ddb] KeySchema=", key_schema)
    print("[ddb] AttrDefs=", attr_defs)

    PK_ATTR = next((k["AttributeName"] for k in key_schema if k["KeyType"] == "HASH"), None)
    SK_ATTR = next((k["AttributeName"] for k in key_schema if k["KeyType"] == "RANGE"), None)
    if SK_ATTR:
        SK_TYPE = next((a.get("AttributeType") for a in attr_defs if a.get("AttributeName") == SK_ATTR), None)

    if not PK_ATTR:
        raise RuntimeError(f"Could not determine PK attribute for table {TABLE_NAME}")


def key_for_message(message_id: str) -> Dict[str, Any]:
    ensure_schema_loaded()
    key: Dict[str, Any] = {PK_ATTR: f"msg#{message_id}"}  # type: ignore[index]
    if SK_ATTR:
        key[SK_ATTR] = DDB_SK_VALUE
    return key
