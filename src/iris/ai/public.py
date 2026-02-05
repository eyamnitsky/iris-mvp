import time
from typing import Dict, Any

from .bedrock_call import call_nova_parser
from .validate import validate_result
from .config import DEFAULT_TZ, CASES_TABLE
from .clients import dynamodb_resource
from .persistence import ddb_upsert_case

def parse_email(event: dict) -> dict:
    """
    Backwards-compatible API used by app.py.
    Expected event keys:
      - body_text (str)
      - thread_id (str) optional
      - message_id (str) optional
      - timezone_default (str) optional
    """
    body_text = event.get("body_text", "") or ""
    thread_id = event.get("thread_id", "thread-smoketest")
    message_id = event.get("message_id", f"msg-{int(time.time())}")
    tz_default = event.get("timezone_default", DEFAULT_TZ)

    try:
        raw, parsed = call_nova_parser(body_text, tz_default)

        if CASES_TABLE:
            table = dynamodb_resource().Table(CASES_TABLE)
            ddb_upsert_case(table, thread_id, message_id, parsed)

        return {
            "ok": True,
            "thread_id": thread_id,
            "message_id": message_id,
            "parsed": parsed,
            "raw": raw,
        }
    except Exception as e:
        fallback = validate_result({}, tz_default)
        return {
            "ok": False,
            "error": str(e),
            "thread_id": thread_id,
            "message_id": message_id,
            "parsed": fallback,
        }


def lambda_handler(event, context):
    return parse_email(event)
