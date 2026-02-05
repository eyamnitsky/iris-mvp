import os
import json
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


# -----------------------------
# Env / Config
# -----------------------------
MODEL_ID = os.environ.get("MODEL_ID", "amazon.nova-lite-v1:0")
BEDROCK_REGION = os.environ.get("BEDROCK_REGION", os.environ.get("AWS_REGION", "us-east-1"))
DEFAULT_TZ = os.environ.get("DEFAULT_TZ", "America/New_York")

CASES_TABLE = os.environ.get("CASES_TABLE")  # optional
PK_NAME = os.environ.get("PK_NAME", "thread_id")
SK_NAME = os.environ.get("SK_NAME")  # optional; set only if your table uses a sort key
STATE_VALUE = os.environ.get("STATE_VALUE", "AI_PARSED")

INFERENCE_CONFIG = {
    "temperature": 0.2,
    "topP": 0.9,
    "maxTokens": 900
}

BOTO_CONFIG = Config(
    connect_timeout=5,
    read_timeout=25,
    retries={"max_attempts": 2, "mode": "standard"},
)


# -----------------------------
# AWS clients
# -----------------------------
def bedrock_client():
    return boto3.client("bedrock-runtime", region_name=BEDROCK_REGION, config=BOTO_CONFIG)


def dynamodb_resource():
    return boto3.resource("dynamodb", region_name=os.environ.get("AWS_REGION", BEDROCK_REGION))


# -----------------------------
# Text cleanup + normalization
# -----------------------------
def clean_email_text(text: str) -> str:
    if not text:
        return ""
    lines = []
    for line in text.splitlines():
        if line.strip().startswith(">"):
            continue
        if re.match(r"^\s*On .* wrote:\s*$", line):
            break
        lines.append(line)
    cleaned = "\n".join(lines).strip()
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned[:6000]


def normalize_slang(text: str) -> str:
    """
    Lightweight normalization to help smaller models.
    Also covers "around 2" already, but this helps "2ish"/"2-ish"/"2:30ish".
    """
    if not text:
        return ""

    t = text

    # 2ish / 2-ish -> around 2
    t = re.sub(r"\b(\d{1,2})\s*-\s*ish\b", r"around \1", t, flags=re.IGNORECASE)
    t = re.sub(r"\b(\d{1,2})\s*ish\b", r"around \1", t, flags=re.IGNORECASE)

    # 2pmish / 2pm-ish -> around 2pm
    t = re.sub(r"\b(\d{1,2}\s*(?:am|pm))\s*-\s*ish\b", r"around \1", t, flags=re.IGNORECASE)
    t = re.sub(r"\b(\d{1,2}\s*(?:am|pm))\s*ish\b", r"around \1", t, flags=re.IGNORECASE)

    # 2:30ish -> around 2:30
    t = re.sub(r"\b(\d{1,2}:\d{2})\s*ish\b", r"around \1", t, flags=re.IGNORECASE)

    # noonish
    t = re.sub(r"\bnoon\s*ish\b", "around noon", t, flags=re.IGNORECASE)

    return t


# -----------------------------
# Prompt (few-shot)
# -----------------------------
def build_prompt(body_text: str, tz_default: str) -> str:
    today_iso = datetime.now(timezone.utc).astimezone().date().isoformat()

    return f"""
You are Iris, an email scheduling assistant.
Extract intent and time information from an email.
Return ONLY valid JSON. No prose. No markdown. No backticks. No extra keys.

Interpret casual time slang:
- "2ish" = around 2:00 PM (fuzzy)
- "around 2" = around 2:00 PM (fuzzy)
- "noonish" = around 12:00 PM (fuzzy)
- "afternoon" = 1:00 PM–5:00 PM
- "morning" = 9:00 AM–12:00 PM
- "evening" = 5:00 PM–8:00 PM

Rules:
- If information is missing or ambiguous, set needs_clarification=true and ask ONE short follow-up question.
- If no time is provided, candidates must be [].
- Confidence must be between 0.0 and 1.0.

--------------------
EXAMPLES (follow exactly)
--------------------

Email: "Iris, schedule a 30 minute sync with Bob and Alice next week."
Output:
{{
  "intent": "NEW_REQUEST",
  "needs_clarification": true,
  "clarifying_question": "What day and time should I schedule the meeting for?",
  "timezone": "{tz_default}",
  "candidates": []
}}

Email: "Tuesday around 2ish works."
Output:
{{
  "intent": "AVAILABILITY",
  "needs_clarification": false,
  "clarifying_question": "",
  "timezone": "{tz_default}",
  "candidates": [
    {{
      "start_local": "Tuesday 2:00 PM",
      "end_local": "Tuesday 2:30 PM",
      "confidence": 0.75,
      "source_text": "around 2ish"
    }}
  ]
}}

Email: "Tuesday around 2 works."
Output:
{{
  "intent": "AVAILABILITY",
  "needs_clarification": false,
  "clarifying_question": "",
  "timezone": "{tz_default}",
  "candidates": [
    {{
      "start_local": "Tuesday 2:00 PM",
      "end_local": "Tuesday 2:30 PM",
      "confidence": 0.7,
      "source_text": "around 2"
    }}
  ]
}}

Email: "Tuesday afternoon works."
Output:
{{
  "intent": "AVAILABILITY",
  "needs_clarification": true,
  "clarifying_question": "What time Tuesday afternoon works best for you (e.g., 1pm, 2pm, or 3pm)?",
  "timezone": "{tz_default}",
  "candidates": [
    {{
      "start_local": "Tuesday 1:00 PM",
      "end_local": "Tuesday 5:00 PM",
      "confidence": 0.4,
      "source_text": "Tuesday afternoon"
    }}
  ]
}}

Email: "2pm Tuesday works for me."
Output:
{{
  "intent": "CONFIRMATION",
  "needs_clarification": false,
  "clarifying_question": "",
  "timezone": "{tz_default}",
  "candidates": [
    {{
      "start_local": "Tuesday 2:00 PM",
      "end_local": "Tuesday 2:30 PM",
      "confidence": 0.9,
      "source_text": "2pm Tuesday"
    }}
  ]
}}

--------------------
TASK
--------------------

Today is: {today_iso}
Default timezone: {tz_default}

Now extract intent and time information from this email.

Email:
{body_text}
""".strip()


# -----------------------------
# Model call + parsing
# -----------------------------
def extract_text_from_converse(resp: Dict[str, Any]) -> str:
    parts = resp.get("output", {}).get("message", {}).get("content", [])
    out = []
    for p in parts:
        if isinstance(p, dict) and "text" in p:
            out.append(p["text"])
    return "".join(out).strip()


def parse_json_strict(text: str) -> Dict[str, Any]:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        m = re.search(r"\{.*\}", text, re.DOTALL)
        if not m:
            raise
        return json.loads(m.group(0))


def validate_result(obj: Any, tz_default: str) -> Dict[str, Any]:
    safe = {
        "intent": "OTHER",
        "needs_clarification": True,
        "clarifying_question": "What day and time would you like to schedule this meeting for?",
        "timezone": tz_default,
        "candidates": []
    }

    if not isinstance(obj, dict):
        return safe

    if obj.get("intent") not in {"NEW_REQUEST", "AVAILABILITY", "CONFIRMATION", "DECLINE", "OTHER"}:
        obj["intent"] = "OTHER"

    if not isinstance(obj.get("needs_clarification"), bool):
        obj["needs_clarification"] = True

    if not isinstance(obj.get("clarifying_question"), str):
        obj["clarifying_question"] = safe["clarifying_question"]

    if not isinstance(obj.get("timezone"), str) or not obj.get("timezone"):
        obj["timezone"] = tz_default

    candidates = obj.get("candidates")
    if not isinstance(candidates, list):
        candidates = []

    normalized = []
    for c in candidates:
        if not isinstance(c, dict):
            continue
        if "start_local" not in c or "end_local" not in c:
            continue

        start_local = c.get("start_local")
        end_local = c.get("end_local")
        source_text = c.get("source_text", "")
        conf = c.get("confidence", 0.0)

        if not isinstance(start_local, str) or not isinstance(end_local, str):
            continue

        try:
            conf_f = float(conf)
        except Exception:
            conf_f = 0.0

        normalized.append({
            "start_local": start_local[:80],
            "end_local": end_local[:80],
            "confidence": max(0.0, min(1.0, conf_f)),
            "source_text": str(source_text)[:200]
        })

    obj["candidates"] = normalized

    # --- FIX 2 (deterministic override) ---
    # If we have a concrete day + time, do not ask "what time" again.
    # Also treat "around 2" as concrete-enough IF the candidate includes a day and a plausible hour.
    day_re = re.compile(r"\b(Mon|Tues|Tue|Wed|Thu|Thurs|Fri|Sat|Sun)(day)?\b", re.IGNORECASE)
    # Matches times like "2", "2:30", "2pm", "2 PM", "14:00" (but we focus on 1-12 here)
    hour_re = re.compile(r"\b([1-9]|1[0-2])(?:\:\d{2})?\b")
    ampm_re = re.compile(r"\b(am|pm)\b", re.IGNORECASE)
    aroundish_re = re.compile(r"\b(around|about|approx|~|ish)\b", re.IGNORECASE)

    has_day_and_time = False
    for c in obj["candidates"]:
        start_local = c.get("start_local", "")
        src = c.get("source_text", "")
        text = f"{start_local} {src}"

        if not day_re.search(start_local):
            continue

        # If it has explicit AM/PM, definitely a concrete time.
        if ampm_re.search(text) and hour_re.search(text):
            has_day_and_time = True
            break

        # If it's "around/about/ish" + an hour (e.g., "around 2"), treat as concrete-enough.
        if aroundish_re.search(text) and hour_re.search(text):
            has_day_and_time = True
            break

    if has_day_and_time:
        obj["needs_clarification"] = False
        obj["clarifying_question"] = ""
    else:
        # If model said no clarification, keep question empty; otherwise keep its question
        if obj["needs_clarification"] is False:
            obj["clarifying_question"] = obj.get("clarifying_question", "") or ""

        # Safety: if no candidates and no clarification, force clarification
        if not obj["candidates"] and obj["needs_clarification"] is False:
            obj["needs_clarification"] = True
            obj["clarifying_question"] = safe["clarifying_question"]

    return obj


def call_nova_parser(email_text: str, tz_default: str) -> Tuple[str, Dict[str, Any]]:
    cleaned = clean_email_text(email_text)
    normalized = normalize_slang(cleaned)
    prompt = build_prompt(normalized, tz_default)

    client = bedrock_client()

    last_err = None
    for attempt in range(2):
        try:
            resp = client.converse(
                modelId=MODEL_ID,
                messages=[{"role": "user", "content": [{"text": prompt}]}],
                inferenceConfig=INFERENCE_CONFIG,
            )
            out_text = extract_text_from_converse(resp)
            parsed = validate_result(parse_json_strict(out_text), tz_default)
            return out_text, parsed
        except Exception as e:
            last_err = e
            time.sleep(0.4 * (attempt + 1))

    raise last_err


# -----------------------------
# DynamoDB persistence (optional)
# -----------------------------
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

    try:
        table.update_item(
            Key=ddb_key(thread_id),
            UpdateExpression=" ".join(expr),
            ExpressionAttributeNames=names,
            ExpressionAttributeValues=values,
        )
        print("DDB_UPSERT_OK:", thread_id, message_id)
    except ClientError as e:
        print("DDB_UPSERT_ERROR:", str(e))
        raise


# -----------------------------
# Lambda handler
# -----------------------------
def lambda_handler(event, context):
    """
    Test event example:
    {
      "thread_id":"thread-demo-001",
      "message_id":"msg-001",
      "body_text":"Tuesday around 2ish works for me."
    }
    """
    body_text = event.get("body_text", "") or "Tuesday around 2 works for me."
    thread_id = event.get("thread_id", "thread-smoketest")
    message_id = event.get("message_id", f"msg-{int(time.time())}")
    tz_default = event.get("timezone_default", DEFAULT_TZ)

    print("CONFIG:", {"BEDROCK_REGION": BEDROCK_REGION, "MODEL_ID": MODEL_ID, "CASES_TABLE": CASES_TABLE or ""})
    print("INPUT:", {"thread_id": thread_id, "message_id": message_id, "preview": body_text[:120]})

    try:
        raw, parsed = call_nova_parser(body_text, tz_default)
        print("NOVA_RAW_OUTPUT:", raw)
        print("PARSED_JSON:", json.dumps(parsed, indent=2))

        if CASES_TABLE:
            table = dynamodb_resource().Table(CASES_TABLE)
            ddb_upsert_case(table, thread_id, message_id, parsed)

        return {"ok": True, "thread_id": thread_id, "message_id": message_id, "parsed": parsed}

    except Exception as e:
        print("ERROR:", repr(e))
        fallback = validate_result({}, tz_default)
        return {"ok": False, "error": str(e), "thread_id": thread_id, "message_id": message_id, "parsed": fallback}
