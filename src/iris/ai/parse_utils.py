import json
import re
from typing import Any, Dict

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
