from __future__ import annotations

import json
from typing import Optional, List
from email.utils import getaddresses
import email
from email import policy

from ..infra.serialization import to_json_safe
def flatten_emails(header_value: Optional[str]) -> List[str]:
    if not header_value:
        return []
    return [addr.lower() for _, addr in getaddresses([header_value]) if addr]


def dedupe(seq: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for x in seq:
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out


def safe_json(obj) -> str:
    try:
        return json.dumps(to_json_safe(obj), default=str)
    except Exception:
        return "<unserializable>"


def extract_plaintext_body(eml: email.message.EmailMessage) -> str:
    body_text = ""
    if eml.is_multipart():
        for part in eml.walk():
            if part.get_content_type() == "text/plain":
                body_text = part.get_content()
                break
    else:
        body_text = eml.get_content()
    return body_text or ""


def parse_eml(raw_bytes: bytes) -> email.message.EmailMessage:
    return email.message_from_bytes(raw_bytes, policy=policy.default)
