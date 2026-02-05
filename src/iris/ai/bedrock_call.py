import time
from typing import Any, Dict, Tuple

from .clients import bedrock_client
from .config import MODEL_ID, INFERENCE_CONFIG
from .text_normalize import clean_email_text, normalize_slang
from .prompt import build_prompt
from .parse_utils import extract_text_from_converse, parse_json_strict
from .validate import validate_result

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
