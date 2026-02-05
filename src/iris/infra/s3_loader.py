from __future__ import annotations

from typing import Tuple, Optional, List

from .aws_clients import s3 as _s3
from ..email.email_utils import dedupe

def load_email_bytes_from_s3(bucket: str, message_id: str, receipt: dict) -> Tuple[bytes, str]:
    candidate_keys: List[str] = []

    action = (receipt or {}).get("action", {}) or {}
    if isinstance(action, dict):
        if action.get("objectKey"):
            candidate_keys.append(action["objectKey"])
        if action.get("key"):
            candidate_keys.append(action["key"])

    candidate_keys.append(f"raw/{message_id}")
    candidate_keys.append(message_id)

    last_err: Optional[Exception] = None
    for k in dedupe(candidate_keys):
        try:
            print(f"[s3] trying key={k}")
            resp = _s3().get_object(Bucket=bucket, Key=k)
            data = resp["Body"].read()
            print(f"[s3] loaded key={k} bytes={len(data)}")
            return data, k
        except Exception as e:
            last_err = e

    raise last_err if last_err else RuntimeError("Failed to load email from S3")
