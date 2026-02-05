from __future__ import annotations

from decimal import Decimal
from typing import Any


def to_ddb_safe(x: Any) -> Any:
    """Convert floats to Decimal recursively for DynamoDB compatibility."""
    if isinstance(x, float):
        return Decimal(str(x))
    if isinstance(x, dict):
        return {k: to_ddb_safe(v) for k, v in x.items()}
    if isinstance(x, list):
        return [to_ddb_safe(v) for v in x]
    if isinstance(x, tuple):
        return [to_ddb_safe(v) for v in x]
    return x


def to_json_safe(x: Any) -> Any:
    """Convert Decimal to JSON-safe types recursively."""
    if isinstance(x, Decimal):
        return float(x)
    if isinstance(x, dict):
        return {k: to_json_safe(v) for k, v in x.items()}
    if isinstance(x, list):
        return [to_json_safe(v) for v in x]
    if isinstance(x, tuple):
        return [to_json_safe(v) for v in x]
    return x


def ddb_clean(item: Any) -> Any:
    """
    Remove dict keys whose values are None or empty collections.
    Recurses into nested dicts/lists while preserving list ordering.
    """
    if isinstance(item, dict):
        cleaned = {}
        for k, v in item.items():
            v_clean = ddb_clean(v)
            if v_clean is None:
                continue
            if isinstance(v_clean, (dict, list, tuple, set)) and len(v_clean) == 0:
                continue
            cleaned[k] = v_clean
        return cleaned
    if isinstance(item, list):
        return [ddb_clean(v) for v in item]
    if isinstance(item, tuple):
        return [ddb_clean(v) for v in item]
    return item


def ddb_sanitize(item: Any) -> Any:
    """Backwards-compatible alias for DynamoDB sanitization."""
    return to_ddb_safe(item)
