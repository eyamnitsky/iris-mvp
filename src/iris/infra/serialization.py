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
