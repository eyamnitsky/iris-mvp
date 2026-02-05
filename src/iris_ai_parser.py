"""
Backwards-compatible shim.

The implementation has moved into iris.ai.public; this file keeps existing imports stable:
    from iris_ai_parser import parse_email
"""
from iris.ai.public import parse_email, lambda_handler  # noqa: F401
