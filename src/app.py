"""
Thin Lambda entrypoint.

Previous versions contained all logic in this file. It now delegates to iris.entrypoints.handler,
keeping the Lambda handler name stable.
"""
from iris.entrypoints.handler import lambda_handler  # noqa: F401
