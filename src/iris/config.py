import os

# Lambda sets AWS_REGION automatically; local dev may rely on AWS_DEFAULT_REGION.
AWS_REGION = (
    os.environ.get("AWS_REGION")
    or os.environ.get("AWS_DEFAULT_REGION")
    or "us-east-1"
)

BUCKET_NAME = os.environ.get("BUCKET_NAME")
TABLE_NAME = os.environ.get("TABLE_NAME")

IRIS_EMAIL = os.environ.get("IRIS_EMAIL", "iris@liazon.cc").lower()
TIMEZONE = os.environ.get("TIMEZONE", "America/New_York")

DEFAULT_START_HOUR = int(os.environ.get("DEFAULT_START_HOUR", "13"))
DEFAULT_DURATION_MINUTES = int(os.environ.get("DEFAULT_DURATION_MINUTES", "30"))

DDB_SK_VALUE = os.environ.get("DDB_SK_VALUE", "STATE")


def require_env() -> None:
    missing = []
    if not BUCKET_NAME:
        missing.append("BUCKET_NAME")
    if not TABLE_NAME:
        missing.append("TABLE_NAME")
    if missing:
        raise RuntimeError("Missing required environment variables: " + ", ".join(missing))
