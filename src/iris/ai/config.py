import os
from botocore.config import Config

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
