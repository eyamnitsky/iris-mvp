import os
import boto3
from .config import BEDROCK_REGION, BOTO_CONFIG

def bedrock_client():
    return boto3.client("bedrock-runtime", region_name=BEDROCK_REGION, config=BOTO_CONFIG)

def dynamodb_resource():
    return boto3.resource("dynamodb", region_name=os.environ.get("AWS_REGION", BEDROCK_REGION))
