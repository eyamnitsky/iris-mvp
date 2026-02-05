from __future__ import annotations

import boto3
from typing import Optional

from .config import AWS_REGION, TABLE_NAME

_s3 = None
_ses = None
_ddb = None
_ddb_client = None


def s3():
    global _s3
    if _s3 is None:
        _s3 = boto3.client("s3", region_name=AWS_REGION)
    return _s3


def ses():
    global _ses
    if _ses is None:
        _ses = boto3.client("ses", region_name=AWS_REGION)
    return _ses


def ddb():
    global _ddb
    if _ddb is None:
        _ddb = boto3.resource("dynamodb", region_name=AWS_REGION)
    return _ddb


def ddb_client():
    global _ddb_client
    if _ddb_client is None:
        _ddb_client = boto3.client("dynamodb", region_name=AWS_REGION)
    return _ddb_client


def table():
    if not TABLE_NAME:
        raise RuntimeError("TABLE_NAME is not set")
    return ddb().Table(TABLE_NAME)
