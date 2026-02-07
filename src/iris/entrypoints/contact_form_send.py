import json
import os
import re
import boto3
from email.utils import parseaddr

SES_REGION = os.environ.get("SES_REGION", "us-east-1")
TO_EMAIL = os.environ["CONTACT_TO_EMAIL"]
FROM_EMAIL = os.environ["CONTACT_FROM_EMAIL"]
ALLOWED_ORIGINS = [o.strip() for o in os.environ.get("ALLOWED_ORIGINS", "").split(",") if o.strip()]

ses = boto3.client("ses", region_name=SES_REGION)

MAX_NAME = 120
MAX_EMAIL = 254
MAX_MESSAGE = 4000


def _resp(status, body, origin=None):
    headers = {"Content-Type": "application/json"}

    # CORS: reflect only allowed origins
    if origin and (not ALLOWED_ORIGINS or origin in ALLOWED_ORIGINS):
        headers.update(
            {
                "Access-Control-Allow-Origin": origin,
                "Vary": "Origin",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Methods": "OPTIONS,POST",
            }
        )

    return {"statusCode": status, "headers": headers, "body": json.dumps(body)}


def _method(event):
    # HTTP API v2
    m = (event.get("requestContext", {}).get("http", {}) or {}).get("method")
    if m:
        return m
    # REST API fallback
    return event.get("httpMethod")


def _origin(event):
    h = event.get("headers") or {}
    return h.get("origin") or h.get("Origin")


def _is_valid_email(addr: str) -> bool:
    if not addr or len(addr) > MAX_EMAIL:
        return False
    _, email = parseaddr(addr)
    if not email or "@" not in email:
        return False
    return bool(re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email))


def lambda_handler(event, context):
    origin = _origin(event)
    method = _method(event)

    # Preflight
    if method == "OPTIONS":
        return _resp(204, {}, origin=origin)

    # Parse JSON
    try:
        body = event.get("body") or ""
        if event.get("isBase64Encoded"):
            import base64

            body = base64.b64decode(body).decode("utf-8", errors="replace")
        data = json.loads(body)
    except Exception:
        return _resp(400, {"ok": False, "error": "Invalid JSON"}, origin=origin)

    name = (data.get("name") or "").strip()
    email = (data.get("email") or "").strip()
    message = (data.get("message") or "").strip()
    company = (data.get("company") or "").strip()

    # Honeypot: add hidden input "website" on the client; bots fill it
    website = (data.get("website") or "").strip()
    if website:
        return _resp(200, {"ok": True}, origin=origin)

    # Validation / limits
    if not name or len(name) > MAX_NAME:
        return _resp(400, {"ok": False, "error": "Invalid name"}, origin=origin)
    if not _is_valid_email(email):
        return _resp(400, {"ok": False, "error": "Invalid email"}, origin=origin)
    # Allow blank name; default to "Anonymous"
    if len(name) > MAX_NAME:
        return _resp(400, {"ok": False, "error": "Invalid name"}, origin=origin)
    if not name:
        name = "Anonymous"

    subject = f"Contact form: {name}"
    text_body = (
        "New contact form submission:\n\n"
        f"Name: {name}\n"
        f"Email: {email}\n"
        f"Company: {company}\n\n"
        "Message:\n"
        f"{message}\n"
    )

    try:
        ses.send_email(
            Source=FROM_EMAIL,
            Destination={"ToAddresses": [TO_EMAIL]},
            Message={
                "Subject": {"Data": subject, "Charset": "UTF-8"},
                "Body": {"Text": {"Data": text_body, "Charset": "UTF-8"}},
            },
            ReplyToAddresses=[email],
        )
    except Exception:
        return _resp(500, {"ok": False, "error": "Failed to send"}, origin=origin)

    return _resp(200, {"ok": True}, origin=origin)
