import json
import os
import uuid
import urllib.parse

import boto3
import urllib3

http = urllib3.PoolManager()
secrets = boto3.client("secretsmanager")

TOKEN_URL = "https://oauth2.googleapis.com/token"
CAL_EVENTS_URL = "https://www.googleapis.com/calendar/v3/calendars/primary/events"


def _get_oauth_secret(secret_name: str) -> dict:
    resp = secrets.get_secret_value(SecretId=secret_name)
    return json.loads(resp["SecretString"])


def _refresh_access_token(client_id: str, client_secret: str, refresh_token: str) -> str:
    body = urllib.parse.urlencode(
        {
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        }
    ).encode("utf-8")

    resp = http.request(
        "POST",
        TOKEN_URL,
        body=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    data = json.loads(resp.data.decode("utf-8"))
    if resp.status >= 400:
        raise Exception(f"Token refresh failed ({resp.status}): {data}")
    return data["access_token"]


def create_meet_event(
    *,
    summary: str,
    start_rfc3339: str,
    end_rfc3339: str,
    attendees: list[str],
    timezone: str = "America/New_York",
    secret_name: str | None = None,
) -> dict:
    """
    Returns: {"event_id": "...", "meet_url": "...", "htmlLink": "..."}
    start_rfc3339 example: "2026-02-10T15:00:00-05:00"
    """
    secret_name = secret_name or os.environ.get("GOOGLE_OAUTH_SECRET_NAME", "liazon/iris/google_oauth")

    s = _get_oauth_secret(secret_name)
    access_token = _refresh_access_token(s["client_id"], s["client_secret"], s["refresh_token"])

    request_id = str(uuid.uuid4())

    payload = {
        "summary": summary,
        "start": {"dateTime": start_rfc3339, "timeZone": timezone},
        "end": {"dateTime": end_rfc3339, "timeZone": timezone},
        "attendees": [{"email": a} for a in attendees],
        "conferenceData": {
            "createRequest": {
                "requestId": request_id,
                "conferenceSolutionKey": {"type": "hangoutsMeet"},
            }
        },
    }

    url = f"{CAL_EVENTS_URL}?conferenceDataVersion=1"
    resp = http.request(
        "POST",
        url,
        body=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        },
    )
    data = json.loads(resp.data.decode("utf-8"))
    if resp.status >= 400:
        raise Exception(f"Event create failed ({resp.status}): {data}")

    return {
        "event_id": data.get("id"),
        "meet_url": data.get("hangoutLink"),
        "htmlLink": data.get("htmlLink"),
    }