from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timedelta, timezone
from typing import Optional

from botocore.exceptions import ClientError

from .aws_clients import scheduler as _scheduler
from .config import (
    REMINDER_LAMBDA_ARN,
    REMINDER_DELAY_SECONDS,
    SCHEDULER_GROUP_NAME,
    SCHEDULER_ROLE_ARN,
)


_SCHED_NAME_RE = re.compile(r"[^A-Za-z0-9_-]+")


def _sanitize_schedule_name(name: str) -> str:
    clean = _SCHED_NAME_RE.sub("-", name).strip("-")
    if not clean:
        clean = "reminder"
    if len(clean) > 64:
        digest = hashlib.sha1(clean.encode("utf-8")).hexdigest()[:10]
        clean = f"{clean[:53]}-{digest}"
    return clean


def reminder_schedule_name(thread_id: str) -> str:
    return _sanitize_schedule_name(f"reminder-{thread_id}")


def ensure_reminder_schedule(thread_id: str) -> Optional[str]:
    if not REMINDER_LAMBDA_ARN or not SCHEDULER_ROLE_ARN:
        print("[reminder] missing REMINDER_LAMBDA_ARN or SCHEDULER_ROLE_ARN")
        return None

    name = reminder_schedule_name(thread_id)
    group = SCHEDULER_GROUP_NAME or "default"
    client = _scheduler()

    print(f"[reminder] schedule attempt name={name} group={group}")

    try:
        client.get_schedule(Name=name, GroupName=group)
        print(f"[reminder] schedule exists name={name}")
        return name
    except client.exceptions.ResourceNotFoundException:
        pass
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code != "ResourceNotFoundException":
            print(f"[reminder] get_schedule failed name={name} err={repr(e)}")
            return None

    fire_at = datetime.now(timezone.utc) + timedelta(seconds=REMINDER_DELAY_SECONDS)
    fire_at_str = fire_at.strftime("%Y-%m-%dT%H:%M:%S")

    try:
        client.create_schedule(
            Name=name,
            GroupName=group,
            ScheduleExpression=f"at({fire_at_str})",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target={
                "Arn": REMINDER_LAMBDA_ARN,
                "RoleArn": SCHEDULER_ROLE_ARN,
                "Input": json.dumps({"thread_id": thread_id}),
            },
        )
        print(f"[reminder] schedule created name={name} at={fire_at_str}")
        return name
    except client.exceptions.ConflictException:
        print(f"[reminder] schedule already exists name={name}")
        return name
    except ClientError as e:
        print(f"[reminder] create_schedule failed name={name} err={repr(e)}")
        return None
