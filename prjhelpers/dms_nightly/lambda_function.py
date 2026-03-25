import os
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import boto3
from botocore.exceptions import ClientError


dms = boto3.client("dms")

TASK_ARN = os.environ["DMS_TASK_ARN"]
INITIAL_CDC_START_POSITION = os.environ.get("INITIAL_CDC_START_POSITION")
USE_COMMIT_TIME = os.environ.get("USE_COMMIT_TIME", "true").lower() == "true"


def _next_cutoff_utc(now_utc: datetime) -> datetime:
    """
    Return the next 1:00 AM America/New_York cutoff as a UTC datetime.
    """
    eastern = ZoneInfo("America/New_York")
    now_et = now_utc.astimezone(eastern)

    today_cutoff_et = now_et.replace(hour=1, minute=0, second=0, microsecond=0)

    if now_et < today_cutoff_et:
        next_cutoff_et = today_cutoff_et
    else:
        next_cutoff_et = today_cutoff_et + timedelta(days=1)

    return next_cutoff_et.astimezone(timezone.utc)


def _format_dms_stop_position(dt_utc: datetime) -> str:
    """
    Format the DMS CDC stop position using either commit_time or server_time.
    """
    prefix = "commit_time" if USE_COMMIT_TIME else "server_time"
    return f"{prefix}:{dt_utc.strftime('%Y-%m-%dT%H:%M:%S')}"


def lambda_handler(event, context):
    now_utc = datetime.now(timezone.utc)
    stop_utc = _next_cutoff_utc(now_utc)
    cdc_stop_position = _format_dms_stop_position(stop_utc)

    tasks = dms.describe_replication_tasks(
        Filters=[{"Name": "replication-task-arn", "Values": [TASK_ARN]}]
    ).get("ReplicationTasks", [])

    if not tasks:
        raise RuntimeError(f"DMS task not found: {TASK_ARN}")

    task = tasks[0]
    status = task.get("Status", "")
    recovery_checkpoint = task.get("RecoveryCheckpoint")
    migration_type = task.get("MigrationType")

    if migration_type != "cdc":
        raise RuntimeError(f"Expected CDC-only task, found migration type: {migration_type}")

    if status in {"running", "starting"}:
        return {
            "ok": True,
            "message": f"Task already {status}; no action taken.",
            "task_arn": TASK_ARN,
            "cdc_stop_position": cdc_stop_position,
        }

    params = {
        "ReplicationTaskArn": TASK_ARN,
        "CdcStopPosition": cdc_stop_position,
    }

    if not recovery_checkpoint:
        if not INITIAL_CDC_START_POSITION:
            raise RuntimeError(
                "First run requires INITIAL_CDC_START_POSITION because the task has no recovery checkpoint yet."
            )
        params["StartReplicationTaskType"] = "start-replication"
        params["CdcStartPosition"] = INITIAL_CDC_START_POSITION
    else:
        params["StartReplicationTaskType"] = "resume-processing"

    try:
        response = dms.start_replication_task(**params)
        started = response["ReplicationTask"]
        return {
            "ok": True,
            "task_arn": started["ReplicationTaskArn"],
            "status": started["Status"],
            "cdc_stop_position": cdc_stop_position,
            "start_type": params["StartReplicationTaskType"],
        }
    except ClientError as exc:
        raise RuntimeError(f"Failed to start DMS task: {exc}") from exc
