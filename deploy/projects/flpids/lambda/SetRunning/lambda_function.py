import os
from datetime import datetime, timezone
import json

import boto3


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def lambda_handler(event, context):
    """
    Step 1: set job status to RUNNING.

    Input:
      {
        "jobId": "uuid",
        "project": "FLPIDS-SCIMS",
        "table_name": "FSA-FileChecks"    # optional if TABLE_NAME env var set
      }

    Updates DynamoDB item (jobId, project):
      status = RUNNING
      startedAt = now (if not exists)
      updatedAt = now
    """
    debug = bool(event.get("debug", False))

    job_id = event.get("jobId")
    project = event.get("project")
    table_name = event.get("table_name") or os.environ.get("TABLE_NAME")

    if not job_id:
        raise ValueError("Missing required input: jobId")
    if not project:
        raise ValueError("Missing required input: project")
    if not table_name:
        raise ValueError("Missing required input: table_name (or env TABLE_NAME)")

    ddb = boto3.resource("dynamodb")
    table = ddb.Table(table_name)

    ts = _now_iso()

   

    resp = table.update_item(
        Key={"jobId": job_id, "project": project},
        UpdateExpression="SET #s = :running, #u = :u, #started = if_not_exists(#started, :u)",
        ExpressionAttributeNames={
            "#s": "status",
            "#u": "updatedAt",
            "#started": "startedAt",
        },
        ExpressionAttributeValues={
            ":running": "RUNNING",
            ":u": ts,
        },
        ReturnValues="ALL_NEW",
    )

    attrs = resp.get("Attributes") or {}
    event = attrs.get("event", {}) or {}

    return {
        "jobId": job_id,
        "project": project,
        "status": attrs.get("status", "RUNNING"),
        "updatedAt": attrs.get("updatedAt", ts),

        # pulled from event map
        "pipeline": event.get("pipeline"),
        "echo_folder": event.get("echo_folder"),
        "project_name": event.get("project_name"),
        "file_pattern": event.get("file_pattern"),
        "echo_subfolder": event.get("echo_subfolder"),
        "lambda_arn": event.get("lambda_arn"),
        "step": event.get("step"),
        "header": event.get("header"),
        "to_queue": event.get("to_queue"),
    }

