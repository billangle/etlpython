import json
import os
from datetime import datetime, timezone

import boto3


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _resolve_transfer_outcome(event: dict) -> tuple:
    """
    Determine whether the transfer step succeeded or failed.

    A failure is signalled exclusively by the presence of a "transferError" key
    in the event (set by the Step Functions Catch block).  If that key is absent
    the transfer is considered successful regardless of the shape of any
    "transfer" response payload.

    Returns:
        ("SUCCESS", {}) on success
        ("FAILURE", error_detail_dict) on failure
    """
    transfer_error = event.get("transferError")
    if transfer_error:
        cause_str = transfer_error.get("Cause", "")
        try:
            cause = json.loads(cause_str)
            error_msg = cause.get("errorMessage", cause_str)
            stack = cause.get("stackTrace", [])
        except (json.JSONDecodeError, TypeError):
            error_msg = cause_str
            stack = []
        return "FAILURE", {
            "error": transfer_error.get("Error", "Exception"),
            "message": error_msg,
            "cause": cause_str,
            "stackTrace": stack,
        }

    # No error — transfer succeeded.
    return "SUCCESS", {}


def lambda_handler(event, context):
    """
    Step 3: finalize job status based on the transfer step output.

    Input (common fields):
        jobId       : str  -- job identifier
        project     : str  -- project identifier
        table_name  : str  -- DynamoDB table (or env TABLE_NAME)
        debug       : bool -- enable verbose logging (optional)

    Transfer result is determined solely by the presence of "transferError":

        No "transferError" key → SUCCESS  (regardless of payload shape)
        "transferError" present → FAILURE (populated by Step Functions Catch block)

    On success:
        DynamoDB: status=COMPLETED, completedAt=now, updatedAt=now, error_result removed

    On failure:
        DynamoDB: status=ERROR, updatedAt=now, error_result=<extracted detail>
    """
    debug = bool(event.get("debug", False))

    job_id     = event.get("jobId")
    project    = event.get("project")
    table_name = event.get("table_name") or os.environ.get("TABLE_NAME")

    if not job_id:
        raise ValueError("Missing required input: jobId")
    if not project:
        raise ValueError("Missing required input: project")
    if not table_name:
        raise ValueError("Missing required input: table_name (or env TABLE_NAME)")

    outcome, error_detail = _resolve_transfer_outcome(event)

    if debug:
        print(f"[DEBUG] transfer outcome={outcome} jobId={job_id} project={project}")
        if outcome == "FAILURE":
            print(f"[DEBUG] error_detail={json.dumps(error_detail, default=str)}")

    ddb   = boto3.resource("dynamodb")
    table = ddb.Table(table_name)
    ts    = _now_iso()

    if outcome == "SUCCESS":
        table.update_item(
            Key={"jobId": job_id, "project": project},
            UpdateExpression="SET #s = :c, #u = :u, #done = :u REMOVE #er",
            ExpressionAttributeNames={
                "#s":    "status",
                "#u":    "updatedAt",
                "#done": "completedAt",
                "#er":   "error_result",
            },
            ExpressionAttributeValues={":c": "COMPLETED", ":u": ts},
        )
        return {"jobId": job_id, "project": project, "status": "COMPLETED", "updatedAt": ts}

    # Failure path
    table.update_item(
        Key={"jobId": job_id, "project": project},
        UpdateExpression="SET #s = :e, #u = :u, #er = :er",
        ExpressionAttributeNames={
            "#s":  "status",
            "#u":  "updatedAt",
            "#er": "error_result",
        },
        ExpressionAttributeValues={":e": "ERROR", ":u": ts, ":er": error_detail},
    )
    return {
        "jobId":        job_id,
        "project":      project,
        "status":       "ERROR",
        "updatedAt":    ts,
        "error_result": error_detail,
    }
