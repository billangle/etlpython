import json
import os
from datetime import datetime, timezone

import boto3


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _extract_error_detail(transfer_error: dict) -> dict:
    """
    Parse the raw transferError dict (set by the Step Functions Catch block)
    into a structured error detail suitable for DynamoDB storage.
    """
    cause_str = transfer_error.get("Cause", "")
    try:
        cause = json.loads(cause_str)
        error_msg = cause.get("errorMessage", cause_str)
        stack = cause.get("stackTrace", [])
    except (json.JSONDecodeError, TypeError):
        error_msg = cause_str
        stack = []
    return {
        "error": transfer_error.get("Error", "Exception"),
        "message": error_msg,
        "cause": cause_str,
        "stackTrace": stack,
    }


def lambda_handler(event, context):
    """
    Step 3: finalize job status based on the path taken through the state machine.

    Input (common fields):
        jobId       : str  -- job identifier
        project     : str  -- project identifier
        table_name  : str  -- DynamoDB table (or env TABLE_NAME)
        path        : str  -- "SUCCESS" (FinalizeJob state) or "CATCH" (FinalizeJobOnCatch state)
        debug       : bool -- enable verbose logging (optional)

    Outcome is driven exclusively by the "path" parameter injected by the step function:

        path == "SUCCESS" → COMPLETED  (normal FinalizeJob branch)
        path == "CATCH"   → ERROR      (Catch branch from TransferAndProcessFile)

    On success:
        DynamoDB: status=COMPLETED, completedAt=now, updatedAt=now, error_result removed

    On failure:
        DynamoDB: status=ERROR, updatedAt=now, error_result=<detail parsed from transferError>
    """
    debug = bool(event.get("debug", False))

    job_id     = event.get("jobId")
    project    = event.get("project")
    table_name = event.get("table_name") or os.environ.get("TABLE_NAME")
    path       = event.get("path", "UNKNOWN")  # "SUCCESS" or "CATCH" set by the step function

    if not job_id:
        raise ValueError("Missing required input: jobId")
    if not project:
        raise ValueError("Missing required input: project")
    if not table_name:
        raise ValueError("Missing required input: table_name (or env TABLE_NAME)")

    is_success = (path == "SUCCESS")
    raw_error = event.get("transfer", {}).get("error", {})
    error_detail = {} if is_success else _extract_error_detail(raw_error)

    if debug:
        print(f"[DEBUG] path={path} is_success={is_success} jobId={job_id} project={project}")
        if not is_success:
            print(f"[DEBUG] error_detail={json.dumps(error_detail, default=str)}")

    ddb   = boto3.resource("dynamodb")
    table = ddb.Table(table_name)
    ts    = _now_iso()

    if is_success:
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
        return {"jobId": job_id, "project": project, "status": "COMPLETED", "updatedAt": ts, "path": path}

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
        "path":         path,
        "error_result": error_detail,
    }
