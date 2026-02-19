import json
import os
from datetime import datetime, timezone

import boto3


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _resolve_transfer_outcome(event: dict) -> tuple:
    """
    Determine whether the transfer step succeeded or failed, handling both
    development and production event shapes.

    Development shape (direct transferStatus field):
        event["transfer"]["transferStatus"] = "SUCCESS" | "FAILURE"

    Production success shape (Step Functions SDK integration wrapper):
        event["transfer"]["StatusCode"]         = 200
        event["transfer"]["Payload"]["statusCode"] = 200
        event["transfer"]["Payload"]["body"]       = '"success"'

    Production failure shape (Step Functions Catch block):
        event["transferError"]["Error"]  = "Exception"
        event["transferError"]["Cause"]  = "<JSON string with errorMessage, stackTrace>"
        (no "transfer" key present)

    Returns:
        ("SUCCESS", {}) on success
        ("FAILURE", error_detail_dict) on failure
    """
    # ── Production failure: Step Functions caught error ──────────────────────
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

    transfer = event.get("transfer") or {}

    # ── Development shape: transferStatus field set directly ─────────────────
    dev_status = (transfer.get("transferStatus") or "").upper()
    if dev_status == "SUCCESS":
        return "SUCCESS", {}
    if dev_status == "FAILURE":
        err = transfer.get("error") or {"message": "Transfer failed", "transfer": transfer}
        return "FAILURE", err

    # ── Production success: Step Functions SDK integration wrapper ────────────
    # Wrapper carries StatusCode + Payload from the invoked Lambda.
    status_code = transfer.get("StatusCode")
    payload = transfer.get("Payload") or {}
    payload_status = payload.get("statusCode")
    payload_body = payload.get("body", "")

    if status_code == 200 and payload_status == 200:
        # body is a JSON-encoded string: '"success"'
        try:
            body_val = json.loads(payload_body) if isinstance(payload_body, str) else payload_body
        except (json.JSONDecodeError, TypeError):
            body_val = payload_body
        if str(body_val).strip().lower() == "success":
            return "SUCCESS", {}

    # ── Production failure: non-200 SDK wrapper (no Catch block triggered) ───
    if status_code is not None:
        return "FAILURE", {
            "error": "TransferFailed",
            "message": (
                f"Transfer Lambda returned StatusCode={status_code}, "
                f"Payload.statusCode={payload_status}, body={payload_body}"
            ),
            "payload": payload,
        }

    # ── Unrecognised shape: treat as failure ─────────────────────────────────
    return "FAILURE", {"message": "Unrecognized transfer response", "transfer": transfer}


def lambda_handler(event, context):
    """
    Step 3: finalize job status based on the transfer step output.

    Input (common fields):
        jobId       : str  -- job identifier
        project     : str  -- project identifier
        table_name  : str  -- DynamoDB table (or env TABLE_NAME)
        debug       : bool -- enable verbose logging (optional)

    Transfer result is read from one of two keys depending on environment:

        Development  → event["transfer"]["transferStatus"] = "SUCCESS"|"FAILURE"
        Production   → event["transfer"]      (Step Functions SDK wrapper, success)
                       event["transferError"] (Step Functions Catch, failure)

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
