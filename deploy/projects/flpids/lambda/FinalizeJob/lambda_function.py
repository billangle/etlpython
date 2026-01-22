import os
from datetime import datetime, timezone

import boto3


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def lambda_handler(event, context):
    """
    Step 3: finalize job status based on previous step output.

    Input:
      {
        "jobId": "...",
        "project": "...",
        "table_name": "FSA-FileChecks",
        "transfer": { "transferStatus": "SUCCESS|FAILURE", ... }   # recommended
      }

    On success:
      status=COMPLETED, completedAt=now, updatedAt=now, remove error_result (if present)

    On failure:
      status=ERROR, updatedAt=now, error_result=<details>
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

    transfer = event.get("transfer") or {}
    transfer_status = (transfer.get("transferStatus") or "").upper()

    ddb = boto3.resource("dynamodb")
    table = ddb.Table(table_name)

    ts = _now_iso()

    if transfer_status == "SUCCESS":
        if debug:
            print(f"[DEBUG] Finalizing job as COMPLETED jobId={job_id} project={project}")
        table.update_item(
            Key={"jobId": job_id, "project": project},
            UpdateExpression="SET #s = :c, #u = :u, #done = :u REMOVE #er",
            ExpressionAttributeNames={
                "#s": "status",
                "#u": "updatedAt",
                "#done": "completedAt",
                "#er": "error_result",
            },
            ExpressionAttributeValues={":c": "COMPLETED", ":u": ts},
        )
        return {"jobId": job_id, "project": project, "status": "COMPLETED", "updatedAt": ts}

    err = transfer.get("error") or {"message": "Transfer failed", "transfer": transfer}

    if debug:
        print(f"[DEBUG] Finalizing job as ERROR jobId={job_id} project={project}")

    table.update_item(
        Key={"jobId": job_id, "project": project},
        UpdateExpression="SET #s = :e, #u = :u, #er = :er",
        ExpressionAttributeNames={"#s": "status", "#u": "updatedAt", "#er": "error_result"},
        ExpressionAttributeValues={":e": "ERROR", ":u": ts, ":er": err},
    )

    return {"jobId": job_id, "project": project, "status": "ERROR", "updatedAt": ts, "error_result": err}
