import json
import os
import time
import hashlib
import boto3
from botocore.exceptions import ClientError


# ----------------------------
# Environment variables
# ----------------------------
STATE_MACHINE_ARN = os.environ["STATE_MACHINE_ARN"]

# If you want constants to always be provided to the SFN input, set these.
DEFAULT_TABLE_NAME = os.environ.get("DEFAULT_TABLE_NAME", "FSA-FileChecks")
DEFAULT_BUCKET = os.environ.get("DEFAULT_BUCKET", "")
DEFAULT_SECRET_ID = os.environ.get("DEFAULT_SECRET_ID", "")
DEFAULT_VERIFY_TLS = os.environ.get("DEFAULT_VERIFY_TLS", "false").lower() == "true"
DEFAULT_TIMEOUT_SECONDS = int(os.environ.get("DEFAULT_TIMEOUT_SECONDS", "120"))
DEFAULT_DEBUG = os.environ.get("DEFAULT_DEBUG", "false").lower() == "true"

# De-dupe / lock control:
# If ENABLE_LOCK=true, Lambda will update the DynamoDB item TRIGGERED->QUEUED
# with a ConditionExpression before starting SFN.
ENABLE_LOCK = os.environ.get("ENABLE_LOCK", "true").lower() == "true"

# When locking, what status to set
LOCK_STATUS_VALUE = os.environ.get("LOCK_STATUS_VALUE", "QUEUED")

# Optional: only trigger for these projects (comma-separated). Empty => all.
PROJECT_ALLOWLIST = [p.strip() for p in os.environ.get("PROJECT_ALLOWLIST", "").split(",") if p.strip()]

ddb = boto3.resource("dynamodb")
sfn = boto3.client("stepfunctions")


def _get_ddb_str(new_image: dict, attr: str) -> str | None:
    """
    Get a DynamoDB Stream NewImage string attribute.
    Example: NewImage['jobId'] = {'S': '...'}
    """
    if not new_image:
        return None
    v = new_image.get(attr) or {}
    return v.get("S")


def _get_ddb_table_name(record: dict) -> str:
    """
    Stream records don't always include the table name explicitly.
    We use DEFAULT_TABLE_NAME (env) in most setups.
    If you embed table_name in item/event map, you can pull it from NewImage.event.M here.
    """
    return DEFAULT_TABLE_NAME


def _should_trigger(event_name: str, new_image: dict) -> bool:
    if event_name not in ("INSERT", "MODIFY"):
        return False
    status = _get_ddb_str(new_image, "status")
    if (status or "").upper() != "TRIGGERED":
        return False
    project = _get_ddb_str(new_image, "project")
    if PROJECT_ALLOWLIST and project not in PROJECT_ALLOWLIST:
        return False
    return True


def _lock_item(table_name: str, job_id: str, project: str, debug: bool) -> bool:
    """
    Atomically update status TRIGGERED -> LOCK_STATUS_VALUE
    Returns True if lock acquired; False if someone else already processed it.
    """
    table = ddb.Table(table_name)
    now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    try:
        table.update_item(
            Key={"jobId": job_id, "project": project},
            UpdateExpression="SET #s = :new, #u = :u",
            ConditionExpression="attribute_exists(jobId) AND attribute_exists(project) AND #s = :expected",
            ExpressionAttributeNames={"#s": "status", "#u": "updatedAt"},
            ExpressionAttributeValues={
                ":new": LOCK_STATUS_VALUE,
                ":expected": "TRIGGERED",
                ":u": now_iso,
            },
        )
        if debug:
            print(f"[LOCK] Acquired: {job_id} {project} -> {LOCK_STATUS_VALUE}")
        return True

    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        # ConditionalCheckFailed means it wasn't TRIGGERED anymore (duplicate / already running)
        if code == "ConditionalCheckFailedException":
            if debug:
                print(f"[LOCK] Not acquired (already processed): {job_id} {project}")
            return False
        raise


def _execution_name(job_id: str, project: str) -> str:
    """
    Step Functions execution names must be <= 80 chars.
    We'll generate a deterministic short name.
    """
    h = hashlib.sha1(f"{job_id}:{project}".encode("utf-8")).hexdigest()[:10]
    # keep readable prefix
    base = f"filechecks-{h}"
    return base[:80]


def _start_sfn(job_id: str, project: str, table_name: str, new_image: dict, debug: bool) -> dict:
    """
    Build SFN input and StartExecution.
    You can pull optional values out of the DynamoDB row if present (e.g., event map).
    """
    # If you store config in the item (recommended), you can read it here.
    # Example: new_image.get("event", {}).get("M", {}) ... etc.
    # For now we use env defaults.
    inp = {
        "jobId": job_id,
        "project": project,
        "table_name": table_name,
        "bucket": DEFAULT_BUCKET,
        "secret_id": DEFAULT_SECRET_ID,
        "verify_tls": DEFAULT_VERIFY_TLS,
        "timeout_seconds": DEFAULT_TIMEOUT_SECONDS,
        "debug": DEFAULT_DEBUG,
        # Useful for debugging / traceability
        "stream": {
            "source": "ddb-stream-lambda",
        },
    }

    # Execution name: optional. If you don’t want name collisions, omit it.
    name = _execution_name(job_id, project)

    if debug:
        print(f"[SFN] Starting execution: {STATE_MACHINE_ARN} name={name}")
        print(f"[SFN] Input: {json.dumps(inp)}")

    resp = sfn.start_execution(
        stateMachineArn=STATE_MACHINE_ARN,
        name=name,
        input=json.dumps(inp),
    )
    return resp


def lambda_handler(event, context):
    debug = DEFAULT_DEBUG
    records = event.get("Records") or []
    if debug:
        print(f"[INFO] Received {len(records)} stream records")

    started = 0
    skipped = 0
    locked_out = 0
    errors: list[str] = []

    for r in records:
        try:
            event_name = r.get("eventName", "")
            ddb_data = r.get("dynamodb") or {}
            new_image = ddb_data.get("NewImage") or {}

            if not _should_trigger(event_name, new_image):
                skipped += 1
                continue

            job_id = _get_ddb_str(new_image, "jobId")
            project = _get_ddb_str(new_image, "project")
            if not job_id or not project:
                skipped += 1
                if debug:
                    print(f"[SKIP] Missing jobId/project in NewImage: {json.dumps(new_image)[:2000]}")
                continue

            table_name = _get_ddb_table_name(r)

            if ENABLE_LOCK:
                ok = _lock_item(table_name, job_id, project, debug=debug)
                if not ok:
                    locked_out += 1
                    continue

            _start_sfn(job_id, project, table_name, new_image, debug=debug)
            started += 1

        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            errors.append(msg)
            print(f"[ERROR] {msg}")
            # Re-raise to retry the whole batch if you prefer.
            # For now, keep going so other records can process.
            # If you want strict batch retry semantics, uncomment:
            # raise

    return {
        "records": len(records),
        "started": started,
        "skipped": skipped,
        "locked_out": locked_out,
        "errors": errors,
    }
