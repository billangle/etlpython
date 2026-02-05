import json
import os
import time
import hashlib
import boto3
from botocore.exceptions import ClientError


STATE_MACHINE_ARN = os.environ.get("STATE_MACHINE_ARN")
if not STATE_MACHINE_ARN:
    raise RuntimeError("Missing required env var: STATE_MACHINE_ARN")

TABLE_NAME = os.environ.get("TABLE_NAME") or "FSA-FileChecks"

DEFAULT_BUCKET = (os.environ.get("LANDING_BUCKET") or os.environ.get("BUCKET") or "").strip()
DEFAULT_VERIFY_TLS = os.environ.get("DEFAULT_VERIFY_TLS", "false").lower() == "true"
DEFAULT_TIMEOUT_SECONDS = int(os.environ.get("DEFAULT_TIMEOUT_SECONDS", "120"))
DEFAULT_DEBUG = os.environ.get("DEFAULT_DEBUG", "false").lower() == "true"

ENABLE_LOCK = os.environ.get("ENABLE_LOCK", "true").lower() == "true"
LOCK_STATUS_VALUE = os.environ.get("LOCK_STATUS_VALUE", "QUEUED")

PROJECT_ALLOWLIST = [
    p.strip()
    for p in os.environ.get("PROJECT_ALLOWLIST", "").split(",")
    if p.strip()
]

ddb = boto3.resource("dynamodb")
sfn = boto3.client("stepfunctions")


def _img_str(img: dict, attr: str) -> str | None:
    v = (img or {}).get(attr)
    return v.get("S") if isinstance(v, dict) else None


def _keys_to_python(typed_keys: dict) -> dict:
    out = {}
    for k, v in (typed_keys or {}).items():
        if not isinstance(v, dict):
            continue
        if "S" in v:
            out[k] = v["S"]
        elif "N" in v:
            n = v["N"]
            out[k] = int(n) if n.isdigit() else n
    return out


def _should_trigger(event_name: str, status: str | None, project: str | None) -> tuple[bool, str]:
    if event_name != "INSERT":
        return (False, f"eventName={event_name}_not_insert")
    if (status or "").upper() != "TRIGGERED":
        return (False, f"status_not_triggered value={status}")
    if PROJECT_ALLOWLIST and project not in PROJECT_ALLOWLIST:
        return (False, f"project_not_allowlisted project={project}")
    return (True, "ok")


def _get_item_by_key(key: dict) -> dict | None:
    table = ddb.Table(TABLE_NAME)
    resp = table.get_item(Key=key, ConsistentRead=True)
    return resp.get("Item")


def _lock_by_key(key: dict) -> tuple[bool, str | None]:
    if not ENABLE_LOCK:
        return (True, None)

    table = ddb.Table(TABLE_NAME)
    now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    try:
        table.update_item(
            Key=key,
            UpdateExpression="SET #s = :new, #u = :u",
            ConditionExpression="attribute_exists(#s) AND #s = :expected",
            ExpressionAttributeNames={"#s": "status", "#u": "updatedAt"},
            ExpressionAttributeValues={":new": LOCK_STATUS_VALUE, ":expected": "TRIGGERED", ":u": now_iso},
        )
        print(f"[LOCK] Acquired key={key} -> {LOCK_STATUS_VALUE}")
        return (True, None)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code == "ConditionalCheckFailedException":
            item = _get_item_by_key(key)
            cur = (item or {}).get("status")
            print(f"[LOCK] ConditionalCheckFailed key={key} current_status={cur}")
            return (False, cur)
        raise


def _execution_name(job_id: str, project: str) -> str:
    h = hashlib.sha1(f"{job_id}:{project}".encode("utf-8")).hexdigest()[:10]
    return f"filechecks-{h}"[:80]


def _execution_arn_from_sm_arn(state_machine_arn: str, name: str) -> str | None:
    if ":stateMachine:" not in state_machine_arn:
        return None
    return state_machine_arn.replace(":stateMachine:", ":execution:") + f":{name}"


def _start_execution_safe(name: str, payload: dict) -> dict:
    try:
        resp = sfn.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            name=name,
            input=json.dumps([payload]),  # ARRAY for UnwrapRecord
        )
        print(f"[SFN] STARTED name={name} executionArn={resp.get('executionArn')}")
        return {"already_exists": False, **resp}
    except ClientError as e:
        err = e.response.get("Error", {}) or {}
        code = err.get("Code", "")
        msg = err.get("Message", "")
        if code == "ExecutionAlreadyExists" or "already exists" in msg.lower():
            exec_arn = _execution_arn_from_sm_arn(STATE_MACHINE_ARN, name)
            print(f"[SFN] ALREADY_EXISTS name={name} executionArn={exec_arn}")
            return {"already_exists": True, "executionArn": exec_arn}
        raise


def lambda_handler(event, context):
    records = event.get("Records") or []
    print(f"[INFO] records={len(records)} TABLE_NAME={TABLE_NAME} enable_lock={ENABLE_LOCK}")
    print(f"[INFO] state_machine_arn={STATE_MACHINE_ARN}")

    if not DEFAULT_BUCKET:
        raise RuntimeError("Missing LANDING_BUCKET (or BUCKET) env var for StreamStartFileChecks")

    started = 0
    already_exists = 0
    skipped = 0
    locked_out = 0
    errors: list[str] = []

    for i, r in enumerate(records):
        try:
            event_name = r.get("eventName", "")
            ddb_data = r.get("dynamodb") or {}
            keys_typed = ddb_data.get("Keys") or {}
            new_image = ddb_data.get("NewImage") or {}

            key = _keys_to_python(keys_typed)

            job_id = _img_str(new_image, "jobId")
            project = _img_str(new_image, "project")
            status = _img_str(new_image, "status")

            print(f"[REC {i}] eventName={event_name} key={key} jobId={job_id} project={project} status={status}")

            ok, reason = _should_trigger(event_name, status, project)
            if not ok:
                skipped += 1
                print(f"[SKIP {i}] {reason}")
                continue

            if not key:
                raise RuntimeError(f"Missing stream Keys; cannot lock. keys_typed={keys_typed}")

            locked, cur_status = _lock_by_key(key)
            if not locked:
                locked_out += 1
                print(f"[LOCKED_OUT {i}] current_status={cur_status}")
                continue

            item = _get_item_by_key(key) or {}
            event_cfg = item.get("event") or {}

            secret_id = (event_cfg.get("secret_id") or "").strip()
            verify_tls = bool(event_cfg.get("verify_tls", DEFAULT_VERIFY_TLS))

            timeout_seconds = event_cfg.get("curl_timeout_seconds", DEFAULT_TIMEOUT_SECONDS)
            try:
                timeout_seconds = int(timeout_seconds)
            except Exception:
                timeout_seconds = DEFAULT_TIMEOUT_SECONDS

            debug = bool(event_cfg.get("debug", DEFAULT_DEBUG))

            if not secret_id:
                raise RuntimeError("DynamoDB item missing event.secret_id (required)")

            payload = {
                "jobId": job_id,
                "project": project,
                "table_name": TABLE_NAME,
                "bucket": DEFAULT_BUCKET,
                "secret_id": secret_id,
                "verify_tls": verify_tls,
                "timeout_seconds": timeout_seconds,
                "debug": debug,
                "stream": {"source": "ddb-stream-lambda"},
            }

            print(f"[SFN] StartExecution input bucket={payload['bucket']} secret_id={payload['secret_id']}")

            name = _execution_name(job_id, project)
            resp = _start_execution_safe(name, payload)

            if resp.get("already_exists"):
                already_exists += 1
            else:
                started += 1

        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            errors.append(msg)
            print(f"[ERROR {i}] {msg}")

    print(f"[SUMMARY] started={started} already_exists={already_exists} skipped={skipped} locked_out={locked_out} errors={len(errors)}")
    return {
        "records": len(records),
        "started": started,
        "already_exists": already_exists,
        "skipped": skipped,
        "locked_out": locked_out,
        "errors": errors,
    }
