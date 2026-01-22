import json
import re
import subprocess
import base64
import boto3
import traceback
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from boto3.dynamodb.conditions import Key


# ----------------------------
# Helpers
# ----------------------------

def fetch_secret(secret_id: str, debug: bool = False) -> dict:
    if debug:
        print(f"[DEBUG] Fetching secret: {secret_id}")

    client = boto3.client("secretsmanager")
    resp = client.get_secret_value(SecretId=secret_id)

    if "SecretString" in resp and resp["SecretString"]:
        if debug:
            print("[DEBUG] Secret fetched from SecretString")
        return json.loads(resp["SecretString"])

    if "SecretBinary" in resp and resp["SecretBinary"]:
        if debug:
            print("[DEBUG] Secret fetched from SecretBinary")
        return json.loads(base64.b64decode(resp["SecretBinary"]).decode("utf-8"))

    raise RuntimeError(f"Secret {secret_id} had no SecretString/SecretBinary")


def _sanitize_cmd_for_logs(cmd: list[str]) -> list[str]:
    """Redact credentials in '--user user:pass' args."""
    safe = []
    i = 0
    while i < len(cmd):
        token = cmd[i]
        if token == "--user" and i + 1 < len(cmd):
            safe.append("--user")
            safe.append("<redacted>")
            i += 2
            continue
        safe.append(token)
        i += 1
    return safe


def _run(cmd: list[str], timeout: int = 30, debug: bool = False) -> tuple[int, str, str]:
    if debug:
        safe_cmd = _sanitize_cmd_for_logs(cmd)
        print(f"[DEBUG] Running command: {' '.join(safe_cmd)} (timeout={timeout}s)")

    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)

    if debug:
        print(f"[DEBUG] Command rc={p.returncode}")
        if p.stdout:
            print(f"[DEBUG] stdout: {p.stdout[:2000]}")
        if p.stderr:
            print(f"[DEBUG] stderr: {p.stderr[:2000]}")

    return p.returncode, p.stdout, p.stderr


def _curl_url(host: str, port: int, mode: str, path: str, debug: bool = False) -> str:
    if not path.startswith("/"):
        path = "/" + path
    scheme = "ftps" if mode == "implicit" else "ftp"
    url = f"{scheme}://{host}:{port}{path}"
    if debug:
        print(f"[DEBUG] Constructed curl URL: {url}")
    return url


def _looks_like_missing_file(err_text: str) -> bool:
    t = (err_text or "").lower()
    return (
        "the file does not exist" in t
        or "no such file" in t
        or "not found" in t
        or "550" in t
    )


def _remove_nones(obj):
    """DynamoDB does not allow NULL values. Remove None recursively."""
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            cleaned = _remove_nones(v)
            if cleaned is not None:
                out[k] = cleaned
        return out
    if isinstance(obj, list):
        out = []
        for v in obj:
            cleaned = _remove_nones(v)
            if cleaned is not None:
                out.append(cleaned)
        return out
    return obj


def _to_ddb_safe(obj):
    """Convert floats -> Decimal recursively for DynamoDB."""
    if isinstance(obj, float):
        return Decimal(str(obj))
    if isinstance(obj, dict):
        return {k: _to_ddb_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_to_ddb_safe(v) for v in obj]
    return obj


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ----------------------------
# Curl FTPS
# ----------------------------

def curl_list_files(
    host: str,
    port: int,
    username: str,
    password: str,
    mode: str,
    remote_dir: str,
    timeout: int = 30,
    verify_tls: bool = False,
    debug: bool = False,
) -> list[str]:
    if debug:
        print(f"[DEBUG] Listing files: host={host}, port={port}, mode={mode}, verify_tls={verify_tls}")

    if not remote_dir.endswith("/"):
        remote_dir += "/"

    url = _curl_url(host, port, mode, remote_dir, debug=debug)

    cmd = [
        "curl",
        "--silent",
        "--show-error",
        "--fail",
        "--list-only",
        "--user", f"{username}:{password}",
        "--ssl-reqd",
        "--ftp-pasv",
        "--tlsv1.2",
    ]

    if not verify_tls:
        if debug:
            print("[DEBUG] TLS verification disabled (--insecure)")
        cmd.append("--insecure")

    cmd.append(url)

    rc, out, err_out = _run(cmd, timeout=timeout, debug=debug)
    if rc != 0:
        raise RuntimeError(f"curl list failed rc={rc}: {err_out.strip() or out.strip()}")

    return [line.strip() for line in out.splitlines() if line.strip()]


def curl_file_size(
    host: str,
    port: int,
    username: str,
    password: str,
    mode: str,
    remote_path: str,
    timeout: int = 30,
    verify_tls: bool = False,
    debug: bool = False,
) -> int | None:
    if debug:
        print(f"[DEBUG] Checking file size: {remote_path}")

    url = _curl_url(host, port, mode, remote_path, debug=debug)

    cmd_head = [
        "curl",
        "--silent",
        "--show-error",
        "--fail",
        "--head",
        "--user", f"{username}:{password}",
        "--ssl-reqd",
        "--ftp-pasv",
        "--tlsv1.2",
    ]

    if not verify_tls:
        cmd_head.append("--insecure")

    cmd_head.append(url)

    rc, out, err_out = _run(cmd_head, timeout=timeout, debug=debug)
    if rc == 0:
        m = re.search(r"(?im)^Content-Length:\s*(\d+)\s*$", out)
        if m:
            return int(m.group(1))

    if rc != 0 and _looks_like_missing_file(err_out or out):
        return None

    cmd_range = [
        "curl",
        "--silent",
        "--show-error",
        "--fail",
        "--user", f"{username}:{password}",
        "--ssl-reqd",
        "--ftp-pasv",
        "--tlsv1.2",
        "--range", "0-0",
    ]

    if not verify_tls:
        cmd_range.append("--insecure")

    cmd_range.append(url)

    rc, out, err_out = _run(cmd_range, timeout=timeout, debug=debug)
    if rc != 0:
        if _looks_like_missing_file(err_out or out):
            return None
        raise RuntimeError(f"curl size check failed rc={rc}: {err_out.strip() or out.strip()}")

    return 1


# ----------------------------
# DynamoDB logic
# ----------------------------

def _select_latest(items: list[dict]) -> dict | None:
    if not items:
        return None
    return sorted(items, key=lambda it: (it.get("createdAt") or ""), reverse=True)[0]


def _validate_table_config(cfg: dict):
    required = ["table_name", "partition_key", "sort_key", "project_gsi_name"]
    missing = [k for k in required if not cfg.get(k)]
    if missing:
        raise ValueError(f"Missing required dynamodb config fields: {missing}")

    if cfg["partition_key"] != "jobId" or cfg["sort_key"] != "project":
        raise ValueError(
            "This Lambda requires DynamoDB keys: partition_key='jobId' and sort_key='project' "
            f"(got partition_key={cfg['partition_key']!r}, sort_key={cfg['sort_key']!r})"
        )


def find_latest_row_for_project_gsi(table, project: str, project_gsi_name: str, debug: bool = False) -> dict | None:
    if debug:
        print(f"[DEBUG] Querying GSI={project_gsi_name} for project={project}")

    resp = table.query(
        IndexName=project_gsi_name,
        KeyConditionExpression=Key("project").eq(project),
    )
    return _select_latest(resp.get("Items", []) or [])


def create_new_row_triggered(
    table,
    job_id: str,
    project: str,
    pipeline: str,
    event: dict,
    file_match: dict,
    host: str,
    ftps_port: int,
    total_file_count: int,
    matched_file_count: int,
    checked_path: str,
    debug: bool = False,
) -> dict:
    created_at = _now_iso()

    item = {
        "jobId": job_id,
        "project": project,                 # <-- event.project_name
        "pipeline": pipeline,               # <-- event.pipeline
        "createdAt": created_at,
        "status": "TRIGGERED",              # set ONLY on create
        "inspectedStatusDate": created_at,  # initialize on create
        "inspectedCount": 1,                # initialize on create

        "event": _remove_nones(event),
        "file": {
            "fileName": file_match.get("name"),
            "fileSize": int(file_match.get("content_length", 0)),
            "remotePath": file_match.get("remote_path"),
            "target": file_match.get("target"),
        },
        "stats": {
            "totalFileCount": int(total_file_count),
            "matchedFileCount": int(matched_file_count),
            "checkedPath": checked_path,
            "ftpsServer": host,
            "ftpsPort": int(ftps_port),
        },
    }

    item = _to_ddb_safe(_remove_nones(item))

    if debug:
        print(f"[DEBUG] Creating NEW row: jobId={job_id} project={project} pipeline={pipeline} status=TRIGGERED")

    table.put_item(
        Item=item,
        ConditionExpression="attribute_not_exists(#pk) AND attribute_not_exists(#sk)",
        ExpressionAttributeNames={
            "#pk": "jobId",
            "#sk": "project",
        },
    )

    return {
        "action": "CREATED",
        "jobId": job_id,
        "project": project,
        "pipeline": pipeline,
        "status": "TRIGGERED",
        "createdAt": created_at,
        "inspectedStatusDate": created_at,
        "inspectedCount": 1,
    }


def update_inspected_date_and_increment_count_only(
    table,
    job_id: str,
    project: str,
    inspected_date: str,
    debug: bool = False,
) -> dict:
    """
    Updates ONLY:
      - inspectedStatusDate
      - inspectedCount (+1)
    Does NOT touch status or pipeline.
    """
    if debug:
        print(f"[DEBUG] Updating inspectedStatusDate + increment inspectedCount ONLY: jobId={job_id} project={project}")

    resp = table.update_item(
        Key={"jobId": job_id, "project": project},
        UpdateExpression="SET #isd = :d ADD #ic :one",
        ExpressionAttributeNames={
            "#isd": "inspectedStatusDate",
            "#ic": "inspectedCount",
        },
        ExpressionAttributeValues={
            ":d": inspected_date,
            ":one": 1,
        },
        ReturnValues="ALL_NEW",
    )

    attrs = resp.get("Attributes") or {}
    return {
        "action": "UPDATED",
        "jobId": job_id,
        "project": project,
        "inspectedStatusDate": inspected_date,
        "inspectedCount": int(attrs.get("inspectedCount") or 0),
    }


# ----------------------------
# Lambda Handler
# ----------------------------

def lambda_handler(event, context):
    """
    Uses:
      - event.project_name as DynamoDB 'project' (sort key)
      - event.pipeline persisted into DynamoDB field 'pipeline' on CREATE only

    When a qualifying file is found:

      - If NO row exists for project OR latest status in {COMPLETED, ERROR}:
            -> CREATE new row with:
                 project = event.project_name
                 pipeline = event.pipeline
                 status = TRIGGERED
                 inspectedStatusDate = now
                 inspectedCount = 1

      - Else if latest status in {TRIGGERED, PROCESSING}:
            -> UPDATE ONLY:
                 inspectedStatusDate = now
                 inspectedCount += 1
            (status is NOT changed)

    Event MUST include:
      - project_name (string)  [used as DynamoDB project]
      - pipeline (string)      [stored in DynamoDB field pipeline on create]
      - dynamodb: {
          table_name: str,
          partition_key: "jobId",
          sort_key: "project",
          project_gsi_name: str
        }
    """
    debug = bool(event.get("debug", False))

    def dbg(msg: str):
        if debug:
            print(f"[DEBUG] {msg}")

    def err(msg: str):
        print(f"[ERROR] {msg}")

    try:
        dbg(f"Lambda invoked with event: {json.dumps(event)}")

        # Required inputs
        file_pattern = event.get("file_pattern")
        echo_folder = event.get("echo_folder")
        secret_id = event.get("secret_id")
        pipeline = event.get("pipeline")
        project_name = event.get("project_name")

        if not file_pattern:
            raise ValueError("Missing required 'file_pattern'")
        if not echo_folder:
            raise ValueError("Missing required 'echo_folder'")
        if not secret_id:
            raise ValueError("Missing required 'secret_id'")
        if not pipeline:
            raise ValueError("Missing required 'pipeline'")
        if not project_name:
            raise ValueError("Missing required 'project_name' (used as DynamoDB project)")

        # Required DDB config in event
        ddb_cfg = event.get("dynamodb") or {}
        _validate_table_config(ddb_cfg)
        table_name = ddb_cfg["table_name"]
        project_gsi_name = ddb_cfg["project_gsi_name"]

        # Optional inputs
        echo_subfolder = event.get("echo_subfolder", "") or ""
        targets = event.get("targets", []) or []
        min_size_bytes = int(event.get("min_size_bytes", 1))
        curl_timeout_seconds = int(event.get("curl_timeout_seconds", 30))
        verify_tls = bool(event.get("verify_tls", False))

        ftps_port = event.get("ftps_port")
        if ftps_port is None:
            raise ValueError("Missing required 'ftps_port' (must be 21 or 990)")
        ftps_port = int(ftps_port)

        if ftps_port == 990:
            ftps_mode = "implicit"
        elif ftps_port == 21:
            ftps_mode = "explicit"
        else:
            raise ValueError(f"Unsupported ftps_port={ftps_port}. Only 21 or 990 are allowed.")

        dbg(f"FTPS resolved: port={ftps_port}, mode={ftps_mode}, verify_tls={verify_tls}")

        secret = fetch_secret(secret_id, debug=debug)
        host = secret["echo_ip"]
        username = secret["echo_dart_username"]
        password = secret["echo_dart_password"]

        echo_path = f"/{secret['echo_dart_path']}/{echo_folder}/in/{echo_subfolder}".rstrip("/")
        dbg(f"Resolved echo_path={echo_path}")

        # 1) List directory
        filenames = curl_list_files(
            host=host,
            port=ftps_port,
            username=username,
            password=password,
            mode=ftps_mode,
            remote_dir=echo_path,
            timeout=curl_timeout_seconds,
            verify_tls=verify_tls,
            debug=debug,
        )

        total_file_count = len(filenames)

        # 2) Regex match + optional targets filter
        compiled = re.compile(file_pattern, re.I)
        targets_lc = [t.lower() for t in targets] if targets else None

        matched = []
        for name in filenames:
            m = compiled.search(name)
            if not m or not m.lastindex or m.lastindex < 1:
                continue
            target = m.group(1).lower()
            if targets_lc and target not in targets_lc:
                continue
            matched.append({"name": name, "target": target})

        matched_file_count = len(matched)
        dbg(f"Matched files after regex/target filtering: {matched_file_count}")

        # 3) Find FIRST qualifying file by size
        first_found = None
        for f in matched:
            remote_file_path = f"{echo_path}/{f['name']}"

            size = curl_file_size(
                host=host,
                port=ftps_port,
                username=username,
                password=password,
                mode=ftps_mode,
                remote_path=remote_file_path,
                timeout=curl_timeout_seconds,
                verify_tls=verify_tls,
                debug=debug,
            )

            if size is None:
                if debug:
                    print(f"[DEBUG] Skipping missing file: {remote_file_path}")
                continue

            if int(size) > 0 and int(size) >= min_size_bytes:
                first_found = {**f, "content_length": int(size), "remote_path": remote_file_path}
                if debug:
                    print(f"[DEBUG] First qualifying file found: {first_found}")
                break

        result = {
            "found": bool(first_found),
            "checked_path": echo_path,
            "file_pattern": file_pattern,
            "targets": targets,
            "min_size_bytes": min_size_bytes,
            "match": first_found,
            "matches": [first_found] if first_found else [],
            "echo_folder": echo_folder,
            "echo_subfolder": echo_subfolder,
            "ftps_port": ftps_port,
            "ftps_mode": ftps_mode,
            "verify_tls": verify_tls,
            "debug": debug,
            "total_file_count": total_file_count,
            "matched_file_count": matched_file_count,
            "project_name": project_name,
            "pipeline": pipeline,
        }

        print(
            "Result summary: "
            f"host={host} path={result['checked_path']} folder={result['echo_folder']} "
            f"found={result['found']} total_file_count={total_file_count} matched_file_count={matched_file_count}"
        )

        if not first_found:
            result["error"] = "file meeting the expected criteria was not found"
            print("No qualifying files found — exiting without DynamoDB write/update")
            return {"statusCode": 200, "body": json.dumps(result)}

        # 4) DDB write/update
        ddb = boto3.resource("dynamodb")
        table = ddb.Table(table_name)

        project = str(project_name)
        now_str = _now_iso()

        latest_row = find_latest_row_for_project_gsi(table, project, project_gsi_name, debug=debug)
        latest_status_u = ((latest_row.get("status") if latest_row else "") or "").upper()

        if latest_row is None or latest_status_u in {"COMPLETED", "ERROR"}:
            new_job_id = str(uuid.uuid4())
            write_info = create_new_row_triggered(
                table=table,
                job_id=new_job_id,
                project=project,
                pipeline=str(pipeline),
                event=_remove_nones(event),
                file_match=first_found,
                host=host,
                ftps_port=ftps_port,
                total_file_count=total_file_count,
                matched_file_count=matched_file_count,
                checked_path=echo_path,
                debug=debug,
            )
            print(
                "DynamoDB write complete: "
                f"action={write_info['action']} table={table_name} jobId={write_info['jobId']} "
                f"project={write_info['project']} pipeline={write_info['pipeline']} status={write_info['status']} "
                f"inspectedStatusDate={write_info['inspectedStatusDate']} inspectedCount={write_info['inspectedCount']}"
            )
        else:
            # TRIGGERED or PROCESSING => update date + increment count only (status unchanged)
            write_info = update_inspected_date_and_increment_count_only(
                table=table,
                job_id=str(latest_row["jobId"]),
                project=str(latest_row["project"]),
                inspected_date=now_str,
                debug=debug,
            )
            print(
                "DynamoDB update complete: "
                f"action={write_info['action']} table={table_name} jobId={write_info['jobId']} "
                f"project={write_info['project']} status_preserved={latest_status_u or 'UNKNOWN'} "
                f"inspectedStatusDate={write_info['inspectedStatusDate']} inspectedCount={write_info['inspectedCount']}"
            )

        result["dynamodb"] = {
            "table_name": table_name,
            "partition_key": ddb_cfg["partition_key"],
            "sort_key": ddb_cfg["sort_key"],
            "project_gsi_name": project_gsi_name,
            "latest_row_status": latest_status_u if latest_row else None,
            **write_info,
        }

        return {"statusCode": 200, "body": json.dumps(result)}

    except Exception as e:
        print("Unhandled exception in lambda_handler")
        err(repr(e))
        print(traceback.format_exc())
        raise
