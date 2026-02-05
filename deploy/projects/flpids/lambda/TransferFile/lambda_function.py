import os
import json
import base64
import subprocess
import uuid
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Tuple, List

import boto3

TMP_DIR = "/tmp"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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


def _sanitize_cmd_for_logs(cmd: List[str]) -> List[str]:
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


def _run_cmd(cmd: List[str], timeout: int = 120, debug: bool = False) -> Tuple[int, str, str]:
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


def _curl_common_args(username: str, password: str, verify_tls: bool):
    args = [
        "curl",
        "--silent",
        "--show-error",
        "--fail",
        "--user", f"{username}:{password}",
        "--ssl-reqd",
        "--ftp-pasv",
        "--tlsv1.2",
    ]
    if not verify_tls:
        args.append("--insecure")
    return args


def _curl_download(host: str, port: int, mode: str, remote_path: str, username: str, password: str,
                   local_path: str, timeout: int, verify_tls: bool, debug: bool):
    url = _curl_url(host, port, mode, remote_path, debug=debug)
    cmd = [*_curl_common_args(username, password, verify_tls), "--output", local_path, url]
    rc, out, err = _run_cmd(cmd, timeout=timeout, debug=debug)
    if rc != 0:
        raise RuntimeError(f"curl download failed rc={rc}: {err.strip() or out.strip()}")


def _curl_delete(host: str, port: int, mode: str, remote_path: str, username: str, password: str,
                 timeout: int, verify_tls: bool, debug: bool):
    parent = remote_path.rsplit("/", 1)[0] or "/"
    parent_url = _curl_url(host, port, mode, parent + "/", debug=debug)
    dele_cmd = f"DELE {remote_path}"
    cmd = [*_curl_common_args(username, password, verify_tls), "-Q", dele_cmd, parent_url]
    rc, out, err = _run_cmd(cmd, timeout=timeout, debug=debug)
    if rc != 0:
        raise RuntimeError(f"curl delete failed rc={rc}: {err.strip() or out.strip()}")


def _resolve_secret_id_from_ddb(table, job_id: str, project: str, debug: bool) -> str:
    resp = table.get_item(Key={"jobId": job_id, "project": project})
    item = resp.get("Item") or {}
    event_cfg = item.get("event") or {}
    secret_id = (event_cfg.get("secret_id") or "").strip()
    if debug:
        print(f"[DEBUG] Resolved secret_id from DynamoDB event.secret_id={secret_id!r}")
    return secret_id


def lambda_handler(event, context):
    debug = bool(event.get("debug", False))

    job_id = event.get("jobId")
    project = event.get("project")
    table_name = event.get("table_name") or os.environ.get("TABLE_NAME")

    # ✅ FIX: prefer LANDING_BUCKET (what deploy.py sets), keep BUCKET as legacy fallback
    bucket = (
        (event.get("bucket") or "").strip()
        or (os.environ.get("LANDING_BUCKET") or "").strip()
        or (os.environ.get("BUCKET") or "").strip()
    )

    # secret_id primarily from input; otherwise DynamoDB event.secret_id
    secret_id = (event.get("secret_id") or "").strip() or (os.environ.get("SECRET_ID") or "").strip()

    if not job_id:
        raise ValueError("Missing required input: jobId")
    if not project:
        raise ValueError("Missing required input: project")
    if not table_name:
        raise ValueError("Missing required input: table_name (or env TABLE_NAME)")
    if not bucket:
        raise ValueError("Missing required input: bucket (event.bucket or env LANDING_BUCKET/BUCKET)")

    verify_tls = bool(event.get("verify_tls", False))
    timeout = int(event.get("timeout_seconds", 120))

    ddb = boto3.resource("dynamodb")
    table = ddb.Table(table_name)
    s3 = boto3.client("s3")

    if not secret_id:
        secret_id = _resolve_secret_id_from_ddb(table, job_id, project, debug=debug)

    if not secret_id:
        raise ValueError("Missing required input: secret_id (input/env empty and DynamoDB event.secret_id empty)")

    # Load row for file + stats details
    resp = table.get_item(Key={"jobId": job_id, "project": project})
    item = resp.get("Item")
    if not item:
        raise RuntimeError(f"DynamoDB row not found for jobId={job_id} project={project}")

    file_obj = item.get("file") or {}
    remote_path = file_obj.get("remotePath")
    file_name = file_obj.get("fileName") or (remote_path.rsplit("/", 1)[-1] if remote_path else None)

    if not remote_path:
        raise RuntimeError("DynamoDB row missing file.remotePath")
    if not file_name:
        raise RuntimeError("DynamoDB row missing file.fileName (and could not derive from remotePath)")

    stats = item.get("stats") or {}
    host = stats.get("ftpsServer")
    port = stats.get("ftpsPort")

    if not host:
        raise RuntimeError("DynamoDB row missing stats.ftpsServer")
    if port is None:
        raise RuntimeError("DynamoDB row missing stats.ftpsPort")

    port = int(port)
    mode = "implicit" if port == 990 else "explicit"

    if debug:
        print(f"[DEBUG] Using bucket={bucket} secret_id={secret_id}")
        print(f"[DEBUG] DDB row remote_path={remote_path} file_name={file_name}")
        print(f"[DEBUG] FTPS host={host} port={port} mode={mode} verify_tls={verify_tls} timeout={timeout}")

    secret = fetch_secret(secret_id, debug=debug)
    username = secret.get("echo_dart_username")
    password = secret.get("echo_dart_password")
    if not username or not password:
        raise RuntimeError("Secret missing echo_dart_username/echo_dart_password")

    local_path = str(Path(TMP_DIR) / f"{uuid.uuid4().hex}_{file_name}")
    s3_key = f"{project.strip('/')}/{file_name}"

    try:
        table.update_item(
            Key={"jobId": job_id, "project": project},
            UpdateExpression="SET #u = :u, #x = :x",
            ExpressionAttributeNames={"#u": "updatedAt", "#x": "transferStage"},
            ExpressionAttributeValues={":u": _now_iso(), ":x": "DOWNLOADING"},
        )

        _curl_download(host, port, mode, remote_path, username, password, local_path, timeout, verify_tls, debug)

        table.update_item(
            Key={"jobId": job_id, "project": project},
            UpdateExpression="SET #u = :u, #x = :x",
            ExpressionAttributeNames={"#u": "updatedAt", "#x": "transferStage"},
            ExpressionAttributeValues={":u": _now_iso(), ":x": "UPLOADING"},
        )

        s3.upload_file(local_path, bucket, s3_key)

        table.update_item(
            Key={"jobId": job_id, "project": project},
            UpdateExpression="SET #u = :u, #x = :x, #s3 = :s3",
            ExpressionAttributeNames={"#u": "updatedAt", "#x": "transferStage", "#s3": "s3"},
            ExpressionAttributeValues={
                ":u": _now_iso(),
                ":x": "DELETING_REMOTE",
                ":s3": {"bucket": bucket, "key": s3_key},
            },
        )

        _curl_delete(host, port, mode, remote_path, username, password, timeout, verify_tls, debug)

        table.update_item(
            Key={"jobId": job_id, "project": project},
            UpdateExpression="SET #u = :u, #x = :x",
            ExpressionAttributeNames={"#u": "updatedAt", "#x": "transferStage"},
            ExpressionAttributeValues={":u": _now_iso(), ":x": "DONE"},
        )

        return {
            "jobId": job_id,
            "project": project,
            "transferStatus": "SUCCESS",
            "bucket": bucket,
            "s3Key": s3_key,
            "remotePath": remote_path,
        }

    except Exception as e:
        err = {
            "message": str(e),
            "type": e.__class__.__name__,
            "traceback": traceback.format_exc(),
        }
        if debug:
            print(f"[DEBUG] Transfer failed: {json.dumps(err)[:4000]}")

        try:
            table.update_item(
                Key={"jobId": job_id, "project": project},
                UpdateExpression="SET #u = :u, #x = :x, #te = :te",
                ExpressionAttributeNames={"#u": "updatedAt", "#x": "transferStage", "#te": "transferError"},
                ExpressionAttributeValues={":u": _now_iso(), ":x": "ERROR", ":te": err},
            )
        except Exception:
            pass

        return {"jobId": job_id, "project": project, "transferStatus": "FAILURE", "error": err}

    finally:
        try:
            if os.path.exists(local_path):
                os.remove(local_path)
        except Exception:
            pass
