import base64
import boto3
import json
import subprocess
import traceback
import uuid
from typing import Any, Dict, List


# ----------------------------
# Print-based logging
# ----------------------------

def _dbg(enabled: bool, msg: str) -> None:
    if enabled:
        print(f"[DEBUG] {msg}")


def _err(msg: str) -> None:
    print(f"[ERROR] {msg}")


# ----------------------------
# AWS helpers
# ----------------------------

def fetch_secret(secret_id: str, debug: bool = False) -> Dict[str, Any]:
    _dbg(debug, f"Fetching secret: {secret_id}")
    client = boto3.client("secretsmanager")
    resp = client.get_secret_value(SecretId=secret_id)

    if resp.get("SecretString"):
        _dbg(debug, "Secret fetched from SecretString")
        return json.loads(resp["SecretString"])

    if resp.get("SecretBinary"):
        _dbg(debug, "Secret fetched from SecretBinary")
        raw = base64.b64decode(resp["SecretBinary"]).decode("utf-8")
        return json.loads(raw)

    raise RuntimeError(f"Secret {secret_id} had no SecretString/SecretBinary")


def read_s3_object(bucket: str, key: str, debug: bool = False) -> bytes:
    _dbg(debug, f"Reading S3 object: s3://{bucket}/{key}")
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read()


# ----------------------------
# curl helpers
# ----------------------------

def _sanitize_cmd(cmd: List[str]) -> List[str]:
    """Redact credentials in '--user user:pass'."""
    safe = []
    i = 0
    while i < len(cmd):
        if cmd[i] == "--user" and i + 1 < len(cmd):
            safe.append("--user")
            safe.append("<redacted>")
            i += 2
        else:
            safe.append(cmd[i])
            i += 1
    return safe


def _run(cmd: List[str], timeout: int, debug: bool) -> Dict[str, Any]:
    _dbg(debug, f"Running: {' '.join(_sanitize_cmd(cmd))} (timeout={timeout}s)")
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    _dbg(debug, f"rc={p.returncode}")
    if p.stdout:
        _dbg(debug, f"stdout: {p.stdout[:2000]}")
    if p.stderr:
        _dbg(debug, f"stderr: {p.stderr[:2000]}")
    return {"rc": p.returncode, "stdout": p.stdout, "stderr": p.stderr}


def _curl_url(host: str, port: int, mode: str, remote_path: str) -> str:
    if not remote_path.startswith("/"):
        remote_path = "/" + remote_path
    scheme = "ftps" if mode == "implicit" else "ftp"
    return f"{scheme}://{host}:{port}{remote_path}"


def _join_posix(base: str, rel: str) -> str:
    base = (base or "").rstrip("/")
    rel = (rel or "").lstrip("/")
    if not base:
        return "/" + rel
    return f"{base}/{rel}"


def curl_upload_file(
    *,
    host: str,
    port: int,
    mode: str,
    username: str,
    password: str,
    local_path: str,
    remote_path: str,
    verify_tls: bool,
    timeout: int,
    debug: bool,
) -> Dict[str, Any]:
    url = _curl_url(host, port, mode, remote_path)

    cmd = [
        "curl",
        "--silent",
        "--show-error",
        "--fail",
        "--user", f"{username}:{password}",
        "--ssl-reqd",
        "--ftp-pasv",
        "--tlsv1.2",
        "--ftp-create-dirs",
        "--upload-file", local_path,
    ]

    if not verify_tls:
        _dbg(debug, "TLS verification disabled (--insecure)")
        cmd.append("--insecure")

    cmd.append(url)

    res = _run(cmd, timeout=timeout, debug=debug)
    ok = (res["rc"] == 0)

    return {
        "ok": ok,
        "rc": res["rc"],
        "remote_path": remote_path,
        "url": url,
        "stderr": (res["stderr"] or "")[:2000],
        "stdout": (res["stdout"] or "")[:2000],
    }


def curl_delete_file(
    *,
    host: str,
    port: int,
    mode: str,
    username: str,
    password: str,
    remote_path: str,
    verify_tls: bool,
    timeout: int,
    debug: bool,
) -> Dict[str, Any]:
    """
    Deletes a remote file by issuing the FTP DELE command via curl.
    """
    url = _curl_url(host, port, mode, remote_path)

    cmd = [
        "curl",
        "--silent",
        "--show-error",
        "--fail",
        "--user", f"{username}:{password}",
        "--ssl-reqd",
        "--ftp-pasv",
        "--tlsv1.2",
        "--quote", f"DELE {remote_path}",
    ]

    if not verify_tls:
        _dbg(debug, "TLS verification disabled (--insecure)")
        cmd.append("--insecure")

    cmd.append(url)

    res = _run(cmd, timeout=timeout, debug=debug)
    ok = (res["rc"] == 0)

    return {
        "ok": ok,
        "rc": res["rc"],
        "remote_path": remote_path,
        "url": url,
        "stderr": (res["stderr"] or "")[:2000],
        "stdout": (res["stdout"] or "")[:2000],
    }


# ----------------------------
# Lambda handler
# ----------------------------

def lambda_handler(event, context):
    """
    One Lambda supports:
      - operation: "upload" (default) or "delete"

    Required event fields:
      - secret_id
      - documents: [
          {
            "s3_bucket": "bucket",
            "s3_key": "path/to/file.ext",
            "remote_path": "relative path under echo_dart_path (ALWAYS relative)"
          }
        ]

    Optional:
      - operation: upload|delete (default upload)
      - ftps_port: 990 (implicit) or 21 (explicit) [default 990]
      - verify_tls: bool [default False => adds --insecure]
      - curl_timeout_seconds: int [default 30]
      - debug: bool [default False]

    Secret must contain:
      - echo_ip or echo_host
      - echo_dart_username (or username)
      - echo_dart_password (or password)
      - echo_dart_path   (remote base path)

    IMPORTANT:
      - remote_path is ALWAYS relative to echo_dart_path.
      - Local /tmp filename is auto-generated using UUID.
    """
    debug = bool(event.get("debug", False))

    try:
        _dbg(debug, f"Input event: {json.dumps(event)}")

        secret_id = event.get("secret_id")
        if not secret_id:
            raise ValueError("Missing required event field 'secret_id'")

        documents = event.get("documents") or []
        if not isinstance(documents, list) or not documents:
            raise ValueError("Missing or empty required event field 'documents'")

        operation = (event.get("operation") or "upload").strip().lower()
        if operation not in ("upload", "delete"):
            raise ValueError("operation must be 'upload' or 'delete'")

        ftps_port = int(event.get("ftps_port", 990))
        if ftps_port == 990:
            ftps_mode = "implicit"
        elif ftps_port == 21:
            ftps_mode = "explicit"
        else:
            raise ValueError("ftps_port must be 21 or 990")

        verify_tls = bool(event.get("verify_tls", False))
        timeout = int(event.get("curl_timeout_seconds", 30))

        # Fetch creds + base remote path from secret
        secret = fetch_secret(secret_id, debug=debug)

        host = secret.get("echo_ip") or secret.get("echo_host")
        if not host:
            raise ValueError("Secret must include 'echo_ip' or 'echo_host'")

        username = secret.get("echo_dart_username") or secret.get("username")
        password = secret.get("echo_dart_password") or secret.get("password")
        if not username or not password:
            raise ValueError("Secret must include username/password (echo_dart_username/echo_dart_password)")

        echo_dart_path = secret.get("echo_dart_path")
        if not echo_dart_path or not isinstance(echo_dart_path, str):
            raise ValueError("Secret must include a string 'echo_dart_path' (remote base path)")
        echo_dart_path = echo_dart_path.rstrip("/")

        connection_info = {
            "host": host,
            "ftps_port": ftps_port,
            "ftps_mode": ftps_mode,
            "verify_tls": verify_tls,
            "curl_timeout_seconds": timeout,
            "echo_dart_path": echo_dart_path,
            "secret_id": secret_id,
            "operation": operation,
            "debug": debug,
        }
        _dbg(debug, f"Connection resolved: {json.dumps(connection_info)}")

        actions = []

        for idx, doc in enumerate(documents):
            bucket = doc.get("s3_bucket")
            key = doc.get("s3_key")
            rel_remote = doc.get("remote_path")

            if not bucket or not key:
                raise ValueError(f"documents[{idx}] must include 's3_bucket' and 's3_key'")
            if not rel_remote or not isinstance(rel_remote, str):
                raise ValueError(f"documents[{idx}] must include string 'remote_path' (relative)")

            # ALWAYS relative to echo_dart_path
            rel_remote = rel_remote.lstrip("/")
            full_remote_path = _join_posix(echo_dart_path, rel_remote)

            # UUID temp file (preserve extension if present)
            basename = key.split("/")[-1]
            ext = ""
            if "." in basename:
                ext = "." + basename.split(".")[-1]
            tmp_name = f"{uuid.uuid4()}{ext}"
            local_path = f"/tmp/{tmp_name}"

            if operation == "upload":
                data = read_s3_object(bucket, key, debug=debug)
                with open(local_path, "wb") as f:
                    f.write(data)

                _dbg(
                    debug,
                    f"Prepared local file: {local_path} bytes={len(data)} "
                    f"from s3://{bucket}/{key} -> remote={full_remote_path}"
                )

                res = curl_upload_file(
                    host=host,
                    port=ftps_port,
                    mode=ftps_mode,
                    username=username,
                    password=password,
                    local_path=local_path,
                    remote_path=full_remote_path,
                    verify_tls=verify_tls,
                    timeout=timeout,
                    debug=debug,
                )

                actions.append({
                    "action": "upload",
                    "source_s3": f"s3://{bucket}/{key}",
                    "remote_path": full_remote_path,
                    "local_tmp": local_path,
                    "ok": res["ok"],
                    "rc": res["rc"],
                    "stderr": res["stderr"],
                    "stdout": res["stdout"],
                    "url": res["url"],
                })

            else:
                _dbg(
                    debug,
                    f"Deleting remote file: remote={full_remote_path} "
                    f"(source label s3://{bucket}/{key})"
                )

                res = curl_delete_file(
                    host=host,
                    port=ftps_port,
                    mode=ftps_mode,
                    username=username,
                    password=password,
                    remote_path=full_remote_path,
                    verify_tls=verify_tls,
                    timeout=timeout,
                    debug=debug,
                )

                actions.append({
                    "action": "delete",
                    "source_s3": f"s3://{bucket}/{key}",
                    "remote_path": full_remote_path,
                    "ok": res["ok"],
                    "rc": res["rc"],
                    "stderr": res["stderr"],
                    "stdout": res["stdout"],
                    "url": res["url"],
                })

        ok = all(a.get("ok") for a in actions)

        result = {
            "ok": ok,
            "connection": connection_info,
            "actions": actions,
            "count": len(actions),
            "success_count": sum(1 for a in actions if a.get("ok")),
            "failure_count": sum(1 for a in actions if not a.get("ok")),
        }

        print(
            f"FTPS {operation} summary: ok={result['ok']} "
            f"success={result['success_count']} fail={result['failure_count']}"
        )

        return {"statusCode": 200 if ok else 207, "body": json.dumps(result)}

    except Exception as e:
        _err("Unhandled exception in upload/delete lambda")
        _err(repr(e))
        print(traceback.format_exc())
        return {
            "statusCode": 500,
            "body": json.dumps({
                "ok": False,
                "error": repr(e),
                "debug": debug,
            })
        }
