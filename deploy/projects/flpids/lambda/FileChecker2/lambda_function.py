import base64
import boto3
import json
import subprocess
import traceback
from typing import Any, Dict, List


# ----------------------------
# Small print-based logging
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


def read_config_from_s3(s3_uri: str, debug: bool = False) -> Dict[str, Any]:
    """
    s3_uri: s3://bucket/key.json
    """
    if not s3_uri.startswith("s3://"):
        raise ValueError(f"config_s3_uri must start with s3://, got: {s3_uri}")

    _, _, rest = s3_uri.partition("s3://")
    bucket, _, key = rest.partition("/")
    if not bucket or not key:
        raise ValueError(f"Invalid s3 uri: {s3_uri}")

    _dbg(debug, f"Loading config from S3: bucket={bucket}, key={key}")
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    text = obj["Body"].read().decode("utf-8")
    return json.loads(text)


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
    """
    Upload local file to remote_path using curl.
    - Uses --upload-file
    - Uses --ftp-create-dirs so nested dirs are created
    - Uses --insecure when verify_tls=False (avoid cert mismatch failure)
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


# ----------------------------
# Document materialization
# ----------------------------

def materialize_document(doc: Dict[str, Any], debug: bool) -> str:
    """
    Creates a local file in /tmp from one of:
      - inline_text
      - inline_base64
      - s3_object {bucket,key}

    Returns local path.
    """
    name = doc.get("name")
    if not name:
        raise ValueError("Each document must have a 'name'")

    local_path = f"/tmp/{name}"
    source = (doc.get("source") or "").strip()

    _dbg(debug, f"Materializing document: name={name}, source={source}")

    if source == "inline_text":
        text = doc.get("content", "")
        if not isinstance(text, str):
            raise ValueError(f"inline_text content must be a string for doc={name}")
        data = text.encode("utf-8")

    elif source == "inline_base64":
        b64 = doc.get("content", "")
        if not isinstance(b64, str):
            raise ValueError(f"inline_base64 content must be a string for doc={name}")
        data = base64.b64decode(b64)

    elif source == "s3_object":
        s3o = doc.get("s3_object") or {}
        bucket = s3o.get("bucket")
        key = s3o.get("key")
        if not bucket or not key:
            raise ValueError(f"s3_object requires s3_object.bucket and s3_object.key for doc={name}")
        data = read_s3_object(bucket, key, debug=debug)

    else:
        raise ValueError(f"Unsupported document source '{source}' for doc={name}")

    with open(local_path, "wb") as f:
        f.write(data)

    _dbg(debug, f"Wrote local file: {local_path} bytes={len(data)}")
    return local_path


# ----------------------------
# Lambda handler
# ----------------------------

def lambda_handler(event, context):
    """
    Event supports either:
      - config_s3_uri: "s3://bucket/path/config.json"   (recommended)
        OR
      - config: {...}  inline config JSON object

    Config must contain:
      - secret_id
      - documents: [...]

    Secret must contain (minimum):
      - echo_ip or echo_host
      - echo_dart_username (or username)
      - echo_dart_password (or password)
      - echo_path   <-- this is used as the remote base path

    Optional config:
      - ftps_port: 990 (implicit) or 21 (explicit) [default 990]
      - verify_tls: bool [default False => adds --insecure]
      - curl_timeout_seconds: int [default 30]

    Documents:
      - name
      - source: inline_text | inline_base64 | s3_object
      - remote_path: relative path under echo_path OR absolute (/...) to override base
    """
    debug = bool(event.get("debug", False))

    try:
        _dbg(debug, f"Input event: {json.dumps(event)}")

        # Load config
        if event.get("config_s3_uri"):
            cfg = read_config_from_s3(event["config_s3_uri"], debug=debug)
        elif event.get("config"):
            cfg = event["config"]
        else:
            raise ValueError("Must provide either 'config_s3_uri' or inline 'config'")

        if not isinstance(cfg, dict):
            raise ValueError("Config must be a JSON object")

        secret_id = cfg.get("secret_id")
        if not secret_id:
            raise ValueError("Config missing 'secret_id'")

        documents = cfg.get("documents") or []
        if not isinstance(documents, list) or not documents:
            raise ValueError("Config must include non-empty 'documents' list")

        ftps_port = int(cfg.get("ftps_port", 990))
        if ftps_port == 990:
            ftps_mode = "implicit"
        elif ftps_port == 21:
            ftps_mode = "explicit"
        else:
            raise ValueError("ftps_port must be 21 or 990")

        verify_tls = bool(cfg.get("verify_tls", False))
        timeout = int(cfg.get("curl_timeout_seconds", 30))

        # Fetch creds and remote base path from secret
        secret = fetch_secret(secret_id, debug=debug)

        host = secret.get("echo_ip") or secret.get("echo_host")
        if not host:
            raise ValueError("Secret must include 'echo_ip' or 'echo_host'")

        username = secret.get("echo_dart_username") or secret.get("username")
        password = secret.get("echo_dart_password") or secret.get("password")
        if not username or not password:
            raise ValueError("Secret must include username/password (echo_dart_username/echo_dart_password)")

        echo_path = secret.get("echo_path")
        if not echo_path or not isinstance(echo_path, str):
            raise ValueError("Secret must include a string 'echo_path' (remote base path)")

        echo_path = echo_path.rstrip("/")  # remote base path
        _dbg(debug, f"Remote base path from secret echo_path={echo_path}")

        connection_info = {
            "host": host,
            "ftps_port": ftps_port,
            "ftps_mode": ftps_mode,
            "verify_tls": verify_tls,
            "curl_timeout_seconds": timeout,
            "echo_path": echo_path,
            "secret_id": secret_id,
            "debug": debug,
        }
        _dbg(debug, f"Connection resolved: {json.dumps(connection_info)}")

        uploads = []

        for doc in documents:
            local_path = materialize_document(doc, debug=debug)

            remote_path = doc.get("remote_path")
            if not remote_path:
                raise ValueError(f"Document '{doc.get('name')}' missing 'remote_path'")

            remote_path = str(remote_path)

            # If remote_path is absolute, use it as-is; otherwise join under echo_path
            if remote_path.startswith("/"):
                full_remote_path = remote_path
            else:
                full_remote_path = _join_posix(echo_path, remote_path)

            _dbg(debug, f"Uploading doc '{doc.get('name')}' => {full_remote_path}")

            up = curl_upload_file(
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

            uploads.append({
                "name": doc.get("name"),
                "source": doc.get("source"),
                "remote_path": full_remote_path,
                "local_path": local_path,
                "ok": up["ok"],
                "rc": up["rc"],
                "stderr": up["stderr"],
                "stdout": up["stdout"],
                "url": up["url"],
            })

        ok = all(u["ok"] for u in uploads)

        result = {
            "ok": ok,
            "connection": connection_info,
            "uploads": uploads,
            "uploaded_count": len(uploads),
            "success_count": sum(1 for u in uploads if u["ok"]),
            "failure_count": sum(1 for u in uploads if not u["ok"]),
        }

        print(f"Upload summary: ok={result['ok']} success={result['success_count']} fail={result['failure_count']}")
        return {"statusCode": 200 if ok else 207, "body": json.dumps(result)}

    except Exception as e:
        _err("Unhandled exception in upload lambda")
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
