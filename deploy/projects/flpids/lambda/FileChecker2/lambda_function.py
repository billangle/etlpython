import json
import re
import subprocess
import urllib.request
import urllib.error
import boto3

# Only needed for implicit mode:
from ftps_client import iFTP_TLS


def fetch_secret(secret_id: str) -> dict:
    client = boto3.client("secretsmanager")
    resp = client.get_secret_value(SecretId=secret_id)

    if "SecretString" in resp and resp["SecretString"]:
        return json.loads(resp["SecretString"])

    if "SecretBinary" in resp and resp["SecretBinary"]:
        import base64
        return json.loads(base64.b64decode(resp["SecretBinary"]).decode("utf-8"))

    raise RuntimeError(f"Secret {secret_id} had no SecretString/SecretBinary")


def http_post_json(url: str, payload: dict, timeout_seconds: int = 10) -> dict:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url=url,
        data=data,
        headers={"Content-Type": "application/json", "User-Agent": "aws-lambda/ftps-file-check"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            return {"ok": True, "status": resp.status, "response_body": body[:4000]}
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace") if e.fp else ""
        return {"ok": False, "status": e.code, "error": str(e), "response_body": body[:4000]}
    except Exception as e:
        return {"ok": False, "status": None, "error": str(e), "response_body": ""}


def _run(cmd: list[str], timeout: int = 30) -> tuple[int, str, str]:
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    return p.returncode, p.stdout, p.stderr


def _curl_url_explicit(host: str, port: int, path: str) -> str:
    """
    Explicit FTPS in your Lambda curl build:
      - use ftp:// + --ssl-reqd
      - DO NOT use ftpes:// (not supported)
    """
    if not path.startswith("/"):
        path = "/" + path
    return f"ftp://{host}:{port}{path}"


def curl_list_files_explicit(
    host: str,
    port: int,
    username: str,
    password: str,
    remote_dir: str,
    timeout: int = 30,
) -> list[str]:
    if not remote_dir.endswith("/"):
        remote_dir = remote_dir + "/"

    url = _curl_url_explicit(host, port, remote_dir)

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
        url,
    ]

    rc, out, err = _run(cmd, timeout=timeout)
    if rc != 0:
        raise RuntimeError(f"curl(explicit) list failed rc={rc}: {err.strip() or out.strip()}")

    return [line.strip() for line in out.splitlines() if line.strip()]


def curl_file_size_explicit(
    host: str,
    port: int,
    username: str,
    password: str,
    remote_path: str,
    timeout: int = 30,
) -> int:
    """
    Best-effort file size for explicit FTPS using curl:
      1) Try HEAD and parse Content-Length
      2) Fallback: range 0-0 (if succeeds => size >= 1)
    """
    url = _curl_url_explicit(host, port, remote_path)

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
        url,
    ]
    rc, out, err = _run(cmd_head, timeout=timeout)
    if rc == 0:
        m = re.search(r"(?im)^Content-Length:\s*(\d+)\s*$", out)
        if m:
            return int(m.group(1))

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
        url,
    ]
    rc, out, err = _run(cmd_range, timeout=timeout)
    if rc != 0:
        raise RuntimeError(f"curl(explicit) size check failed rc={rc}: {err.strip() or out.strip()}")

    return 1


def compute_echo_path(secret: dict, echo_folder: str, echo_subfolder: str) -> str:
    return "/{root}/{folder}/in/{subfolder}".format(
        root=secret["echo_dart_path"],
        folder=echo_folder,
        subfolder=echo_subfolder or "",
    ).rstrip("/")


def list_and_size_explicit(
    host: str,
    port: int,
    username: str,
    password: str,
    echo_path: str,
    file_pattern: str,
    targets: list[str],
    min_size_bytes: int,
    curl_timeout_seconds: int,
) -> list[dict]:
    filenames = curl_list_files_explicit(
        host=host,
        port=port,
        username=username,
        password=password,
        remote_dir=echo_path,
        timeout=curl_timeout_seconds,
    )

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

    found = []
    for f in matched:
        remote_file_path = f"{echo_path}/{f['name']}"
        size = curl_file_size_explicit(
            host=host,
            port=port,
            username=username,
            password=password,
            remote_path=remote_file_path,
            timeout=curl_timeout_seconds,
        )
        if int(size) >= min_size_bytes:
            found.append({**f, "content_length": int(size), "remote_path": remote_file_path})

    return found


def list_and_size_implicit(
    host: str,
    port: int,
    username: str,
    password: str,
    echo_path: str,
    file_pattern: str,
    targets: list[str],
    min_size_bytes: int,
    list_retries: int = 0,
) -> list[dict]:
    """
    Implicit FTPS via Python (ftps_client.py).
    IMPORTANT: your FTPS server behavior may require specific parsing of LIST output;
    the ftps_client.py below is built to handle common Windows/IIS-ish formats.
    """
    files = []
    ftps = iFTP_TLS(timeout=30)
    try:
        ftps.make_connection(
            host=host,
            port=port,
            username=username,
            password=password,
            log_level=0,
            mode="implicit",
            disable_epsv=True,
        )
        ftps.cwd(echo_path)

        # filter_entries returns a Polars DF with: name, content_length, last_modified, target, system_date
        df = ftps.filter_entries(file_pattern=file_pattern, path="", targets=targets, list_retries=list_retries)
        rows = df.rows(named=True) if df.height > 0 else []

        # Require size > 0 (or min_size_bytes)
        for r in rows:
            size = int(r.get("content_length", 0) or 0)
            if size >= min_size_bytes:
                files.append(
                    {
                        "name": r.get("name"),
                        "target": r.get("target"),
                        "content_length": size,
                        "last_modified": str(r.get("last_modified")),
                        "system_date": str(r.get("system_date")),
                        "remote_path": f"{echo_path}/{r.get('name')}",
                    }
                )
    finally:
        try:
            ftps.quit()
        except Exception:
            try:
                ftps.close()
            except Exception:
                pass

    return files


def lambda_handler(event, context):
    """
    Hybrid behavior:
      - ftps_mode == "explicit": use curl (known-good in your Lambda runtime)
      - ftps_mode == "implicit": use Python ftps_client.py (curl implicit fails for you)

    Required:
      - secret_id, jenkins_url, echo_folder, file_pattern
    """
    print("Input event:", json.dumps(event))

    file_pattern = event.get("file_pattern")
    echo_folder = event.get("echo_folder")
    echo_subfolder = event.get("echo_subfolder", "") or ""
    jenkins_url = event.get("jenkins_url")
    secret_id = event.get("secret_id")

    if not file_pattern:
        return {"statusCode": 400, "body": json.dumps({"error": "Missing required 'file_pattern'."})}
    if not echo_folder:
        return {"statusCode": 400, "body": json.dumps({"error": "Missing required 'echo_folder'."})}
    if not jenkins_url:
        return {"statusCode": 400, "body": json.dumps({"error": "Missing required 'jenkins_url'."})}
    if not secret_id:
        return {"statusCode": 400, "body": json.dumps({"error": "Missing required 'secret_id'."})}

    targets = event.get("targets", []) or []
    min_size_bytes = int(event.get("min_size_bytes", 1))
    jenkins_timeout_seconds = int(event.get("jenkins_timeout_seconds", 10))
    curl_timeout_seconds = int(event.get("curl_timeout_seconds", 30))
    list_retries = int(event.get("list_retries", 0))

    ftps_mode = (event.get("ftps_mode") or "explicit").lower().strip()
    ftps_port = int(event.get("ftps_port") or (990 if ftps_mode == "implicit" else 21))

    secret = fetch_secret(secret_id)

    host = secret["echo_ip"]
    username = secret["echo_dart_username"]
    password = secret["echo_dart_password"]

    echo_path = compute_echo_path(secret, echo_folder, echo_subfolder)

    print(
        json.dumps(
            {
                "host": host,
                "ftps_mode": ftps_mode,
                "ftps_port": ftps_port,
                "echo_path": echo_path,
                "file_pattern": file_pattern,
                "targets": targets,
                "min_size_bytes": min_size_bytes,
            }
        )
    )

    # --- hybrid transport ---
    if ftps_mode == "implicit":
        found = list_and_size_implicit(
            host=host,
            port=ftps_port,
            username=username,
            password=password,
            echo_path=echo_path,
            file_pattern=file_pattern,
            targets=targets,
            min_size_bytes=min_size_bytes,
            list_retries=list_retries,
        )
        transport = "python_ftplib_implicit"
    else:
        found = list_and_size_explicit(
            host=host,
            port=ftps_port,
            username=username,
            password=password,
            echo_path=echo_path,
            file_pattern=file_pattern,
            targets=targets,
            min_size_bytes=min_size_bytes,
            curl_timeout_seconds=curl_timeout_seconds,
        )
        transport = "curl_explicit"
    # -----------------------

    result = {
        "found": bool(found),
        "transport": transport,
        "checked_path": echo_path,
        "file_pattern": file_pattern,
        "targets": targets,
        "min_size_bytes": min_size_bytes,
        "matches": found,
        "pipeline": event.get("pipeline"),
        "step": event.get("step"),
        "echo_folder": echo_folder,
        "echo_subfolder": echo_subfolder,
        "header": event.get("header"),
        "to_queue": event.get("to_queue"),
    }

    print("Check result:", json.dumps(result))

    if not found:
        return {"statusCode": 200, "body": json.dumps(result)}

    # Trigger Jenkins webhook
    jenkins_payload = {
        "source": "ftps_file_check_lambda",
        "aws_request_id": getattr(context, "aws_request_id", None),
        "function_name": getattr(context, "function_name", None),
        "log_stream_name": getattr(context, "log_stream_name", None),
        **result,
    }

    jenkins_resp = http_post_json(jenkins_url, jenkins_payload, timeout_seconds=jenkins_timeout_seconds)
    result["jenkins_call"] = {
        "url": jenkins_url,
        "ok": jenkins_resp["ok"],
        "status": jenkins_resp["status"],
        "error": jenkins_resp.get("error"),
        "response_body": jenkins_resp.get("response_body"),
    }

    print("Jenkins response:", json.dumps(result["jenkins_call"]))

    return {"statusCode": 200, "body": json.dumps(result)}
