import json
import re
import subprocess
import urllib.request
import urllib.error
import boto3


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
        headers={
            "Content-Type": "application/json",
            "User-Agent": "aws-lambda/ftps-file-check",
        },
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


def _curl_url(host: str, port: int, mode: str, path: str) -> str:
    """
    Build a URL that works with curl builds that do NOT support ftpes://.
      - explicit FTPS (AUTH TLS): use ftp:// + --ssl-reqd
      - implicit FTPS (TLS on connect): use ftps://
    """
    if not path.startswith("/"):
        path = "/" + path

    mode = (mode or "explicit").lower().strip()
    scheme = "ftps" if mode == "implicit" else "ftp"
    return f"{scheme}://{host}:{port}{path}"


def curl_list_files(
    host: str,
    port: int,
    username: str,
    password: str,
    mode: str,
    remote_dir: str,
    timeout: int = 30,
    verify_tls: bool = False,  # ✅ NEW: default skip cert validation (fixes curl 60)
) -> list[str]:
    """
    Directory listing using curl.
    - Explicit FTPS: ftp://... with --ssl-reqd
    - Implicit FTPS: ftps://...

    TLS verification:
      - verify_tls=False => adds --insecure (accept invalid/untrusted/mismatched certs)
      - verify_tls=True  => normal curl certificate verification
    """
    if not remote_dir.endswith("/"):
        remote_dir = remote_dir + "/"

    url = _curl_url(host, port, mode, remote_dir)

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

    # ✅ Do not fail when cert is invalid / SAN mismatch (curl rc=60)
    if not verify_tls:
        cmd.append("--insecure")

    cmd.append(url)

    rc, out, err = _run(cmd, timeout=timeout)
    if rc != 0:
        raise RuntimeError(f"curl list failed rc={rc}: {err.strip() or out.strip()}")

    return [line.strip() for line in out.splitlines() if line.strip()]


def curl_file_size(
    host: str,
    port: int,
    username: str,
    password: str,
    mode: str,
    remote_path: str,
    timeout: int = 30,
    verify_tls: bool = False,  # ✅ NEW: default skip cert validation (fixes curl 60)
) -> int:
    """
    Best-effort file size check using curl.
      1) Try --head and parse Content-Length
      2) Fallback: request first byte (range 0-0); if succeeds, size >= 1

    TLS verification:
      - verify_tls=False => adds --insecure (accept invalid/untrusted/mismatched certs)
      - verify_tls=True  => normal curl certificate verification
    """
    url = _curl_url(host, port, mode, remote_path)

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
    ]

    if not verify_tls:
        cmd_range.append("--insecure")

    cmd_range.append(url)

    rc, out, err = _run(cmd_range, timeout=timeout)
    if rc != 0:
        raise RuntimeError(f"curl size check failed rc={rc}: {err.strip() or out.strip()}")

    return 1


def lambda_handler(event, context):
    """
    Required event fields:
      - file_pattern: regex with >=1 capture group (group1=target, group2 optional date)
      - echo_folder: e.g. "plas", "gls", "nats"
      - jenkins_url: webhook URL to call when a matching non-empty file is found
      - secret_id: Secrets Manager secret id/name that contains echo_* keys

    FTPS selection:
      - ftps_port: 990 => implicit, 21 => explicit (required)

    Optional:
      - verify_tls: bool (default False). When False, curl uses --insecure to ignore invalid certs.
      - echo_subfolder: default ""
      - targets: [] filter by group(1) values (case-insensitive)
      - min_size_bytes: default 1 (means >0)
      - jenkins_timeout_seconds: default 10
      - curl_timeout_seconds: default 30
      - header, to_queue, pipeline, step: passed through for traceability
    """
    print("Input event:", json.dumps(event))

    file_pattern = event.get("file_pattern")
    echo_folder = event.get("echo_folder")
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

    echo_subfolder = event.get("echo_subfolder", "") or ""
    targets = event.get("targets", []) or []
    min_size_bytes = int(event.get("min_size_bytes", 1))
    jenkins_timeout_seconds = int(event.get("jenkins_timeout_seconds", 10))
    curl_timeout_seconds = int(event.get("curl_timeout_seconds", 30))

    # ✅ NEW: default False => ignore invalid cert (fixes curl rc=60 SAN mismatch)
    verify_tls = bool(event.get("verify_tls", False))

    ftps_port = event.get("ftps_port")
    if ftps_port is None:
        raise ValueError("Missing required 'ftps_port' (must be 21 or 990)")
    ftps_port = int(ftps_port)

    if ftps_port == 21:
        ftps_mode = "explicit"
    elif ftps_port == 990:
        ftps_mode = "implicit"
    else:
        raise ValueError(f"Unsupported ftps_port {ftps_port}. Only 21 or 990 are allowed.")

    print(f"FTPS configuration resolved from port: port={ftps_port}, mode={ftps_mode}")
    print(f"TLS verification enabled: {verify_tls}")

    # Secrets
    secret = fetch_secret(secret_id)

    host = secret["echo_ip"]
    username = secret["echo_dart_username"]
    password = secret["echo_dart_password"]

    echo_path = "/{root}/{folder}/in/{subfolder}".format(
        root=secret["echo_dart_path"],
        folder=echo_folder,
        subfolder=echo_subfolder,
    ).rstrip("/")

    print(
        json.dumps(
            {
                "host": host,
                "ftps_mode": ftps_mode,
                "ftps_port": ftps_port,
                "echo_path": echo_path,
                "targets": targets,
                "min_size_bytes": min_size_bytes,
                "file_pattern": file_pattern,
                "verify_tls": verify_tls,
            }
        )
    )

    # 1) list directory
    filenames = curl_list_files(
        host=host,
        port=ftps_port,
        username=username,
        password=password,
        mode=ftps_mode,
        remote_dir=echo_path,
        timeout=curl_timeout_seconds,
        verify_tls=verify_tls,
    )

    # 2) regex match + target filter
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

    # 3) size check
    found = []
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
        )

        if size >= min_size_bytes:
            found.append({**f, "content_length": int(size), "remote_path": remote_file_path})

    result = {
        "found": bool(found),
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
        "verify_tls": verify_tls,
    }

    print("Check result:", json.dumps(result))

    if not found:
        return {"statusCode": 200, "body": json.dumps(result)}

    # 4) Trigger Jenkins webhook
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
