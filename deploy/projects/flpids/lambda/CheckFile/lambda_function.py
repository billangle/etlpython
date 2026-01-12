import json
import re
import subprocess
import urllib.request
import urllib.error
import boto3
import traceback


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
        import base64
        return json.loads(base64.b64decode(resp["SecretBinary"]).decode("utf-8"))

    raise RuntimeError(f"Secret {secret_id} had no SecretString/SecretBinary")


def http_post_json(url: str, payload: dict, timeout_seconds: int = 10, debug: bool = False) -> dict:
    if debug:
        print(f"[DEBUG] Posting JSON to Jenkins: url={url}, timeout={timeout_seconds}s")

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
            if debug:
                print(f"[DEBUG] Jenkins response status={resp.status}")
            return {"ok": True, "status": resp.status, "response_body": body[:4000]}
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace") if e.fp else ""
        if debug:
            print(f"[ERROR] Jenkins HTTP error: {e.code}")
        return {"ok": False, "status": e.code, "error": str(e), "response_body": body[:4000]}
    except Exception as e:
        if debug:
            print(f"[ERROR] Jenkins request failed: {repr(e)}")
        return {"ok": False, "status": None, "error": str(e), "response_body": ""}


def _sanitize_cmd_for_logs(cmd: list[str]) -> list[str]:
    """
    Redact credentials in '--user user:pass' args.
    """
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

    files = [line.strip() for line in out.splitlines() if line.strip()]
    if debug:
        print(f"[DEBUG] Files listed: count={len(files)}")

    return files


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
) -> int:
    if debug:
        print(f"[DEBUG] Checking file size: {remote_path}")

    url = _curl_url(host, port, mode, remote_path, debug=debug)

    # 1) HEAD attempt
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
            size = int(m.group(1))
            if debug:
                print(f"[DEBUG] Size via HEAD Content-Length: {size}")
            return size

    if debug:
        print("[DEBUG] HEAD size not available, falling back to range request")

    # 2) Range fallback
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
        raise RuntimeError(f"curl size check failed rc={rc}: {err_out.strip() or out.strip()}")

    if debug:
        print("[DEBUG] Range request succeeded (size >= 1)")
    return 1


# ----------------------------
# Lambda Handler
# ----------------------------

def lambda_handler(event, context):
    """
    Required event fields:
      - file_pattern: regex with >=1 capture group (group1=target, group2 optional date)
      - echo_folder: e.g. "plas", "gls", "nats"
      - jenkins_url: webhook URL to call when a matching non-empty file is found
      - secret_id: Secrets Manager secret id/name that contains echo_* keys
      - ftps_port: 990 (implicit) or 21 (explicit)

    Optional:
      - debug: bool (default False). Enables/disables print-based debug output.
      - verify_tls: bool (default False). When False, curl uses --insecure (accept invalid certs).
      - echo_subfolder: default ""
      - targets: [] filter by group(1) values (case-insensitive)
      - min_size_bytes: default 1 (means >0)
      - jenkins_timeout_seconds: default 10
      - curl_timeout_seconds: default 30
    """
    debug = bool(event.get("debug", False))

    def dbg(msg: str):
        if debug:
            print(f"[DEBUG] {msg}")

    def err(msg: str):
        # Errors should always print
        print(f"[ERROR] {msg}")

    try:
        dbg(f"Lambda invoked with event: {json.dumps(event)}")

        file_pattern = event.get("file_pattern")
        echo_folder = event.get("echo_folder")
        jenkins_url = event.get("jenkins_url")
        secret_id = event.get("secret_id")

        if not file_pattern:
            raise ValueError("Missing required 'file_pattern'")
        if not echo_folder:
            raise ValueError("Missing required 'echo_folder'")
        if not jenkins_url:
            raise ValueError("Missing required 'jenkins_url'")
        if not secret_id:
            raise ValueError("Missing required 'secret_id'")

        echo_subfolder = event.get("echo_subfolder", "") or ""
        targets = event.get("targets", []) or []
        min_size_bytes = int(event.get("min_size_bytes", 1))
        jenkins_timeout_seconds = int(event.get("jenkins_timeout_seconds", 10))
        curl_timeout_seconds = int(event.get("curl_timeout_seconds", 30))

        # TLS verification: False => --insecure => do not fail on bad/mismatched certs
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
        dbg(f"Targets count={len(targets)} min_size_bytes={min_size_bytes}")

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
            debug=debug,
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

        dbg(f"Matched files after regex/target filtering: {len(matched)}")

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
                debug=debug,
            )

            if size >= min_size_bytes:
                found.append({**f, "content_length": int(size), "remote_path": remote_file_path})

        dbg(f"Files meeting size threshold: {len(found)}")

        result = {
            "found": bool(found),
            "checked_path": echo_path,
            "file_pattern": file_pattern,
            "targets": targets,
            "min_size_bytes": min_size_bytes,
            "matches": found,
            "echo_folder": echo_folder,
            "echo_subfolder": echo_subfolder,
            "ftps_port": ftps_port,
            "ftps_mode": ftps_mode,
            "verify_tls": verify_tls,
            "debug": debug,
        }

        print(f"Result summary: found={result['found']} matches={len(found)}")

        if not found:
            print("No qualifying files found — exiting without Jenkins call")
            return {"statusCode": 200, "body": json.dumps(result)}

        # 4) Trigger Jenkins webhook
        jenkins_payload = {
            "source": "ftps_file_check_lambda",
            "aws_request_id": getattr(context, "aws_request_id", None),
            "function_name": getattr(context, "function_name", None),
            "log_stream_name": getattr(context, "log_stream_name", None),
            **result,
        }

        jenkins_resp = http_post_json(
            jenkins_url,
            jenkins_payload,
            timeout_seconds=jenkins_timeout_seconds,
            debug=debug,
        )

        result["jenkins_call"] = {
            "url": jenkins_url,
            "ok": jenkins_resp["ok"],
            "status": jenkins_resp["status"],
            "error": jenkins_resp.get("error"),
            "response_body": jenkins_resp.get("response_body"),
        }

        print(f"Jenkins call complete: URL={result['jenkins_call']['url']} ok={result['jenkins_call']['ok']} status={result['jenkins_call']['status']} body={result['jenkins_call']['response_body']}")
        return {"statusCode": 200, "body": json.dumps(result)}

    except Exception as e:
        # Errors should always be printed, even if debug=False
        print("Unhandled exception in lambda_handler")
        err(repr(e))
        print(traceback.format_exc())
        raise
