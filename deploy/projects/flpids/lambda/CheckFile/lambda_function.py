import json
import re
import subprocess
import urllib.request
import urllib.error
import urllib.parse
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
    """
    POST JSON payload to url.

    Returns dict:
      { ok: bool, status: int|None, error: str|None, response_body: str }
    """
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


def _looks_like_missing_file(err_text: str) -> bool:
    """
    Heuristics for "file missing" responses from curl for FTP/FTPS.
    """
    t = (err_text or "").lower()
    return (
        "the file does not exist" in t
        or "no such file" in t
        or "not found" in t
        or "550" in t
    )


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
) -> int | None:
    """
    Returns:
      - int size if determinable
      - 1 if range request succeeded but size unknown
      - None if the file does not exist (expected condition)

    Raises:
      - RuntimeError for unexpected curl failures (network, auth, etc.)
    """
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

    if rc != 0 and _looks_like_missing_file(err_out or out):
        if debug:
            print("[DEBUG] File does not exist (from HEAD); returning None")
        return None

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
        if _looks_like_missing_file(err_out or out):
            if debug:
                print("[DEBUG] File does not exist (from RANGE); returning None")
            return None
        raise RuntimeError(f"curl size check failed rc={rc}: {err_out.strip() or out.strip()}")

    if debug:
        print("[DEBUG] Range request succeeded (size >= 1)")
    return 1


# ----------------------------
# Lambda Handler
# ----------------------------

def lambda_handler(event, context):
    """
    Behavior:
      - List directory
      - Regex match + (optional) target filter
      - Find FIRST file that matches and has size > 0
      - Call Jenkins URL once (if found)

    Required event fields:
      - file_pattern
      - echo_folder
      - jenkins_url
      - secret_id
      - ftps_port (21 or 990)

    Optional:
      - debug: bool (default False)
      - verify_tls: bool (default False) => uses --insecure if False
      - echo_subfolder: str
      - targets: list[str]
      - min_size_bytes: int (default 1)
      - jenkins_timeout_seconds: int (default 10)
      - curl_timeout_seconds: int (default 30)
      - jenkins_file_count_param: str (default "file_count") -> query param added to Jenkins URL
    """
    debug = bool(event.get("debug", False))

    def dbg(msg: str):
        if debug:
            print(f"[DEBUG] {msg}")

    def err(msg: str):
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
        verify_tls = bool(event.get("verify_tls", False))

        # New: URL query param name for file count
        jenkins_file_count_param = (event.get("jenkins_file_count_param") or "file_count").strip() or "file_count"

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

        # 2) Regex match + (optional) target filter
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

        # 3) Find FIRST qualifying file (size >= min_size_bytes; and > 0 implied)
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

            # Missing file => expected => skip
            if size is None:
                dbg(f"Skipping missing file: {remote_file_path}")
                continue

            # Require size > 0 and >= min_size_bytes
            if int(size) > 0 and int(size) >= min_size_bytes:
                first_found = {**f, "content_length": int(size), "remote_path": remote_file_path}
                dbg(f"First qualifying file found: {first_found}")
                break

        # Build result
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
            "total_file_count": total_file_count,  # total files listed in the directory
            "matched_file_count": len(matched),     # matched after regex/targets
        }

        print(
            "Result summary: "
            f"host={host} path={result['checked_path']} folder={result['echo_folder']} "
            f"found={result['found']} total_file_count={total_file_count} matched_file_count={len(matched)}"
        )

        if not first_found:
            result["error"] = "file meeting the expected criteria was not found"
            print("No qualifying files found — exiting without Jenkins call")
            return {"statusCode": 200, "body": json.dumps(result)}

        # 4) Add total_file_count as a query param to Jenkins URL
        parsed = urllib.parse.urlsplit(jenkins_url)
        q = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)

        # Replace existing param if present; otherwise add it
        q = [(k, v) for (k, v) in q if k != jenkins_file_count_param]
        q.append((jenkins_file_count_param, str(total_file_count)))

        new_query = urllib.parse.urlencode(q, doseq=True)
        jenkins_url_with_count = urllib.parse.urlunsplit(
            (parsed.scheme, parsed.netloc, parsed.path, new_query, parsed.fragment)
        )

        dbg(f"Jenkins URL (with file count): {jenkins_url_with_count}")

        # 5) Trigger Jenkins webhook with JSON payload (also contains counts)
        jenkins_payload = {
            "source": "ftps_file_check_lambda",
            "aws_request_id": getattr(context, "aws_request_id", None),
            "function_name": getattr(context, "function_name", None),
            "log_stream_name": getattr(context, "log_stream_name", None),
            **result,
        }

        jenkins_resp = http_post_json(
            jenkins_url_with_count,
            jenkins_payload,
            timeout_seconds=jenkins_timeout_seconds,
            debug=debug,
        )

        result["jenkins_call"] = {
            "url": jenkins_url_with_count,
            "ok": jenkins_resp["ok"],
            "status": jenkins_resp["status"],
            "error": jenkins_resp.get("error"),
            "response_body": jenkins_resp.get("response_body"),
            "file_count_param": jenkins_file_count_param,
            "file_count_value": total_file_count,
        }

        print(
            "Jenkins call complete: "
            f"URL={result['jenkins_call']['url']} "
            f"ok={result['jenkins_call']['ok']} "
            f"status={result['jenkins_call']['status']} "
            f"body={result['jenkins_call']['response_body']}"
        )
        return {"statusCode": 200, "body": json.dumps(result)}

    except Exception as e:
        print("Unhandled exception in lambda_handler")
        err(repr(e))
        print(traceback.format_exc())
        raise
