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

def dbg(msg: str):
    print(f"[DEBUG] {msg}")


def err(msg: str):
    print(f"[ERROR] {msg}")


def fetch_secret(secret_id: str) -> dict:
    dbg(f"Fetching secret: {secret_id}")
    client = boto3.client("secretsmanager")
    resp = client.get_secret_value(SecretId=secret_id)

    if "SecretString" in resp and resp["SecretString"]:
        dbg("Secret fetched from SecretString")
        return json.loads(resp["SecretString"])

    if "SecretBinary" in resp and resp["SecretBinary"]:
        dbg("Secret fetched from SecretBinary")
        import base64
        return json.loads(base64.b64decode(resp["SecretBinary"]).decode("utf-8"))

    raise RuntimeError(f"Secret {secret_id} had no SecretString/SecretBinary")


def http_post_json(url: str, payload: dict, timeout_seconds: int = 10) -> dict:
    dbg(f"Posting JSON to Jenkins: url={url}, timeout={timeout_seconds}s")
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
            dbg(f"Jenkins response status={resp.status}")
            return {"ok": True, "status": resp.status, "response_body": body[:4000]}
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace") if e.fp else ""
        err(f"Jenkins HTTP error: {e.code}")
        return {"ok": False, "status": e.code, "error": str(e), "response_body": body[:4000]}
    except Exception as e:
        err(f"Jenkins request failed: {repr(e)}")
        return {"ok": False, "status": None, "error": str(e), "response_body": ""}


def _run(cmd: list[str], timeout: int = 30) -> tuple[int, str, str]:
    safe_cmd = ["<redacted>" if ":" in c else c for c in cmd]
    dbg(f"Running command: {' '.join(safe_cmd)} (timeout={timeout}s)")
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    dbg(f"Command rc={p.returncode}")
    if p.stdout:
        dbg(f"stdout: {p.stdout[:2000]}")
    if p.stderr:
        dbg(f"stderr: {p.stderr[:2000]}")
    return p.returncode, p.stdout, p.stderr


def _curl_url(host: str, port: int, mode: str, path: str) -> str:
    if not path.startswith("/"):
        path = "/" + path

    scheme = "ftps" if mode == "implicit" else "ftp"
    url = f"{scheme}://{host}:{port}{path}"
    dbg(f"Constructed curl URL: {url}")
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
) -> list[str]:
    dbg(f"Listing files: host={host}, port={port}, mode={mode}, verify_tls={verify_tls}")

    if not remote_dir.endswith("/"):
        remote_dir += "/"

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

    if not verify_tls:
        dbg("TLS verification disabled (--insecure)")
        cmd.append("--insecure")

    cmd.append(url)

    rc, out, err_out = _run(cmd, timeout)
    if rc != 0:
        raise RuntimeError(f"curl list failed rc={rc}: {err_out.strip() or out.strip()}")

    files = [line.strip() for line in out.splitlines() if line.strip()]
    dbg(f"Files listed: count={len(files)}")
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
) -> int:
    dbg(f"Checking file size: {remote_path}")

    url = _curl_url(host, port, mode, remote_path)

    cmd = [
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
        cmd.append("--insecure")

    cmd.append(url)

    rc, out, err_out = _run(cmd, timeout)
    if rc == 0:
        m = re.search(r"(?im)^Content-Length:\s*(\d+)\s*$", out)
        if m:
            size = int(m.group(1))
            dbg(f"Size via HEAD: {size}")
            return size

    dbg("HEAD failed, falling back to range request")

    cmd = [
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
        cmd.append("--insecure")

    cmd.append(url)

    rc, out, err_out = _run(cmd, timeout)
    if rc != 0:
        raise RuntimeError(f"curl size check failed rc={rc}: {err_out.strip() or out.strip()}")

    dbg("Range request succeeded (size >= 1)")
    return 1


# ----------------------------
# Lambda Handler
# ----------------------------

def lambda_handler(event, context):
    try:
        dbg(f"Lambda invoked with event: {json.dumps(event)}")

        file_pattern = event.get("file_pattern")
        echo_folder = event.get("echo_folder")
        jenkins_url = event.get("jenkins_url")
        secret_id = event.get("secret_id")

        if not file_pattern or not echo_folder or not jenkins_url or not secret_id:
            raise ValueError("Missing required input parameters")

        echo_subfolder = event.get("echo_subfolder", "") or ""
        targets = event.get("targets", []) or []
        min_size_bytes = int(event.get("min_size_bytes", 1))
        verify_tls = bool(event.get("verify_tls", False))

        ftps_port = int(event["ftps_port"])
        ftps_mode = "implicit" if ftps_port == 990 else "explicit"

        dbg(f"FTPS resolved: port={ftps_port}, mode={ftps_mode}, verify_tls={verify_tls}")

        secret = fetch_secret(secret_id)

        host = secret["echo_ip"]
        username = secret["echo_dart_username"]
        password = secret["echo_dart_password"]

        echo_path = f"/{secret['echo_dart_path']}/{echo_folder}/in/{echo_subfolder}".rstrip("/")
        dbg(f"Resolved echo_path={echo_path}")

        filenames = curl_list_files(
            host, ftps_port, username, password, ftps_mode,
            echo_path, verify_tls=verify_tls
        )

        compiled = re.compile(file_pattern, re.I)
        targets_lc = [t.lower() for t in targets] if targets else None

        matched = []
        for name in filenames:
            m = compiled.search(name)
            if not m or not m.lastindex:
                continue
            target = m.group(1).lower()
            if targets_lc and target not in targets_lc:
                continue
            matched.append({"name": name, "target": target})

        dbg(f"Matched files after regex/target filtering: {len(matched)}")

        found = []
        for f in matched:
            path = f"{echo_path}/{f['name']}"
            size = curl_file_size(
                host, ftps_port, username, password,
                ftps_mode, path, verify_tls=verify_tls
            )
            if size >= min_size_bytes:
                found.append({**f, "content_length": size, "remote_path": path})

        dbg(f"Files meeting size threshold: {len(found)}")

        result = {
            "found": bool(found),
            "matches": found,
            "echo_path": echo_path,
        }

        if not found:
            dbg("No qualifying files found — exiting")
            return {"statusCode": 200, "body": json.dumps(result)}

        payload = {
            "source": "ftps_file_check_lambda",
            "aws_request_id": context.aws_request_id,
            **result,
        }

        jenkins_resp = http_post_json(jenkins_url, payload)
        result["jenkins_call"] = jenkins_resp

        dbg("Lambda execution complete")
        return {"statusCode": 200, "body": json.dumps(result)}

    except Exception as e:
        err("Unhandled exception in lambda_handler")
        err(repr(e))
        print(traceback.format_exc())
        raise
