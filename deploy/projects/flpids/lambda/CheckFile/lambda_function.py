import json
import re
import subprocess
import urllib.request
import urllib.error
import urllib.parse
import base64
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
        return json.loads(resp["SecretString"])

    if "SecretBinary" in resp and resp["SecretBinary"]:
        return json.loads(base64.b64decode(resp["SecretBinary"]).decode("utf-8"))

    raise RuntimeError(f"Secret {secret_id} had no SecretString/SecretBinary")


def http_post_json(
    url: str,
    payload: dict,
    timeout_seconds: int = 10,
    debug: bool = False,
    basic_auth: tuple[str, str] | None = None,
) -> dict:
    """
    POST JSON payload to url with optional HTTP Basic Auth.
    """
    if debug:
        print(f"[DEBUG] Posting JSON to Jenkins: url={url}, timeout={timeout_seconds}s, basic_auth={'yes' if basic_auth else 'no'}")

    data = json.dumps(payload).encode("utf-8")

    headers = {
        "Content-Type": "application/json",
        "User-Agent": "aws-lambda/ftps-file-check",
    }

    if basic_auth:
        user, pwd = basic_auth
        token = base64.b64encode(f"{user}:{pwd}".encode("utf-8")).decode("ascii")
        headers["Authorization"] = f"Basic {token}"

    req = urllib.request.Request(
        url=url,
        data=data,
        headers=headers,
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


def _sanitize_cmd_for_logs(cmd: list[str]) -> list[str]:
    safe = []
    i = 0
    while i < len(cmd):
        if cmd[i] == "--user" and i + 1 < len(cmd):
            safe.extend(["--user", "<redacted>"])
            i += 2
        else:
            safe.append(cmd[i])
            i += 1
    return safe


def _run(cmd: list[str], timeout: int = 30, debug: bool = False) -> tuple[int, str, str]:
    if debug:
        print(f"[DEBUG] Running command: {' '.join(_sanitize_cmd_for_logs(cmd))}")

    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    return p.returncode, p.stdout, p.stderr


def _curl_url(host: str, port: int, mode: str, path: str) -> str:
    if not path.startswith("/"):
        path = "/" + path
    scheme = "ftps" if mode == "implicit" else "ftp"
    return f"{scheme}://{host}:{port}{path}"


def _looks_like_missing_file(err_text: str) -> bool:
    t = (err_text or "").lower()
    return any(x in t for x in ("no such file", "not found", "550", "does not exist"))


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

    if not remote_dir.endswith("/"):
        remote_dir += "/"

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
        cmd.append("--insecure")

    cmd.append(_curl_url(host, port, mode, remote_dir))

    rc, out, err = _run(cmd, timeout, debug)
    if rc != 0:
        raise RuntimeError(f"curl list failed: {err or out}")

    return [l.strip() for l in out.splitlines() if l.strip()]


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

    cmd.append(_curl_url(host, port, mode, remote_path))

    rc, out, err = _run(cmd, timeout, debug)
    if rc == 0:
        m = re.search(r"(?im)^Content-Length:\s*(\d+)", out)
        if m:
            return int(m.group(1))

    if _looks_like_missing_file(err or out):
        return None

    # Range fallback
    cmd = [
        "curl",
        "--silent",
        "--show-error",
        "--fail",
        "--range", "0-0",
        "--user", f"{username}:{password}",
        "--ssl-reqd",
        "--ftp-pasv",
        "--tlsv1.2",
    ]

    if not verify_tls:
        cmd.append("--insecure")

    cmd.append(_curl_url(host, port, mode, remote_path))

    rc, _, err = _run(cmd, timeout, debug)
    if rc != 0:
        if _looks_like_missing_file(err):
            return None
        raise RuntimeError(f"curl range failed: {err}")

    return 1


# ----------------------------
# Lambda Handler
# ----------------------------

def lambda_handler(event, context):
    debug = bool(event.get("debug", False))

    try:
        file_pattern = event["file_pattern"]
        echo_folder = event["echo_folder"]
        jenkins_url = event["jenkins_url"]
        secret_id = event["secret_id"]
        ftps_port = int(event["ftps_port"])

        # Optional Jenkins Basic Auth
        jenkins_login = event.get("jenkins_login")
        jenkins_password = event.get("jenkins_password")
        jenkins_auth = (
            (jenkins_login, jenkins_password)
            if jenkins_login and jenkins_password
            else None
        )

        jenkins_file_count_param = event.get("jenkins_file_count_param", "file_count")

        ftps_mode = "implicit" if ftps_port == 990 else "explicit"

        secret = fetch_secret(secret_id, debug)
        host = secret["echo_ip"]
        username = secret["echo_dart_username"]
        password = secret["echo_dart_password"]

        echo_subfolder = event.get("echo_subfolder", "")
        echo_path = f"/{secret['echo_dart_path']}/{echo_folder}/in/{echo_subfolder}".rstrip("/")

        filenames = curl_list_files(
            host, ftps_port, username, password, ftps_mode,
            echo_path,
            verify_tls=bool(event.get("verify_tls", False)),
            debug=debug,
        )

        total_file_count = len(filenames)

        regex = re.compile(file_pattern, re.I)

        first_found = None
        for name in filenames:
            if not regex.search(name):
                continue

            size = curl_file_size(
                host, ftps_port, username, password, ftps_mode,
                f"{echo_path}/{name}",
                verify_tls=bool(event.get("verify_tls", False)),
                debug=debug,
            )

            if size and size > 0:
                first_found = {
                    "name": name,
                    "content_length": size,
                    "remote_path": f"{echo_path}/{name}",
                }
                break

        result = {
            "found": bool(first_found),
            "match": first_found,
            "checked_path": echo_path,
            "total_file_count": total_file_count,
        }

        if not first_found:
            return {"statusCode": 200, "body": json.dumps(result)}

        # Add file_count to Jenkins URL
        parsed = urllib.parse.urlsplit(jenkins_url)
        q = dict(urllib.parse.parse_qsl(parsed.query))
        q[jenkins_file_count_param] = str(total_file_count)

        jenkins_url = urllib.parse.urlunsplit(
            (parsed.scheme, parsed.netloc, parsed.path, urllib.parse.urlencode(q), parsed.fragment)
        )

        resp = http_post_json(
            jenkins_url,
            payload={
                **result,
                "aws_request_id": context.aws_request_id,
                "function_name": context.function_name,
            },
            timeout_seconds=int(event.get("jenkins_timeout_seconds", 10)),
            debug=debug,
            basic_auth=jenkins_auth,
        )

        result["jenkins_call"] = resp
        return {"statusCode": 200, "body": json.dumps(result)}

    except Exception:
        print(traceback.format_exc())
        raise
