import json
import base64
import subprocess
from datetime import datetime, timezone
from typing import Tuple, List

import boto3

# ----------------------------
# Helpers (curl + secrets)
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


def _sanitize_cmd_for_logs(cmd: List[str]) -> List[str]:
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


def run_cmd(cmd: List[str], timeout: int = 120, debug: bool = False) -> Tuple[int, str, str]:
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


def curl_url(host: str, port: int, mode: str, path: str, debug: bool = False) -> str:
    """
    mode:
      - "implicit" => scheme ftps (typically port 990)
      - "explicit" => scheme ftp (typically port 21) with --ssl-reqd
    """
    if not path.startswith("/"):
        path = "/" + path
    scheme = "ftps" if mode == "implicit" else "ftp"
    url = f"{scheme}://{host}:{port}{path}"
    if debug:
        print(f"[DEBUG] Constructed curl URL: {url}")
    return url


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
