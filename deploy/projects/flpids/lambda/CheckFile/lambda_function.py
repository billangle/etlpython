import os
import json
import urllib.request
import urllib.error
import boto3

from ftps_client import iFTP_TLS


def fetch_secret(secret_id: str) -> dict:
    client = boto3.client("secretsmanager")
    resp = client.get_secret_value(SecretId=secret_id)

    if "SecretString" in resp and resp["SecretString"]:
        return json.loads(resp["SecretString"])

    # if stored as binary
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


def lambda_handler(event, context):
    """
    Required event fields:
      - file_pattern: regex with >=1 capture group (group1=target, group2 optional date)
      - echo_folder: e.g. "plas", "gls", "nats"
      - pipeline, step: passed through to webhook payload (for traceability)
      - jenkins_url: webhook URL to call when a matching non-empty file is found

    Optional:
      - echo_subfolder: default ""
      - targets: [] filter by target group values
      - min_size_bytes: default 1 (means >0)
      - jenkins_timeout_seconds: default 10
      - header, to_queue: accepted and passed through (not used in check logic)
    """
    print("Input event:", json.dumps(event))

    file_pattern = event.get("file_pattern")
    echo_folder = event.get("echo_folder")
    jenkins_url = event.get("jenkins_url")

    if not file_pattern:
        return {"statusCode": 400, "body": json.dumps({"error": "Missing required 'file_pattern'."})}
    if not echo_folder:
        return {"statusCode": 400, "body": json.dumps({"error": "Missing required 'echo_folder'."})}
    if not jenkins_url:
        return {"statusCode": 400, "body": json.dumps({"error": "Missing required 'jenkins_url'."})}

    echo_subfolder = event.get("echo_subfolder", "") or ""
    targets = event.get("targets", []) or []
    min_size_bytes = int(event.get("min_size_bytes", 1))
    timeout_seconds = int(event.get("jenkins_timeout_seconds", 10))

    # Secrets
    secret_id = event.get("secret_id")
    secret = fetch_secret(secret_id)   # using the corrected fetch_secret above


    echo_connection = {
        "host": secret["echo_ip"],
        "port": int(str(secret["echo_port"]).strip()),
        "username": secret["echo_dart_username"],
        "password": secret["echo_dart_password"],
        "log_level": 0,
    }


    echo_path = "/{root}/{folder}/in/{subfolder}".format(
        root=secret["echo_dart_path"],
        folder=echo_folder,
        subfolder=echo_subfolder,
    ).rstrip("/")

    print({"echo_path": echo_path, "targets": targets, "min_size_bytes": min_size_bytes})

    # List / filter / match
    with iFTP_TLS(timeout=10) as ftps:
        ftps.make_connection(**echo_connection)
        ftps.cwd(echo_path)

        # filter_entries supports your patterns:
        # - group(1)=target
        # - group(2) optional: YYYYMMDD or YYYYMMDDHHMMSS
        files_df = ftps.filter_entries(file_pattern=file_pattern, path="", targets=targets)
        files = files_df.rows(named=True) if files_df.height > 0 else []

    found_files = [f for f in files if int(f.get("content_length", 0)) >= min_size_bytes]

    result = {
        "found": bool(found_files),
        "checked_path": echo_path,
        "file_pattern": file_pattern,
        "targets": targets,
        "min_size_bytes": min_size_bytes,
        "matches": [
            {
                "name": f.get("name"),
                "content_length": int(f.get("content_length", 0)),
                "last_modified": str(f.get("last_modified")),
                "target": f.get("target"),
                "system_date": str(f.get("system_date")),
            }
            for f in found_files
        ],
        # passthrough fields (handy for traceability / routing)
        "pipeline": event.get("pipeline"),
        "step": event.get("step"),
        "echo_folder": echo_folder,
        "echo_subfolder": echo_subfolder,
        "header": event.get("header"),
        "to_queue": event.get("to_queue"),
    }

    print("Check result:", json.dumps(result))

    if not found_files:
        return {"statusCode": 200, "body": json.dumps(result)}

    # Trigger Jenkins webhook
    jenkins_payload = {
        "source": "ftps_file_check_lambda",
        "aws_request_id": getattr(context, "aws_request_id", None),
        "function_name": getattr(context, "function_name", None),
        "log_stream_name": getattr(context, "log_stream_name", None),
        **result,
    }

    jenkins_resp = http_post_json(jenkins_url, jenkins_payload, timeout_seconds=timeout_seconds)
    result["jenkins_call"] = {
        "url": jenkins_url,
        "ok": jenkins_resp["ok"],
        "status": jenkins_resp["status"],
        "error": jenkins_resp.get("error"),
        "response_body": jenkins_resp.get("response_body"),
    }

    print("Jenkins response:", json.dumps(result["jenkins_call"]))

    return {"statusCode": 200, "body": json.dumps(result)}
