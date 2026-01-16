import json
import base64
import urllib.request
import urllib.error
import traceback


# ----------------------------
# HTTP Helpers
# ----------------------------

def _basic_auth_header(login: str, password: str) -> str:
    token = base64.b64encode(f"{login}:{password}".encode("utf-8")).decode("ascii")
    return f"Basic {token}"


def http_post_json(
    url: str,
    payload: dict,
    timeout_seconds: int = 10,
    debug: bool = False,
    basic_auth: tuple[str, str] | None = None,
) -> dict:
    """
    POST JSON payload to url with optional HTTP Basic Auth.

    Returns dict:
      { ok: bool, status: int|None, error: str|None, response_body: str }
    """
    if debug:
        print(
            f"[DEBUG] Posting JSON: url={url}, timeout={timeout_seconds}s, "
            f"basic_auth={'enabled' if basic_auth else 'disabled'}"
        )

    data = json.dumps(payload).encode("utf-8")

    headers = {
        "Content-Type": "application/json",
        "User-Agent": "aws-lambda/http-webhook",
    }

    if basic_auth:
        login, password = basic_auth
        headers["Authorization"] = _basic_auth_header(login, password)

    req = urllib.request.Request(
        url=url,
        data=data,
        headers=headers,
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            if debug:
                print(f"[DEBUG] HTTP response status={resp.status}")
            return {"ok": True, "status": resp.status, "response_body": body[:4000]}
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace") if getattr(e, "fp", None) else ""
        if debug:
            print(f"[ERROR] HTTP error: {e.code}")
        return {"ok": False, "status": e.code, "error": str(e), "response_body": body[:4000]}
    except Exception as e:
        if debug:
            print(f"[ERROR] HTTP request failed: {repr(e)}")
        return {"ok": False, "status": None, "error": str(e), "response_body": ""}


# ----------------------------
# Lambda Handler
# ----------------------------

def lambda_handler(event, context):
    """
    Behavior:
      - POST the *input event* as JSON to event['url'] (or event['jenkins_url'] for backward-compat)
      - If jenkins_user + jenkins_password are provided in the event, use HTTP Basic Auth
      - Continue to log requests/results similarly to the prior script:
          - ALWAYS print "Result summary: ..." on success path
          - ALWAYS print "Jenkins call complete: ..." after POST attempt

    Required event fields:
      - url  (preferred) OR jenkins_url (legacy)

    Optional event fields:
      - debug: bool
      - timeout_seconds: int (default 10)
      - jenkins_user: str  (basic auth username)
      - jenkins_password: str (basic auth password / API token)
    """
    debug = bool(event.get("debug", False))

    def dbg(msg: str):
        if debug:
            print(f"[DEBUG] {msg}")

    def err(msg: str):
        print(f"[ERROR] {msg}")

    try:
        dbg(f"Lambda invoked with event: {json.dumps(event)}")

        # URL (keep legacy key support)
        url = event.get("url") or event.get("jenkins_url")
        if not url:
            raise ValueError("Missing required 'url' (or legacy 'jenkins_url')")

        timeout_seconds = int(event.get("timeout_seconds", event.get("jenkins_timeout_seconds", 10)))

        # Optional Basic Auth (requested names)
        jenkins_user = event.get("jenkins_user")
        jenkins_password = event.get("jenkins_password")
        basic_auth = (jenkins_user, jenkins_password) if jenkins_user and jenkins_password else None

        # ---- CRITICAL SUCCESS LOG LINE (ALWAYS ON SUCCESS PATH) ----
        # Keep a stable, parseable line like before.
        print(
            "Result summary: "
            f"url={url} "
            f"basic_auth={'true' if basic_auth else 'false'} "
            f"timeout_seconds={timeout_seconds}"
        )

        # POST the *event itself* as the request payload
        resp = http_post_json(
            url=url,
            payload=event,
            timeout_seconds=timeout_seconds,
            debug=debug,
            basic_auth=basic_auth,
        )

        # ---- CRITICAL SUCCESS LOG LINE (ALWAYS AFTER POST ATTEMPT) ----
        print(
            "Jenkins call complete: "
            f"URL={url} "
            f"ok={resp['ok']} "
            f"status={resp['status']} "
            f"body={resp.get('response_body')}"
        )

        # Return a body that includes the POST outcome while preserving the original event
        result = {
            "posted_to": url,
            "basic_auth_used": bool(basic_auth),
            "aws_request_id": getattr(context, "aws_request_id", None),
            "function_name": getattr(context, "function_name", None),
            "log_stream_name": getattr(context, "log_stream_name", None),
            "request": event,
            "response": {
                "ok": resp["ok"],
                "status": resp["status"],
                "error": resp.get("error"),
                "response_body": resp.get("response_body"),
            },
        }

        return {"statusCode": 200, "body": json.dumps(result)}

    except Exception as e:
        print("Unhandled exception in lambda_handler")
        err(repr(e))
        print(traceback.format_exc())
        raise
