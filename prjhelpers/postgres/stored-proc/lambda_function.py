from __future__ import annotations

import json
import os
import re
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import boto3
import psycopg2
import psycopg2.extras


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)

def safe_filename(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", name)

def md_escape(s: str) -> str:
    return (s or "").replace("|", "\\|")


# ----------------------------
# AWS helpers
# ----------------------------

def get_glue_connection(name: str) -> Dict[str, Any]:
    glue = boto3.client("glue")
    return glue.get_connection(Name=name, HidePassword=True)["Connection"]

def get_secret_json(secret_id: str, region: Optional[str] = None) -> Dict[str, Any]:
    sm = boto3.client("secretsmanager", region_name=region) if region else boto3.client("secretsmanager")
    resp = sm.get_secret_value(SecretId=secret_id)
    if resp.get("SecretString"):
        return json.loads(resp["SecretString"])
    if resp.get("SecretBinary"):
        import base64
        raw = base64.b64decode(resp["SecretBinary"]).decode("utf-8")
        return json.loads(raw)
    raise RuntimeError(f"Secret {secret_id} had no SecretString/SecretBinary")


# ----------------------------
# Build Postgres connection dict from your secret schema
# ----------------------------

def build_pg_conn_dict_from_secret(secret_dict: Dict[str, Any],
                                  secret_prefix: str,
                                  sslmode: str = "require") -> Dict[str, Any]:
    """
    Matches your Glue job pattern:
      { '<prefix>_postgres_database_name', '<prefix>_postgres_username', '<prefix>_postgres_password', '<prefix>_postgres_hostname', 'postgres_port' }

    Example prefix: 'edv'  => keys:
      edv_postgres_database_name
      edv_postgres_username
      edv_postgres_password
      edv_postgres_hostname
      postgres_port
    """
    db_key = f"{secret_prefix}_postgres_database_name"
    user_key = f"{secret_prefix}_postgres_username"
    pw_key = f"{secret_prefix}_postgres_password"
    host_key = f"{secret_prefix}_postgres_hostname"

    missing = [k for k in (db_key, user_key, pw_key, host_key, "postgres_port") if k not in secret_dict]
    if missing:
        raise RuntimeError(
            f"Secret JSON missing required keys for prefix '{secret_prefix}': {missing}"
        )

    return {
        "dbname": secret_dict[db_key],
        "user": secret_dict[user_key],
        "password": secret_dict[pw_key],
        "host": secret_dict[host_key],
        "port": int(secret_dict["postgres_port"]),
        "sslmode": sslmode,
    }


# ----------------------------
# pg_stat_activity query
# ----------------------------

def proc_match_patterns(proc: str, match_mode: str) -> List[str]:
    proc = proc.strip()
    if match_mode == "any":
        return [f"%{proc}%"]
    if match_mode == "call":
        return [f"%call {proc}%", f"%CALL {proc}%"]
    if match_mode == "select":
        return [f"%select {proc}%", f"%SELECT {proc}%"]
    return [f"%{proc}%"]

def fetch_activity(pg_conn, proc: str, match_mode: str) -> List[Dict[str, Any]]:
    patterns = proc_match_patterns(proc, match_mode)
    where = " OR ".join(["query ILIKE %s" for _ in patterns])

    sql = f"""
    SELECT
      pid,
      usename,
      application_name,
      client_addr,
      state,
      wait_event_type,
      wait_event,
      backend_type,
      datname,
      query_start,
      xact_start,
      state_change,
      now() - query_start AS runtime,
      left(query, 4000) AS query
    FROM pg_stat_activity
    WHERE ({where})
      AND pid <> pg_backend_pid()
    ORDER BY query_start NULLS LAST;
    """
    with pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, patterns)
        return [dict(r) for r in cur.fetchall()]

def fetch_blocking(pg_conn, pid: int) -> Tuple[List[int], List[Dict[str, Any]]]:
    with pg_conn.cursor() as cur:
        cur.execute("SELECT pg_blocking_pids(%s);", (pid,))
        blockers = cur.fetchone()[0] or []

    details: List[Dict[str, Any]] = []
    if blockers:
        sql = """
        SELECT
          pid,
          usename,
          application_name,
          client_addr,
          state,
          wait_event_type,
          wait_event,
          now() - query_start AS runtime,
          left(query, 1000) AS query
        FROM pg_stat_activity
        WHERE pid = ANY(%s)
        ORDER BY query_start NULLS LAST;
        """
        with pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (blockers,))
            details = [dict(r) for r in cur.fetchall()]

    return blockers, details


# ----------------------------
# Markdown output
# ----------------------------

def render_md(proc: str,
              match_mode: str,
              glue_conn_name: str,
              secret_name: str,
              secret_prefix: str,
              pg_conn_dict: Dict[str, Any],
              activities: List[Dict[str, Any]]) -> str:
    lines: List[str] = []
    lines.append(f"# Stored Procedure Status: `{proc}`")
    lines.append("")
    lines.append(f"- Generated (UTC): `{utc_now().isoformat()}`")
    lines.append(f"- Glue connection (VPC): `{md_escape(glue_conn_name)}`")
    lines.append(f"- Secret: `{md_escape(secret_name)}`")
    lines.append(f"- Secret prefix: `{md_escape(secret_prefix)}`")
    lines.append(f"- DB: `{md_escape(str(pg_conn_dict.get('dbname','')))}` on `{md_escape(str(pg_conn_dict.get('host','')))}`:`{pg_conn_dict.get('port')}`")
    lines.append(f"- Match mode: `{match_mode}` (searches `pg_stat_activity.query` text)")
    lines.append("")

    if not activities:
        lines.append("## Result")
        lines.append("")
        lines.append("No active sessions matched the procedure pattern.")
        lines.append("")
        return "\n".join(lines)

    lines.append(f"## Matching Sessions ({len(activities)})")
    lines.append("")
    lines.append("| PID | User | App | State | Wait | Runtime | Query start (UTC) |")
    lines.append("|---:|---|---|---|---|---:|---|")
    for a in activities:
        qstart = a.get("query_start")
        qstart_s = qstart.isoformat() if hasattr(qstart, "isoformat") and qstart else ""
        wait = md_escape(f"{a.get('wait_event_type') or ''}:{a.get('wait_event') or ''}".strip(":"))
        lines.append(
            f"| `{a.get('pid')}` | `{md_escape(str(a.get('usename','')))}` | `{md_escape(str(a.get('application_name','')))}`"
            f" | `{md_escape(str(a.get('state','')))}` | `{wait}` | `{md_escape(str(a.get('runtime','')))}` | `{qstart_s}` |"
        )

    lines.append("")
    lines.append("## Details")
    lines.append("")
    for a in activities:
        pid = a.get("pid")
        lines.append(f"### PID `{pid}`")
        lines.append("")
        lines.append(f"- **State:** `{md_escape(str(a.get('state','')))}`")
        wait_str = f"{a.get('wait_event_type') or ''}:{a.get('wait_event') or ''}".strip(':')
        lines.append(f"- **Wait:** `{md_escape(wait_str)}`")
        lines.append(f"- **Runtime:** `{md_escape(str(a.get('runtime','')))}`")
        lines.append("")
        lines.append("```sql")
        lines.append((a.get("query") or "").rstrip())
        lines.append("```")
        lines.append("")
    return "\n".join(lines)


# ----------------------------
# Lambda handler
# ----------------------------

def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    proc = (event.get("proc") or "").strip()
    if not proc:
        raise ValueError("event.proc is required (e.g., my_schema.my_proc)")

    match_mode = (event.get("match") or "call").strip().lower()
    if match_mode not in ("call", "select", "any"):
        match_mode = "call"

    # Your pattern: pass secret_name; also pass secret_prefix (like 'edv')
    secret_name = (event.get("secret_name") or os.environ.get("SECRET_NAME") or "").strip()
    if not secret_name:
        raise ValueError("event.secret_name (or env SECRET_NAME) is required")

    secret_prefix = (event.get("secret_prefix") or os.environ.get("SECRET_PREFIX") or "").strip()
    if not secret_prefix:
        raise ValueError("event.secret_prefix (or env SECRET_PREFIX) is required (example: 'edv')")

    aws_region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-east-1"

    glue_conn_name = os.environ.get("GLUE_CONNECTION_NAME", "FSA-PROD-PG-DART115")
    out_bucket = os.environ.get("OUTPUT_BUCKET", "fsa-prod-ops")
    out_prefix = os.environ.get("OUTPUT_PREFIX", "postgres-analysis").strip("/")

    # Load secret JSON & build psycopg2 connection dict exactly like your Glue job
    secret_dict = get_secret_json(secret_name, region=aws_region)
    pg_conn_dict = build_pg_conn_dict_from_secret(secret_dict, secret_prefix, sslmode="require")

    # Connect from inside VPC
    pg_conn = psycopg2.connect(**pg_conn_dict)
    pg_conn.autocommit = True

    try:
        activities = fetch_activity(pg_conn, proc, match_mode)
        for a in activities:
            blockers, details = fetch_blocking(pg_conn, int(a["pid"]))
            a["blocking_pids"] = blockers
            a["blocking_details"] = details

        md = render_md(
            proc=proc,
            match_mode=match_mode,
            glue_conn_name=glue_conn_name,
            secret_name=secret_name,
            secret_prefix=secret_prefix,
            pg_conn_dict=pg_conn_dict,
            activities=activities,
        )

        # Add blocking section
        if activities:
            md += "\n\n## Blocking / Locks\n\n"
            for a in activities:
                pid = a["pid"]
                blockers = a.get("blocking_pids") or []
                md += f"### PID `{pid}`\n\n"
                if not blockers:
                    md += "- No blocking PIDs detected.\n\n"
                    continue
                md += f"- Blocking PIDs: `{blockers}`\n\n"

        key = f"{out_prefix}/{safe_filename(proc)}.details"
        boto3.client("s3").put_object(
            Bucket=out_bucket,
            Key=key,
            Body=md.encode("utf-8"),
            ContentType="text/markdown; charset=utf-8",
        )

        return {
            "status": "ok",
            "s3_bucket": out_bucket,
            "s3_key": key,
            "matched_sessions": len(activities),
        }
    finally:
        try:
            pg_conn.close()
        except Exception:
            pass