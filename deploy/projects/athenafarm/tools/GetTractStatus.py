#!/usr/bin/env python3
import json
import os
import re
import sys
from datetime import datetime, timezone

try:
    import boto3
except Exception:
    print("ERROR: boto3 is required. In CloudShell run: pip3 install --user boto3", file=sys.stderr)
    sys.exit(2)


def _fmt_ms(ms):
    if not ms:
        return "-"
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def _duration_seconds(started_ms, completed_ms):
    if not started_ms:
        return None
    if completed_ms:
        return int((completed_ms - started_ms) / 1000)
    now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    return int((now_ms - started_ms) / 1000)


def _fallback_progress_pct(state, elapsed_seconds, timeout_minutes):
    if elapsed_seconds is None or timeout_minutes in (None, 0):
        return None
    if state not in {"STARTING", "RUNNING", "STOPPING", "WAITING"}:
        return None
    total_seconds = int(timeout_minutes) * 60
    if total_seconds <= 0:
        return None
    pct = int((elapsed_seconds / total_seconds) * 100)
    if pct < 0:
        pct = 0
    if pct > 99:
        pct = 99
    return pct


_PROGRESS_RE = re.compile(
    r"\[PROGRESS\]\s+progress_pct=(\d+)\s+milestone=([^\s]+)\s+elapsed_seconds=([0-9]+(?:\.[0-9]+)?)"
)


def _extract_progress(message):
    match = _PROGRESS_RE.search(message or "")
    if not match:
        return None
    return {
        "progress_pct": int(match.group(1)),
        "milestone": match.group(2),
        "elapsed_seconds": float(match.group(3)),
    }


def _latest_progress(logs, run_id, started_ms, completed_ms):
    if not started_ms:
        return None, {"status": "no_start_time"}

    end_ms = completed_ms or int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    groups = ["/aws-glue/jobs/output", "/aws-glue/jobs/error", "/aws-glue/jobs"]
    events = []
    scanned_streams = 0
    scanned_events = 0

    for group in groups:
        if not run_id or run_id == "-":
            continue
        try:
            streams_resp = logs.describe_log_streams(
                logGroupName=group,
                logStreamNamePrefix=run_id,
                orderBy="LogStreamName",
                descending=True,
                limit=20,
            )
        except Exception:
            continue

        for stream in streams_resp.get("logStreams", []):
            stream_name = stream.get("logStreamName")
            if not stream_name:
                continue
            scanned_streams += 1
            next_token = None
            page_count = 0
            while True:
                kwargs = {
                    "logGroupName": group,
                    "logStreamName": stream_name,
                    "startTime": started_ms,
                    "endTime": end_ms,
                    "startFromHead": True,
                    "limit": 1000,
                }
                if next_token:
                    kwargs["nextToken"] = next_token
                try:
                    response = logs.get_log_events(**kwargs)
                except Exception:
                    break

                page_events = response.get("events", [])
                scanned_events += len(page_events)
                events.extend(page_events)

                token = response.get("nextForwardToken")
                page_count += 1
                if not token or token == next_token or page_count >= 8:
                    break
                next_token = token

    for group in groups:
        next_token = None
        page_count = 0
        while True:
            kwargs = {
                "logGroupName": group,
                "startTime": started_ms,
                "endTime": end_ms,
                "filterPattern": "PROGRESS",
                "limit": 200,
            }
            if next_token:
                kwargs["nextToken"] = next_token

            try:
                response = logs.filter_log_events(**kwargs)
            except Exception:
                break

            page_events = response.get("events", [])
            scanned_events += len(page_events)
            events.extend(page_events)
            token = response.get("nextToken")
            page_count += 1
            if not token or token == next_token or page_count >= 5:
                break
            next_token = token

    if not events:
        return None, {
            "status": "no_events",
            "scanned_streams": scanned_streams,
            "scanned_events": scanned_events,
        }

    run_events = [event for event in events if run_id and run_id in (event.get("logStreamName") or "")]
    candidates = run_events or events

    parsed = []
    for event in candidates:
        progress = _extract_progress(event.get("message", ""))
        if progress:
            parsed.append((event.get("timestamp", 0), progress))

    if not parsed:
        return None, {
            "status": "no_progress_lines",
            "scanned_streams": scanned_streams,
            "scanned_events": scanned_events,
        }

    timestamp_ms, progress = max(parsed, key=lambda row: row[0])
    progress["event_time_utc"] = _fmt_ms(timestamp_ms)
    return progress, {
        "status": "ok",
        "scanned_streams": scanned_streams,
        "scanned_events": scanned_events,
    }


def main() -> int:
    env = os.getenv("TRACT_ENV", "PROD")
    region = os.getenv("TRACT_REGION", os.getenv("AWS_REGION", "us-east-1"))
    project = os.getenv("TRACT_PROJECT", "ATHENAFARM")
    max_results = max(1, min(int(os.getenv("TRACT_MAX_RESULTS", "25")), 100))

    env_upper = env.upper()
    project_upper = project.upper()
    default_job_name = f"FSA-{env_upper}-{project_upper}-Transform-Tract-Producer-Year"
    job_name = os.getenv("TRACT_JOB_NAME", default_job_name)

    glue = boto3.client("glue", region_name=region)
    logs = boto3.client("logs", region_name=region)

    try:
        resp = glue.get_job_runs(JobName=job_name, MaxResults=max_results)
    except glue.exceptions.EntityNotFoundException:
        print(f"ERROR: Glue job not found: {job_name}", file=sys.stderr)
        return 1
    except Exception as ex:
        print(f"ERROR: Failed to query Glue job runs for {job_name}: {ex}", file=sys.stderr)
        return 1

    runs = resp.get("JobRuns", [])
    if not runs:
        print(json.dumps({
            "environment": env_upper,
            "region": region,
            "project": project_upper,
            "job_name": job_name,
            "status": "NO_RUNS_FOUND"
        }, indent=2))
        return 0

    active_states = {"STARTING", "RUNNING", "STOPPING", "WAITING"}
    active_run = next((run for run in runs if run.get("JobRunState") in active_states), None)
    selected = active_run or runs[0]
    selected_run = "active" if active_run else "latest"

    state = selected.get("JobRunState", "UNKNOWN")
    started_on = selected.get("StartedOn")
    completed_on = selected.get("CompletedOn")
    started_ms = int(started_on.timestamp() * 1000) if started_on else None
    completed_ms = int(completed_on.timestamp() * 1000) if completed_on else None
    run_id = selected.get("Id", "-")
    progress, progress_diag = _latest_progress(logs, run_id, started_ms, completed_ms)

    elapsed_seconds = _duration_seconds(started_ms, completed_ms)
    timeout_minutes = selected.get("Timeout")
    explicit_progress_pct = progress.get("progress_pct") if progress else None
    fallback_progress_pct = _fallback_progress_pct(state, elapsed_seconds, timeout_minutes)
    effective_progress_pct = explicit_progress_pct if explicit_progress_pct is not None else fallback_progress_pct

    result = {
        "environment": env_upper,
        "region": region,
        "project": project_upper,
        "job_name": job_name,
        "selected_run": selected_run,
        "run_id": run_id,
        "state": state,
        "attempt": selected.get("Attempt"),
        "worker_type": selected.get("WorkerType"),
        "number_of_workers": selected.get("NumberOfWorkers"),
        "execution_time_seconds": selected.get("ExecutionTime"),
        "elapsed_seconds": elapsed_seconds,
        "started_on_utc": _fmt_ms(started_ms),
        "completed_on_utc": _fmt_ms(completed_ms),
        "timeout_minutes": timeout_minutes,
        "error_message": selected.get("ErrorMessage", ""),
        "runtime_progress_pct": effective_progress_pct,
        "runtime_progress_pct_source": "progress_logs" if explicit_progress_pct is not None else ("timeout_estimate" if fallback_progress_pct is not None else None),
        "runtime_progress_pct_estimate": fallback_progress_pct,
        "runtime_progress_milestone": progress.get("milestone") if progress else None,
        "runtime_progress_elapsed_seconds": progress.get("elapsed_seconds") if progress else None,
        "runtime_progress_event_time_utc": progress.get("event_time_utc") if progress else None,
        "runtime_progress_lookup_status": progress_diag.get("status") if progress_diag else None,
        "runtime_progress_scanned_streams": progress_diag.get("scanned_streams") if progress_diag else None,
        "runtime_progress_scanned_events": progress_diag.get("scanned_events") if progress_diag else None,
    }

    print(json.dumps(result, indent=2))

    if state in active_states:
        return 3
    if state in {"FAILED", "TIMEOUT", "ERROR", "STOPPED"}:
        return 4
    return 0


if __name__ == "__main__":
    raise SystemExit(main())