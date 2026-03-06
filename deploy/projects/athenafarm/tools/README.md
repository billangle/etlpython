# athenafarm tools

## Get Tract job status (CloudShell)

This folder provides a CloudShell-ready status checker for the Glue Tract job.

- `GetTractStatus.py` — boto3 status query implementation
- `get_tract_status.sh` — wrapper that sets environment variables and calls Python

## Quick start

```bash
cd deploy/projects/athenafarm/tools
chmod +x GetTractStatus.py get_tract_status.sh
./get_tract_status.sh
```

Default behavior:
- `TRACT_ENV=PROD`
- `TRACT_REGION=us-east-1`
- `TRACT_PROJECT=ATHENAFARM`
- `TRACT_JOB_NAME=FSA-PROD-ATHENAFARM-Transform-Tract-Producer-Year`
- `TRACT_MAX_RESULTS=25`

## Override with arguments

```bash
./get_tract_status.sh STEAMDEV us-east-1
./get_tract_status.sh PROD us-east-1 ATHENAFARM
./get_tract_status.sh PROD us-east-1 ATHENAFARM FSA-PROD-ATHENAFARM-Transform-Tract-Producer-Year 50
```

Positional args:
1. `ENV`
2. `REGION`
3. `PROJECT`
4. `JOB_NAME`
5. `MAX_RESULTS`

## Override with environment variables

```bash
export TRACT_ENV=PROD
export TRACT_REGION=us-east-1
export TRACT_PROJECT=ATHENAFARM
export TRACT_JOB_NAME=FSA-PROD-ATHENAFARM-Transform-Tract-Producer-Year
export TRACT_MAX_RESULTS=25
./get_tract_status.sh
```

## Exit codes

- `0` — latest/selected run is successful or no runs found
- `3` — run is active (`STARTING`, `RUNNING`, `STOPPING`, `WAITING`)
- `4` — run failed (`FAILED`, `TIMEOUT`, `ERROR`, `STOPPED`)
- `1` — API/job lookup error
- `2` — dependency error (for example missing boto3)

## Runtime progress fields

When progress log lines are available in CloudWatch, the JSON also includes:

- `runtime_progress_pct`
- `runtime_progress_milestone`
- `runtime_progress_elapsed_seconds`
- `runtime_progress_event_time_utc`

Progress percent behavior:

- `runtime_progress_pct` uses progress logs when present.
- If progress logs are missing, it falls back to a timeout-based estimate (`elapsed_seconds / (timeout_minutes * 60)` capped at 99 while running).
- `runtime_progress_pct_source` indicates `progress_logs` or `timeout_estimate`.
- `runtime_progress_pct_estimate` always contains the timeout-based estimate when calculable.

Lookup diagnostics are also included:

- `runtime_progress_lookup_status` (`ok`, `no_events`, `no_progress_lines`, `no_start_time`)
- `runtime_progress_scanned_streams`
- `runtime_progress_scanned_events`

If no progress lines are found yet, these fields are `null`.
