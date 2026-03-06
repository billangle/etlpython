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
