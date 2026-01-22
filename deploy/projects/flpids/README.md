# Fpac FLPIDS DynaCheckFile Lambda

## Overview

`FSA-steam-dev-FpacFLPIDS-DynaCheckFile` is an AWS Lambda function that monitors an FTPS directory for expected inbound files and records inspection activity in DynamoDB.

It replaces direct Jenkins triggering with a durable, queryable inspection record while supporting repeated checks without duplicating work.

---

## High-Level Flow

1. Resolve FTPS credentials from AWS Secrets Manager  
2. List files in the remote directory via `curl`  
3. Match files using a regex pattern and minimum size check  
4. Query DynamoDB for the latest job for the project  
5. Create or update a DynamoDB row based on job status  

---

## Required Event Fields

```json
{
  "file_pattern": "regex",
  "echo_folder": "plas | gls | nats",
  "pipeline": "Human readable pipeline name",
  "project_name": "Project identifier",
  "secret_id": "Secrets Manager secret name",
  "ftps_port": 21 | 990
}
```

---

## DynamoDB Configuration

```json
{
  "dynamodb": {
    "table_name": "FSA-FileChecks",
    "partition_key": "jobId",
    "sort_key": "project",
    "project_gsi_name": "project_gsi_name"
  }
}
```

### Table Expectations

| Field | Description |
|-----|-------------|
| jobId | Partition key (UUID) |
| project | Sort key (from project_name) |
| pipeline | Copied from event |
| status | TRIGGERED / PROCESSING / COMPLETED / ERROR |
| inspectedStatusDate | Last inspection time |
| inspectedCount | Number of inspections |
| createdAt | Job creation time |

---

## Status-Based Behavior

### New Row Creation
A new row is created when:
- No prior row exists for the project, or
- Latest row status is `COMPLETED` or `ERROR`

New rows are created with:
- `status = TRIGGERED`
- `inspectedCount = 1`
- `inspectedStatusDate = now`

### Existing Row Update
If latest status is `TRIGGERED` or `PROCESSING`:
- `inspectedStatusDate` is updated
- `inspectedCount` is incremented
- `status` is not changed

---

## Testing Overview

Testing is done with POSIX-compliant shell scripts that:
- Run on macOS and Amazon CloudShell
- Use `aws lambda invoke`
- Build JSON payloads using `python3`
- Reflect real Jenkins job configurations

---

## Available Test Scripts

| Script | Jenkins Job | Schedule |
|------|------------|----------|
| test_rc540_weekly.sh | FSA-PROD-DART-ECHO-FETCH-RC540-WEEKLY | 0 12 * * 7 |
| test_rc540_monthly.sh | FSA-PROD-DART-ECHO-FETCH-RC540-MONTHLY | 0 6 1 * * |
| test_flpids_scims.sh | FSA-PROD-DART-ECHO-FETCH-FLPIDS-SCIMS | 30 23 * * 2-6 |
| test_flpidsrpt.sh | FSA-PROD-DART-ECHO-FETCH-FLPIDSRPT | 05 4 * * 1-6 |
| test_flpidsload.sh | FSA-PROD-DART-ECHO-FETCH-FLPIDSLOAD | 0 3 * * 2-6 |
| test_flpids_nats.sh | FSA-PROD-DART-ECHO-FETCH-FLPIDS-NATS | 45 2 * * 2-6 |

---

## Notes

- The Lambda is safe to run repeatedly on a schedule.
- Downstream systems should update job status to PROCESSING and COMPLETED.
- All inspection activity is retained for auditing and diagnostics.
