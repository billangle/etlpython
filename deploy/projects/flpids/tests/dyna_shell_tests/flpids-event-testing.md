# FLPIDS Event Testing

This document describes the **six test executions** (Lambda invoke scripts) and the **data load script** used to validate the FLPIDS “event” flow end-to-end:  
**file detection → DynamoDB state → downstream trigger (Jenkins/pipeline)**.

---

## What you’re testing

These test scripts exercise the same Lambda function:

- **Lambda under test:** `FSA-steam-dev-FpacFLPIDS-DynaCheckFile`
- **Invocation method:** `aws lambda invoke` with a generated JSON payload
- **Primary behaviors validated**
  1. **Regex file matching** against a remote folder (via secret-configured FTPS/SFTP details).
  2. **Minimum-size filtering** (`min_size_bytes`).
  3. **Project/job naming** derived from Jenkins job name.
  4. **DynamoDB writes** (table + keys + GSI name are included in the payload).
  5. **Response shape + logs** that confirm “found/matched” behavior.

The **data load script** triggers a separate Lambda used to load test data so that the checkfile tests have something to discover.

---

## Files covered by this document

### Six test execution scripts

1. `test_flpids_nats.sh` – NATS file pattern and folder defaults  
2. `test_flpids_scims.sh` – SCIMS pattern and folder defaults  
3. `test_flpidsload.sh` – general “load” pattern for GLS folder  
4. `test_flpidsrpt.sh` – RPT pattern (timestamped suffix)  
5. `test_rc540_monthly.sh` – RC540 monthly pattern  
6. `test_rc540_weekly.sh` – RC540 weekly pattern and **weekly** subfolder

### Data load script

- `loaddata.sh` – invokes `FSA-steam-dev-FpacFLPIDS-TestFileLoader` with payload from `loadpunk.json`

---

## Prerequisites

### Local / Shell requirements

- POSIX shell (`/bin/sh`)  
- `aws` CLI configured (credentials + default region or explicit `--region`)  
- `python3` available (used to generate JSON payload safely)  
- Permission to invoke Lambda and (optionally) read logs/DynamoDB for verification

### AWS/IAM permissions (typical)

You’ll usually need:

- `lambda:InvokeFunction` on the functions:
  - `FSA-steam-dev-FpacFLPIDS-DynaCheckFile`
  - `FSA-steam-dev-FpacFLPIDS-TestFileLoader`
- For verification steps (recommended but optional):
  - `logs:FilterLogEvents` / `logs:GetLogEvents` for Lambda log groups
  - `dynamodb:GetItem`, `dynamodb:Query` (and possibly `Scan` for troubleshooting) on `FSA-FileChecks`

---

## Common payload structure (all six tests)

Each test script builds a payload like:

```json
{
  "file_pattern": "...regex...",
  "echo_folder": "...",
  "echo_subfolder": "...",
  "pipeline": "...",
  "project_name": "...derived...",
  "secret_id": "...",
  "ftps_port": 21,
  "min_size_bytes": 1,
  "verify_tls": false,
  "curl_timeout_seconds": 30,
  "debug": true,
  "dynamodb": {
    "table_name": "FSA-FileChecks",
    "partition_key": "jobId",
    "sort_key": "project",
    "project_gsi_name": "project_gsi_name"
  }
}
```

### Key conventions

- `project_name` is derived from the **Jenkins job name** by stripping everything up through `ECHO-FETCH-`.
- DynamoDB key schema is communicated to the Lambda via the payload:
  - partition key = `jobId`
  - sort key = `project`
- `secret_id` is used by the Lambda to locate connection details (for the remote file system).

---

## Test execution process (step-by-step)

### 1) Make scripts executable

```sh
chmod +x test_flpids_nats.sh test_flpids_scims.sh test_flpidsload.sh   test_flpidsrpt.sh test_rc540_monthly.sh test_rc540_weekly.sh loaddata.sh
```

### 2) (Optional) Load test data first

Run the loader so there is data to detect:

```sh
./loaddata.sh
```

This writes `response.json` and depends on a local JSON payload file named `loadpunk.json` existing in the current directory.

### 3) Run one or more of the six checkfile tests

Example:

```sh
./test_rc540_weekly.sh
```

Each script:

1. Creates a temporary JSON payload file under `/tmp`
2. Invokes the Lambda with `--cli-binary-format raw-in-base64-out`
3. Writes the response to `response.json` (or `$RESP_FILE` if overridden)
4. Prints the response to stdout

### 4) Validate results

Validate at *three* levels (recommended):

1. **CLI response file**: ensure Lambda returned expected JSON
2. **CloudWatch Logs**: confirm “found/matched” outcome and any downstream call
3. **DynamoDB**: verify the state row(s) were created/updated as expected

---

## The six tests

### Test 1 — FLPIDS NATS

**Script:** `test_flpids_nats.sh`  
**Jenkins job name:** `FSA-PROD-DART-ECHO-FETCH-FLPIDS-NATS`  
**Defaults:**
- `echo_folder`: `nats`
- `echo_subfolder`: *(blank)*
- `pipeline`: `FLPIDS_ODS`
- `file_pattern`: `^(FILE_NATS_\w+)\.(\d{8})\.csv$`

**What it validates**
- Correct matching of NATS naming convention with an 8-digit date segment
- Folder routing to the NATS folder
- Standard ODS pipeline label in payload

**Run:**
```sh
./test_flpids_nats.sh
```

---

### Test 2 — FLPIDS SCIMS

**Script:** `test_flpids_scims.sh`  
**Jenkins job name:** `FSA-PROD-DART-ECHO-FETCH-FLPIDS-SCIMS`  
**Defaults:**
- `echo_folder`: `plas`
- `echo_subfolder`: *(blank)*
- `pipeline`: `FLPIDS_RPT (SCIMS)`
- `file_pattern`: `(MOYWRSC_[A-Z]{4})\.txt$`

**What it validates**
- SCIMS pattern matching for 4-letter suffixes
- Use of PLAS folder for SCIMS source
- Distinct pipeline label to route downstream behavior

**Run:**
```sh
./test_flpids_scims.sh
```

---

### Test 3 — FLPIDS LOAD

**Script:** `test_flpidsload.sh`  
**Jenkins job name:** `FSA-PROD-DART-ECHO-FETCH-FLPIDSLOAD`  
**Defaults:**
- `echo_folder`: `gls`
- `echo_subfolder`: *(blank)*
- `pipeline`: `FLPIDS_ODS`
- `file_pattern`: `^(\w+)$`

**What it validates**
- Broad “word-only” match (useful as a smoke test)
- GLS folder routing
- ODS pipeline label consistency

**Run:**
```sh
./test_flpidsload.sh
```

---

### Test 4 — FLPIDS RPT

**Script:** `test_flpidsrpt.sh`  
**Jenkins job name:** `FSA-PROD-DART-ECHO-FETCH-FLPIDSRPT`  
**Defaults:**
- `echo_folder`: `plas`
- `echo_subfolder`: *(blank)*
- `pipeline`: `FLPIDS_RPT`
- `file_pattern`: `(MFO900\.MOYRPT\.\w{1,128})\.DATA\.(\d{14})$`

**What it validates**
- Matching of a report naming convention with a 14-digit timestamp suffix
- PLAS folder routing for report input
- Report-specific pipeline identifier

**Run:**
```sh
./test_flpidsrpt.sh
```

---

### Test 5 — RC540 Monthly

**Script:** `test_rc540_monthly.sh`  
**Jenkins job name:** `FSA-PROD-DART-ECHO-FETCH-RC540-MONTHLY`  
**Defaults:**
- `echo_folder`: `plas`
- `echo_subfolder`: *(blank)*
- `pipeline`: `FLPIDS_RC540 Monthly`
- `file_pattern`: `^(mo\.moyr540)\.data$`

**What it validates**
- Monthly file pattern for RC540
- PLAS folder routing
- Monthly-specific pipeline label (useful for downstream branching)

**Run:**
```sh
./test_rc540_monthly.sh
```

---

### Test 6 — RC540 Weekly

**Script:** `test_rc540_weekly.sh`  
**Jenkins job name:** `FSA-PROD-DART-ECHO-FETCH-RC540-WEEKLY`  
**Defaults:**
- `echo_folder`: `plas`
- `echo_subfolder`: `weekly`
- `pipeline`: `FLPIDS_RC540 Weekly`
- `file_pattern`: `^(wk\.moyr540)\.data$`

**What it validates**
- Weekly file pattern for RC540
- Routing to a specific **subfolder** (`weekly`)
- Weekly-specific pipeline label (separate from monthly)

**Run:**
```sh
./test_rc540_weekly.sh
```

---

## Override knobs (how to test variations)

All six scripts support the same override environment variables, including:

- `AWS_REGION` (default `us-east-1`)
- `FUNCTION_NAME` (default `FSA-steam-dev-FpacFLPIDS-DynaCheckFile`)
- `FTPS_PORT` (default `21`)
- `DDB_TABLE_NAME` (default `FSA-FileChecks`)
- `DDB_PROJECT_GSI_NAME` (default `project_gsi_name`)
- `SECRET_ID` (default `FSA-CERT-Secrets`)
- `FILE_PATTERN`, `ECHO_FOLDER`, `ECHO_SUBFOLDER`, `PIPELINE`
- `MIN_SIZE_BYTES`, `VERIFY_TLS`, `CURL_TIMEOUT_SECONDS`, `DEBUG`
- `RESP_FILE` (default `response.json`)

### Example: Force “no match” behavior

```sh
FILE_PATTERN='^DOES_NOT_EXIST_\d+$' ./test_flpids_nats.sh
```

Expected outcome: response should indicate no matches, and DynamoDB/logs should reflect a “checked but not found” scenario.

### Example: Verify minimum size filter

```sh
MIN_SIZE_BYTES=99999999 ./test_rc540_weekly.sh
```

Expected outcome: file may exist but be rejected for being too small (depending on file size).

---

## Verification checklist

### A) CLI response

- `response.json` exists and is valid JSON
- Response includes enough detail to confirm:
  - file count, match count, matched file names (if any)
  - whether a downstream trigger was invoked (if your Lambda returns that info)

### B) CloudWatch Logs

Check the Lambda log group for:

- request start/end
- summary including folder + found/matched counts
- any downstream HTTP/Jenkins call (status code, etc.) if implemented

### C) DynamoDB (`FSA-FileChecks`)

Verify that the row(s) for the run exist and were updated:

- Partition key: `jobId`
- Sort key: `project`
- Attributes reflect the latest check and match outcomes
- If your implementation creates a new `jobId` when a prior run is `COMPLETED`, validate:
  - a new row is created
  - the previous row remains unchanged / archived by key

---

## Data load script (`loaddata.sh`)

**Purpose:** Load or seed files/data via a dedicated loader Lambda so the checkfile tests can detect something.

**Lambda invoked:** `FSA-steam-dev-FpacFLPIDS-TestFileLoader`  
**Payload file:** `loadpunk.json` (must exist in the current working directory)

Run:

```sh
./loaddata.sh
```

Notes:
- The script uses `fileb://loadpunk.json` (binary mode) and writes output to `response.json`.
- After running, proceed to execute one of the six checkfile tests to confirm detection.

---

## Suggested test order (quick confidence)

1. `./loaddata.sh`
2. `./test_flpidsload.sh` (broad smoke test)
3. `./test_flpids_nats.sh` (specific regex + folder)
4. `./test_flpidsrpt.sh` (timestamped pattern)
5. `./test_rc540_monthly.sh`
6. `./test_rc540_weekly.sh` (subfolder routing)

---

## Troubleshooting

### “Invalid UTF-8 / could not parse payload”
Use the pattern in these scripts: generate JSON with `python3` and invoke using:

- `--cli-binary-format raw-in-base64-out`
- `--payload file://...`

### “AccessDeniedException”
Your CLI principal likely lacks `lambda:InvokeFunction` or KMS/Secrets/DynamoDB permissions needed by the Lambda.

### “No matches but you expect matches”
Check:
- `echo_folder` / `echo_subfolder` are correct
- the secret points to the correct remote path and credentials
- `FILE_PATTERN` matches the exact filename (escape dots properly)

---

## Appendix: one-liner runs

```sh
./test_flpids_nats.sh
./test_flpids_scims.sh
./test_flpidsload.sh
./test_flpidsrpt.sh
./test_rc540_monthly.sh
./test_rc540_weekly.sh
