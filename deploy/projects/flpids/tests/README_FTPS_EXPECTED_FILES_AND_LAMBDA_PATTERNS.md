# FTPS File Discovery – Expected Files, Directories, and Lambda Call Patterns

This document describes:
1) The **exact directory paths** and **filenames** the Lambda is expected to find on the FTPS server, and  
2) The **exact `file_pattern`** (and example event payload) you should pass when invoking the Lambda for each case.

---

## Base Path Construction

The Lambda constructs the FTPS path as:

```
/{echo_dart_path}/{echo_folder}/in/{echo_subfolder}
```

Where:
- `echo_dart_path` comes from Secrets Manager (e.g., `DART`)
- `echo_folder` comes from the event (e.g., `plas`, `gls`, `nats`)
- `echo_subfolder` is optional (default: empty)

> **Note:** In your environment, this commonly results in paths like `/DART/plas/in`, `/DART/gls/in`, `/DART/nats/in`, etc.

---

## Common Lambda Invoke Template

Replace values as needed.

### AWS CLI (example)
```bash
aws lambda invoke   --function-name <YOUR_LAMBDA_NAME>   --cli-binary-format raw-in-base64-out   --payload '{
    "secret_id": "<SECRETS_MANAGER_SECRET_ID>",
    "jenkins_url": "https://jenkins.example/job/my-job/build?token=REDACTED",
    "ftps_mode": "explicit",
    "ftps_port": 21,
    "echo_folder": "plas",
    "echo_subfolder": "",
    "file_pattern": "^(wk.moyr540)\\.data$",
    "min_size_bytes": 1,
    "pipeline": "Example Pipeline",
    "step": "Example Step",
    "header": false,
    "to_queue": false
  }' response.json
```

### Required parameters per call
- `secret_id`
- `jenkins_url`
- `echo_folder`
- `file_pattern`

Recommended (but optional):
- `ftps_mode` (`explicit` recommended)
- `ftps_port` (`21` for explicit, `990` for implicit)
- `min_size_bytes` (defaults to `1`)
- `pipeline`, `step` (traceability)

---

# 1) Weekly RC540 (PLAS)

### Directory Checked
```
/DART/plas/in
```

### Expected File
```
wk.moyr540.data
```

### `file_pattern`
```
^(wk.moyr540)\.data$
```

### Example Lambda payload
```json
{
  "secret_id": "<SECRETS_MANAGER_SECRET_ID>",
  "jenkins_url": "https://jenkins.example/job/rc540-weekly/build?token=REDACTED",
  "ftps_mode": "explicit",
  "ftps_port": 21,
  "echo_folder": "plas",
  "file_pattern": "^(wk.moyr540)\\.data$",
  "pipeline": "FLPIDS_RC540 Weekly",
  "step": "FLPIDS_RC540 Weekly Landing Zone",
  "header": false
}
```

---

# 2) Monthly RC540 (PLAS)

### Directory Checked
```
/DART/plas/in
```

### Expected File
```
mo.moyr540.data
```

### `file_pattern`
```
^(mo.moyr540)\.data$
```

### Example Lambda payload
```json
{
  "secret_id": "<SECRETS_MANAGER_SECRET_ID>",
  "jenkins_url": "https://jenkins.example/job/rc540-monthly/build?token=REDACTED",
  "ftps_mode": "explicit",
  "ftps_port": 21,
  "echo_folder": "plas",
  "file_pattern": "^(mo.moyr540)\\.data$",
  "pipeline": "FLPIDS_RC540 Monthly",
  "step": "FLPIDS_RC540 Monthly Landing Zone",
  "header": false
}
```

---

# 3) SCIMS Reports (PLAS)

### Directory Checked
```
/DART/plas/in
```

### Expected Files
```
MOYWRSC_ABCD.txt
MOYWRSC_WXYZ.txt
```

### `file_pattern`
```
(MOYWRSC_[A-Z]{4})\.txt$
```

### Example Lambda payload
```json
{
  "secret_id": "<SECRETS_MANAGER_SECRET_ID>",
  "jenkins_url": "https://jenkins.example/job/scims-rpt/build?token=REDACTED",
  "ftps_mode": "explicit",
  "ftps_port": 21,
  "echo_folder": "plas",
  "file_pattern": "(MOYWRSC_[A-Z]{4})\\.txt$",
  "pipeline": "FLPIDS_RPT (SCIMS)",
  "step": "FLPIDS_RPT (SCIMS) Landing Zone",
  "to_queue": true,
  "header": false
}
```

---

# 4) MFO900 Weekly Reports (Timestamped, PLAS)

### Directory Checked
```
/DART/plas/in
```

### Expected File Example
```
MFO900.MOYRPT.FLPNAME.DATA.20250107153045
```

### `file_pattern`
```
(MFO900\.MOYRPT\.\w{1,128})\.DATA\.(\d{14})$
```

### Example Lambda payload
```json
{
  "secret_id": "<SECRETS_MANAGER_SECRET_ID>",
  "jenkins_url": "https://jenkins.example/job/mfo900-weekly/build?token=REDACTED",
  "ftps_mode": "explicit",
  "ftps_port": 21,
  "echo_folder": "plas",
  "file_pattern": "(MFO900\\.MOYRPT\\.\\w{1,128})\\.DATA\\.(\\d{14})$",
  "pipeline": "FLPIDS_RPT",
  "step": "flpids_rpt Landing Zone (Weekly)",
  "header": false
}
```

---

# 5) ODS Generic Files (GLS)

### Directory Checked
```
/DART/gls/in
```

### Expected Files
```
ABCDEF
LOANSTAT
BATCH001
```

### `file_pattern`
```
^(\w+)$
```

### Example Lambda payload
```json
{
  "secret_id": "<SECRETS_MANAGER_SECRET_ID>",
  "jenkins_url": "https://jenkins.example/job/flpids-ods/build?token=REDACTED",
  "ftps_mode": "explicit",
  "ftps_port": 21,
  "echo_folder": "gls",
  "file_pattern": "^(\\w+)$",
  "pipeline": "FLPIDS_ODS",
  "step": "FLPIDS_ODS Landing Zone",
  "header": false
}
```

---

# 6) NATS CSV Files (NATS)

### Directory Checked
```
/DART/nats/in
```

### Expected File Example
```
FILE_NATS_LOANS.20250107.csv
```

### `file_pattern`
```
^(FILE_NATS_\w+)\.(\d{8})\.csv$
```

### Example Lambda payload
```json
{
  "secret_id": "<SECRETS_MANAGER_SECRET_ID>",
  "jenkins_url": "https://jenkins.example/job/flpids-ods-nats/build?token=REDACTED",
  "ftps_mode": "explicit",
  "ftps_port": 21,
  "echo_folder": "nats",
  "file_pattern": "^(FILE_NATS_\\w+)\\.(\\d{8})\\.csv$",
  "pipeline": "FLPIDS_ODS",
  "step": "FLPIDS_ODS Landing Zone"
}
```

---

# 7) Subfolder Example

### Example Event (subfolder)
```json
{
  "echo_folder": "plas",
  "echo_subfolder": "weekly"
}
```

### Directory Checked
```
/DART/plas/in/weekly
```

### Expected File
```
wk.moyr540.data
```

### Example Lambda payload
```json
{
  "secret_id": "<SECRETS_MANAGER_SECRET_ID>",
  "jenkins_url": "https://jenkins.example/job/rc540-weekly/build?token=REDACTED",
  "ftps_mode": "explicit",
  "ftps_port": 21,
  "echo_folder": "plas",
  "echo_subfolder": "weekly",
  "file_pattern": "^(wk.moyr540)\\.data$",
  "pipeline": "FLPIDS_RC540 Weekly",
  "step": "FLPIDS_RC540 Weekly Landing Zone",
  "header": false
}
```

---

## File Acceptance Rules

A file is considered **valid and triggers Jenkins** if:

1. File exists in the computed directory
2. Filename matches the configured regex (`file_pattern`)
3. File size is `>= min_size_bytes` (default: `1` → “greater than zero”)
4. Optional `targets` filter matches (if provided)

---

## Recommended Smoke Test

If you want the simplest test case:

- Put this file on the server:
  ```
  /DART/plas/in/wk.moyr540.data
  ```
- Invoke the Lambda with:
  ```json
  {
    "echo_folder": "plas",
    "file_pattern": "^(wk.moyr540)\\.data$"
  }
  ```

---

## Notes (Why curl is used)

- Directory listings and size checks are performed using **curl over FTPS**
- **Explicit FTPS** uses `ftp://` with `--ssl-reqd` (because `ftpes://` isn’t supported in your Lambda’s curl build)
- **Implicit FTPS** uses `ftps://`
- This approach avoids TLS session reuse instability common with Python `ftplib` in strict FTPS environments
