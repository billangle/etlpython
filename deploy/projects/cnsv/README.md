# CNSV — Conservation Deployment Pipelines

The CNSV project contains **five deployment pipelines**, each with its own deployer script and set of AWS resources. All resource names follow the `FSA-{ENV}-CNSV-*` naming convention where `{ENV}` is `PROD`, `STEAMDEV`, or `DEV`.

---

## Jenkins Jobs

| Jenkins Job | Pipeline(s) |
|---|---|
| [DeployCNSV (dev)](https://jenkinsfsa-dev.dl.usda.gov/job/BuildProcess/job/DeployCNSV/) | EXEC-SQL + Config Upload |
| [DeployCNSV](https://jenkinsfsa.dl.usda.gov/job/BuildProcess/job/DeployCNSV/) | EXEC-SQL + Config Upload |
| [DeployCNSVBase](https://jenkinsfsa.dl.usda.gov/job/BuildProcess/job/DeployCNSVBase/) | CNSV Base |
| [DeployCNSVMaint](https://jenkinsfsa.dl.usda.gov/job/BuildProcess/job/DeployCNSVMaint/) | Contract Maintenance |
| [DeployCNSVPymts](https://jenkinsfsa.dl.usda.gov/job/BuildProcess/job/DeployCNSVPymts/) | Conservation Payments |

> `DeployCNSV` runs both `deploy.py` (EXEC-SQL AWS resources) **and** `deploy_config.sh` (SQL/JSON config files to S3) together in the same job. All pipelines deploy to all environments (DEV, STEAMDEV, PROD).

---

## Pipeline 1 — Config Upload (`deploy_config.sh`)

**Deployer:** `deploy_config.sh`  
**Jenkins:** DeployCNSV (runs alongside `deploy.py`)

### Overview
Zips the `configs/_configs/` directory and uploads it to S3, then invokes the `UploadConfig` Lambda to expand the files to the output S3 bucket. This is a **file-only deployment** — no Glue jobs, Lambdas, or Step Functions are created or changed.

### What Gets Deployed
The `configs/_configs/` folder contains SQL and JSON configuration files consumed by the EXEC-SQL Glue job at runtime:

| Layer | Contents |
|---|---|
| `STG/` | Per-table SQL files for the Stage layer (`CNSV_*.sql`, `CCMS_*.sql`) |
| `EDV/` | JSON configs for the Enterprise Data Vault layer |
| `DM/` | JSON configs for the Data Mart layer |
| `CMN_DIM_DM/` | JSON configs for the Common Dimension Data Mart layer |

### How It Works
1. `scripts/make_and_upload_zip.sh` — zips `configs/_configs/` and uploads to the input S3 bucket
2. `scripts/invoke_expand_lambda.sh` — invokes the `UploadConfig` Lambda with the zip location
3. Lambda expands the zip to the output S3 bucket/prefix where the Glue job reads configs at runtime

### Configuration (`configUpload` block in the JSON config)

| Key | Description |
|---|---|
| `functionName` | Lambda that expands the zip (e.g. `FSA-PROD-UploadConfig`) |
| `inputBucket` / `inputPrefix` | S3 destination for the zip upload |
| `outputBucket` / `outputPrefix` | S3 location the Glue job reads configs from at runtime |
| `sourceDir` | Local directory to zip (relative to `deploy/projects/cnsv/`) |

---

## Pipeline 2 — EXEC-SQL / EDV (`deploy.py`)

**Deployer:** `deploy.py`  
**Jenkins:** DeployCNSV (runs alongside `deploy_config.sh`)  
**project-type:** `cnsv`

### Overview
Deploys the EXEC-SQL pipeline which executes Athena SQL to move data from S3 Parquet through the Stage (STG) and Enterprise Data Vault (EDV) Iceberg layers. A Lambda builds a processing plan, STG tables are processed in parallel via a Glue job fan-out, followed by sequential EDV group processing.

**Flow:** Build Processing Plan → Process Stage Tables (parallel Map) → Check Stage Results → Process EDV Groups (sequential, parallel within group) → Check EDV Results → Finalize

### AWS Resources

**Glue Jobs**

| Job Name | Script |
|---|---|
| `FSA-{ENV}-CNSV-EXEC-SQL` | `glue/EXEC-SQL.py` |

**Step Functions**

| State Machine | Purpose |
|---|---|
| `FSA-{ENV}-CNSV-EXEC-SQL` | Orchestrates the full STG → EDV SQL execution pipeline |

**Lambda Functions**

| Function | Purpose |
|---|---|
| `FSA-{ENV}-CNSV-edv-build-processing-plan` | Builds the list of STG tables and EDV groups to process |
| `FSA-{ENV}-CNSV-edv-check-results` | Validates Map results before proceeding to the next layer |
| `FSA-{ENV}-CNSV-edv-finalize-pipeline` | Records final pipeline outcome |
| `FSA-{ENV}-CNSV-edv-handle-failure` | Handles and publishes pipeline failures |

**ASL:** `states/EXEC-SQL.asl.json`

---

## Pipeline 3 — CNSV Base (`deploy_base.py`)

**Deployer:** `deploy_base.py`  
**Jenkins:** DeployCNSVBase  
**project-type:** `cnsvbase`

### Overview
The primary CNSV CDC ingestion pipeline. Pulls incremental records from the source system into S3 Landing, transforms them into S3 Final Raw DM, and records process control updates.

### AWS Resources

**Glue Jobs**

| Job Name | Script |
|---|---|
| `FSA-{ENV}-CNSV-LandingFiles` | `glue/LandingFiles.py` |
| `FSA-{ENV}-CNSV-Raw-DM` | `glue/Raw-DM.py` |

**Step Functions**

| State Machine | Purpose |
|---|---|
| `FSA-{ENV}-CNSV-Main` | Orchestrates the full end-to-end base pipeline |
| `FSA-{ENV}-CNSV-Incremental-to-S3Landing` | Pulls incremental records to S3 Landing |
| `FSA-{ENV}-CNSV-S3Landing-to-S3Final-Raw-DM` | Transforms landing data to Final Raw DM |
| `FSA-{ENV}-CNSV-Process-Control-Update` | Updates process control tracking |

**Lambda Functions**

| Function | Purpose |
|---|---|
| `FSA-{ENV}-CNSV-get-incremental-tables` | Identifies tables with new incremental data |
| `FSA-{ENV}-CNSV-RAW-DM-etl-update-data-ppln-job` | Updates ETL workflow job status in DM |
| `FSA-{ENV}-CNSV-RAW-DM-sns-step-function-errors` | Publishes Step Function errors to SNS |
| `FSA-{ENV}-CNSV-Job-Logging-End` | Records job completion in the logging table |
| `FSA-{ENV}-CNSV-validation-check` | Validates row counts and data quality |
| `FSA-{ENV}-CNSV-sns-validations-report` | Publishes validation summary to SNS |

**ASL files:**
```
states/Main.param.asl.json
states/Incremental-to-S3Landing.param.asl.json
states/S3Landing-to-S3Final-Raw-DM.param.asl.json
states/Process-Control-Update.param.asl.json
```

---

## Pipeline 4 — Contract Maintenance (`deploy_maint.py`)

**Deployer:** `deploy_maint.py`  
**Jenkins:** DeployCNSVMaint  
**project-type:** `cnsvmaint`

### Overview
The Contract Maintenance sub-pipeline. Mirrors the Base pipeline structure (Landing → Final Raw DM → Process Control) scoped to the contract maintenance data domain. Includes dedicated Glue crawlers for the Final Zone and CDC S3 paths.

### AWS Resources

**Glue Jobs**

| Job Name | Script |
|---|---|
| `FSA-{ENV}-CNSV-Cntr-Maint-LandingFiles` | `glue/Cntr-Maint-LandingFiles.py` |
| `FSA-{ENV}-CNSV-Cntr-Maint-Raw-DM` | `glue/Cntr-Maint-Raw-DM.py` |

**Step Functions**

| State Machine | Purpose |
|---|---|
| `FSA-{ENV}-CNSV-Cntr-Maint-Main` | Orchestrates the full Cntr-Maint pipeline |
| `FSA-{ENV}-CNSV-Cntr-Maint-Incremental-to-S3Landing` | Pulls incremental records to S3 Landing |
| `FSA-{ENV}-CNSV-Cntr-Maint-S3Landing-to-S3Final-Raw-DM` | Transforms landing data to Final Raw DM |
| `FSA-{ENV}-CNSV-Cntr-Maint-Process-Control-Update` | Updates process control tracking |

**Lambda Functions**

| Function | Purpose |
|---|---|
| `FSA-{ENV}-CNSV-Cntr-Maint-get-incremental-tables` | Identifies tables with new incremental data |
| `FSA-{ENV}-CNSV-Cntr-Maint-RAW-DM-etl-update-data-pplnjob` | Updates ETL workflow job status |
| `FSA-{ENV}-CNSV-Cntr-Maint-RAW-DM-sns-step-function-errors` | Publishes Step Function errors to SNS |
| `FSA-{ENV}-CNSV-Cntr-Maint-Job-Logging-End` | Records job completion in the logging table |
| `FSA-{ENV}-CNSV-Cntr-Maint-validation-check` | Validates row counts and data quality |
| `FSA-{ENV}-CNSV-Cntr-Maint-sns-validations-report` | Publishes validation summary to SNS |

**Glue Crawlers**

| Crawler | Target |
|---|---|
| `FSA-{ENV}-CNSV-CNTR-MAINT` | Final Zone S3 path |
| `FSA-{ENV}-CNSV-CNTR-MAINT-cdc` | CDC S3 path |

**ASL files:**
```
states/Cntr-Maint-Main.param.asl.json
states/Cntr-Maint-Incremental-to-S3Landing.param.asl.json
states/Cntr-Maint-S3Landing-to-S3Final-Raw-DM.param.asl.json
states/Cntr-Maint-Process-Control-Update.param.asl.json
```

---

## Pipeline 5 — Conservation Payments (`deploy_pymts.py`)

**Deployer:** `deploy_pymts.py`  
**Jenkins:** DeployCNSVPymts  
**project-type:** `cnsvpymts`

### Overview
The Conservation Payments sub-pipeline. Same Landing → Final Raw DM → Process Control structure as the Base pipeline, scoped to conservation payments data with its own dedicated crawlers.

### AWS Resources

**Glue Jobs**

| Job Name | Script |
|---|---|
| `FSA-{ENV}-CNSV-Cons-Pymts-LandingFiles` | `glue/Cons-Pymts-LandingFiles.py` |
| `FSA-{ENV}-CNSV-Cons-Pymts-Raw-DM` | `glue/Cons-Pymts-Raw-DM.py` |

**Step Functions**

| State Machine | Purpose |
|---|---|
| `FSA-{ENV}-CNSV-Cons-Pymts-Main` | Orchestrates the full Cons-Pymts pipeline |
| `FSA-{ENV}-CNSV-Cons-Pymts-Incremental-to-S3Landing` | Pulls incremental records to S3 Landing |
| `FSA-{ENV}-CNSV-Cons-Pymts-S3Landing-to-S3Final-Raw-DM` | Transforms landing data to Final Raw DM |
| `FSA-{ENV}-CNSV-Cons-Pymts-Process-Control-Update` | Updates process control tracking |

**Lambda Functions**

| Function | Purpose |
|---|---|
| `FSA-{ENV}-CNSV-Cons-Pymts-get-incremental-tables` | Identifies tables with new incremental data |
| `FSA-{ENV}-CNSV-Cons-Pymts-RAW-DM-etl-update-data-pplnjob` | Updates ETL workflow job status |
| `FSA-{ENV}-CNSV-Cons-Pymts-RAW-DM-sns-step-function-errors` | Publishes Step Function errors to SNS |
| `FSA-{ENV}-CNSV-Cons-Pymts-Job-Logging-End` | Records job completion in the logging table |
| `FSA-{ENV}-CNSV-Cons-Pymts-validation-check` | Validates row counts and data quality |
| `FSA-{ENV}-CNSV-Cons-Pymts-sns-validations-report` | Publishes validation summary to SNS |

**Glue Crawlers**

| Crawler | Target |
|---|---|
| `FSA-{ENV}-CNSV-CONS-PYMTS` | Final Zone S3 path |
| `FSA-{ENV}-CNSV-CONS-PYMTS-cdc` | CDC S3 path |

**ASL files:**
```
states/Cons-Pymts-Main.param.asl.json
states/Cons-Pymts-Incremental-to-S3Landing.param.asl.json
states/Cons-Pymts-S3Landing-to-S3Final-Raw-DM.param.asl.json
states/Cons-Pymts-Process-Control-Update.param.asl.json
```

---

## Config Files

| File | Environment |
|---|---|
| `../../config/cnsv/dev.json` | DEV |
| `../../config/cnsv/steamdev.json` | STEAMDEV |
| `../../config/cnsv/prod.json` | PROD |

---

## Running Deployers Locally

All Python deployers are invoked from the `deploy/` root directory via the master dispatcher:

```bash
cd deploy/

# Config Upload (S3 files only — no AWS resource changes)
bash projects/cnsv/deploy_config.sh config/cnsv/steamdev.json

# EXEC-SQL (AWS resources)
python deploy.py --config config/cnsv/steamdev.json --region us-east-1 --project-type cnsv

# CNSV Base
python deploy.py --config config/cnsv/steamdev.json --region us-east-1 --project-type cnsvbase

# Contract Maintenance
python deploy.py --config config/cnsv/steamdev.json --region us-east-1 --project-type cnsvmaint

# Conservation Payments
python deploy.py --config config/cnsv/steamdev.json --region us-east-1 --project-type cnsvpymts
```
