# CNSV — Conservation Deployment Pipelines

The CNSV project contains **five deployment pipelines**, each with its own deployer script and set of AWS resources. All resource names follow the `FSA-{ENV}-CNSV-*` naming convention where `{ENV}` is `PROD`, `STEAMDEV`, or `DEV`.

---


## Git Branching and Automated DEV Builds

All projects follow the same branch-driven CI/CD model.

- Branch naming: `<PROJECT>-<JIRA-TICKET>` (example: `CNSV-POPSUP-7557`)
- `CNSV` is the project code used in the command examples below.
- Pushing a feature branch triggers the BitBucket webhook and builds/deploys to DEV using FPACDEV naming.
- A pull request into `main` is required before PROD deployment.

For the full standard, see:
- [Project-level branching standard](../README.md#git-branching-and-automated-dev-builds)

### Common Git Commands

Branch names shown here are examples only; replace POPSUP-7557 with your assigned Jira ticket when creating your branch.

```bash
# Start feature work from latest main
git checkout main
git pull origin main
git checkout -b CNSV-POPSUP-7557

# Commit and push (triggers DEV automation)
git add .
git commit -m "CNSV-POPSUP-7557: describe change"
git push -u origin CNSV-POPSUP-7557

# Daily sync main into feature branch to reduce merge conflicts
git fetch origin
git checkout main
git pull origin main
git checkout CNSV-POPSUP-7557
git merge main
git push
```

## Naming Convention

FPACDEV indicates resources created by CI/CD automation in the development environment. Resources named with DEV were created manually and are NOT overwritten by automation.


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

### One-Paragraph Overview (EXEC-SQL)
The EXEC-SQL Step Function orchestrates a metadata-driven STG -> EDV load where `edv-build-processing-plan` reads table lists and dependencies from `configs/_configs/metadata/*.csv`, then fans out STG table processing through the `FSA-{ENV}-CNSV-EXEC-SQL` Glue job (which loads SQL/templates from `configs/_configs/STG`), evaluates results with `edv-check-results`, optionally processes EDV groups in dependency order using SQL/config in `configs/_configs/EDV`, and finally calls `edv-finalize-pipeline` (or `edv-handle-failure`) to record and publish the overall run outcome.

### Text Flow Diagram (EXEC-SQL)
```text
Start Execution (FSA-{ENV}-CNSV-EXEC-SQL)
	|
	v
[BuildProcessingPlan Lambda]
	Reads: configs/_configs/metadata/stg_tables.csv
				 configs/_configs/metadata/dv_tables.csv
				 configs/_configs/metadata/dv_tbl_dpnds.csv
	Output: plan.stgTables, plan.dvGroups, plan.counts
	|
	v
[CheckTablesExist]
	|-- if plan.counts.stg == 0 --> [NoTablesToProcess] --> END
	|
	v
[ProcessStageTables Map (parallel)]
	For each STG table:
		-> [ExecuteStageSQL Glue: FSA-{ENV}-CNSV-EXEC-SQL, layer=STG]
			 Reads SQL/config from expanded S3 _configs/STG/<TABLE>/...
		-> [StageTableSuccess or StageTableFailed]
	Output: stageResults[]
	|
	v
[CheckStageResults Lambda]
	Output: stageCheck{successCount, failedCount, failedTables, skippedCount, skippedTables, canContinue}
	|
	v
[ShouldProcessEDV]
	|-- if plan.counts.dv > 0 AND stageCheck.canContinue == true -->
	|      [ProcessEDVGroups Map (group order)]
	|         For each group:
	|           [ProcessEDVTablesInGroup Map (parallel)]
	|             -> [ExecuteEDVSQL Glue: FSA-{ENV}-CNSV-EXEC-SQL, layer=EDV]
	|                Reads SQL/config from expanded S3 _configs/EDV/<TABLE>/...
	|             -> [EDVTableSuccess or EDVTableFailed]
	|      Output: edvResults[]
	|      -> [CheckEDVResults Lambda] -> edvCheck{successCount, failedCount, failedTables, skippedCount, skippedTables, canContinue}
	|
	|-- else --> [SetDefaultEDVCheck Pass] -> edvCheck={0/0/[]/0/[]/false}
	|
	v
[FinalizePipeline Lambda]
	Input: stageCheck, edvCheck, counts
	Output: finalResult (SUCCESS | PARTIAL_SUCCESS | FAILED)
	|
	v
[PipelineSuccess] END

Any unhandled error in main path:
	-> [HandlePipelineFailure Lambda]
	-> [PipelineFailed] END
```

### Concrete Full Run Example (Using Real CNSV Files)
Example run shown with one real STG table: `ccms_ctr_stat`.

1. Build processing plan
	- Step Function starts at `BuildProcessingPlan` in `states/EXEC-SQL.asl.json`.
	- Lambda reads metadata from:
	  - `configs/_configs/metadata/stg_tables.csv`
	  - `configs/_configs/metadata/dv_tables.csv`
	  - `configs/_configs/metadata/dv_tbl_dpnds.csv`
	- `ccms_ctr_stat` is present in `stg_tables.csv`, so it is queued for STG processing.

2. Run STG Glue task for one table
	- `ProcessStageTables` Map dispatches `ExecuteStageSQL` with arguments like:
	  - `--table_name=ccms_ctr_stat`
	  - `--layer=STG`
	  - `--run_type=incremental`
	  - `--start_date=<run start date>`
	- Glue job `FSA-{ENV}-CNSV-EXEC-SQL` is invoked.

3. Glue reads and executes SQL from config
	- Glue script: `glue/EXEC-SQL.py`.
	- For this table it resolves SQL from:
	  - `configs/_configs/STG/CCMS_CTR_STAT/CCMS_CTR_STAT.sql`
	- SQL placeholders (for example `{env}`, `{etl_start_date}`, `{etl_end_date}`) are substituted at runtime before execution.

4. Result aggregation and branch decision
	- Table result is returned as SUCCESS or FAILED to the Map result set.
	- `CheckStageResults` computes `successCount`, `failedCount`, and `canContinue`.
	- If `canContinue=true`, workflow continues to EDV groups from metadata (`dv_tables.csv` + `dv_tbl_dpnds.csv`); if false, EDV is skipped.

5. Finalization
	- `FinalizePipeline` stores summary counts and final status.
	- If any state fails hard, `HandlePipelineFailure` executes and the workflow ends as failed.

6. Concrete EDV branch example (real CNSV files)
	- After STG passes checks, `ProcessEDVGroups` uses EDV metadata from:
	  - `configs/_configs/metadata/dv_tables.csv`
	  - `configs/_configs/metadata/dv_tbl_dpnds.csv`
	- One real EDV table listed in `dv_tables.csv` is `ccms_ctr_stat_hs`.
	- The EDV stage is executed by the same Glue job (`FSA-{ENV}-CNSV-EXEC-SQL`) with arguments like:
	  - `--table_name=ccms_ctr_stat_hs`
	  - `--layer=EDV`
	- A real EDV config artifact currently in this repo is:
	  - `configs/_configs/EDV/cs_tbl_cmmt_item_source.json`
	- EDV SQL scripts are resolved by `glue/EXEC-SQL.py` from the expanded S3 `_configs/EDV/<TABLE>/` path at runtime (uploaded by `deploy_config.sh`).

Notes
- These file paths are shown from the repository (`configs/_configs/...`).
- At runtime, `deploy_config.sh` uploads and expands this same config structure to S3, and Glue reads from the S3-expanded `_configs` path.
- Missing per-table SQL/config artifacts are treated as skipped (not hard failures), and are surfaced in final summary fields `skipped` and `skippedTables`.

### How To Run In PROD (EXEC-SQL)
Start this state machine: `FSA-PROD-CNSV-EXEC-SQL`.

Execution input JSON:
```json
{}
```

Why `{}` is valid in this pipeline:
- `data_src_nm`, `run_type`, `start_date`, and `env` are injected into the ASL at deploy time from `config/cnsv/prod.json` by `deploy.py` placeholder substitution.

Current effective PROD values (from deploy config):
- `data_src_nm`: `cnsv`
- `run_type`: `incremental`
- `start_date`: `2026-01-27`
- `env`: `PROD`

Expected successful terminal output shape (from Finalize step):
```json
{
	"status": "SUCCESS | PARTIAL_SUCCESS | FAILED",
	"summary": {
		"data_src_nm": "cnsv",
		"run_type": "incremental",
		"start_date": "2026-01-27",
		"stage": {"total": 0, "success": 0, "failed": 0, "skipped": 0, "failedTables": [], "skippedTables": []},
		"edv": {"total": 0, "groups": 0, "success": 0, "failed": 0, "skipped": 0, "failedTables": [], "skippedTables": []},
		"totals": {"tables": 0, "success": 0, "failed": 0, "successRate": "0.0%"}
	}
}
```

Expected failure path:
- Any unhandled error routes to `HandlePipelineFailure`, then terminal state `PipelineFailed`.

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