# COMPONENTS.md — Lambda → Step Functions → Glue Job Mapping

This document maps **AWS Lambda functions**, **AWS Step Functions states**, and **AWS Glue jobs** in this repo so developers can quickly answer:

- *Which Lambda is invoked by which Step Function state?*
- *Which Glue job runs in which pipeline stage?*
- *Where do I change behavior for a given step?*

> **Note on accuracy:** This repo contains both **ASL JSON definitions** and **Python state machine builders**.
> Without parsing those files here, this mapping is based on:
> - the **directory structure**
> - the **naming conventions used in recent implementation discussions**
> - the **intended responsibility of each component**
>
> If you rename a Lambda or add/remove states, update this file alongside the ASL/Python state definitions.

---

## Source of Truth Locations

- **Lambda runtime code:** `lambda/<function-name>/lambda_function.py`
- **State machine definitions:**
  - Python builder: `states/cars_stepfunction.py`
- **Glue scripts (job bodies):** `glue/*.py`
- **Deployer wiring / resource creation:** `deploy.py`
- **Pipeline configuration:** `car/_configs/**`

---

## Components Inventory

### Lambda Functions

| Lambda folder | Purpose / responsibility (expected) | Used by |
|---|---|---|
| `lambda/DownloadZip/` | Download from S3 prefix and stream a ZIP to destination S3 bucket (supports ignore patterns like `*.zip`) | Step Functions or ad-hoc invocation |
| `lambda/UploadConfig/` | Upload config/metadata payload(s) to S3 or a config bucket/prefix; used to bootstrap runs | Step Functions or CI scripts |
| `lambda/edv-build-processing-plan/` | Build an EDV processing plan (derive steps, partitions, manifests, or run graph) | EDV Step Function |
| `lambda/edv-check-results/` | Validate results of upstream processing (counts, output existence, checkpoints) | EDV Step Function |
| `lambda/edv-finalize-pipeline/` | Finalize run: mark status complete, publish summary, cleanup transient artifacts | EDV Step Function |
| `lambda/edv-handle-failure/` | Failure handler: capture error context, update status, route notifications, perform cleanup | EDV Step Function (Catch/Fail path) |

### Glue Jobs

| Glue script | Glue job role (expected) | Pipeline |
|---|---|---|
| `glue/FSA-CERT-DATAMART-EXEC-DB-SQL.py` | Execute SQL against Datamart/Aurora Postgres (load/merge/transform) | EDV load pipeline |

### Step Functions

| State machine definition | Intended orchestration | Notes |
|---|---|---|
| `states/FSA-CERT-CARS-s3parquet-to-postgres-edv-load.asl.json` | EDV load: S3 parquet → Postgres/Aurora, validation, finalize/failure handling | This is JUST for reference |
| `states/cars_stepfunction.py` | Python builder for constructing CARS state machine(s) programmatically | This is what is deployed |

---

## Primary Pipelines

---

### Pipeline — EDV: S3 Parquet → Postgres (CERT)

**State machine:** `states/FSA-CERT-CARS-s3parquet-to-postgres-edv-load.asl.json`  
**Primary Glue job:** `glue/FSA-CERT-DATAMART-EXEC-DB-SQL.py`  
**Primary Lambdas:** `lambda/edv-*`

#### Expected Orchestration (logical)

| Step Function state (logical) | Type | Invokes | Notes |
|---|---|---|---|
| `BuildProcessingPlan` | Lambda Task | **Lambda:** `edv-build-processing-plan` | Derives what to load, partitions, target tables |
| `RunLoad` | Glue Task / Activity | **Glue:** DATAMART-EXEC-DB-SQL | Executes SQL loads/merges into Postgres |
| `CheckResults` | Lambda Task | **Lambda:** `edv-check-results` | Validates row counts, existence, checksums, etc. |
| `FinalizePipeline` | Lambda Task | **Lambda:** `edv-finalize-pipeline` | Status updates, summaries, cleanup |
| `HandleFailure` (Catch) | Lambda Task | **Lambda:** `edv-handle-failure` | Records failure context and marks run failed |

**Where to change what**
- Plan construction logic: `lambda/edv-build-processing-plan/lambda_function.py`
- Validation logic: `lambda/edv-check-results/lambda_function.py`
- Final status and cleanup: `lambda/edv-finalize-pipeline/lambda_function.py`
- Error capture and side-effects: `lambda/edv-handle-failure/lambda_function.py`
- SQL execution details: `glue/FSA-CERT-DATAMART-EXEC-DB-SQL.py`
- Orchestration (timeouts/retries/catches): `states/FSA-CERT-CARS-s3parquet-to-postgres-edv-load.asl.json`

---

## Cross-Cutting Utility Components

### DownloadZip (utility Lambda)

**Lambda:** `lambda/DownloadZip/`  
**Common uses:**
- Package S3 folder trees into a ZIP for downstream transfer
- Support test harnesses and local reproduction
- Provide artifact snapshots (e.g., landing payload bundles)

**Typical invocation sources:**
- Manual `aws lambda invoke`
- CI scripts in `scripts/`
- Optional Step Function task in a pre-/post-processing branch

### UploadConfig (utility Lambda)

**Lambda:** `lambda/UploadConfig/`  
**Common uses:**
- Upload run configuration payloads to S3
- Seed pipeline runs with environment-specific config
- Support automated Jenkins workflows

---

## Quick “What do I edit?” Guide

- **Change the ETL transformation:** `glue/…`
- **Change pipeline orchestration `states/cars_stepfunction.py`
- **Change step-level validation / finalization:** `lambda/edv-*/lambda_function.py`
- **Change resource names / env wiring:** `car/_configs/**` and `deploy.py`

---

## Keeping This Mapping Correct

When you update state machines or rename functions:
1. Update the ASL JSON (or Python builder) in `states/`
2. Update the config in `car/_configs/` (if names/inputs changed)
3. Update this mapping in `COMPONENTS.md`

Suggested convention:
- Keep Step Function state names aligned with folder names:
  - `BuildProcessingPlan` → `edv-build-processing-plan`
  - `CheckResults` → `edv-check-results`
  - `FinalizePipeline` → `edv-finalize-pipeline`
  - `HandleFailure` → `edv-handle-failure`

---

## Appendix — Directory Reference

```
glue/        # Glue job scripts
lambda/      # One folder per Lambda function (packaged independently)
states/      # Step Function ASL JSON and builders
scripts/     # Local & CI helper scripts
tests/       # Ad-hoc validation scripts and example outputs
uploader/    # End-to-end and upload/zip workflow tests
car/_configs/  # Env + domain configuration inputs to deployer
```
