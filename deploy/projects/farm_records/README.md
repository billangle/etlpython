# farm_records

Multi-track ETL pipeline that ingests FSA Farm Records data from SAP/S3 and PostgreSQL sources and delivers it to a PostgreSQL EDV database and S3 Final Zone Data Mart.

**AWS Region:** `us-east-1`  
**Project key:** `farmrec` (in `--project-type`)  
**Resource naming:** `FSA-{deployEnv}-FpacFARM-{ResourceName}`

---

## Overview

The pipeline has three parallel tracks coordinated by a top-level Step Functions orchestrator:

| Track | Source | Target |
|---|---|---|
| **SAP Load** | S3 landing zone (`farm_records/etl-jobs/`), SAP-sourced parquet | S3 cleansed/final zones |
| **Postgres Load** | S3 Parquet (cleansed/final) + Athena | PostgreSQL EDV + CRM_ODS DB |
| **Data Mart** | S3 cleansed/final | S3 final zone + Glue Data Catalog |

DB credentials are fetched from Secrets Manager at runtime. All tracks feed from a DynamoDB metadata table (`FsaFpacMetadata`) for job tracking.

---

## Environments

| Config file | `deployEnv` | AWS Account | Buckets |
|---|---|---|---|
| [config/farmdm/dev.json](../../config/farmdm/dev.json) | `FPACDEV` | `241533156429` | `c108-dev-fpacfsa-*` |
| [config/farmdm/steamdev.json](../../config/farmdm/steamdev.json) | `steam-dev` | `335965711887` | `punkdev-fpacfsa-*` |
| [config/farmdm/prod.json](../../config/farmdm/prod.json) | `PROD` | `253490756794` | `c108-prod-fpacfsa-*` |

---

## Glue Jobs

### Framework Jobs (deployed by this project's `deploy.py`)

| Job name (template) | Script | Source | Target |
|---|---|---|---|
| `FSA-{env}-FpacFARM-Step1-LandingFiles` | `glue/landingFiles/landing_job.py` | S3 landing zone CSV/parquet | S3 landing `etl-jobs/` |
| `FSA-{env}-FpacFARM-Step2-CleansedFiles` | `glue/cleaningFiles/cleaning_job.py` | S3 landing `etl-jobs/` | S3 cleansed zone |
| `FSA-{env}-FpacFARM-Step3-FinalFiles` | `glue/finalFiles/final_job.py` | S3 cleansed zone | S3 final zone |

**Glue config (prod):** `WorkerType: G.4X`, `NumberOfWorkers: 2`, AutoScaling enabled, `GlueVersion: 4.0`, `TimeoutMinutes: 480`

### Standalone Production Glue Scripts (in `glue/`)

| Script | Source | Target |
|---|---|---|
| `FSA-PROD-FarmRecords-Athena-to-PG-FRR.py` | Athena catalog + S3 Parquet | EDV PostgreSQL + CRM_ODS |
| `FSA-PROD-FarmRecords-EDV-s3Parquet-INCR.py` | CDC parquet files from `SOURCE_FILE_BUCKET/SOURCE_FILE_PREFIX` | EDV PostgreSQL + destination S3 |
| `FSA-PROD-FarmRecords-STG-EDV.py` | S3 landing/staging parquet | EDV PostgreSQL; tracks via process-control DB |

PG connections: `FSA-PROD-PG-DART115` + `FSA-PROD-PG-DART114`, VPC `vpc-092b2aee3fba9e5e6`. Credentials via Secrets Manager keys: `edv_postgres_hostname`, `edv_postgres_database_name`, `edv_postgres_username/password`, `CRM_ODS`.

---

## Lambda Functions

All functions use Node.js 20.x with two Lambda layers (`thirdPartyLayerArn` + `customLayerArn`). Shared env vars: `PROJECT=farm`, `LANDING_BUCKET`, `TABLE_NAME=FsaFpacMetadata`, `BUCKET_REGION`.

| Function name (template) | Source | Purpose |
|---|---|---|
| `FSA-{env}-FpacFARM-ValidateInput` | `lambda/Validate/index.mjs` | Lists S3 landing DBO prefix; raises `Error` if no files present. |
| `FSA-{env}-FpacFARM-CreateNewId` | `lambda/CreateNewId/index.mjs` | Generates UUID job ID; writes DynamoDB record with `jobState: "IN_PROGRESS"`. |
| `FSA-{env}-FpacFARM-LogResults` | `lambda/LogResults/index.mjs` | Updates DynamoDB record with final Glue `JobRunState` and full response. |

---

## Step Functions

### Top-Level Orchestrator — `FSA-PROD-FarmRecords-Main`

Defined in [`states/FSA-PROD-FarmRecords-Main.asl.json`](states/FSA-PROD-FarmRecords-Main.asl.json):

```
FSA-PROD-FarmRecords-SAP-Load-MAIN
  → FSA-PROD-FarmRecords-Postgres-Main
    → FSA-PROD-FarmRecords-DM-Main
```

### SAP Load Track — `FSA-PROD-FarmRecords-SAP-Load-MAIN`
```
SAP-Load-Step1 → SAP-Load-Step2 → SAP-Load-Step3 → SAP-Load-Step4
```

### Postgres Track — `FSA-PROD-FarmRecords-Postgres-MAIN`
Sub-state machines defined in `states/pg/`:
```
PG-Load-Step1-Updated → PG-Load-Step2-Updated → ... (additional steps)
```

### Data Mart Track — `FSA-PROD-FarmRecords-DM-Main`
```
Step1 → Step2-1 → Step2 → Step3 → Step4 → Step5
```

### Framework State Machines (Python builder via `farm_rec_stepfunctions.py`)

| State machine | Entry point | Sequence |
|---|---|---|
| `FSA-{env}-FpacFARM-PipelineStep1` | Lambda triggers | ValidateInput → CreateNewId → Step1GlueJob |
| `FSA-{env}-FpacFARM-PipelineStep2` | After Step1 | Step2GlueJob → Step3GlueJob → LogGlueResults → FinalLogResults → StartCrawler → WasGlueSuccessful |
| `FSA-{env}-FpacFARM-Pipeline` | Parent entry | Run Step1 (child SM) → Run Step2 (child SM) |

---

## Key Configuration (prod.json via config/farmdm/)

| Parameter | Value |
|---|---|
| Landing bucket | `c108-prod-fpacfsa-landing-zone` |
| Cleansed bucket | `c108-prod-fpacfsa-cleansed-zone` |
| Final bucket | `c108-prod-fpacfsa-final-zone` |
| Artifact bucket | `fsa-prod-ops` (prefix: `farm/`) |
| Glue catalog DB | `fsa-fpac-db` |
| DynamoDB table | `FsaFpacMetadata` |
| PG connections | `FSA-PROD-PG-DART115`, `FSA-PROD-PG-DART114` |
| Glue role | `arn:aws:iam::253490756794:role/disc-fsa-prod-glue-servicerole` |
| Lambda role | `arn:aws:iam::253490756794:role/disc-fsa-prod-lambda-servicerole` |
| SFN role | `arn:aws:iam::253490756794:role/disc-fsa-prod-stepfunction-servicerole` |

---

## Deploying

> **Note:** The config files for this project are stored under `config/farmdm/` (not `config/farm_records/`).

```bash
cd deploy/

# Production
python deploy.py --config config/farmdm/prod.json --region us-east-1 --project-type farmrec

# Dev
python deploy.py --config config/farmdm/dev.json --region us-east-1 --project-type farmrec

# Steam-dev
python deploy.py --config config/farmdm/steamdev.json --region us-east-1 --project-type farmrec
```

## Project Structure

```
farm_records/
├── deploy.py
├── farm_rec_stepfunctions.py       # Python ASL builder for framework SMs
├── glue/
│   ├── landingFiles/landing_job.py
│   ├── cleaningFiles/cleaning_job.py
│   ├── finalFiles/final_job.py
│   ├── FSA-PROD-FarmRecords-Athena-to-PG-FRR.py
│   ├── FSA-PROD-FarmRecords-EDV-s3Parquet-INCR.py
│   └── FSA-PROD-FarmRecords-STG-EDV.py
├── lambda/
│   ├── Validate/index.mjs
│   ├── CreateNewId/index.mjs
│   └── LogResults/index.mjs
└── states/
    ├── FSA-PROD-FarmRecords-Main.asl.json
    ├── FSA-PROD-FarmRecords-SAP-Load-MAIN.asl.json
    ├── FSA-PROD_FarmRecords-Postgres-MAIN.asl.json
    ├── FSA-PROD-FarmRecords-DM_main.asl.json
    ├── pg/                          # PostgreSQL sub-state machines
    └── sap/                         # SAP load sub-state machines
```
