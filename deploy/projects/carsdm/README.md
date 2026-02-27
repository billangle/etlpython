# carsdm

Ingests CARS (Commodity and Assistance Reporting System) data from AWS DMS-replicated S3 Parquet files (full LOAD + incremental CDC) and publishes final curated and CDC-zone parquet files to the FSA Final Zone.

**AWS Region:** `us-east-1`  
**Project key:** `carsdm`  
**Resource naming:** `FSA-{deployEnv}-CARSDM-{ResourceName}`

---

## Overview

```
DMS (SQL Server → S3) parquet files
  ├── LOAD files  ─┐
  └── CDC files   ─┴─► LandingFiles Glue ──► Datasets/ (landing zone)
                                                 │
                                                 └─► Cars-Raw-DM Glue
                                                       ├─► Final Zone: /cars/<TABLE>/
                                                       └─► CDC Zone: /cars/_cdc/<TABLE>/dart_filedate=YYYY-MM-DD/
```

A PostgreSQL process control database (`dart_process_control.data_ppln_job`, via Secrets Manager) tracks job start/end, and SNS publishes failure alerts per table.

---

## Environments

| Config file | `deployEnv` | AWS Account | Landing bucket | Final zone bucket |
|---|---|---|---|---|
| [config/carsdm/dev.json](../../config/carsdm/dev.json) | `FPACDEV` | `241533156429` | `c108-dev-fpacfsa-landing-zone` | `c108-dev-fpacfsa-final-zone` |
| [config/carsdm/carssteam.json](../../config/carsdm/carssteam.json) | `steam-dev` | `335965711887` | `punkdev-fpacfsa-landing-zone` | `punkdev-fpacfsa-final-zone` |
| [config/carsdm/prod.json](../../config/carsdm/prod.json) | `PROD` | `253490756794` | `c108-prod-fpacfsa-landing-zone` | `c108-prod-fpacfsa-final-zone` |

---

## Glue Jobs

| Job name (PROD) | Script | Source | Target | Description |
|---|---|---|---|---|
| `FSA-PROD-CARSDM-LandingFiles` | `glue/FSA-CERT-CARS-LandingFiles.py` | S3 Landing Zone DMS parquet (`<SourcePrefix>/dbo/<TABLE>/`) | `s3://landing/<DestinationPrefix>/Datasets/<TABLE>/` | Detects new DMS LOAD/CDC parquet files, schema-aligns, merges multi-file dates, writes consolidated Parquet datasets. Up to 8 tables in parallel; ~1 GiB snappy parts. |
| `FSA-PROD-CARSDM-Cars-Raw-DM` | `glue/FSA-CERT-Cars-Raw-DM.py` | S3 Landing `Datasets/<TABLE>/` | Final Zone `/cars/<TABLE>/` + `_cdc/<TABLE>/dart_filedate=YYYY-MM-DD/` | Applies schema typing, PK-based deduplication/merge; writes permanent Final Zone (no deletes) and 10-day CDC Zone (all ops including deletes). |

**Worker configuration (prod):**

| Job | WorkerType | Workers | AutoScaling | Timeout |
|---|---|---|---|---|
| LandingFiles | `G.4X` | 2 | true | 480 min |
| Cars-Raw-DM | `G.4X` | 2 | true | 480 min |

**Glue version:** 4.0  
**Artifact bucket:** `fsa-prod-ops` (prefix: `carsdm/`)

---

## Lambda Functions

| Function name (PROD) | Source | Purpose |
|---|---|---|
| `FSA-PROD-CARSDM-get-incremental-tables` | `lambda/get-incremental-tables/` | Scans S3 Datasets folder to discover which tables were produced for the current job; strips `-LOAD` suffixes; returns `{"tables": [...]}` for the Map state. |
| `FSA-PROD-CARSDM-RAW-DM-sns-publish-step-function-errors` | `lambda/sns-publish-step-function-errors/` | On Glue job failure in the Map state, parses `JobRunState`, `JobName`, `TableName`, and `ErrorMessage`; publishes to SNS. |
| `FSA-PROD-CARSDM-RAW-DM-etl-workflow-update-data-ppln-job` | `lambda/etl-workflow-update-data-ppln-job/` | Updates `dart_process_control.data_ppln_job` in the process-control Postgres DB; sets job end datetime and final status. |

---

## Step Functions

### `FSA-PROD-CARSDM-Cars-S3Landing-to-S3Final-Raw-DM`

Defined by the Python builder in [`states/carsdm_stepfunction.py`](states/carsdm_stepfunction.py) — ASL is generated at deploy time. Pre-baked static ASL reference files also exist in `states/` for cert and prod environments.

```
GetIncrementalTableListFromS3   (Lambda: get-incremental-tables)
          │
CARS Tables                     (Pass: route JobId, SfName, Target="cars")
          │
Map (MaxConcurrency=12, per table)
  ├── Pass                       (extract JobId, table, Target)
  ├── MoveToFinalZone           (Glue startJobRun.sync: Cars-Raw-DM per table)
  │     └── on error → SNS Lambda RAW DM → Pass (status="Error")
          │
data_ppl_job_update RAW DM      (Lambda: etl-workflow-update-data-ppln-job)
          │
RunBothCrawlers (Parallel)
  ├── FSA-{ENV}-CARS Final Zone Crawler
  └── FSA-{ENV}-CARS CDC Crawler
```

---

## Key Configuration (prod.json)

| Parameter | Value |
|---|---|
| Landing bucket | `c108-prod-fpacfsa-landing-zone` |
| Final zone bucket | `c108-prod-fpacfsa-final-zone` |
| Artifact bucket | `fsa-prod-ops` (prefix: `carsdm/`) |
| Spark UI logs | `s3://fsa-prod-ops/glue-logs/spark-ui/carsdm/` |
| Temp path | `s3://fsa-prod-ops/temp/glue/` |
| PG process control connection (prod) | `FSA-PROD-PG-DART115` + `FSA-PROD-PG-DART114` |
| Crawlers (prod) | `FSA-PROD-CARS-CRAWLER`, `FSA-PROD-CARS-cdc` |
| Glue role | `arn:aws:iam::253490756794:role/disc-fsa-prod-glue-servicerole` |
| Lambda role | `arn:aws:iam::253490756794:role/disc-fsa-prod-lambda-servicerole` |
| SFN role | `arn:aws:iam::253490756794:role/disc-fsa-prod-stepfunction-servicerole` |

---

## Deploying

```bash
cd deploy/

# Production
python deploy.py --config config/carsdm/prod.json --region us-east-1 --project-type carsdm

# Dev
python deploy.py --config config/carsdm/dev.json --region us-east-1 --project-type carsdm

# Steam-dev sandbox
python deploy.py --config config/carsdm/carssteam.json --region us-east-1 --project-type carsdm
```

The deployer uploads both Glue scripts to the artifact bucket, creates/updates the 3 Lambda functions, 2 Glue jobs, and the Step Functions state machine.

## Project Structure

```
carsdm/
├── deploy.py
├── glue/
│   ├── FSA-CERT-CARS-LandingFiles.py   # DMS parquet → Datasets/
│   └── FSA-CERT-Cars-Raw-DM.py         # Datasets/ → Final Zone + CDC Zone
├── lambda/
│   ├── get-incremental-tables/
│   ├── sns-publish-step-function-errors/
│   └── etl-workflow-update-data-ppln-job/
└── states/
    ├── carsdm_stepfunction.py          # Python ASL builder (deployed)
    ├── FSA-CERT-Cars-S3Landing-to-S3Final-Raw-DM.asl.json  # Reference
    └── FSA-PROD-Cars-S3Landing-to-S3Final-Raw-DM.asl.json  # Reference
```
