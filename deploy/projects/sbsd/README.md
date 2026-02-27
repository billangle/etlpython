# sbsd

SBSD (Subsidy) Data Mart pipeline. Full end-to-end ETL from Staging → EDV (Data Vault) → PYMT_DM (Payment Data Mart) → Redshift, deployed for the SBSD Farm Program subsidy dataset.

**AWS Region:** `us-east-1`  
**Project key:** `sbsd`  
**Resource naming:** `FSA-{deployEnv}-SBSD-{ResourceName}`

---

## Overview

Two Glue jobs serve as universal SQL executors. Two Python-built Step Functions manage the EDV and DM ETL phases independently.

```
PostgreSQL (DART114/115)
  └─── STG tables (34 tables via SQL)
         └─── EDV (Data Vault: RH/RS/L/LS patterns, concurrent Map)
                └─── PYMT_DM (11 DIM/FACT tables, multi-level Map)
                       └─── Redshift (via PG-TO-REDSHIFT Glue job)
```

SQL definitions for all three layers are stored in `configs/_configs/` and uploaded to S3 as part of deployment.

---

## Environments / Configs

| Config file | `deployEnv` | AWS Account |
|---|---|---|
| [config/sbsd/dev.json](../../config/sbsd/dev.json) | `FPACDEV` | `241533156429` |
| [config/sbsd/cert.json](../../config/sbsd/cert.json) | `CERT` | `241533156429` |
| [config/sbsd/steamdev.json](../../config/sbsd/steamdev.json) | `steam-dev` | `335965711887` |
| [config/sbsd/prod.json](../../config/sbsd/prod.json) | `PROD` | `253490756794` |

---

## Glue Jobs

**Naming pattern:** `FSA-{deployEnv}-SBSD-{JobName}`

| Job | Function | Connections |
|---|---|---|
| `FSA-{env}-SBSD-DATAMART-EXEC-DB-SQL` | Universal SQL executor — runs SQL scripts from S3 against PostgreSQL; handles STG loads, EDV inserts, DM population | `FSA-{env}-PG-DART114` (dev/cert) or both `FSA-PROD-PG-DART114` + `FSA-PROD-PG-DART115` (prod) |
| `FSA-{env}-SBSD-DART-PG-TO-REDSHIFT` | Copies tables from PostgreSQL DM schema to Redshift target | `FSA-{env}-PG-DART114` (dev/cert), both DART connections (prod), Redshift connection |

**Worker config (prod):** `G.4X`, `NumberOfWorkers: 2`, AutoScaling enabled, `Timeout: 480 min`, GlueVersion 4.0

**Key Glue job arguments:**

| Argument | Value |
|---|---|
| `--env` | `PROD` / `FPACDEV` / etc. |
| `--project` | `SBSD` |
| `--sql_bucket` | `c108-{env}-fpacfsa-landing-zone` |
| `--sql_prefix` | `sbsd/configs` |
| `--region` | `us-east-1` |

---

## Lambda Functions

Four Lambda functions manage the EDV processing plan lifecycle:

**Naming pattern:** `FSA-{deployEnv}-SBSD-{Suffix}`

| Function (template) | Purpose |
|---|---|
| `FSA-{env}-SBSD-edv-build-processing-plan` | Reads EDV config from S3; builds ordered table processing plan; writes plan to DynamoDB |
| `FSA-{env}-SBSD-edv-check-results` | Reads DynamoDB processing plan; checks for any table failures from Map state results |
| `FSA-{env}-SBSD-edv-finalize-pipeline` | Marks pipeline complete in DynamoDB; sends SNS success notification |
| `FSA-{env}-SBSD-edv-handle-failure` | Called on Map failure; marks plan as failed; sends SNS failure notification |

---

## Step Functions

Built by [`sbsd_stepfunction.py`](sbsd_stepfunction.py) and [`sbsd_dm_etl_stepfunction.py`](sbsd_dm_etl_stepfunction.py).

### EDV Pipeline — `FSA-{env}-SBSD-EDV-Pipeline`

Orchestrates STG load and EDV Data Vault population with configurable concurrency.

```
BuildProcessingPlan  (Lambda invoke → edv-build-processing-plan)
  → CheckTablesExist (Choice: tableCount > 0?)
      └── YES → ProcessStageTables (Map MaxConcurrency=20)
                  │  each item: ExecDBSQL Glue job (STG SQL)
                  └─► CheckStageResults (Lambda → edv-check-results)
                        → ProcessEDVGroups (Map MaxConcurrency=1 outer, 10 inner)
                            │  each group: ExecDBSQL Glue job (EDV SQL)
                            └─► FinalizePipeline (Lambda → edv-finalize-pipeline)
                  (on any failure) → HandleFailure (Lambda → edv-handle-failure)
      └── NO  → FinalizePipeline (success, nothing to process)
```

### DM ETL Pipeline — `FSA-{env}-SBSD-DM-ETL-Pipeline`

Orchestrates PYMT_DM population and Redshift export in dependency-ordered levels.

```
Level1ProcessTables (Map MaxConcurrency=20)
  │  each item: ExecDBSQL (PYMT_DM Level-1 DIM/FACT SQL)
  └─► Level2ProcessTables (Map MaxConcurrency=20)
        │  each item: ExecDBSQL (PYMT_DM Level-2 SQL)
        └─► Level3RedshiftLoad (Map MaxConcurrency=5)
              │  each item: RedshiftGlue (PG-TO-REDSHIFT job, Level-3 tables)
              └─► Level3L2RedshiftLoad (Map MaxConcurrency=5)
                    │  each item: RedshiftGlue (Level-3 L2 tables)
                    └─► End
```

All Glue states have `TimeoutSeconds: 28800` (8 h).

---

## SQL Config Layers

SQL scripts are stored in `configs/_configs/` and uploaded to `s3://{landing_bucket}/sbsd/configs/` during deployment.

| Layer | Tables | Pattern |
|---|---|---|
| STG | ~34 tables | Initial load + Incremental scripts per table |
| EDV (Data Vault) | Multiple | Hub (H), Hub Satellite (HS), Link (L), Link Satellite (LS), Reference Hub (RH), Reference Satellite (RS) patterns |
| PYMT_DM | ~11 tables | DIM_*, FACT_* payment dimension/fact tables |

---

## Key Configuration (prod)

| Parameter | Value |
|---|---|
| Landing bucket | `c108-prod-fpacfsa-landing-zone` |
| Cleansed bucket | `c108-prod-fpacfsa-cleansed-zone` |
| Final bucket | `c108-prod-fpacfsa-final-zone` |
| Artifact bucket | `fsa-prod-ops` |
| Glue connections | `FSA-PROD-PG-DART115`, `FSA-PROD-PG-DART114` |
| Glue role | `arn:aws:iam::253490756794:role/disc-fsa-prod-glue-servicerole` |
| Lambda role | `arn:aws:iam::253490756794:role/disc-fsa-prod-lambda-servicerole` |
| SFN role | `arn:aws:iam::253490756794:role/disc-fsa-prod-stepfunction-servicerole` |
| SNS topic | `FSA-PROD-SBSD` |

---

## Deploying

```bash
cd deploy/

# Production
python deploy.py --project-type sbsd --config config/sbsd/prod.json --region us-east-1

# Dev
python deploy.py --project-type sbsd --config config/sbsd/dev.json --region us-east-1

# Cert
python deploy.py --project-type sbsd --config config/sbsd/cert.json --region us-east-1

# Steam-dev
python deploy.py --project-type sbsd --config config/sbsd/steamdev.json --region us-east-1
```

## Running the Pipelines

```bash
# EDV Pipeline
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:us-east-1:253490756794:stateMachine:FSA-PROD-SBSD-EDV-Pipeline \
  --input '{"env": "PROD", "project": "SBSD"}' \
  --region us-east-1

# DM ETL Pipeline (run after EDV)
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:us-east-1:253490756794:stateMachine:FSA-PROD-SBSD-DM-ETL-Pipeline \
  --input '{"env": "PROD", "project": "SBSD"}' \
  --region us-east-1
```

## Project Structure

```
sbsd/
├── __init__.py
├── deploy.py                           # Project deployer
├── sbsd_stepfunction.py                # Python ASL builder → EDV-Pipeline
├── sbsd_dm_etl_stepfunction.py         # Python ASL builder → DM-ETL-Pipeline
├── configs/
│   └── _configs/
│       ├── stg/                        # ~34 STG table SQL scripts
│       ├── edv/                        # EDV Data Vault SQL (H/HS/L/LS/RH/RS)
│       └── pymt_dm/                    # ~11 DIM/FACT SQL scripts
└── lambda/
    ├── edv-build-processing-plan/
    ├── edv-check-results/
    ├── edv-finalize-pipeline/
    └── edv-handle-failure/
```
