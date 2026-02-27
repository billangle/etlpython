# nps (FWADM)

NPS / FWADM (Financial & Warehouse Administration Data Mart) pipeline. Reads National Payment System (NPS) and NRRS financial data from PostgreSQL via JDBC and builds Parquet archive tables in S3, catalogued for Athena.

> **Note:** The internal project identifier in all config files is `FWADM`, not `nps`.  
> Resource names follow: `FSA-{deployEnv}-FWADM-{ResourceName}`

**AWS Region:** `us-east-1`  
**Deploy project-type:** `nps`

---

## Overview

```
PostgreSQL (DART114 / DART115) ──JDBC──► Glue Jobs ──► S3 Parquet (landing/final zone)
                                                           └──► Glue Catalog (fwadm_dw)
```

Schema definition JSON files are uploaded to S3 via the `UploadConfig` Lambda as part of deployment. Seven parallel-capable Glue jobs build financial archive tables for NPS and NRRS source systems.

---

## Environments / Configs

| Config file | `deployEnv` | `project` | AWS Account |
|---|---|---|---|
| [config/nps/dev.json](../../config/nps/dev.json) | `dev` | `FWADM` | `241533156429` (FPACDEV) |
| [config/nps/steamdev.json](../../config/nps/steamdev.json) | `steam-dev` | `FWADM` | `335965711887` (STEAMDEV) |
| [config/nps/prod.json](../../config/nps/prod.json) | `PROD` | `FWADM` | `253490756794` (PROD) |

---

## Glue Jobs

**Naming pattern:** `FSA-{deployEnv}-FWADM-{JobName}`

| Job | Source table / purpose | Workers | Max timeout |
|---|---|---|---|
| `FSA-{env}-FWADM-NPS_build_archive_table` | Top-level NPS archive builder; `--area nps --schema_name fwadm_dw --force_full_rebuild False` | G.4X × 4 | 2880 min (48 h) |
| `FSA-{env}-FWADM-payable_dim_tlincremental` | Payable dimension incremental load | G.4X × 4 | 2880 min |
| `FSA-{env}-FWADM-payment_summary` | Payment summary aggregation | G.4X × 5 | 2880 min |
| `FSA-{env}-FWADM-payment_transaction_fact_final` | Payment transaction fact final merge | G.4X × 4 | 4880 min |
| `FSA-{env}-FWADM-payment_transaction_fact_part1` | Transaction fact — part 1 of 3 | G.4X × 4 | 2880 min |
| `FSA-{env}-FWADM-payment_transaction_fact_part2` | Transaction fact — part 2 of 3 | G.4X × 4 | 2880 min |
| `FSA-{env}-FWADM-payment_transaction_fact_part3` | Transaction fact — part 3 of 3 | G.4X × 4 | 2880 min |

**Common Glue job arguments:**

| Argument | Value |
|---|---|
| `--schema_name` | `fwadm_dw` |
| `--area` | `nps` |
| `--force_full_rebuild` | `False` (configurable per run) |
| `--landing_bucket` | `c108-{env}-fpacfsa-landing-zone` |
| `--final_bucket` | `c108-{env}-fpacfsa-final-zone` |
| `--region` | `us-east-1` |

**PythonLibraryPath (prod):** `s3://c108-prod-fpacfsa-landing-zone/dmart/fwadm_utils/utils.py`  
**Glue connections (prod):** `FSA-PROD-PG-DART115`, `FSA-PROD-PG-DART114`  
**AutoScaling:** enabled  
**GlueVersion:** 4.0

---

## Lambda / configUpload

No pipeline Lambda functions. One Lambda is used only during deployment:

| Lambda | Purpose |
|---|---|
| `FSA-{env}-UploadConfig` | Uploads FWADM schema JSON files from `config/raw/fwadm/SCHEMA_JSON/` to `s3://{landing_bucket}/dmart/` |

Schema JSON files define source table structures for NPS and NRRS that Glue jobs read at runtime.

---

## Step Functions

**State machine:** `FSA-{deployEnv}-FWADM-NPS_build_archive_tables`  
**Source:** [`states/NPS_build_archive_tables.asl.json`](states/NPS_build_archive_tables.asl.json)  
**Type:** ASL with placeholder substitution (not Python builder)

**Flow:**

```
RunNPS_build_archive_table  (GlueStartJobRun.sync)
  → RunPayableDimTLIncremental (GlueStartJobRun.sync)
  → RunPaymentSummary         (GlueStartJobRun.sync)
  → RunPaymentTxnFactPart1    (GlueStartJobRun.sync)
  → RunPaymentTxnFactPart2    (GlueStartJobRun.sync)
  → RunPaymentTxnFactPart3    (GlueStartJobRun.sync)
  → RunPaymentTxnFactFinal    (GlueStartJobRun.sync)
  ──► End
```

All states have `TimeoutSeconds: 28800` (8 h). On `FAILED`/`ABORTED`/`TIMED_OUT`, an SNS notification is sent to `FSA-{env}-FWADM-NPS`.

---

## Key Configuration (prod)

| Parameter | Value |
|---|---|
| Landing bucket | `c108-prod-fpacfsa-landing-zone` |
| Final bucket | `c108-prod-fpacfsa-final-zone` |
| Artifact bucket | `fsa-prod-ops` |
| Schema JSON S3 prefix | `s3://c108-prod-fpacfsa-landing-zone/dmart/` |
| Glue catalog DB | `fwadm_dw` |
| Glue role | `arn:aws:iam::253490756794:role/disc-fsa-prod-glue-servicerole` |
| SFN role | `arn:aws:iam::253490756794:role/disc-fsa-prod-stepfunction-servicerole` |
| SNS topic | `FSA-PROD-FWADM-NPS` |
| PG connections | `FSA-PROD-PG-DART115`, `FSA-PROD-PG-DART114` |

---

## Deploying

```bash
cd deploy/

# Production
python deploy.py --config config/nps/prod.json --region us-east-1 --project-type nps

# Dev
python deploy.py --config config/nps/dev.json --region us-east-1 --project-type nps

# Steam-dev
python deploy.py --config config/nps/steamdev.json --region us-east-1 --project-type nps
```

## Running the Pipeline

Start via Step Functions console or CLI:

```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:us-east-1:253490756794:stateMachine:FSA-PROD-FWADM-NPS_build_archive_tables \
  --input '{"force_full_rebuild": false, "area": "nps"}' \
  --region us-east-1
```

## Project Structure

```
nps/
├── __init__.py
├── deploy.py                           # Project deployer
├── config/
│   └── raw/
│       └── fwadm/
│           └── SCHEMA_JSON/            # NPS + NRRS schema definitions
│               ├── NPS_*.json
│               └── NRRS_*.json
├── glue/
│   ├── NPS_build_archive_table.py
│   ├── payable_dim_tlincremental.py
│   ├── payment_summary.py
│   ├── payment_transaction_fact_final.py
│   ├── payment_transaction_fact_part1.py
│   ├── payment_transaction_fact_part2.py
│   └── payment_transaction_fact_part3.py
└── states/
    └── NPS_build_archive_tables.asl.json
```
