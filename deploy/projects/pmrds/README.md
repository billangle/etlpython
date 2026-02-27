# pmrds

Payment Management Reporting Data Store (PMRDS). Aggregates federal farm program payment data from five ODS source systems (CPS, SBSD, ARCPLC, NPS, NRRS) into five financial summary tables in the Final Zone.

**AWS Region:** `us-east-1`  
**Project key:** `pmrds`  
**Resource naming:** `FSA-{deployEnv}-PMRDS-{ResourceName}`

---

## Overview

Two sequential Glue jobs implement the full pipeline. No Lambdas are deployed.

```
CPS ODS   ─┐
SBSD ODS  ─┤
ARCPLC ODS─┼──► Stage1 Glue (ODS→STG)  ──► Stage2 Glue (STG→Final) ──► S3 Final Zone
NPS ODS   ─┤                                                               └──► SNS Notification
NRRS ODS  ─┘
```

After each stage, the Step Functions state machine reads a status JSON written to S3 by the Glue job to determine success or failure before proceeding.

---

## Environments / Configs

| Config file | `deployEnv` | AWS Account |
|---|---|---|
| [config/pmrds/dev.json](../../config/pmrds/dev.json) | `FPACDEV` | `241533156429` |
| [config/pmrds/cert.json](../../config/pmrds/cert.json) | `CERT` | `241533156429` |
| [config/pmrds/steamdev.json](../../config/pmrds/steamdev.json) | `steam-dev` | `335965711887` |
| [config/pmrds/prod.json](../../config/pmrds/prod.json) | `PROD` | `253490756794` |

---

## Glue Jobs

**Naming pattern:** `FSA-{deployEnv}-PMRDS-{JobName}`

| Job | Function | Source | Target |
|---|---|---|---|
| `FSA-{env}-PMRDS-LOAD-STAGING` | Loads all 5 ODS sources into staging tables; applies fiscal-year aggregation | `s3://{cleansed_bucket}/` (CPS, SBSD, ARCPLC, NPS, NRRS ODS Parquet) | `s3://{cleansed_bucket}/pmrds/stg/` + S3 status JSON |
| `FSA-{env}-PMRDS-LOADING-STG2FINAL` | Promotes 5 staging summaries to final financial tables | `s3://{cleansed_bucket}/pmrds/stg/` | `s3://{final_bucket}/pmrds/` + S3 status JSON |

**Shared job arguments:**

| Argument | Value |
|---|---|
| `--env` | `PROD` / `FPACDEV` / etc. |
| `--system_date` | Injected at runtime by Step Functions |
| `--cleansed_bucket` | `c108-{env}-fpacfsa-cleansed-zone` |
| `--final_bucket` | `c108-{env}-fpacfsa-final-zone` |
| `--region` | `us-east-1` |

**Worker config (prod):** `G.4X`, `NumberOfWorkers: 2`, AutoScaling enabled, `Timeout: 480 min`, `MaxConcurrency: 20`, GlueVersion 4.0

---

## Lambda Functions

None. Pipeline status is communicated via S3 status JSON files written by each Glue job.

---

## Step Functions

**State machine:** `FSA-{deployEnv}-PMRDS-ODS-STG-FINAL`  
**Source:** [`states/PMRDS-ODS-STG-FINAL.asl.json`](states/PMRDS-ODS-STG-FINAL.asl.json)

**Flow:**

```
Stage1_ODS_to_STG   (GlueStartJobRun.sync, LOAD-STAGING)
  → Read_Stage1_Status    (S3:GetObject — reads status JSON)
    → Check_Stage1_Error  (Choice: status == "SUCCESS"?)
        ├── NO  → Format_Message → Send_SNS_Notification → Final_Status_Check (Fail)
        └── YES → Stage2_STG_to_Final (GlueStartJobRun.sync, LOADING-STG2FINAL)
                    → Read_Stage2_Status   (S3:GetObject)
                      → Check_Stage2_Error (Choice: status == "SUCCESS"?)
                          ├── NO  → Format_Message → Send_SNS_Notification → Final_Status_Check (Fail)
                          └── YES → Send_SNS_Notification → Final_Status_Check (Succeed)
```

**Required execution input:**

```json
{
  "stage1_job_name": "FSA-PROD-PMRDS-LOAD-STAGING",
  "stage2_job_name": "FSA-PROD-PMRDS-LOADING-STG2FINAL",
  "env": "PROD",
  "system_date": "2024-01-15",
  "cleansed_bucket": "c108-prod-fpacfsa-cleansed-zone",
  "final_bucket": "c108-prod-fpacfsa-final-zone",
  "sns_topic_arn": "arn:aws:sns:us-east-1:253490756794:FSA-PROD-PMRDS"
}
```

All states have `TimeoutSeconds: 28800` (8 h).

---

## Financial Summary Tables (Final Zone)

| Table (S3 prefix) | Description |
|---|---|
| `FNCL_OTLY_SUMM` | Financial outlays summary |
| `FNCL_RCV_SUMM` | Financial receivables summary |
| `FNCL_COLL_SUMM` | Financial collections summary |
| `FNCL_PRMPT_PYMT_INT_SUMM` | Prompt-pay interest summary |
| `FNCL_AMT_SUMM` | Combined net financial amounts summary |

---

## Key Configuration (prod)

| Parameter | Value |
|---|---|
| Cleansed bucket | `c108-prod-fpacfsa-cleansed-zone` |
| Final bucket | `c108-prod-fpacfsa-final-zone` |
| Artifact bucket | `fsa-prod-ops` |
| Glue role | `arn:aws:iam::253490756794:role/disc-fsa-prod-glue-servicerole` |
| SFN role | `arn:aws:iam::253490756794:role/disc-fsa-prod-stepfunction-servicerole` |
| SNS topic ARN | `arn:aws:sns:us-east-1:253490756794:FSA-PROD-PMRDS` |

---

## Deploying

```bash
cd deploy/

# Production
python deploy.py --config config/pmrds/prod.json --region us-east-1 --project-type pmrds

# Dev
python deploy.py --config config/pmrds/dev.json --region us-east-1 --project-type pmrds

# Cert
python deploy.py --config config/pmrds/cert.json --region us-east-1 --project-type pmrds

# Steam-dev
python deploy.py --config config/pmrds/steamdev.json --region us-east-1 --project-type pmrds
```

## Running the Pipeline

```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:us-east-1:253490756794:stateMachine:FSA-PROD-PMRDS-ODS-STG-FINAL \
  --input '{
    "stage1_job_name": "FSA-PROD-PMRDS-LOAD-STAGING",
    "stage2_job_name": "FSA-PROD-PMRDS-LOADING-STG2FINAL",
    "env": "PROD",
    "system_date": "2024-01-15",
    "cleansed_bucket": "c108-prod-fpacfsa-cleansed-zone",
    "final_bucket": "c108-prod-fpacfsa-final-zone",
    "sns_topic_arn": "arn:aws:sns:us-east-1:253490756794:FSA-PROD-PMRDS"
  }' \
  --region us-east-1
```

## Project Structure

```
pmrds/
├── __init__.py
├── deploy.py                           # Project deployer
├── glue/
│   ├── LOAD-STAGING.py                 # ODS → STG (5 source systems)
│   └── LOADING-STG2FINAL.py            # STG → 5 FNCL_* final tables
└── states/
    └── PMRDS-ODS-STG-FINAL.asl.json   # Step Functions ASL (with substitution)
```
