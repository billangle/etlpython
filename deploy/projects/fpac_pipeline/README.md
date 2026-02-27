# fpac_pipeline

Base shared FSA/FPAC ETL pipeline framework. Deploys a reusable three-stage data processing pipeline (Landing → Cleansed → Final) parameterised per project and environment. Multiple downstream projects (`cars`, `sbsd`, `pmrds`, etc.) use this framework.

**AWS Region:** `us-east-2` (Glue/Lambda/SFN); S3 in `us-east-1`  
**Project key:** `fpac`  
**Resource naming:** `FSA-{deployEnv}-Fpac{PROJECT.upper()}-{ResourceName}`

---

## Overview

Raw CSV files are deposited in S3, validated by Lambda, assigned a DynamoDB tracking ID, then processed by three sequential Glue jobs that progressively refine the data into a Glue-catalogued Final Zone parquet.

```
S3 Landing Zone /dbo/  ──► ValidateInput (λ) ──► CreateNewId (λ) ──► Step1GlueJob
                                                                           │
                                                              Step2GlueJob ─►  Step3GlueJob
                                                                               └──► Crawler ──► Glue Catalog (Athena)
```

---

## Environments / Configs

These configs live at the root `deploy/config/` level (not in a subdirectory):

| Config file | `deployEnv` | `project` | AWS Account | `--region` |
|---|---|---|---|---|
| [config/fpacprod.json](../../config/fpacprod.json) | `prod` | `pyprod` | `253490756794` | `us-east-2` |
| [config/py2deployer.json](../../config/py2deployer.json) | `dev` | `py2dev` | `241533156429` | `us-east-2` |
| [config/pydemo.json](../../config/pydemo.json) | `demo` | `pydemo` | `241533156429` | `us-east-2` |
| [config/depeast2.json](../../config/depeast2.json) | varies | varies | varies | `us-east-2` |

---

## Glue Jobs

**Naming pattern:** `FSA-{deployEnv}-Fpac{PROJECT.upper()}-Step{N}-{StageName}`

| Job name (template) | Script | Source | Target |
|---|---|---|---|
| `FSA-{env}-Fpac{PROJ}-Step1-LandingFiles` | `glue/landingFiles/landing_job.py` | `s3://{landing_bucket}/{project}/dbo/` (CSV) | `s3://{landing_bucket}/{project}/etl-jobs/` (Parquet) |
| `FSA-{env}-Fpac{PROJ}-Step2-CleansedFiles` | `glue/cleaningFiles/cleaning_job.py` | `s3://{landing_bucket}/{project}/etl-jobs/` | `s3://{clean_bucket}/{project}/etl-jobs/` (Parquet) |
| `FSA-{env}-Fpac{PROJ}-Step3-FinalFiles` | `glue/finalFiles/final_job.py` | `s3://{clean_bucket}/{project}/etl-jobs/` | `s3://{final_bucket}/{project}/` (Parquet) |
| `FSA-{env}-Fpac{PROJ}-CRAWLER` | Glue Crawler | `s3://{final_bucket}/` | Glue DB `fsa-fpac-db` |

**Default args injected on all jobs:** `--env`, `--project`, `--landing_bucket`, `--clean_bucket`, `--final_bucket`, `--bucket_region`, `--step`

**Worker config (prod):** `WorkerType: G.4X`, `NumberOfWorkers: 2`, GlueVersion 4.0, AutoScaling enabled, `TimeoutMinutes: 480`

---

## Lambda Functions

Runtime: Node.js 20.x. Shared env vars: `PROJECT`, `LANDING_BUCKET`, `TABLE_NAME` (`FsaFpacMetadata`), `BUCKET_REGION`.

**Naming pattern:** `FSA-{deployEnv}-Fpac{PROJECT.upper()}-{Suffix}`

| Function name (template) | Source | Purpose |
|---|---|---|
| `FSA-{env}-Fpac{PROJ}-ValidateInput` | `lambda/Validate/index.mjs` | Lists `s3://{landing_bucket}/{project}/dbo` with `MaxKeys:1`; throws `Error` if `KeyCount == 0`. Gate at pipeline start. |
| `FSA-{env}-Fpac{PROJ}-CreateNewId` | `lambda/CreateNewId/index.mjs` | Generates UUID v4 job ID; writes DynamoDB record with `jobState: "IN_PROGRESS"`, `project`, and `timestamp`. |
| `FSA-{env}-Fpac{PROJ}-LogResults` | `lambda/LogResults/index.mjs` | Receives Step Functions output; updates DynamoDB record with final Glue `JobRunState` and full response. |

---

## Step Functions

Built by [`fpac_stepfunctions.py`](fpac_stepfunctions.py) via `FpacStateMachineBuilder`. Three state machines per deployment.

**Naming pattern:** `FSA-{deployEnv}-Fpac{PROJECT.upper()}-{Suffix}`

### `FSA-{env}-Fpac{PROJ}-PipelineStep1`
```
ValidateInput (Lambda invoke)
  → CreateNewId (Lambda invoke)
    → Step1GlueJob (glue:startJobRun.sync) ──► End
```

### `FSA-{env}-Fpac{PROJ}-PipelineStep2`
```
Step2GlueJob (glue:startJobRun.sync)
  → Step3GlueJob (glue:startJobRun.sync)
    → LogGlueResults (Pass)
      → FinalLogResults (Lambda invoke → LogResults fn)
        → StartCrawler (aws-sdk:glue:startCrawler)
          → WasGlueSuccessful (Choice)
              SUCCEEDED → Success (Succeed)
              default   → Fail (Fail)
```

### `FSA-{env}-Fpac{PROJ}-Pipeline` ← Parent / entry point
```
Run Step1 (states:startExecution.sync:2 → PipelineStep1)
  → Run Step2 (states:startExecution.sync:2 → PipelineStep2) ──► End
```

---

## Key Configuration (fpacprod.json)

| Parameter | Value |
|---|---|
| Landing bucket | `c108-prod-fpacfsa-landing-zone` |
| Cleansed bucket | `c108-prod-fpacfsa-cleansed-zone` |
| Final bucket | `c108-prod-fpacfsa-final-zone` |
| Artifact bucket | `fsa-prod-ops` (prefix: `pyprod/`) |
| Glue catalog DB | `fsa-fpac-db` |
| DynamoDB table | `FsaFpacMetadata` |
| Glue role | `arn:aws:iam::253490756794:role/disc-fsa-prod-glue-servicerole` |
| Lambda role | `arn:aws:iam::253490756794:role/disc-fsa-prod-lambda-servicerole` |
| SFN role | `arn:aws:iam::253490756794:role/disc-fsa-prod-stepfunction-servicerole` |

---

## Deploying

```bash
cd deploy/

# Production
python deploy.py --config config/fpacprod.json --region us-east-2 --project-type fpac

# Dev
python deploy.py --config config/py2deployer.json --region us-east-2 --project-type fpac

# Demo
python deploy.py --config config/pydemo.json --region us-east-2 --project-type fpac
```

Glue scripts are uploaded to `s3://{artifactBucket}/{prefix}glue/{landing_job,cleaning_job,final_job}.py`.

## Project Structure

```
fpac_pipeline/
├── __init__.py
├── deploy.py                   # Project deployer
├── fpac_stepfunctions.py       # Python ASL builder for all 3 SMs
├── glue/
│   ├── landingFiles/landing_job.py
│   ├── cleaningFiles/cleaning_job.py
│   └── finalFiles/final_job.py
└── lambda/
    ├── Validate/index.mjs      # Input file existence check
    ├── CreateNewId/index.mjs   # DynamoDB job ID creation
    └── LogResults/index.mjs    # Final status logging
```
