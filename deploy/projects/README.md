# FPAC ETL Projects

This directory contains one sub-folder per deployed project. Each project is an
independent AWS ETL pipeline with its own deployer, Glue jobs, Lambda functions,
Step Functions state machines, and (where applicable) S3-uploaded configuration
data.

---

## Directory Layout

```
deploy/
├── common/                     # Shared helpers (aws_common.py, GlueJobSpec, etc.)
├── config/                     # Per-project, per-environment config JSON files
│   ├── athenafarm/
│   │   ├── dev.json
│   │   ├── prod.json
│   │   └── steamdev.json
│   ├── cnsv/
│   │   ├── dev.json
│   │   ├── prod.json
│   │   └── steamdev.json
│   └── <project>/              # Same pattern for every project
│       ├── dev.json
│       ├── prod.json
│       ├── steamdev.json
│       └── cert.json           # Some projects only
├── deploy.py                   # Master deployer — delegates to each project
└── projects/
    ├── athenafarm/
    ├── cars/
    ├── carsdm/
    ├── cnsv/
    ├── farm_records/
    ├── farmdm/
    ├── fbp_rpt/
    ├── flpids/
    ├── fmmi/
    ├── fpac_pipeline/
    ├── nps/
    ├── pmrds/
    ├── sbsd/
    └── tsthooks/
```

### Standard Project Sub-folder Structure

```
projects/<project>/
├── deploy.py               # Main deployer (always present)
├── deploy_*.py             # Additional pipeline deployers (multi-pipeline projects)
├── deploy_config.sh        # S3 config uploader — only in projects that push config data
├── glue/                   # Glue Spark job Python scripts
│   └── *.py
├── lambda/                 # Lambda function source directories
│   └── <function-name>/
│       └── lambda_function.py
├── states/                 # Step Functions ASL JSON files
│   └── *.asl.json | *.param.asl.json
├── configs/                # Local config data to upload to S3 (select projects only)
│   └── _configs/
│       ├── STG/
│       ├── EDV/
│       ├── DM/
│       └── metadata/
├── pipe/                   # Jenkins pipeline definitions
├── scripts/                # Utility / one-off scripts
└── tests/                  # Unit / integration tests
```

---

## Config Files (`deploy/config/`)

Each project has a `deploy/config/<project>/` directory containing one JSON file
per deployment environment. These are passed to the deployer at runtime:

```bash
python deploy.py --config config/cnsv/prod.json --region us-east-1 --project-type cnsv
```

### Environments

| File | Environment |
|---|---|
| `dev.json` | Development |
| `steamdev.json` | Steam / Cert-Dev |
| `cert.json` | Certification (selected projects) |
| `prod.json` | Production |

### Config File Structure — `cnsv/prod.json` Example

```json
{
  "deployEnv": "PROD",           // Token used in all resource names: FSA-PROD-...
  "project":   "CNSV",          // Project tag baked into every resource name
  "region":    "us-east-1",
  "secretId":  "FSA-PROD-secrets",

  // Project-specific metadata (DynamoDB table, Athena DB, etc.)
  "configData": {
    "databaseName":    "fsa-fpac-db",
    "dynamoTableName": "FsaFpacMetadata"
  },

  // Parameterised ARNs and bucket names — referenced by the deployer
  "strparams": {
    "landingBucketNameParam":   "c108-prod-fpacfsa-landing-zone",
    "cleanBucketNameParam":     "c108-prod-fpacfsa-cleansed-zone",
    "finalBucketNameParam":     "c108-prod-fpacfsa-final-zone",
    "glueJobRoleArnParam":      "arn:aws:iam::253490756794:role/disc-fsa-prod-glue-servicerole",
    "etlRoleArnParam":          "arn:aws:iam::253490756794:role/disc-fsa-prod-lambda-servicerole",
    "snsArnParam":              "arn:aws:sns:us-east-1:253490756794:FSA-PROD-CNSV"
  },

  // S3 bucket + prefix where Glue scripts and Lambda ZIPs are uploaded
  "artifacts": {
    "artifactBucket": "fsa-prod-ops",
    "prefix":         "cnsv/"
  },

  // Step Functions IAM roles
  "stepFunctions": {
    "roleArn":             "arn:aws:iam::253490756794:role/disc-fsa-prod-stepfunction-servicerole",
    "nestedCallerRoleArn": "arn:aws:iam::253490756794:role/disc-fsa-prod-stepfunction-nestedcaller-servicerole"
  },

  // Present only in projects that upload config data to S3 (see section below)
  "configUpload": {
    "sourceDir":          "configs",         // local configs/_configs/ subtree
    "outputBucket":       "c108-prod-fpacfsa-final-zone",
    "outputPrefix":       "cnsv",
    "minExpectedObjects": 2,
    "debug": true
  },

  // Array of per-Glue-job overrides; stem name must match the script filename
  "GlueConfig": [
    {
      "LandingFiles": {
        "WorkerType":         "G.2X",
        "NumberOfWorkers":    "2",
        "TimeoutMinutes":     "2880",
        "AutomaticScalingEnabled": "true",
        "GlueVersion":        "4.0",
        "Connections":        [{ "ConnectionName": "" }],
        "JobParameters": {
          "--DestinationBucket": "c108-prod-fpacfsa-landing-zone",
          "--PipelineName":      "cnsv"
        }
      }
    }
  ]
}
```

Every field in `strparams` becomes a deployer variable. `GlueConfig` entries are
matched by script stem name (e.g. `"LandingFiles"` → `LandingFiles.py`) and
merged with global Glue defaults before calling `ensure_glue_job()`.

---

## Projects

### Resource Summary

| Project | Deploy Scripts | Glue Jobs | Lambda Functions | Step Functions | S3 Config Upload |
|---|:---:|:---:|:---:|:---:|:---:|
| **athenafarm** | 1 | 7 | 1 | 2 | — |
| **cars** | 1 | 5 | — | 3 | — |
| **carsdm** | 1 | 2 | — | 2 | — |
| **cnsv** | 5 | 7 | 22 | 13 | ✓ |
| **farm_records** | 1 | 3 | — | 4 | — |
| **farmdm** | 1 | — | — | — | ✓ |
| **fbp_rpt** | — | — | — | — | — |
| **flpids** | 1 | — | ✓ | ✓ | — |
| **fmmi** | 1 | 3 | — | 1 | — |
| **fpac_pipeline** | 1 | ✓ | 3 | — | — |
| **nps** | 1 | 7 | — | 1 | — |
| **pmrds** | 1 | 2 | — | 1 | — |
| **sbsd** | 1 | 4 | — | ✓ | ✓ |
| **tsthooks** | 1 | — | ✓ | — | — |

---

### athenafarm

**Purpose:** Ingests SSS/IBase farm data and PostgreSQL reference + CDC tables
into Apache Iceberg on S3, then transforms the result into `tract_producer_year`
and `farm_producer_year` gold tables and optionally syncs delta rows back to RDS.

| Resource | Count | Notes |
|---|:---:|---|
| Glue jobs | 7 | Ingest-SSS-Farmrecords, Ingest-PG-Reference-Tables, Ingest-PG-CDC-Targets, Transform-Tract-Producer-Year, Transform-Farm-Producer-Year, Sync-Iceberg-To-RDS, Iceberg-Maintenance |
| Lambda | 1 | `notify_pipeline` — SNS notification |
| Step Functions | 2 | `Main` (ingest → transform → sync), `Maintenance` (Iceberg compaction) |
| Config environments | 3 | dev, prod, steamdev |

**Key config fields:** `icebergWarehouse`, `sssFarmrecordsS3Path`, `secretId`

---

### cars

**Purpose:** CARS (Common Area Reporting System) ETL — S3 landing → S3 final
raw/DM zone movement and PostgreSQL EDV load.

| Resource | Count | Notes |
|---|:---:|---|
| Glue jobs | 5 | S3 landing-to-final, parquet-to-postgres, validation |
| Step Functions | 3 | Landing-to-Final-Raw-DM, CERT datamart ETL, S3-to-postgres EDV load |
| Config environments | 4 | dev, prod, steamdev, carssteam |

---

### carsdm

**Purpose:** CARS Data Mart — builds the CARS dimensional model from raw zone
data into the final-zone DM.

| Resource | Count | Notes |
|---|:---:|---|
| Glue jobs | 2 | S3-to-DM transformation |
| Step Functions | 2 | PROD and CERT variants |
| Config environments | 3 | dev, prod, carssteam |

---

### cnsv

**Purpose:** Conservation (CNSV) multi-pipeline ETL — processes EXEC-SQL EDV
data, base conservation records, contract maintenance, and conservation payment
data. The most complex project with 5 separate deployment pipelines.

| Resource | Count | Notes |
|---|:---:|---|
| Glue jobs | 7 | LandingFiles, StagingFiles, CleansingFiles, and pipeline-specific variants |
| Lambda functions | 22 | edv-* orchestration Lambdas, Job-Logging-End, SNS error handlers, validation-check, get-incremental-tables across Base, Cntr-Maint, and Cons-Pymts pipelines |
| Step Functions | 13 | EXEC-SQL, Base, Cntr-Maint (x4), Cons-Pymts (x4), and supporting state machines |
| Config environments | 3 | dev, prod, steamdev |
| **S3 Config Upload** | ✓ | `configs/_configs/` → `CMN_DIM_DM/`, `DM/`, `EDV/`, `STG/`, `metadata/` |

**Deploy scripts:**

| Script | Jenkins Job | Purpose |
|---|---|---|
| `deploy.py` + `deploy_config.sh` | DeployCNSV | EXEC-SQL pipeline + S3 config upload |
| `deploy_base.py` | DeployCNSVBase | Base conservation pipeline |
| `deploy_maint.py` | DeployCNSVMaint | Contract Maintenance pipeline |
| `deploy_pymts.py` | DeployCNSVPymts | Conservation Payments pipeline |

**S3 Config Upload:** `deploy_config.sh` zips the `configs/_configs/` subtree
and uploads it to S3 via the `configUpload` block in the config JSON. This
provides EDV query templates, staging mappings, and DM transformation rules
consumed at runtime by the Glue jobs and Lambdas.

---

### farm_records

**Purpose:** Farm Records pipeline — orchestrates SAP load, PostgreSQL sync, and
data mart ETL for FSA farm records.

| Resource | Count | Notes |
|---|:---:|---|
| Glue jobs | 3 | SAP load, PostgreSQL, DM jobs |
| Step Functions | 4 | Main, SAP-Load-Main, Postgres-Main, DM-Main |

---

### farmdm

**Purpose:** Farm Data Mart config deployment — uploads DM configuration data to
S3. No Glue jobs or Lambda; purely a config promotion pipeline.

| Resource | Count | Notes |
|---|:---:|---|
| Glue jobs | — | Config-only project |
| **S3 Config Upload** | ✓ | `configs/_configs/` → `dm/`, `edv/`, `prep/`, `stg/`, `metadata/` |

**Deploy script:** `deploy_config.sh` only.

---

### fbp_rpt

**Purpose:** FBP (Farm Business Plan) Reporting — contains an Athena SQL
definition. No AWS infrastructure deployment; report definition only.

---

### flpids

**Purpose:** FLPIDS FTPS File Check pipeline — monitors and validates FTPS file
transfers for the FLPIDS program.

| Resource | Count | Notes |
|---|:---:|---|
| Lambda | ✓ | File-check Lambda functions |
| Step Functions | ✓ | FLPIDS_FTPS_File_Check_Full pipeline |

---

### fmmi

**Purpose:** FMMI (Financial Management / Modern Infrastructure) ETL — CSV
staging through ODS to final zone.

| Resource | Count | Notes |
|---|:---:|---|
| Glue jobs | 3 | CSV-STG, ODS, and ODS-to-final |
| Step Functions | 1 | CSV-STG-ODS pipeline |
| Config environments | 3 | dev, prod, steamdev |

---

### fpac_pipeline

**Purpose:** FPAC shared pipeline orchestration — common Glue landing/cleaning/
final file-processing jobs and Lambda utility functions shared across programs.

| Resource | Count | Notes |
|---|:---:|---|
| Glue jobs | ✓ | `landingFiles/`, `cleaningFiles/`, `finalFiles/` |
| Lambda | 3 | `CreateNewId`, `LogResults`, `Validate` |

---

### nps

**Purpose:** NPS (National Programs Standard) ETL — builds archive tables from
NPS source data through landing to final zone.

| Resource | Count | Notes |
|---|:---:|---|
| Glue jobs | 7 | Landing, staging, archival, and DM jobs |
| Step Functions | 1 | NPS build-archive-tables pipeline |
| Config environments | 3 | dev, prod, steamdev |

---

### pmrds

**Purpose:** PMRDS ETL — processes PMRDS data from ODS through staging to final
DM zone.

| Resource | Count | Notes |
|---|:---:|---|
| Glue jobs | 2 | ODS-STG and STG-FINAL jobs |
| Step Functions | 1 | PMRDS-ODS-STG-FINAL pipeline |
| Config environments | 4 | dev, prod, steamdev, cert |

---

### sbsd

**Purpose:** SBSD (Shared Business Services Data) ETL — builds the SBSD DM from
staging data with config-driven transformation rules.

| Resource | Count | Notes |
|---|:---:|---|
| Glue jobs | 4 | STG and DM transformation jobs |
| **S3 Config Upload** | ✓ | `configs/_configs/` → `PYMT_DM/`, `STG/`, `edv/`, `metadata/` |
| Config environments | 4 | dev, prod, steamdev, cert |

**Deploy script:** `deploy.py` + `deploy_config.sh` (config upload).

---

### tsthooks

**Purpose:** Test hooks — Jenkins webhook integration Lambda functions used for
pipeline testing and CI/CD hook validation.

| Resource | Count | Notes |
|---|:---:|---|
| Lambda | ✓ | Webhook handler Lambdas |
| Config environments | 2 | fpacdev, steam |

---

## Projects with S3 Config Uploads

Three projects push local configuration data to S3 as part of deployment. The
configs define runtime query templates, field mappings, and DM transformation
rules that Glue jobs and Lambdas consume directly from S3.

| Project | Local Path | S3 Destination | Subdirectories |
|---|---|---|---|
| **cnsv** | `cnsv/configs/_configs/` | `c108-prod-fpacfsa-final-zone/cnsv/` | `CMN_DIM_DM/`, `DM/`, `EDV/`, `STG/`, `metadata/` |
| **farmdm** | `farmdm/configs/_configs/` | *(set in deploy_config.sh)* | `dm/`, `edv/`, `prep/`, `stg/`, `metadata/` |
| **sbsd** | `sbsd/configs/_configs/` | *(set in deploy_config.sh)* | `PYMT_DM/`, `STG/`, `edv/`, `metadata/` |

The upload is driven by `deploy_config.sh` in each project directory. For
`cnsv`, the destination bucket and prefix are controlled by the `configUpload`
block in the project's config JSON so they can differ per environment.

---

## Running a Deploy

```bash
# From deploy/ root — preferred (uses master deployer)
python deploy.py \
  --config config/athenafarm/prod.json \
  --region us-east-1 \
  --project-type athenafarm

# Standalone (from inside the project directory)
cd projects/cnsv
python deploy.py --config ../../config/cnsv/prod.json [--dry-run]

# Config-only upload (cnsv, sbsd, farmdm)
cd projects/cnsv
bash deploy_config.sh prod
```

### Dry Run

All deployers support `--dry-run`. No AWS resources are created or modified;
the deployer prints what it would do and exits.
