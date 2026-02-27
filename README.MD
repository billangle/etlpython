# Python Based ETL Deployer

This repository implements a **modular, Python-based CI/CD deployment framework**
for FPAC FSA data pipelines. It uses Python and Boto3 to automate the creation
and updating of AWS Glue jobs, Lambda functions, Step Functions state machines,
and Glue crawlers. No AWS CDK is used.

---

## Quick Links

| Document | Description |
|---|---|
| [deploy/projects/README.md](deploy/projects/README.md) | Project catalogue — all 14 pipelines, resource counts, S3 config upload details |
| [deploy/config/README.md](deploy/config/README.md) | Config file structure — full field reference with annotated examples |

---

## Solution Structure

```
deploy/
├── deploy.py                   # Master deployer — dispatches to each project
├── common/
│   ├── __init__.py
│   └── aws_common.py           # Shared helpers: GlueJobSpec, ensure_glue_job, ensure_lambda, etc.
├── config/                     # Per-project, per-environment JSON config files
│   ├── README.md               # ← Config file field reference
│   ├── athenafarm/
│   │   ├── dev.json
│   │   ├── prod.json
│   │   └── steamdev.json
│   └── <project>/              # Same pattern for all 14 projects
│       ├── dev.json
│       ├── steamdev.json
│       ├── cert.json           # Selected projects only
│       └── prod.json
└── projects/
    ├── README.md               # ← Project catalogue and resource inventory
    ├── athenafarm/             # Iceberg / farm data pipeline
    ├── cars/                   # CARS reporting pipeline
    ├── carsdm/                 # CARS data mart
    ├── cnsv/                   # Conservation multi-pipeline (5 deployers)
    ├── farm_records/           # Farm Records pipeline
    ├── farmdm/                 # Farm Data Mart config upload
    ├── flpids/                 # FLPIDS FTPS file checks
    ├── fmmi/                   # FMMI CSV→ODS pipeline
    ├── fpac_pipeline/          # Shared FPAC landing/cleaning/final jobs
    ├── nps/                    # NPS archive table pipeline
    ├── pmrds/                  # PMRDS ODS→STG→FINAL pipeline
    ├── sbsd/                   # SBSD data mart + config upload
    └── tsthooks/               # CI/CD webhook test Lambdas
```

## Overview

The framework supports **14 independent ETL projects**, each deploying a
combination of Glue Spark jobs, Lambda functions, and Step Functions state
machines. All resource names, IAM ARNs, bucket references, and job parameters
are driven by JSON config files — nothing is hardcoded in the deployer scripts.

Resources follow a consistent naming convention:
```
FSA-{deployEnv}-{project}-{ResourceName}
```
e.g. `FSA-PROD-ATHENAFARM-Ingest-SSS-Farmrecords` (Glue), `FSA-PROD-CNSV-Main` (Step Functions).

---

## Deployment via Jenkins

The master deployer is invoked by a Jenkins pipeline with three variables:

- **Config file** — JSON file defining all environment-specific parameters
- **AWS Region** — target region for API calls
- **Project type** — which project deployer to dispatch to

```bash
python deploy.py \
  --config config/athenafarm/prod.json \
  --region us-east-1 \
  --project-type athenafarm
```

### Environments

| Config file | Environment |
|---|---|
| `dev.json` | Development |
| `steamdev.json` | Steam / Cert-Dev |
| `cert.json` | Certification (selected projects) |
| `prod.json` | Production |

The framework is designed for integration with BitBucket webhooks to trigger
environment-targeted deployments based on:

- Commits to specific branches
- Pull requests and branch merges
- Feature branch tags

---

## Key Features

### Automated Deployments

- No manual console configuration required
- Fully idempotent deployments across environments

---

### Modular Project Architecture

- Shared utilities and helpers live in `common/`
- Individual pipelines are isolated under `projects/`
- Each pipeline can evolve independently without impacting others

```
common/       → Shared AWS utilities
projects/     → Independent, deployable pipelines
config/       → Environment-specific configuration
```

---

### Step Functions–Driven Workflow Orchestration

- Data pipeline logic is coordinated via **AWS Step Functions**

#### Current Design — Parameterised ASL JSON

Workflow definitions are stored as `*.param.asl.json` files alongside each project's deployer. At deploy time the deployer reads the file, substitutes environment-specific parameters (ARNs, bucket names, resource tags, etc.) from the project's JSON config, and creates or updates the state machine via the AWS API.

```
projects/
├── athenafarm/states/
│   ├── Main.param.asl.json          # Pipeline — ingest → transform → sync
│   └── Maintenance.param.asl.json
└── cnsv/states/
    ├── Main.param.asl.json
    ├── Incremental-to-S3Landing.param.asl.json
    └── ...                          # One .param.asl.json per state machine
```

Projects using this approach: **athenafarm**, **cnsv**

#### Legacy Design — Python-Based ASL Builder

Earlier projects constructed ASL definitions programmatically using Python builder classes. These files remain in place and are still deployed but are not the target pattern for new pipelines.

Projects still using Python builders: **cars**, **carsdm**, **farm_records**, **flpids**, **fpac_pipeline**, **sbsd**

```
projects/
├── cars/states/
│   ├── cars_stepfunction.py
│   └── cars_dm_etl_stepfunction.py
├── carsdm/states/carsdm_stepfunction.py
├── farm_records/farm_rec_stepfunctions.py
├── flpids/stepfunctions/fpac_stepfunctions.py
├── fpac_pipeline/fpac_stepfunctions.py
└── sbsd/states/
    ├── sbsd_stepfunction.py
    └── sbsd_dm_etl_stepfunction.py
```

---

### Lambda-Based Validation and Control Logic

- Discrete Lambda functions handle:
  - Input validation
  - ID generation
  - Result logging
- Each function is isolated in its own directory for clarity and packaging

```
lambda/
├── Validate/
├── CreateNewId/
└── LogResults/
```

---

### AWS Glue ETL Processing

- Data transformation is performed using **AWS Glue jobs**
- ETL stages are separated by responsibility:

```
glue/
├── landingFiles/     → Raw ingestion
├── cleaningFiles/    → Data cleansing & normalization
└── finalFiles/       → Final curated outputs
```

- Each stage is independently testable and replaceable

---

### Environment-Aware Configuration

- Environment-specific settings are externalized into JSON config files

```
config/
├── dev.json
└── prod.json
```

- Enables:
  - DEV / PROD parity
  - Safer promotions
  - Zero code changes between environments

---

### CI/CD Pipeline-Specific Deployment Entry Points

- Root-level `deploy.py` for global or shared resources
- CI/CD deployment pipeline-specific logic lives alongside the data pipeline implementation

```
deploy/deploy.py
projects/fpac_pipeline/deploy.py
```

- Allows:
  - Targeted deployments
  - Faster iteration
  - Clear ownership boundaries

---

## Design Principles

- Least privilege by default
- Clear separation of orchestration vs execution
- Reusable infrastructure components
- Environment isolation
- Audit-friendly, version-controlled deployments
