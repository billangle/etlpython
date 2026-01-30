# CARS / EDV Data Pipeline – Source Code Guide

This repository contains the **deployment tooling, AWS Lambda functions, AWS Glue jobs, and AWS Step Functions**
used to implement the CARS and EDV data pipelines.  
It is designed to support **multi-environment (DEV/ CERT / PROD)** deployments using a **custom Python deployer**
with strong emphasis on **idempotency, least-privilege IAM, and repeatable CI/CD execution**.

This document is intended to help developers **quickly navigate the codebase**, understand **why each directory exists**,
and know **where to make changes** when extending or troubleshooting the pipelines.

---

## High-Level Architecture

At a high level, the system consists of:

- **Custom Python Deployer**
  - Creates and updates AWS resources (Lambda, Glue, Step Functions, IAM)
- **AWS Glue Jobs**
  - Perform ETL and SQL execution for CARS / EDV
- **AWS Lambda Functions**
  - Control pipeline flow, validation, packaging, uploads, and error handling
- **AWS Step Functions**
  - Orchestrate Lambda and Glue execution
- **Shell Scripts**
  - Support local testing, packaging, and CI/CD execution

---

## Repository Root

```
.
├── deploy.py
├── deploy_config.sh
├── upload.json
├── lambda_response.json
├── car/
├── glue/
├── lambda/
├── pipe/
├── scripts/
├── states/
├── tests/
└── uploader/
```

---

## Deployment Layer

### `deploy.py`
**Primary deployment entry point**

- Implements the custom Python-based deployer
- Responsible for:
  - Creating and updating Lambda functions
  - Deploying Glue jobs
  - Creating Step Functions state machines
  - Wiring IAM roles and permissions
- Designed to be **safe to run repeatedly**
- Mirrors CDK-style naming conventions without requiring CDK at runtime

Typical usage:
- Jenkins pipelines
- Local developer execution
- Environment-based deployments using config files
- config files are outside of the project 
  - ../../config/cars for this project
  - prod.json - production
  - dev.json - dev/cert
  - carssteam.json - steampunk AWS environment

---

### `deploy_config.sh`
Shell helper script used to:
- Exports the SQL scripts and CSV files to S3
- Files and folders are in the car directory under _configs

---

## Configuration Layer

### `car/_configs/`
These are the SQL scripts and CSV files deployed on S3.

```
car/_configs/
├── CAR_DM/
├── DM/
├── EDV/
├── STG/
└── metadata/
```

---

## AWS Glue Jobs

### `glue/`
Contains AWS Glue ETL and SQL execution scripts.

```
glue/
└── FSA-CERT-DATAMART-EXEC-DB-SQL.py
```

**Typical responsibilities:**
- Landing zone ingestion
- Raw → Data Mart transformations
- SQL execution against Aurora / Postgres
- Parquet handling and partitioning
- Schema enforcement

These scripts are:
- Registered as Glue jobs by the deployer
- Invoked from Step Functions
- configuration per environment is in the config file 
  - GlueJobParameters

---

## AWS Lambda Functions

### `lambda/`
Each subdirectory represents **one independently deployed Lambda function**.

```
lambda/
├── DownloadZip/
├── UploadConfig/
├── edv-build-processing-plan/
├── edv-check-results/
├── edv-finalize-pipeline/
└── edv-handle-failure/
```

Each Lambda directory typically contains:
```
lambda_function.py
```

**Common Lambda patterns used in this repo:**
- Step Function task Lambdas
- Pipeline control and orchestration logic
- Validation and status reporting
- Zip creation and streaming uploads to S3
- Centralized error handling and cleanup

Design principles:
- One Lambda = one responsibility
- Explicit inputs and outputs
- Safe retries and structured logging
- Built for Step Function integration

---

## Step Functions

### `states/`
Contains Step Function definitions and builders.

```
states/
├── cars_stepfunction.py - this is what gets deployed
├── FSA-CERT-CARS-s3parquet-to-postgres-edv-load.asl.json
└── FSA-PROD-Cars-S3Landing-to-S3Final-Raw-DM.asl.json
```

**Includes:**
- Raw ASL (Amazon States Language) JSON definitions - JUST FOR REFERENCE
- The cars_stepfunction.py - this is what gets deployed and were changes should be made


These state machines orchestrate:
- Lambda invocations
- Glue job execution
- Conditional branching
- Failure and recovery paths

---

## Shell Scripts & Utilities

### `scripts/`
Utility scripts used to create a zip file for deploying the SQL scripts and other configuration files in the car/_configs folder. These tools will create and deploy files to S3. Called by deploy_config.sh

```
scripts/
├── invoke_expand_lambda.sh
└── make_and_upload_zip.sh
```

Used for:
- Artifact packaging
- S3 uploads
- Debugging outside Jenkins

---

## CI / Pipeline Integration

### `pipe/`
Reserved for pipeline and integration artifacts.

```
pipe/
└── jenkins/
```

This directory is intended for:
- Jenkins pipeline definitions
- EventBridge or pipeline wiring
- Future CI/CD extensions

---

## Testing & Validation

### `tests/`
Local and integration test helpers.

```
tests/
├── downdev.sh
├── download.sh
└── response.json
```

Used to:
- Validate Lambda responses
- Test download logic
- Verify payload formats

---

### `uploader/`
Focused test scripts for upload and zip workflows.

```
uploader/
├── test_end_to_end.sh
├── test_lambda_expand_only.sh
└── test_local_zip_upload.sh
```

Covers:
- End-to-end pipeline validation
- Zip expansion logic
- Local vs Lambda execution parity

---

## Design Philosophy

This repository follows these principles:

- **Idempotent deployments**
- **Least-privilege IAM**
- **Environment-agnostic deployer**
- **Stateless Lambda functions**
- **Explicit Step Function orchestration**
- **Cost-aware Glue execution**
- **No manual AWS console changes**

Everything is designed so that:
- Pipelines can be recreated from scratch
- CI/CD behavior is deterministic
- Failures are observable and recoverable
- Developers know exactly where to make changes

---

## Where to Make Changes

- **Deployment logic:** `deploy.py`
- **S3 SQL scripts and config files:** `car/_configs/`
- **ETL logic:** `glue/`
- **Pipeline control logic:** `lambda/`
- **Orchestration flow:** `states/`
- **Local / CI utilities:** `scripts/`, `tests/`, `uploader/`

---

## Intended Audience

This repository is intended for:
- Data engineers
- AWS platform engineers
- CI/CD maintainers
- On-call support engineers

