# Python based deployer

This document provides an overview of the Python based deployer. The solution utilizes Python and Boto3 to automate deployments of Lambda functions, Glue Jobs, Step functions and Glue crawlers. This solution does not utilize the AWS CDK.

---

# Solution Structure Template

```
deploy/
├── deploy.py
├── common/
│   ├── __init__.py
│   └── aws_common.py
├── projects/
│   ├── __init__.py
│   └── fpac_pipeline/
│       ├── __init__.py
│       ├── deploy.py
│       ├── fpac_stepfunctions.py
│       ├── lambda/
│       │   ├── Validate/
│       │   ├── CreateNewId/
│       │   └── LogResults/
│       └── glue/
│           ├── landingFiles/landing_job.py
│           ├── cleaningFiles/cleaning_job.py
│           └── finalFiles/final_job.py
└── config/
    ├── dev.json
    └── prod.json

```

# Overview

This solution implements a **modular, Python based CI/CD deployment pipeline framework** designed to support environment-specific deployments, reusable infrastructure components, and event-driven processing using AWS Step Functions, Lambda, and Glue.

The structure emphasizes **separation of concerns**, **reusability**, and **least-privilege deployment practices**.

---
## Deployment via Jenkins 

The root level deployer is called via a Jenkins pipeline. This requires the following variables to be resolved:

- Configuration file - defines all of the parameters for the targeted environment
- AWS Region - AWS region
- Project-type - this is the data pipeline that will be deployed

```
Example:

python deploy.py --config config/depeast2.json --region us-east-2 --project-type fpac
```

The goal is to integrate this with BitBucket via webhooks. This will allow deployments to occur in specific environments based on the types of git actions. These can include:

- Code Commits on specific branches
- PR (pull requests) to merge specific branches
- Feature Tags on specific branches

The goal will be to automate the deployment of data pipelines, using a git branching and deployment strategy.

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
- Workflow definitions are centralized in:

```
projects/fpac_pipeline/fpac_stepfunctions.py
```

- Supports:
  - Multi-stage execution
  - Error handling and retries
  - Clear separation between orchestration and execution logic

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
