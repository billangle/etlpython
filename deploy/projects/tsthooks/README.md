# tsthooks

CI/CD utility project. Deploys two Lambda functions that support Jenkins webhook triggering and FTPS test data file management for integration testing. No Glue jobs or Step Functions.

**AWS Region:** `us-east-1`  
**Project key:** `tsthooks`  
**Resource naming:** `FSA-{deployEnv}-FpacTSTHOOKS-{ResourceName}`

---


## Git Branching and Automated DEV Builds

All projects follow the same branch-driven CI/CD model.

- Branch naming: `<PROJECT>-<JIRA-TICKET>` (example: `TSTHOOKS-POPSUP-7557`)
- `TSTHOOKS` is the project code used in the command examples below.
- Pushing a feature branch triggers the BitBucket webhook and builds/deploys to DEV using FPACDEV naming.
- A pull request into `main` is required before PROD deployment.

For the full standard, see:
- [Project-level branching standard](../README.md#git-branching-and-automated-dev-builds)

### Common Git Commands

Branch names shown here are examples only; replace POPSUP-7557 with your assigned Jira ticket when creating your branch.

```bash
# Start feature work from latest main
git checkout main
git pull origin main
git checkout -b TSTHOOKS-POPSUP-7557

# Commit and push (triggers DEV automation)
git add .
git commit -m "TSTHOOKS-POPSUP-7557: describe change"
git push -u origin TSTHOOKS-POPSUP-7557

# Daily sync main into feature branch to reduce merge conflicts
git fetch origin
git checkout main
git pull origin main
git checkout TSTHOOKS-POPSUP-7557
git merge main
git push
```

## Naming Convention

FPACDEV indicates resources created by CI/CD automation in the development environment. Resources named with DEV were created manually and are NOT overwritten by automation.


## Overview

```
Jenkins Webhook  ◄──── JenkinsHook Lambda (HTTP POST)

Echo DART FTPS   ◄──── TestDataFiles Lambda (upload / delete via curl)
                            └── reads files from S3
```

Used during CI pipelines to kick off Jenkins jobs and pre-stage (or clean up) test data files on the Echo DART FTPS server before/after automated tests.

---

## Environments / Configs

No production config exists — this is a dev/test-only utility.

| Config file | `deployEnv` | AWS Account |
|---|---|---|
| [config/tsthooks/fpacdev.json](../../config/tsthooks/fpacdev.json) | `dev` | `241533156429` (FPACDEV) |
| [config/tsthooks/steam.json](../../config/tsthooks/steam.json) | `steam-dev` | `335965711887` (STEAMDEV) |

---

## Glue Jobs

None.

---

## Lambda Functions

Runtime: **Python 3.12**.  
Naming pattern: `FSA-{deployEnv}-FpacTSTHOOKS-{Suffix}`

### `FSA-{env}-FpacTSTHOOKS-JenkinsHook`

Triggers a Jenkins pipeline job via its webhook URL.

| Detail | Value |
|---|---|
| Source | `lambda/JenkinsHook/` |
| Input | `{ "url": "https://jenkins.example.com/...", "payload": {...}, "auth": { "user": "...", "token": "..." } }` |
| Method | HTTP POST with `Content-Type: application/json`; optional Basic Auth header |
| Output | HTTP status code and response body from Jenkins |
| Layers | `FSA-DEV-MDART-layer`, `FSA-polars` |

### `FSA-{env}-FpacTSTHOOKS-TestDataFiles`

Uploads or deletes files on the Echo DART FTPS server using `curl` subprocess calls.

| Detail | Value |
|---|---|
| Source | `lambda/TestDataFiles/` |
| Input | `{ "action": "upload" | "delete", "s3_bucket": "...", "s3_key": "...", "ftps_host": "...", "ftps_path": "...", "ftps_user": "...", "ftps_pass": "..." }` |
| Upload method | `curl --ssl-reqd --tlsv1.2 --ftp-pasv -T <local_file> ftp://{host}/{path}` |
| Delete method | `curl --ssl-reqd --tlsv1.2 --ftp-pasv -Q "DELE {path}" ftp://{host}/` |
| Pre-step | Downloads file from S3 to `/tmp/` before upload |
| Layers | `FSA-DEV-MDART-layer`, `FSA-polars` |

---

## Step Functions

None. The SFN IAM role is referenced in config for potential future use.

---

## Key Configuration (fpacdev)

| Parameter | Value |
|---|---|
| Artifact bucket | `fsa-dev-ops` |
| Lambda role | `arn:aws:iam::241533156429:role/disc-fsa-dev-lambda-servicerole` |
| Lambda layers | `FSA-DEV-MDART-layer`, `FSA-polars` |

---

## Deploying

```bash
cd deploy/

# Dev
python deploy.py --config config/tsthooks/fpacdev.json --region us-east-1 --project-type tsthooks

# Steam-dev
python deploy.py --config config/tsthooks/steam.json --region us-east-1 --project-type tsthooks
```

## Invoking Lambdas

```bash
# Trigger a Jenkins job
aws lambda invoke \
  --function-name FSA-dev-FpacTSTHOOKS-JenkinsHook \
  --payload '{"url":"https://jenkins.example.com/generic-webhook-trigger/invoke?token=mytoken","payload":{"branch":"main"},"auth":{"user":"admin","token":"apitoken123"}}' \
  --region us-east-1 \
  response.json

# Upload a test file to FTPS
aws lambda invoke \
  --function-name FSA-dev-FpacTSTHOOKS-TestDataFiles \
  --payload '{"action":"upload","s3_bucket":"c108-dev-fpacfsa-landing-zone","s3_key":"test-data/sample.csv","ftps_host":"ftps.echo.usda.gov","ftps_path":"/inbound/sample.csv","ftps_user":"myuser","ftps_pass":"mypass"}' \
  --region us-east-1 \
  response.json
```

## Project Structure

```
tsthooks/
├── __init__.py
├── deploy.py                       # Project deployer
└── lambda/
    ├── JenkinsHook/                # Jenkins webhook trigger (Python 3.12)
    │   └── handler.py
    └── TestDataFiles/              # FTPS file upload/delete (Python 3.12)
        └── handler.py
```