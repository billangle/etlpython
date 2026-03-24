# fbp_rpt

FBP Reporting SQL reference folder. Contains draft Athena SQL queries for Farm Business Plan (FBP) reporting tables. **This project has no deployable infrastructure** — no Glue jobs, Lambdas, Step Functions, or `deploy.py`.

---


## Git Branching and Automated DEV Builds

All projects follow the same branch-driven CI/CD model.

- Branch naming: `<PROJECT>-<JIRA-TICKET>` (example: `FBP_RPT-POPSUP-7557`)
- `FBP_RPT` is the project code used in the command examples below.
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
git checkout -b FBP_RPT-POPSUP-7557

# Commit and push (triggers DEV automation)
git add .
git commit -m "FBP_RPT-POPSUP-7557: describe change"
git push -u origin FBP_RPT-POPSUP-7557

# Daily sync main into feature branch to reduce merge conflicts
git fetch origin
git checkout main
git pull origin main
git checkout FBP_RPT-POPSUP-7557
git merge main
git push
```

## Naming Convention

FPACDEV indicates resources created by CI/CD automation in the development environment. Resources named with DEV were created manually and are NOT overwritten by automation.


## Overview

SQL scripts target the `fsa-prod-dmcleanstart` Athena data source and define three reporting tables in the `FBP_RPT` schema. These are work-in-progress drafts intended as references for future ETL or view definition work.

**Status:** Draft / WIP — queries contain incomplete CTEs and are not production-ready.

---

## SQL Queries

| Query file | Target table | Description |
|---|---|---|
| `sql/FBP_ACCT_TYPE.sql` | `FBP_RPT.FBP_ACCT_TYPE` | FBP account type dimension — maps FBP account codes to type descriptions |
| `sql/FBP_CUST.sql` | `FBP_RPT.FBP_CUST` | FBP customer/producer reference — core producer identifiers |
| `sql/FBP_SCOR_DET.sql` | `FBP_RPT.FBP_SCOR_DET` | FBP score detail — scoring/eligibility detail records |

All queries use Athena SQL syntax targeting the `fsa-prod-dmcleanstart` catalog.

---

## Deploying

Not applicable. This project is not registered in the master `deploy.py` and contains no deployer.

---

## Usage

Queries are intended to be run directly in the Athena console or used as the basis for Glue jobs / views in a future ETL deployment:

1. Open the AWS Athena console in `us-east-1`
2. Select the `fsa-prod-dmcleanstart` data source
3. Copy the desired SQL from the `sql/` folder and run in the query editor

---

## Project Structure

```
fbp_rpt/
└── sql/
    ├── FBP_ACCT_TYPE.sql
    ├── FBP_CUST.sql
    └── FBP_SCOR_DET.sql
```