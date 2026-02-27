# fbp_rpt

FBP Reporting SQL reference folder. Contains draft Athena SQL queries for Farm Business Plan (FBP) reporting tables. **This project has no deployable infrastructure** — no Glue jobs, Lambdas, Step Functions, or `deploy.py`.

---

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
