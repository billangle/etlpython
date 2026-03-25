# Farm Records Pipeline — Text-Based Component Graph

## Overview

This document provides a text-based graph of the components described in the source text file for the Farm Records pipeline.

Source reference:
- Text file: `Pasted text.txt`

---

## High-Level End-to-End Graph

```text
SAP Flat Text Files
    |
    v
FSA-PROD-farm-records_FTP  [Glue Job]
    |  Pulls raw SAP files via FTP
    v
s3://c108-prod-fpacfsa-landing-zone/farm_records/IncrementalLoad/RAW_SAP_TEXT_FILES/
    |
    +--> job_id.json
    |       |
    |       v
    |   Get JobID from JSON  [S3 GetObject]
    |   Extracts: JobId, DatePortion
    |
    v
FSA-PROD-FarmRecords-S3-SAPTxt-to-Parquet  [Glue Job]
    |  Converts SAP text files to Parquet
    v
s3://c108-prod-fpacfsa-landing-zone/farm_records/IncrementalLoad/RAW_SAP_PARQUET_FILES/
    |
    v
FSA-PROD-sap-farm-records-validation  [Glue Crawler]
    |  Updates Glue Data Catalog
    v
Glue DB: FSA-PROD-FARM-RECORDS
    |
    v
Athena
    |
    v
FSA-PROD-FarmRecords-Athena-to-PG-FRR  [Glue Job]
    |  Runs INSERT/UPDATE SQL from S3
    |  Outputs CSV results
    v
s3://c108-prod-fpacfsa-cleansed-zone/farm_records/etl-jobs/IncrementalLoad/SAP_WEBFRS_SQL_SCRIPTS/
    |
    v
Postgres: EDV.FARM_RECORDS_REPORTING
    |
    v
SQL_FARM_RCD_STG
    |
    v
DM Pipeline
    |
    v
Redshift
```

---

## Top-Level Orchestration Graph

```text
FSA-PROD-FarmRecords-Main  [Top-Level Step Function]
|
+--> FSA-PROD-FarmRecords-SAP-Load-MAIN
|
+--> FSA-PROD-FarmRecords-Postgres-Main
|
+--> FSA-PROD-FarmRecords-DM-Main
```

---

## Step Function 1 — SAP Load

```text
FSA-PROD-FarmRecords-SAP-Load-MAIN
|
+--> FSA-PROD-farm-records_FTP  [Glue Job]
|       Pull SAP flat text files from FTP
|       Output:
|       s3://c108-prod-fpacfsa-landing-zone/farm_records/IncrementalLoad/RAW_SAP_TEXT_FILES/
|
+--> Get JobID from JSON  [S3 GetObject]
|       Reads:
|       job_id.json
|       Extracts:
|       - JobId
|       - DatePortion
|
+--> FSA-PROD-FarmRecords-S3-SAPTxt-to-Parquet  [Glue Job]
|       Inputs:
|       - job_id
|       - date_portion
|       Converts text to Parquet with correct datatypes
|       Output:
|       s3://c108-prod-fpacfsa-landing-zone/farm_records/IncrementalLoad/RAW_SAP_PARQUET_FILES/
|
+--> FSA-PROD-sap-farm-records-validation  [Glue Crawler]
        Crawls Parquet output
        Registers data for Athena / Glue Catalog
```

---

## Step Function 2 — Postgres Main

```text
FSA-PROD-FarmRecords-Postgres-Main
|
+--> FSA-PROD-FarmRecords-PG-Load-Step1-Inserts
|       Groups 1-4
|       Action: INSERT
|       Glue Job:
|       FSA-PROD-FarmRecords-Athena-to-PG-FRR
|
+--> FSA-PROD-FarmRecords-PG-Load-Step2-Inserts
|       Groups 5-8
|       Action: INSERT
|       Glue Job:
|       FSA-PROD-FarmRecords-Athena-to-PG-FRR
|
+--> FSA-PROD-FarmRecords-PG-Load-Step1-Updated
|       Groups 1-4
|       Action: UPDATE
|       Glue Job:
|       FSA-PROD-FarmRecords-Athena-to-PG-FRR
|
+--> FSA-PROD-FarmRecords-PG-Load-Step2-Updated
        Groups 5-8
        Action: UPDATE
        Glue Job:
        FSA-PROD-FarmRecords-Athena-to-PG-FRR
```

### Postgres Load Data Path

```text
Glue DB / Athena
    |
    v
INSERT / UPDATE SQL Scripts
    |
    v
FSA-PROD-FarmRecords-Athena-to-PG-FRR  [Glue Job]
    |
    +--> Runs Athena SQL scripts
    +--> Writes CSV output to S3
    +--> Uses FARM_RECORDS_REPORTING_PG_DTYPE metadata
    +--> Casts datatypes
    +--> Loads final Parquet / structured data into Postgres
    v
EDV.FARM_RECORDS_REPORTING
```

---

## DM-Main Detailed Graph

```text
FSA-PROD-FarmRecords-DM-Main
|
+--> FSA-PROD-FarmRecords-Step1
|       Runs ~70+ FSA-PROD-FarmRecords-STG-EDV Glue jobs
|       Covers:
|       - reference/history tables (_rh)
|       - linking tables (_l)
|       - staging tables (_rs, _ls, _hs)
|
+--> FSA-PROD-FarmRecords-Step2
|       Calls FSA-PROD-FarmRecords-Step2-1
|       Parallel groups:
|       - FSA-PROD-FarmRecords-EDV-s3Parquet-INCR
|       - FSA-PROD-FarmRecords-s3Parquet-DM-INCR
|       Tables:
|       - FARM_DIM
|       - TR_DIM
|       - FLD_YR_DIM
|
+--> FSA-PROD-FarmRecords-Step3
|       Calls FSA-PROD-FarmRecords-Step2
|       Runs FSA-PROD-FarmRecords-EDV-s3Parquet-INCR
|       28 DIM tables in 3 parallel groups
|
+--> FSA-PROD-FarmRecords-Step4
|       Calls FSA-PROD-FarmRecords-Step3
|       Runs FSA-PROD-FarmRecords-EDV-s3Parquet-INCR
|       12 FACT tables in 1 parallel group
|
+--> FSA-PROD-FarmRecords-Step5
|       Calls FSA-PROD-FarmRecords-Step4
|       Runs FSA-PROD-FarmRecords-s3Parquet-DM-INCR
|       Same 28 DIM tables as Step3
|
+--> FSA-PROD-FarmRecords-Step6
        Calls FSA-PROD-FarmRecords-Step5
        Runs FSA-PROD-FarmRecords-s3Parquet-DM-INCR
        Same 12 FACT tables as Step4
```

---

## Error Handling / Notification Graph

```text
Any Step Function Error
    |
    v
FSA-PROD-FarmRecords-SAP-sns-publish-step-function-errors  [Lambda]
    |
    +--> Formats message with:
    |       - JobName
    |       - TableName
    |       - JobRunState
    |       - ErrorMessage
    |
    +--> Publishes combined SNS notification
    |
    +--> Raises exception to fail the state machine
```

---

## Component Inventory

### Step Functions

```text
FSA-PROD-FarmRecords-Main
FSA-PROD-FarmRecords-SAP-Load-MAIN
FSA-PROD-FarmRecords-Postgres-Main
FSA-PROD-FarmRecords-PG-Load-Step1-Inserts
FSA-PROD-FarmRecords-PG-Load-Step2-Inserts
FSA-PROD-FarmRecords-PG-Load-Step1-Updated
FSA-PROD-FarmRecords-PG-Load-Step2-Updated
FSA-PROD-FarmRecords-DM-Main
FSA-PROD-FarmRecords-Step1
FSA-PROD-FarmRecords-Step2
FSA-PROD-FarmRecords-Step3
FSA-PROD-FarmRecords-Step4
FSA-PROD-FarmRecords-Step5
FSA-PROD-FarmRecords-Step6
```

### Glue Jobs

```text
FSA-PROD-farm-records_FTP
FSA-PROD-FarmRecords-S3-SAPTxt-to-Parquet
FSA-PROD-FarmRecords-Athena-to-PG-FRR
FSA-PROD-FarmRecords-STG-EDV
FSA-PROD-FarmRecords-EDV-s3Parquet-INCR
FSA-PROD-FarmRecords-s3Parquet-DM-INCR
FSA-PROD-FarmRecords-Refresh-DM-MVIEWS
```

### Lambda

```text
FSA-PROD-FarmRecords-SAP-sns-publish-step-function-errors
```

### Glue Crawler

```text
FSA-PROD-sap-farm-records-validation
```

---

## Key S3 Locations

```text
s3://c108-prod-fpacfsa-landing-zone/farm_records/IncrementalLoad/RAW_SAP_TEXT_FILES/job_id.json
s3://c108-prod-fpacfsa-landing-zone/farm_records/IncrementalLoad/RAW_SAP_PARQUET_FILES/
s3://c108-prod-fpacfsa-cleansed-zone/farm_records/RAW_SAP_PARQUETS/
s3://c108-prod-fpacfsa-cleansed-zone/farm_records/etl-jobs/IncrementalLoad/SAP_WEBFRS_SQL_SCRIPTS/
```

---

## Condensed One-Page Graph

```text
SAP
 |
 v
FSA-PROD-farm-records_FTP
 |
 v
Landing S3: RAW_SAP_TEXT_FILES
 |  |  +--> job_id.json --> Get JobID / DatePortion
 |
 v
FSA-PROD-FarmRecords-S3-SAPTxt-to-Parquet
 |
 v
Landing S3: RAW_SAP_PARQUET_FILES
 |
 v
FSA-PROD-sap-farm-records-validation
 |
 v
Glue Catalog / Athena
 |
 v
FSA-PROD-FarmRecords-Postgres-Main
 |
 +--> PG-Load-Step1-Inserts
 +--> PG-Load-Step2-Inserts
 +--> PG-Load-Step1-Updated
 +--> PG-Load-Step2-Updated
         |
         v
   FSA-PROD-FarmRecords-Athena-to-PG-FRR
         |
         v
   EDV.FARM_RECORDS_REPORTING
         |
         v
   SQL_FARM_RCD_STG
         |
         v
   FSA-PROD-FarmRecords-DM-Main
         |
         +--> Step1  STG-EDV
         +--> Step2  EDV-s3Parquet-INCR / DM-INCR
         +--> Step3  DIM loads
         +--> Step4  FACT loads
         +--> Step5  DIM DM loads
         +--> Step6  FACT DM loads
         |
         v
      Redshift
```
