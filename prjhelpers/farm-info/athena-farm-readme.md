# AthenaFarm Pipeline - Text-Based Component Graph

## Overview

This document maps the athenafarm pipeline into a text graph, using the same
style as the farm-records component graph, but scoped to what athenafarm
actually deploys and runs.

Pipeline characteristics:
- Parallel ingest from SSS and PostgreSQL sources into Iceberg
- Parallel tract and farm producer-year transforms
- Optional sync from Iceberg gold tables back to PostgreSQL
- Separate weekly maintenance state machine for Iceberg housekeeping
- Lambda notification at pipeline completion/failure

---

## High-Level End-to-End Graph

```text
SSS Athena catalog + PostgreSQL (EDV / CRM_ODS)
    |
    v
FSA-{ENV}-ATHENAFARM-Main  [Step Functions]
    |
    +--> IngestParallel
    |       +--> Ingest-SSS-Farmrecords         -> glue_catalog.athenafarm_prod_raw
    |       +--> Ingest-PG-Reference-Tables     -> glue_catalog.athenafarm_prod_ref
    |       +--> Ingest-PG-CDC-Targets          -> glue_catalog.athenafarm_prod_cdc
    |
    +--> TransformParallel
    |       +--> Transform-Tract-Producer-Year  -> glue_catalog.athenafarm_prod_gold.tract_producer_year
    |       +--> Transform-Farm-Producer-Year   -> glue_catalog.athenafarm_prod_gold.farm_producer_year
    |
    +--> CheckSkipRDSSync (default skip)
    |       +--> Sync-Iceberg-To-RDS (only when skip_rds_sync=false)
    |
    +--> NotifyPipeline Lambda
            +--> CloudWatch logs
            +--> SNS publish (if SNS_TOPIC_ARN is configured)
```

---

## Main State Machine Graph

```text
State Machine: FSA-{ENV}-ATHENAFARM-Main
StartAt: IngestParallel

IngestParallel  [Parallel]
  Branch 1:
    StartSSSCrawler -> WaitForSSSCrawler -> GetSSSCrawlerState
      -> CheckSSSCrawlerDone -> CheckSSSCrawlerStatus -> IngestSSS
    IngestSSS runs Glue job:
      __INGEST_SSS_GLUE_JOB_NAME__

  Branch 2:
    IngestPGReferenceTables runs Glue job:
      __INGEST_PG_REFS_GLUE_JOB_NAME__

  Branch 3:
    IngestPGCDCTargets runs Glue job:
      __INGEST_PG_CDC_GLUE_JOB_NAME__

Then -> TransformParallel  [Parallel]
  Branch 1:
    TransformTractProducerYearSingleFast
      Glue job: __TRANSFORM_TRACT_PY_GLUE_JOB_NAME__
      Args: --task_mode=single_fast

  Branch 2:
    TransformFarmProducerYear
      Glue job: __TRANSFORM_FARM_PY_GLUE_JOB_NAME__
      Args: --full_load=true

Then -> CheckSkipRDSSync  [Choice]
  if skip_rds_sync=false -> SyncToRDS
  else -> Notify

SyncToRDS
  Glue job: __SYNC_RDS_GLUE_JOB_NAME__

Notify
  Lambda invoke: __SNS_NOTIFY_FN_ARN__
```

---

## Maintenance State Machine Graph

```text
State Machine: FSA-{ENV}-ATHENAFARM-Maintenance
StartAt: RunIcebergMaintenance

RunIcebergMaintenance
  Glue job: __ICEBERG_MAINT_GLUE_JOB_NAME__
  Args include: --snapshot_retention_hours=168

Then -> Notify
  Lambda invoke: __SNS_NOTIFY_FN_ARN__
```

---

## Ingest Layer Graph

```text
Ingest-SSS-Farmrecords
  Source: sss-farmrecords Athena catalog (+ fsa-prod-farm-records farm table)
  Output DB: glue_catalog.athenafarm_prod_raw
  Tables:
    ibib, ibsp, ibst, ibin, ibpart, crmd_partner, z_ibase_comp_detail,
    comm_pr_frg_rel, zmi_farm_partn, zmi_field_excp, tbl_fr_bp_relationship,
    fsa_farm_records_farm

Ingest-PG-Reference-Tables
  Source: PostgreSQL schemas farm_records_reporting and crm_ods
  Output DB: glue_catalog.athenafarm_prod_ref
  Tables:
    time_period, county_office_control, farm, tract, farm_year, tract_year, but000

Ingest-PG-CDC-Targets
  Source: PostgreSQL farm_records_reporting
  Output DB: glue_catalog.athenafarm_prod_cdc
  Tables:
    tract_producer_year, farm_producer_year
```

---

## Transform and Publish Graph

```text
Transform-Tract-Producer-Year
  Reads from:
    athenafarm_prod_raw + athenafarm_prod_ref
  Writes to:
    athenafarm_prod_gold.tract_producer_year
  Runtime mode used by state machine:
    single_fast (single-pass production path)

Transform-Farm-Producer-Year
  Reads from:
    athenafarm_prod_raw.fsa_farm_records_farm
    athenafarm_prod_ref.county_office_control
    athenafarm_prod_ref.farm
    athenafarm_prod_ref.farm_year
  Writes to:
    athenafarm_prod_gold.farm_producer_year
  Mode:
    full-load path
```

---

## Optional RDS Sync Graph

```text
CheckSkipRDSSync
  |
  +--> skip_rds_sync=true or not provided -> skip sync
  |
  +--> skip_rds_sync=false -> run Sync-Iceberg-To-RDS

Sync-Iceberg-To-RDS
  Reads:
    athenafarm_prod_gold.tract_producer_year
    athenafarm_prod_gold.farm_producer_year
  Writes:
    PostgreSQL farm_records_reporting.tract_producer_year
    PostgreSQL farm_records_reporting.farm_producer_year
  State tracking:
    SSM snapshot parameter per table
```

---

## Weekly Iceberg Maintenance Graph

```text
FSA-{ENV}-ATHENAFARM-Maintenance
  |
  +--> Iceberg-Maintenance
        |
        +--> rewrite_data_files (OPTIMIZE)
        +--> expire_snapshots (retention window)
        +--> rewrite_manifests
        |
        +--> Applies across raw, ref, and gold table sets
```

---

## Notification and Error Handling Graph

```text
Any caught failure in main or maintenance state machine
    |
    v
Notify state invokes NotifyPipeline Lambda
    |
    +--> Detects Error/error keys in execution payload
    +--> Marks status as SUCCEEDED or FAILED
    +--> Logs full execution payload to CloudWatch
    +--> Publishes SNS message when SNS_TOPIC_ARN is present
```

---

## Component Inventory

### Step Functions

```text
FSA-{ENV}-ATHENAFARM-Main
FSA-{ENV}-ATHENAFARM-Maintenance
```

### Glue Jobs

```text
FSA-{ENV}-ATHENAFARM-Ingest-SSS-Farmrecords
FSA-{ENV}-ATHENAFARM-Ingest-PG-Reference-Tables
FSA-{ENV}-ATHENAFARM-Ingest-PG-CDC-Targets
FSA-{ENV}-ATHENAFARM-Transform-Tract-Producer-Year
FSA-{ENV}-ATHENAFARM-Transform-Farm-Producer-Year
FSA-{ENV}-ATHENAFARM-Sync-Iceberg-To-RDS
FSA-{ENV}-ATHENAFARM-Iceberg-Maintenance
```

### Lambda

```text
FSA-{ENV}-ATHENAFARM-NotifyPipeline
```

---

## Runtime Defaults Used by the Pipeline

```text
Main state machine defaults:
  full_load=true
  skip_rds_sync=true

Transform tract branch:
  task_mode=single_fast
  WorkerType=G.4X
  NumberOfWorkers=40
```

This means the normal run is full-load transforms with no RDS sync unless
explicitly requested by passing skip_rds_sync=false in execution input.
