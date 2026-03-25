# Farm Records + AthenaFarm - Integrated Pipeline Graph

## Purpose

This graph answers: where does athenafarm fit in the larger farm records
pipeline?

Short answer:
- FarmRecords remains the broad enterprise pipeline from SAP ingestion to PG/DM/Redshift.
- AthenaFarm is a domain-focused sidecar for producer-year data.
- AthenaFarm reads shared upstream data (SSS and PG reference/CDC snapshots),
  computes tract/farm producer-year outputs in Iceberg, and can write those
  results back into the same Postgres farm records schema used by downstream
  enterprise consumers.

---

## Integrated System Graph (Big Picture)

```text
                                        +--------------------------------------+
                                        |  Legacy / Enterprise FarmRecords     |
                                        +--------------------------------------+

SAP Flat Text Files
    |
    v
FSA-PROD-farm-records_FTP (Glue)
    |
    v
Landing S3 RAW_SAP_TEXT_FILES + job_id.json
    |
    v
FSA-PROD-FarmRecords-S3-SAPTxt-to-Parquet (Glue)
    |
    v
Landing S3 RAW_SAP_PARQUET_FILES
    |
    v
FSA-PROD-sap-farm-records-validation (Crawler)
    |
    v
Glue Catalog: FSA-PROD-FARM-RECORDS / Athena
    |
    v
FSA-PROD-FarmRecords-Athena-to-PG-FRR (Glue)
    |
    v
Postgres: EDV.FARM_RECORDS_REPORTING
    |
    +--------------------------+
    |                          |
    |                          v
    |                    SQL_FARM_RCD_STG -> DM Pipeline -> Redshift
    |
    |  (Shared data boundary with AthenaFarm)
    |
    v
+---------------------------------------------------------------+
| AthenaFarm Producer-Year Sidecar                              |
| FSA-{ENV}-ATHENAFARM-Main                                     |
|                                                               |
|  IngestParallel                                               |
|   - Ingest-SSS-Farmrecords      -> athenafarm_prod_raw        |
|   - Ingest-PG-Reference-Tables  -> athenafarm_prod_ref        |
|   - Ingest-PG-CDC-Targets       -> athenafarm_prod_cdc        |
|                                                               |
|  TransformParallel                                            |
|   - Transform-Tract-Producer-Year -> athenafarm_prod_gold     |
|   - Transform-Farm-Producer-Year  -> athenafarm_prod_gold     |
|                                                               |
|  Optional Sync-Iceberg-To-RDS (skip_rds_sync=false)           |
|   - Writes to EDV.FARM_RECORDS_REPORTING                      |
|     - tract_producer_year                                     |
|     - farm_producer_year                                      |
+---------------------------------------------------------------+
```

---

## Orchestration Relationship Graph

```text
FarmRecords top-level orchestration:
  FSA-PROD-FarmRecords-Main
    -> SAP-Load-MAIN
    -> Postgres-Main
    -> DM-Main

AthenaFarm orchestration (separate state machines):
  FSA-{ENV}-ATHENAFARM-Main
    -> IngestParallel
    -> TransformParallel
    -> CheckSkipRDSSync
    -> (optional) SyncToRDS
    -> Notify

  FSA-{ENV}-ATHENAFARM-Maintenance
    -> RunIcebergMaintenance
    -> Notify
```

Interpretation:
- AthenaFarm is not a branch inside FarmRecords-Main.
- It is a separate pipeline that integrates through shared datasets and
  shared target schema boundaries.

---

## Data Contract Touchpoints

```text
Touchpoint A - Shared upstream source domain:
  SSS/FarmRecords catalog data
    used by FarmRecords (Athena/PG load)
    and ingested by AthenaFarm (Ingest-SSS-Farmrecords)

Touchpoint B - Shared Postgres domain:
  EDV.FARM_RECORDS_REPORTING
    populated by FarmRecords-Athena-to-PG-FRR
    referenced/snapshotted by AthenaFarm ingest jobs
    optionally updated by AthenaFarm Sync-Iceberg-To-RDS

Touchpoint C - Shared downstream consumers:
  Consumers downstream of EDV.FARM_RECORDS_REPORTING
  (SQL_FARM_RCD_STG -> DM Pipeline -> Redshift)
  continue to consume the same schema boundary.
```

---

## How AthenaFarm Fits (Decision View)

```text
If question is "Where does AthenaFarm sit?"

It sits between:
  - upstream SSS/Athena + PG reference/CDC sources
  and
  - downstream PG producer-year tables consumed by enterprise reporting/DM.

Role:
  - compute-focused producer-year pipeline on Iceberg
  - decoupled orchestration from FarmRecords-Main
  - integrated at data boundaries, not by nested state machine calls
```

---

## Minimal One-Page Integrated Graph

```text
SAP/SSS inputs
   |
   +--> FarmRecords core path --> EDV.FARM_RECORDS_REPORTING --> SQL_FARM_RCD_STG --> DM --> Redshift
   |
   +--> AthenaFarm sidecar ingest (SSS + PG ref/cdc) --> Iceberg raw/ref/cdc
                                            |
                                            v
                                  AthenaFarm transforms (tract/farm producer-year)
                                            |
                                            v
                                    Iceberg gold producer-year tables
                                            |
                                            v
                              optional sync back to EDV.FARM_RECORDS_REPORTING
```
