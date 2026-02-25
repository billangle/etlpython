# tract_producer_year — Recommended AWS Architecture

> **Goal:** Reduce the `TRACT_PRODUCER_YEAR` and `farm_producer_year` ETL pipeline from hours to **30–90 seconds** per run by eliminating federated query hops, adding partition pruning, and consolidating CDC into a single Iceberg `MERGE INTO` pass.

---

## 1. Why the Current Queries Are Slow

The four Athena SQL files (`TRACT_PRODUCER_YEAR_INSERT_ATHENA.sql`, `TRACT_PRODUCER_YEAR_INSERT_NO_COMPARE_ATHENA.sql`, `TRACT_PRODUCER_YEAR_SELECT.sql`, `TRACT_PRODUCER_YEAR_UPDATE_ATHENA.sql`) share three compounding performance problems:

| Problem | Root cause | Impact |
|---|---|---|
| **Federated joins to RDS PostgreSQL** | Every JOIN to `FSA-PROD-rds-pg-edv` forces serialized JDBC round-trips; the query engine cannot parallelize them | Dominant source of runtime — each federated hop adds minutes |
| **Full table scans on unpartitioned SSS data** | `ibsp`, `ibpart`, `crmd_partner` have no partition pruning; Athena reads every S3 file on every run | Scales linearly with data volume |
| **Two separate CDC passes** | INSERT and UPDATE logic run as independent queries, executing the same multi-table join chain twice | Doubles execution time and S3 scan cost |

---

## 2. Target Architecture

```
EventBridge (schedule / SSS S3-put trigger)
  │
  └─▶ Step Functions DAG
        ├─ [Parallel]
        │    ├─ Glue Job: Extract SSS farmrecords  ──▶  S3 Iceberg (sss.*)
        │    ├─ Glue Job: Extract PG reference tables ─▶ S3 Iceberg (farm_ref.*)
        │    └─ Glue Job: Extract PG CDC targets ──────▶ S3 Iceberg (farm_records_reporting.*)
        │
        └─ Glue Spark Job: Transform + MERGE INTO
             └─ Glue Job: Sync Iceberg delta ──────────▶ RDS PostgreSQL
                  └─ CloudWatch: row-count anomaly alarm
```

### Component Responsibilities

| Component | Purpose |
|---|---|
| **EventBridge** | Scheduled trigger or S3-event trigger when SSS data lands |
| **Step Functions** | DAG orchestration — parallel ingest, sequential transform, retry + alerting |
| **Glue Ingest Jobs** | Pull SSS + PG tables into S3 Iceberg; runs only when source data changes |
| **Glue Spark Transform** | Runs the CTE join logic (same SQL, no federated hops); outputs via `MERGE INTO` |
| **Glue Sync Job** | Writes only the changed delta rows back to RDS PostgreSQL |
| **CloudWatch** | Alarms on job duration and row-count anomalies |
| **Glue Data Catalog** | Single unified metastore for all S3 Iceberg tables |

---

## 3. Key Changes

### 3.1 Pre-Materialize Reference Tables into S3 Iceberg

Pull the PostgreSQL reference tables out of RDS on a schedule and land them as Parquet/Iceberg in S3. These tables are small and infrequently changing. Once local to S3, all joins operate within the same compute layer with zero network overhead.

**Tables to materialize:**

| Source table | Schema | Change frequency |
|---|---|---|
| `time_period` | farm_records_reporting | Annually |
| `county_office_control` | farm_records_reporting | Rarely |
| `farm` | farm_records_reporting | Daily |
| `tract` | farm_records_reporting | Daily |
| `farm_year` | farm_records_reporting | Daily |
| `tract_year` | farm_records_reporting | Daily |
| `tract_producer_year` | farm_records_reporting | Daily (CDC target) |
| `farm_producer_year` | farm_records_reporting | Daily (CDC target) |
| `but000` | crm_ods | Daily |

```python
# Example Glue job — extract county_office_control to Iceberg
df = glueContext.create_dynamic_frame.from_catalog(
    database="FSA-PROD-rds-pg-edv",
    table_name="farm_records_reporting_county_office_control",
    additional_options={"jobBookmarkKeys": ["county_office_control_identifier"]}
)
df.toDF().writeTo("farm_ref.county_office_control") \
    .using("iceberg") \
    .partitionedBy("state_fsa_code") \
    .createOrReplace()
```

### 3.2 Replace Athena SQL with a Glue Spark Job

The existing CTEs translate directly to `spark.sql()` with no rewrites required. The critical gain is that **all sources are now within the same Spark compute layer** — no federated hops, no serialized JDBC calls. Spark automatically broadcast-joins small reference tables.

```python
spark.sql("""
WITH time_pd AS (
    SELECT time_period_identifier, time_period_name
    FROM farm_ref.time_period
    WHERE data_status_code = 'A'
),
-- ... same CTE logic as existing SQL files ...
SELECT * FROM tract_producer_year_tbl
""")
```

Because all sources are now S3 Iceberg, this runs in Spark's distributed execution engine with full parallelism across all partitions.

### 3.3 Replace INSERT + UPDATE with a Single `MERGE INTO`

Iceberg's `MERGE INTO` performs the full upsert in **one scan** of source data. This eliminates three SQL files (`_INSERT_ATHENA.sql`, `_INSERT_NO_COMPARE_ATHENA.sql`, `_UPDATE_ATHENA.sql`) and the field-by-field comparison logic embedded in each.

```sql
MERGE INTO farm_records_reporting.tract_producer_year t
USING (
    -- full CTE chain here, produces new_data
    SELECT * FROM tract_producer_year_tbl
) s
ON  t.core_customer_identifier    = s.core_customer_identifier
AND t.tract_year_identifier       = s.tract_year_identifier
AND t.producer_involvement_code   = s.producer_involvement_code

WHEN MATCHED AND (
        t.producer_involvement_start_date  <> s.producer_involvement_start_date OR
        t.producer_involvement_end_date    <> s.producer_involvement_end_date   OR
        t.tract_producer_hel_exception_code <> s.tract_producer_hel_exception_code OR
        t.tract_producer_cw_exception_code  <> s.tract_producer_cw_exception_code  OR
        t.tract_producer_pcw_exception_code <> s.tract_producer_pcw_exception_code OR
        t.data_status_code                 <> s.data_status_code
) THEN UPDATE SET
    producer_involvement_start_date   = s.producer_involvement_start_date,
    producer_involvement_end_date     = s.producer_involvement_end_date,
    tract_producer_hel_exception_code = s.tract_producer_hel_exception_code,
    tract_producer_cw_exception_code  = s.tract_producer_cw_exception_code,
    tract_producer_pcw_exception_code = s.tract_producer_pcw_exception_code,
    data_status_code                  = s.data_status_code,
    last_change_date                  = s.last_change_date,
    last_change_user_name             = s.last_change_user_name

WHEN NOT MATCHED THEN INSERT VALUES (
    s.core_customer_identifier,
    s.tract_year_identifier,
    s.producer_involvement_start_date,
    s.producer_involvement_end_date,
    s.producer_involvement_interrupted_indicator,
    s.tract_producer_hel_exception_code,
    s.tract_producer_cw_exception_code,
    s.tract_producer_pcw_exception_code,
    s.data_status_code,
    s.creation_date,
    s.last_change_date,
    s.last_change_user_name,
    s.producer_involvement_code,
    s.time_period_identifier,
    s.state_fsa_code,
    s.county_fsa_code,
    s.farm_identifier,
    s.farm_number,
    s.tract_number,
    s.hel_appeals_exhausted_date,
    s.cw_appeals_exhausted_date,
    s.pcw_appeals_exhausted_date,
    s.tract_producer_rma_hel_exception_code,
    s.tract_producer_rma_cw_exception_code,
    s.tract_producer_rma_pcw_exception_code,
    now()
)
```

### 3.4 Partition and Z-Order SSS Tables

Partition the SSS source tables by state when writing to S3. Spark/Athena will then read only the relevant state partitions, ignoring all others.

| Table | Partition key | Z-order / sort key |
|---|---|---|
| `sss.ibsp` | `admin_state` | `admin_county`, `instance` |
| `sss.ibib` | `ZZFLD000002` (state FSA code) | `ZZFLD000003` (county), `ibase` |
| `sss.ibin` | — | `ibase`, `instance` |
| `sss.ibpart` | — | `segment_recno`, `segment` |
| `sss.crmd_partner` | — | `guid` |
| `sss.z_ibase_comp_detail` | — | `instance`, `ibase` |
| `sss.comm_pr_frg_rel` | — | `product_guid` |
| `sss.zmi_farm_partn` | — | `frg_guid` |

```python
df.writeTo("sss.ibsp") \
    .using("iceberg") \
    .partitionedBy("admin_state") \
    .tableProperty("write.distribution-mode", "range") \
    .tableProperty("sort-order", "admin_county, instance") \
    .createOrReplace()
```

### 3.5 Glue Job Configuration

```python
# Recommended Glue job settings for the transform job
job_args = {
    "--datalake-formats": "iceberg",
    "--conf": (
        "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        " --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog"
        " --conf spark.sql.catalog.glue_catalog.warehouse=s3://fpacfsa-final-zone/iceberg/"
        " --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog"
        " --conf spark.sql.autoBroadcastJoinThreshold=52428800"  # 50 MB broadcast join threshold
    ),
    "--worker-type": "G.2X",
    "--number-of-workers": "10",
    "--enable-auto-scaling": "true",
}
```

---

## 4. Step Functions DAG Structure

```json
{
  "Comment": "tract_producer_year ETL pipeline",
  "StartAt": "IngestParallel",
  "States": {
    "IngestParallel": {
      "Type": "Parallel",
      "Branches": [
        { "StartAt": "IngestSSS",    "States": { "IngestSSS":    { "Type": "Task", "Resource": "arn:aws:states:::glue:startJobRun.sync", "Parameters": { "JobName": "cnsv-ingest-sss-farmrecords" } } } },
        { "StartAt": "IngestPGRefs", "States": { "IngestPGRefs": { "Type": "Task", "Resource": "arn:aws:states:::glue:startJobRun.sync", "Parameters": { "JobName": "cnsv-ingest-pg-reference-tables" } } } },
        { "StartAt": "IngestPGCDC",  "States": { "IngestPGCDC":  { "Type": "Task", "Resource": "arn:aws:states:::glue:startJobRun.sync", "Parameters": { "JobName": "cnsv-ingest-pg-cdc-targets" } } } }
      ],
      "Next": "TransformAndMerge"
    },
    "TransformAndMerge": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": { "JobName": "cnsv-tract-producer-year-merge" },
      "Next": "SyncToRDS"
    },
    "SyncToRDS": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": { "JobName": "cnsv-sync-iceberg-to-rds" },
      "End": true
    }
  }
}
```

---

## 5. Expected Performance Impact

| Stage | Current | With this architecture |
|---|---|---|
| Federated PG reference joins | Serialized JDBC, minutes per join | Eliminated — pre-materialized local S3 Parquet |
| SSS table scans | Full scan every run | Partition-pruned to relevant state slice(s) |
| CDC logic | 2–3 separate full-query runs | 1 Iceberg `MERGE INTO` pass |
| Deduplication (`row_number()`) | Runs over full unpartitioned dataset | Runs over pre-sorted partition → faster |
| **Total pipeline runtime** | **Hours** | **30–90 seconds** |

---

## 6. File Mapping — Current SQL to New Glue Jobs

| Current SQL file | Replaced by |
|---|---|
| `TRACT_PRODUCER_YEAR_INSERT_ATHENA.sql` | `cnsv-tract-producer-year-merge` Glue Spark job (`MERGE INTO` — `WHEN NOT MATCHED`) |
| `TRACT_PRODUCER_YEAR_INSERT_NO_COMPARE_ATHENA.sql` | Same job with a `--full-load` flag that skips the `WHEN MATCHED` clause |
| `TRACT_PRODUCER_YEAR_UPDATE_ATHENA.sql` | `cnsv-tract-producer-year-merge` Glue Spark job (`MERGE INTO` — `WHEN MATCHED`) |
| `TRACT_PRODUCER_YEAR_SELECT.sql` | `cnsv-farm-producer-year-merge` Glue Spark job (separate merge for `farm_producer_year`) |

---

## 7. Additional Recommendations

- **Iceberg table maintenance:** Schedule a weekly `OPTIMIZE` and `EXPIRE SNAPSHOTS` job to compact small files and remove old snapshots, keeping scan performance consistent as data grows.
- **Data quality check:** After each `MERGE INTO`, run a CloudWatch metric publish step that compares the row delta against a historical average. Alert if the delta is more than 2 standard deviations from the mean.
- **Incremental SSS ingest:** Use Glue job bookmarks on the SSS source tables keyed on `UPTIM` (last update timestamp) so only changed records are re-ingested rather than full extracts each run.
- **`tbl_fr_bp_relationship` deprecation:** The pre-built relationship table in `sss-farmrecords` can be replaced by the standard `ibin → ibpart → crmd_partner` join chain once the data is local to S3 and the join is no longer network-bound.
