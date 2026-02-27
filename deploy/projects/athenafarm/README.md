# athenafarm — tract/farm Producer Year Iceberg Pipeline

Replaces four Athena SQL files with a six-job AWS Glue + Step Functions pipeline
that runs in **30–90 seconds** instead of hours.

See [prjhelpers/postgres/athena/arch.md](../../../../prjhelpers/postgres/athena/arch.md)
for the full architecture rationale.

---

## Folder Structure

```
deploy/projects/athenafarm/
├── deploy.py                        # Deployer (callable standalone or via master deploy.py)
├── glue/
│   ├── Ingest-SSS-Farmrecords.py           # SSS Athena → S3 Iceberg
│   ├── Ingest-PG-Reference-Tables.py       # PG reference tables → S3 Iceberg
│   ├── Ingest-PG-CDC-Targets.py            # PG CDC targets → S3 Iceberg
│   ├── Transform-Tract-Producer-Year.py    # CTE + MERGE INTO tract_producer_year
│   ├── Transform-Farm-Producer-Year.py     # CTE + MERGE INTO farm_producer_year
│   ├── Sync-Iceberg-To-RDS.py             # Delta sync Iceberg → RDS PostgreSQL
│   └── Iceberg-Maintenance.py             # Weekly OPTIMIZE + EXPIRE SNAPSHOTS
├── lambda/
│   └── notify_pipeline/
│       └── lambda_function.py              # Pipeline completion/failure notifier
└── states/
    ├── Main.param.asl.json          # Main ETL pipeline Step Functions DAG
    └── Maintenance.param.asl.json   # Weekly maintenance Step Functions DAG
```

---

## Old SQL → New Glue Job Mapping

| Original Athena SQL file | Replaced by |
|---|---|
| `TRACT_PRODUCER_YEAR_INSERT_ATHENA.sql` | `Transform-Tract-Producer-Year` (MERGE INTO — `WHEN NOT MATCHED`) |
| `TRACT_PRODUCER_YEAR_INSERT_NO_COMPARE_ATHENA.sql` | `Transform-Tract-Producer-Year` with `--full_load=true` |
| `TRACT_PRODUCER_YEAR_UPDATE_ATHENA.sql` | `Transform-Tract-Producer-Year` (MERGE INTO — `WHEN MATCHED`) |
| `TRACT_PRODUCER_YEAR_SELECT.sql` | `Transform-Farm-Producer-Year` |

---

## Pipeline DAG

```
Manual / Lambda / CI trigger (no EventBridge — see "Running Without EventBridge" below)
  │
  └─▶ Step Functions: FSA-{ENV}-ATHENAFARM-Main
        ├─ [IngestParallel]
        │    ├─ StartSSSCrawler → WaitForSSSCrawler → poll loop → CheckSSSCrawlerStatus
        │    │    └─ Ingest-SSS-Farmrecords     (SSS Athena → athenafarm_prod_raw Iceberg)
        │    ├─ Ingest-PG-Reference-Tables      (PG refs → athenafarm_prod_ref Iceberg)
        │    └─ Ingest-PG-CDC-Targets           (PG CDC → athenafarm_prod_cdc Iceberg)
        │
        ├─ [TransformParallel]                  (both transforms run concurrently)
        │    ├─ Transform-Tract-Producer-Year   (CTE + MERGE INTO tract_producer_year)
        │    └─ Transform-Farm-Producer-Year    (CTE + MERGE INTO farm_producer_year)
        │
        ├─ CheckSkipRDSSync                     (default: skip — must pass skip_rds_sync=false to run sync)
        ├─ Sync-Iceberg-To-RDS                  (delta → RDS PostgreSQL)  [opt-in only]
        └─ Notify                               (FSA-{ENV}-ATHENAFARM-NotifyPipeline Lambda)

Manual / Lambda / CI trigger (on schedule — no EventBridge)
  └─▶ Step Functions: FSA-{ENV}-ATHENAFARM-Maintenance
        └─ Iceberg-Maintenance                  (OPTIMIZE + EXPIRE SNAPSHOTS)
```

---

## Deploy

Config files live in `deploy/config/athenafarm/` (`dev.json`, `prod.json`, `steamdev.json`).

### 1. Run the deployer

Via the master deployer (preferred):

```bash
cd deploy

# Dry run first
python deploy.py --config config/athenafarm/dev.json --region us-east-1 --project-type athenafarm

# Deploy
python deploy.py --config config/athenafarm/prod.json --region us-east-1 --project-type athenafarm
```

Or standalone:

```bash
cd deploy/projects/athenafarm
python deploy.py --config ../../config/athenafarm/dev.json --dry-run
python deploy.py --config ../../config/athenafarm/prod.json
```

### 2. Trigger a full-load run

Pass `full_load: "true"` in the Step Functions input to force a full snapshot
(equivalent to `INSERT_NO_COMPARE` — rebuilds all Iceberg tables from scratch
and truncates RDS target tables before re-inserting):

```json
{ "full_load": "true", "skip_rds_sync": false }
```

### 3. RDS sync is opt-in (default: skipped)

`skip_rds_sync` defaults to **true** — the sync step is **opt-in**. If `skip_rds_sync`
is absent from the execution input or is `true`, `Sync-Iceberg-To-RDS` is skipped.

Normal incremental run (no RDS sync):

```json
{ "full_load": "false" }
```

To explicitly enable the RDS sync, pass `skip_rds_sync: false`:

```json
{ "full_load": "false", "skip_rds_sync": false }
```

---

## Running Without EventBridge

EventBridge is **not available** in this AWS environment.  The Step Functions
state machines and individual Glue jobs must be triggered manually, via the
AWS Console, or via an S3-event Lambda — all without EventBridge.

### Option 1 — AWS CLI (recommended for ad-hoc / CI runs)

```bash
ACCOUNT="241533156429"   # change per environment
ENV="FPACDEV"            # FPACDEV | STEAMDEV | PROD
REGION="us-east-1"

# ------- Main ETL pipeline (incremental, no RDS sync — DEFAULT) -------
aws stepfunctions start-execution \
  --region "${REGION}" \
  --state-machine-arn "arn:aws:states:${REGION}:${ACCOUNT}:stateMachine:FSA-${ENV}-ATHENAFARM-Main" \
  --name "manual-$(date +%Y%m%d-%H%M%S)" \
  --input '{"full_load": "false"}'

# ------- Main ETL pipeline (incremental, WITH RDS sync) -------
aws stepfunctions start-execution \
  --region "${REGION}" \
  --state-machine-arn "arn:aws:states:${REGION}:${ACCOUNT}:stateMachine:FSA-${ENV}-ATHENAFARM-Main" \
  --name "manual-sync-$(date +%Y%m%d-%H%M%S)" \
  --input '{"full_load": "false", "skip_rds_sync": false}'

# ------- Full load + RDS sync -------
aws stepfunctions start-execution \
  --region "${REGION}" \
  --state-machine-arn "arn:aws:states:${REGION}:${ACCOUNT}:stateMachine:FSA-${ENV}-ATHENAFARM-Main" \
  --name "fullload-$(date +%Y%m%d-%H%M%S)" \
  --input '{"full_load": "true", "skip_rds_sync": false}'

# ------- Weekly Maintenance pipeline -------
aws stepfunctions start-execution \
  --region "${REGION}" \
  --state-machine-arn "arn:aws:states:${REGION}:${ACCOUNT}:stateMachine:FSA-${ENV}-ATHENAFARM-Maintenance" \
  --name "maint-$(date +%Y%m%d-%H%M%S)" \
  --input '{}'
```

Poll for completion (replace `<execution-arn>` with the ARN returned above):

```bash
aws stepfunctions describe-execution \
  --region us-east-1 \
  --execution-arn "<execution-arn>" \
  --query 'status'
```

### Option 2 — AWS Console

1. Open the [Step Functions console](https://console.aws.amazon.com/states/home).
2. In the left panel choose **State machines**.
3. Search for `FSA-{ENV}-ATHENAFARM-Main` (or `Maintenance`).
4. Click **Start execution**.
5. In the **Input** box enter one of:
   - `{ "full_load": "false" }` — incremental MERGE, no RDS sync (**default**)
   - `{ "full_load": "false", "skip_rds_sync": false }` — incremental MERGE + RDS sync
   - `{ "full_load": "true", "skip_rds_sync": false }` — full rebuild + RDS sync
   - `{ "full_load": "true" }` — full rebuild, no RDS sync
6. Click **Start execution**.

Monitor progress in the **Graph inspector** tab; each node goes green when
the Glue job finishes.

### Option 3 — Run a single Glue job (testing / debugging)

You can start any job independently.  Pass all required arguments explicitly:

```bash
ENV="FPACDEV"
WAREHOUSE="s3://c108-dev-fpacfsa-final-zone/athenafarm/iceberg"

# Example: re-ingest SSS farmrecords only
aws glue start-job-run \
  --region us-east-1 \
  --job-name "FSA-${ENV}-ATHENAFARM-Ingest-SSS-Farmrecords" \
  --arguments '{
    "--env":              "'"${ENV}"'",
    "--iceberg_warehouse":"'"${WAREHOUSE}"'",
    "--full_load":        "false"
  }'

# Example: re-run the tract/producer-year transform only
aws glue start-job-run \
  --region us-east-1 \
  --job-name "FSA-${ENV}-ATHENAFARM-Transform-Tract-Producer-Year" \
  --arguments '{
    "--env":              "'"${ENV}"'",
    "--iceberg_warehouse":"'"${WAREHOUSE}"'",
    "--full_load":        "false"
  }'
```

Wait for the run to finish:

```bash
aws glue get-job-run \
  --region us-east-1 \
  --job-name "FSA-${ENV}-ATHENAFARM-Transform-Tract-Producer-Year" \
  --run-id "<JobRunId from start-job-run output>" \
  --query 'JobRun.JobRunState'
```

### Option 4 — S3-event Lambda (event-driven, no EventBridge)

For an **automated, event-driven trigger** without EventBridge, deploy a
small Lambda that fires on S3 object-created events when SSS data lands:

```python
# lambda_handler.py (Python 3.12)
import boto3, os, datetime

SFN_ARN = os.environ["STATE_MACHINE_ARN"]   # set in Lambda env vars
sfn = boto3.client("stepfunctions")

def lambda_handler(event, context):
    ts = datetime.datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    sfn.start_execution(
        stateMachineArn=SFN_ARN,
        name=f"s3trigger-{ts}",
        input='{"full_load": "false"}'
    )
```

Wire the Lambda to the SSS source bucket in **S3 → Properties →
Event notifications** (Object Created, prefix = SSS data prefix).
S3 event notifications require **no EventBridge** — they send directly to Lambda.

The Lambda needs an IAM role with:
- `states:StartExecution` on the Main state machine ARN
- No other permissions required

### Option 5 — Jenkins / CI pipeline

Add a pipeline step that calls the AWS CLI commands from Option 1.  The
`Jenkinsfile` in `prjhelpers/cicd/` can be extended with a `stage('Run ETL')`
block:

```groovy
stage('Run ETL') {
  sh """
    aws stepfunctions start-execution \
      --state-machine-arn "${STATE_MACHINE_ARN}" \
      --name "ci-\$(date +%Y%m%d-%H%M%S)" \
      --input '{"full_load": "false"}'
  """
}
```

---

---

## SSS Farmrecords Crawler

The SSS branch of the Step Function manages the Glue crawler
(`FSA-{ENV}-SSS-Farmrecords-Crawler`) that populates the `sss-farmrecords` Glue
catalog database from the SSS Parquet landing zone on S3.

### What the crawler does

The crawler scans `sssFarmrecordsS3Path` (configured in `prod.json`) and registers
the discovered Parquet tables into the `sss-farmrecords` Glue catalog database.
`Ingest-SSS-Farmrecords` then reads those catalog tables via `spark.table()` and
writes them to Iceberg in `athenafarm_prod_raw`.

### Poll loop (inside the Step Function)

```
StartSSSCrawler
  │  CrawlerRunningException → WaitForSSSCrawler   (concurrent exec already started it)
  │  States.ALL              → IngestSSSFailed
  ▼
WaitForSSSCrawler (30 s)
  ▼
GetSSSCrawlerState
  ▼
CheckSSSCrawlerDone
  ├─ Crawler.State == READY  → CheckSSSCrawlerStatus
  └─ anything else           → WaitForSSSCrawler   (still running — loop)
  ▼
CheckSSSCrawlerStatus
  ├─ LastCrawl.Status == FAILED → IngestSSSFailed  (tables may be stale/missing)
  └─ default                    → IngestSSS
```

**Key assumptions:**
- The crawler is provisioned by `deploy.py` (create/update only — it does **not** start it).
  Execution is entirely within the Step Function so the timing is pipeline-controlled.
- `sss-farmrecords` database must exist before the crawler runs. `deploy.py` creates it
  automatically during deployment.
- S3 subfolder names under `sssFarmrecordsS3Path` are treated as table names by the crawler.
  The SSS landing zone uses **UPPERCASE** subfolder names (e.g. `IBIB/`, `IBPART/`).
  The resulting catalog table names are lowercase (`ibib`, `ibpart`, etc.) after Glue normalises them.
- If the crawler finishes with `State=READY` but `LastCrawl.Status=FAILED`, the pipeline
  aborts to `IngestSSSFailed` rather than running ingest on potentially stale or missing tables.
- A `CrawlerRunningException` on start means another concurrent execution already started the
  crawler — the pipeline falls directly into the poll loop and waits for it to finish normally.

---

## Expected Runtimes (PROD, G.2X workers)

| Job | Workers | Typical runtime | Notes |
|---|---|---|---|
| SSS Crawler | — | 1–3 min | Depends on S3 object count in landing zone |
| Ingest-SSS-Farmrecords | 10 | 5–15 min | Reads SSS Parquet, writes Iceberg with partition sort |
| Ingest-PG-Reference-Tables | 4 | 10–30 min | Parallel JDBC reads (20 partitions per table, PK range) |
| Ingest-PG-CDC-Targets | 4 | 10–30 min | Same parallel JDBC approach |
| Transform-Tract-Producer-Year | 20 | 15–45 min | Large CTE join; incremental MERGE is fastest |
| Transform-Farm-Producer-Year | 20 | 5–20 min | Smaller dataset than tract |
| Sync-Iceberg-To-RDS | 4 | 2–15 min | Delta-only (snapshot diff); full load is longer |
| Iceberg-Maintenance | 2 | 5–20 min | OPTIMIZE + EXPIRE SNAPSHOTS across all tables |

IngestParallel and TransformParallel branches run concurrently, so total wall-clock time
for a normal incremental run (no RDS sync) is roughly:

```
max(Crawler + IngestSSS, IngestPGRefs, IngestPGCDC)   ~30 min
+ max(TransformTract, TransformFarm)                   ~45 min
= ~75 min worst-case incremental
```

---

## Optimizations

### Parallel JDBC reads (Ingest-PG-Reference-Tables, Ingest-PG-CDC-Targets)

Large PostgreSQL tables are read in parallel using JDBC partitioning on the integer
primary key. For each table the deployer queries `information_schema` to find the PK
column, then computes a `lowerBound`/`upperBound` split across `numPartitions=20`
parallel JDBC connections. This turns what was a single-threaded sequential read into
20 concurrent reads, cutting ingest time by up to 20×.

### NaN-safe JDBC reads

PostgreSQL `NUMERIC`/`DECIMAL` columns can contain `NaN` (IEEE 754 not-a-number), which
is valid in PostgreSQL but illegal in Java `BigDecimal` — the JDBC driver throws
`PSQLException: Bad value for type BigDecimal: NaN` when it encounters one.

Every JDBC read is wrapped via `nan_safe_table_expr()`, which queries
`information_schema.columns` to identify all `numeric`/`decimal` columns in the table
and builds a subquery that wraps each one in:

```sql
CASE WHEN col::text = 'NaN' THEN NULL ELSE col END AS col
```

This converts PostgreSQL `NaN` to SQL `NULL` before the JDBC driver reads it.

### `spark.table()` for Iceberg catalog reads

All Iceberg table reads use `spark.table("glue_catalog.db.table")` — **not**
`spark.read.format("iceberg").load("glue_catalog.db.table")`.

`DataFrameReader.load()` expects an S3 path; passing a catalog FQN causes
`DataSourceV2Utils.loadV2Source` to throw `None.get` (NullPointerException in Scala).
`spark.table()` routes through Spark's `CatalogManager`, which is correctly configured
by `--datalake-formats iceberg`.

Exception: the incremental `#changes` read in `Sync-Iceberg-To-RDS` uses a direct S3
path (`{warehouse}/{db}/{table}#changes`) because the `#changes` suffix is an Iceberg
S3-path convention and cannot be used with a catalog FQN.

### Broadcast join threshold

`spark.sql.autoBroadcastJoinThreshold=52428800` (50 MB) is set globally so that small
reference/dimension tables are broadcast-joined rather than shuffle-joined, avoiding
expensive all-to-all data shuffles in the transform CTE chains.

### NULL-safe MERGE conditions

MERGE INTO `WHEN MATCHED` conditions use `NOT (t.col <=> s.col)` (NULL-safe equality)
instead of `t.col <> s.col`. Standard `<>` returns `NULL` when either side is `NULL`,
which means rows with any NULL column would never match the update condition and would
accumulate as duplicates. `<=>` is always `true` or `false`.

### Iceberg snapshot count (no full scan)

Post-MERGE row counts are read from Iceberg snapshot metadata
(`spark.table("glue_catalog.db.table.snapshots")`) rather than running a full
`SELECT COUNT(*)` scan over all Parquet files.

---

## Glue Job Arguments

| Argument | Jobs | Description | Default |
|---|---|---|---|
| `--env` | all | Deployment environment | (required) |
| `--iceberg_warehouse` | all | S3 URI for Iceberg warehouse root | (required) |
| `--secret_id` | PG ingest, Sync | AWS Secrets Manager secret ID for PG credentials | from config |
| `--full_load` | Transform, Sync | `true` = full rebuild; Sync also truncates RDS before insert | `false` |
| `--sss_database` | Transform | Glue catalog DB for SSS Iceberg tables | `athenafarm_prod_raw` |
| `--ref_database` | Transform | Glue catalog DB for PG reference Iceberg tables | `athenafarm_prod_ref` |
| `--target_database` | Ingest PG, Transform | Glue catalog DB for target Iceberg tables | `athenafarm_prod_gold` |
| `--snapshot_id_param` | Sync | SSM parameter path storing last-synced Iceberg snapshot ID | `/athenafarm/{env}/last_sync_snapshot` |
| `--snapshot_retention_hours` | Maintenance | Hours of snapshots to keep | `168` (7 days) |

## Step Functions Execution Input

| Parameter | Type | Description | Default |
|---|---|---|---|
| `full_load` | boolean/string | Force full rebuild of all Iceberg tables; Sync truncates RDS before reload | `false` |
| `skip_rds_sync` | boolean | Skip `Sync-Iceberg-To-RDS`. **Default is `true` (absent = skip)**. Pass `false` explicitly to run the sync. | `true` |

---

## Required Glue Catalog Databases

These are created automatically by `deploy.py` during deployment. They must
exist before the first pipeline run:

| Database | Contains |
|---|---|
| `athenafarm_prod_raw` | Materialised SSS/SAP farmrecords Iceberg tables (written by Ingest-SSS-Farmrecords) |
| `athenafarm_prod_ref` | Materialised PG reference Iceberg tables (written by Ingest-PG-Reference-Tables) |
| `athenafarm_prod_cdc` | PG CDC target Iceberg tables (written by Ingest-PG-CDC-Targets) |
| `athenafarm_prod_gold` | Transform output: `tract_producer_year`, `farm_producer_year` |
| `sss-farmrecords` | Raw SSS catalog tables populated by the Glue crawler (read by Ingest-SSS-Farmrecords) |

---

## CloudWatch Logging

All Glue jobs write continuous logs to CloudWatch. Three args are injected by
`deploy.py` for every job — no manual setup required:

| Arg | Value |
|---|---|
| `--enable-continuous-cloudwatch-log` | `true` |
| `--continuous-log-logGroup` | `/aws-glue/jobs/FSA-{ENV}-ATHENAFARM-{JobStem}` |
| `--continuous-log-logStreamPrefix` | `FSA-{ENV}-ATHENAFARM-{JobStem}/` |

### Log group names (PROD)

| Job | Log group |
|---|---|
| Ingest-SSS-Farmrecords | `/aws-glue/jobs/FSA-PROD-ATHENAFARM-Ingest-SSS-Farmrecords` |
| Ingest-PG-Reference-Tables | `/aws-glue/jobs/FSA-PROD-ATHENAFARM-Ingest-PG-Reference-Tables` |
| Ingest-PG-CDC-Targets | `/aws-glue/jobs/FSA-PROD-ATHENAFARM-Ingest-PG-CDC-Targets` |
| Transform-Tract-Producer-Year | `/aws-glue/jobs/FSA-PROD-ATHENAFARM-Transform-Tract-Producer-Year` |
| Transform-Farm-Producer-Year | `/aws-glue/jobs/FSA-PROD-ATHENAFARM-Transform-Farm-Producer-Year` |
| Sync-Iceberg-To-RDS | `/aws-glue/jobs/FSA-PROD-ATHENAFARM-Sync-Iceberg-To-RDS` |
| Iceberg-Maintenance | `/aws-glue/jobs/FSA-PROD-ATHENAFARM-Iceberg-Maintenance` |

### Log stream naming

Within each log group, streams are prefixed with the job name so runs don't
overwrite each other. Each run produces three streams:

```
FSA-PROD-ATHENAFARM-{JobStem}/{job-run-id}           ← driver stdout (print statements)
FSA-PROD-ATHENAFARM-{JobStem}/{job-run-id}/error      ← driver stderr (exceptions, stack traces)
FSA-PROD-ATHENAFARM-{JobStem}/{job-run-id}/out        ← Glue system output
```

### Finding logs for a failed run

The Step Functions execution output includes the Glue job run ID in the `cause`
block (field `Id`). Use it to go directly to the relevant stream:

```bash
ENV="PROD"
JOB="FSA-PROD-ATHENAFARM-Transform-Tract-Producer-Year"
RUN_ID="jr_abc123..."

aws logs get-log-events \
  --region us-east-1 \
  --log-group-name "/aws-glue/jobs/${JOB}" \
  --log-stream-name "${JOB}/${RUN_ID}/error" \
  --query 'events[*].message' \
  --output text
```

Or in the console: **CloudWatch → Log groups → `/aws-glue/jobs/FSA-PROD-ATHENAFARM-{JobStem}`
→ filter by the run ID**.

### Glue Exception Analysis events

Glue 4.0 also emits a structured `GlueExceptionAnalysisListener` event to the
`error` stream on job failure. It contains:
- `Failure Reason` — full Python traceback
- `Stack Trace` — parsed frame list with file, class, method, line number
- `Last Executed Line number` — last line reached before the exception
- `script` — the script filename that failed

This is the fastest way to identify root cause without reading raw log output.

---

## Iceberg Glue Job Configuration

The following Spark conf is passed to all jobs via `--conf`:

```
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
spark.sql.autoBroadcastJoinThreshold=52428800
```

`--datalake-formats=iceberg` is also set so Glue provisions the Iceberg JAR automatically.
