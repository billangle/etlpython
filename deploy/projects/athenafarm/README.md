# athenafarm — tract/farm Producer Year Iceberg Pipeline

Modernized eight-job AWS Glue + Step Functions pipeline using DataFrame-first
full-load transforms over Iceberg tables.

See [prjhelpers/postgres/athena/arch.md](../../../../prjhelpers/postgres/athena/arch.md)
for the full architecture rationale.

---

## Recent Changes (2026-03-04)

- Tract transform is now orchestrated as eight Step Functions-managed stages:
  - `TransformTractProducerYearPreprocessSpine` (`--task_mode=preprocess_spine`)
  - `TransformTractProducerYearPreprocessStructureLink` (`--task_mode=preprocess_structure_link`)
  - `TransformTractProducerYearPreprocessStructureFarm` (`--task_mode=preprocess_structure_farm`)
  - `TransformTractProducerYearPreprocessCore2` (`--task_mode=preprocess_core2`)
  - `TransformTractProducerYearPreprocessPartner` (`--task_mode=preprocess_partner`)
  - `TransformTractProducerYearPreprocessEnrich` (`--task_mode=preprocess_enrich`)
  - `TransformTractProducerYearFinalizeResolve` (`--task_mode=finalize_resolve`)
  - `TransformTractProducerYearFinalizePublish` (`--task_mode=finalize_publish`)
- Tract branch timeout tuning by stage (strictly under 20 minutes each):
  - `preprocess_spine`: 1140s
  - `preprocess_structure_link`: 1140s
  - `preprocess_structure_farm`: 1140s
  - `preprocess_core2`: 1140s
  - `preprocess_partner`: 1140s
  - `preprocess_enrich`: 1140s
  - `finalize_resolve`: 1140s
  - `finalize_publish`: 1140s
- Tract script-level fail-fast (`--max_job_seconds`) is now passed per stage from Step Functions:
  - `preprocess_spine`: 1020s
  - `preprocess_structure_link`: 1020s
  - `preprocess_structure_farm`: 1020s
  - `preprocess_core2`: 1020s
  - `preprocess_partner`: 1020s
  - `preprocess_enrich`: 1020s
  - `finalize_resolve`: 1020s
  - `finalize_publish`: 900s
- Tract per-stage resource profile is now tuned in Step Functions with explicit Glue worker settings and shuffle partitions:
  - `preprocess_spine`: `WorkerType=G.2X`, `NumberOfWorkers=40`, `--shuffle_partitions=1400`
  - `preprocess_structure_link`: `WorkerType=G.2X`, `NumberOfWorkers=24`, `--shuffle_partitions=900`
  - `preprocess_structure_farm`: `WorkerType=G.2X`, `NumberOfWorkers=40`, `--shuffle_partitions=1400`
  - `preprocess_core2`: `WorkerType=G.2X`, `NumberOfWorkers=32`, `--shuffle_partitions=1200`
  - `preprocess_partner`: `WorkerType=G.2X`, `NumberOfWorkers=24`, `--shuffle_partitions=900`
  - `preprocess_enrich`: `WorkerType=G.2X`, `NumberOfWorkers=24`, `--shuffle_partitions=900`
  - `finalize_resolve`: `WorkerType=G.2X`, `NumberOfWorkers=16`, `--shuffle_partitions=700`
  - `finalize_publish`: `WorkerType=G.2X`, `NumberOfWorkers=12`, `--shuffle_partitions=500`
- Tract branch now uses conservative transient-failure retries per stage in Step Functions:
  - `Retry` on `States.TaskFailed`, `Glue.ConcurrentRunsExceededException`, `Glue.InternalServiceException`, `Glue.OperationTimeoutException`, `States.Timeout`
  - Backoff profile: `IntervalSeconds=20`, `MaxAttempts=2`, `BackoffRate=2.0`
- Tract script supports `task_mode` (`single|preprocess|preprocess_base|preprocess_spine|preprocess_structure_link|preprocess_structure_farm|preprocess_core2|preprocess_partner|preprocess_enrich|finalize_resolve|finalize_publish|finalize`) with stage-table handoff for smaller, faster task boundaries.
- Farm transform is pinned to a known-good baseline (`Transform-Farm-Producer-Year`, git commit `fc3fe5f`), with observed full-load completion under 4 minutes.
- PG CDC ingest retains runtime reductions from removal of pre-write cache/count and pre-write repartition/sort passes.
- Contract checks were updated to keep Farm implementation stable and avoid forcing Farm-script rewrites.

## Known Good Baselines

- Farm Producer script: `deploy/projects/athenafarm/glue/Transform-Farm-Producer-Year.py`
- Known-good commit: `fc3fe5f`
- Operational note: full-load observed to complete in under 4 minutes.
- Validation date: `2026-03-04`
- Validation environment: known-good operational run (exact `ENV` value not captured in repository metadata).

Future baseline template:
- Script: `<path>`
- Commit: `<hash>`
- Validation date: `<YYYY-MM-DD>`
- Validation environment: `<ENV>`
- Runtime note: `<e.g., full-load completed under N minutes>`

---

## Folder Structure

```
deploy/projects/athenafarm/
├── deploy.py                        # Deployer (callable standalone or via master deploy.py)
├── glue/
│   ├── Ingest-SSS-Farmrecords.py           # SSS Athena → S3 Iceberg
│   ├── Ingest-PG-Reference-Tables.py       # PG reference tables → S3 Iceberg
│   ├── Ingest-PG-CDC-Targets.py            # PG CDC targets → S3 Iceberg
│   ├── Transform-Tract-Producer-Year.py    # Tract transform (full-load only)
│   ├── Transform-Farm-Producer-Year.py     # Farm transform (full-load only)
│   ├── Sync-Iceberg-To-RDS.py             # Iceberg → RDS PostgreSQL sync (optional)
│   └── Iceberg-Maintenance.py             # Weekly OPTIMIZE + EXPIRE SNAPSHOTS
├── lambda/
│   └── notify_pipeline/
│       └── lambda_function.py              # Pipeline completion/failure notifier
└── states/
    ├── Main.param.asl.json          # Main ETL pipeline Step Functions DAG
    └── Maintenance.param.asl.json   # Weekly maintenance Step Functions DAG
```

---

## Legacy Query Artifacts → Current Glue Job Mapping

| Legacy query artifact | Current Glue job |
|---|---|
| `TRACT_PRODUCER_YEAR_INSERT_ATHENA.sql` | `Transform-Tract-Producer-Year` (full-load reset path) |
| `TRACT_PRODUCER_YEAR_INSERT_NO_COMPARE_ATHENA.sql` | `Transform-Tract-Producer-Year` (full-load reset path) |
| `TRACT_PRODUCER_YEAR_UPDATE_ATHENA.sql` | `Transform-Tract-Producer-Year` (full-load reset path) |
| `TRACT_PRODUCER_YEAR_SELECT.sql` | `Transform-Farm-Producer-Year` |

---

## Pipeline DAG

Main Step Function defaults:
- `full_load`: `true`
- `skip_rds_sync`: `true`

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
        ├─ [TransformParallel]
        │    ├─ Tract branch:
        │    │    ├─ TransformTractProducerYearPreprocessSpine (`--task_mode=preprocess_spine`)
        │    │    ├─ TransformTractProducerYearPreprocessStructureLink (`--task_mode=preprocess_structure_link`)
        │    │    ├─ TransformTractProducerYearPreprocessStructureFarm (`--task_mode=preprocess_structure_farm`)
        │    │    ├─ TransformTractProducerYearPreprocessCore2 (`--task_mode=preprocess_core2`)
        │    │    ├─ TransformTractProducerYearPreprocessPartner (`--task_mode=preprocess_partner`)
        │    │    ├─ TransformTractProducerYearPreprocessEnrich (`--task_mode=preprocess_enrich`)
        │    │    ├─ TransformTractProducerYearFinalizeResolve (`--task_mode=finalize_resolve`)
        │    │    └─ TransformTractProducerYearFinalizePublish (`--task_mode=finalize_publish`)
        │    └─ Farm branch:
        │         └─ Transform-Farm-Producer-Year           (full-load only)
        │
        ├─ CheckSkipRDSSync                     (default: skip — must pass skip_rds_sync=false to run sync)
        ├─ Sync-Iceberg-To-RDS                  (Iceberg → RDS PostgreSQL) [opt-in only]
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

### 2. Trigger a main pipeline run

The transform jobs run full-load only. `full_load` is retained for compatibility
and defaults to `true` at orchestration level.

Main Step Function default run values:
- `full_load`: `true`
- `skip_rds_sync`: `true`

Run full-load with RDS sync:

```json
{ "full_load": "true", "skip_rds_sync": false }
```

### 3. RDS sync is opt-in (default: skipped)

`skip_rds_sync` defaults to **true** — the sync step is **opt-in**. If `skip_rds_sync`
is absent from the execution input or is `true`, `Sync-Iceberg-To-RDS` is skipped.

Default run (full-load, no RDS sync):

```json
{ "full_load": "true", "skip_rds_sync": true }
```

To explicitly enable the RDS sync, pass `skip_rds_sync: false`:

```json
{ "skip_rds_sync": false }
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

# ------- Main ETL pipeline (DEFAULT: full_load=true, skip_rds_sync=true) -------
aws stepfunctions start-execution \
  --region "${REGION}" \
  --state-machine-arn "arn:aws:states:${REGION}:${ACCOUNT}:stateMachine:FSA-${ENV}-ATHENAFARM-Main" \
  --name "manual-$(date +%Y%m%d-%H%M%S)" \
  --input '{"full_load": "true", "skip_rds_sync": true}'

# ------- Main ETL pipeline (full-load, WITH RDS sync) -------
aws stepfunctions start-execution \
  --region "${REGION}" \
  --state-machine-arn "arn:aws:states:${REGION}:${ACCOUNT}:stateMachine:FSA-${ENV}-ATHENAFARM-Main" \
  --name "manual-sync-$(date +%Y%m%d-%H%M%S)" \
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
  - `{ "full_load": "true", "skip_rds_sync": true }` — full-load, no RDS sync (**default**)
  - `{ "skip_rds_sync": false }` — full-load + RDS sync
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
    "--iceberg_warehouse":"'"${WAREHOUSE}"'"
  }'

# Example: re-run the tract/producer-year transform only
aws glue start-job-run \
  --region us-east-1 \
  --job-name "FSA-${ENV}-ATHENAFARM-Transform-Tract-Producer-Year" \
  --arguments '{
    "--env":              "'"${ENV}"'",
    "--iceberg_warehouse":"'"${WAREHOUSE}"'"
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
    input='{"full_load": "true"}'
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
      --input '{"full_load": "true"}'
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
| Transform-Tract-Producer-Year | 20 | 5–20 min | Full-load overwrite via Iceberg DataFrameWriter |
| Transform-Farm-Producer-Year | 20 | 3–12 min | Full-load overwrite via Iceberg DataFrameWriter |
| Sync-Iceberg-To-RDS | 4 | 2–15 min | Distributed partition upsert to RDS (full-load truncates first) |
| Iceberg-Maintenance | 2 | 5–20 min | OPTIMIZE + EXPIRE SNAPSHOTS across all tables |

IngestParallel and TransformParallel branches run concurrently, so total wall-clock time
for a normal full-load run (no RDS sync) is roughly:

```
max(Crawler + IngestSSS, IngestPGRefs, IngestPGCDC)   ~30 min
+ max(TransformTract, TransformFarm)                   ~20 min
= ~50 min worst-case full-load
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

Exception: the delta `#changes` read in `Sync-Iceberg-To-RDS` uses a direct S3
path (`{warehouse}/{db}/{table}#changes`) because the `#changes` suffix is an Iceberg
S3-path convention and cannot be used with a catalog FQN.

### Broadcast join threshold

`spark.sql.autoBroadcastJoinThreshold=52428800` (50 MB) is set globally so that small
reference/dimension tables are broadcast-joined rather than shuffle-joined, avoiding
expensive all-to-all data shuffles in transform join stages.

### Transform full-load write path

Both transform jobs now write through the Iceberg DataFrame API:

1. Build transformed source DataFrame
2. `write.format("iceberg").mode("overwrite").saveAsTable(...)`

This keeps write behavior deterministic and fail-fast for full-load replacements.

### Runtime optimization safeguards

The transform scripts intentionally avoid `source_df.cache()` and
`source_df.count()` before writes. Those debug-only actions caused expensive
shuffle spill and disk pressure (`No space left on device`) on large runs.

Current behavior:
- full-load transforms avoid debug cache/count actions and write once via overwrite
- sync writes use distributed `foreachPartition` JDBC upserts (no driver-side collect)
- jobs emit phase timings for CloudWatch-based performance tracking

### Iceberg snapshot count (no full scan)

Post-write row counts are read from Iceberg snapshot metadata
(`spark.table("glue_catalog.db.table.snapshots")`) rather than running a full
`SELECT COUNT(*)` scan over all Parquet files.

---

## Glue Job Arguments

| Argument | Jobs | Description | Default |
|---|---|---|---|
| `--env` | all | Deployment environment | (required) |
| `--iceberg_warehouse` | all | S3 URI for Iceberg warehouse root | (required) |
| `--secret_id` | PG ingest, Sync | AWS Secrets Manager secret ID for PG credentials | from config |
| `--full_load` | Transform-Tract, Transform-Farm, Sync | Compatibility flag; transforms are full-load only. For Sync, `true` truncates RDS target tables before insert. | `true` |
| `--sss_database` | Transform | Glue catalog DB for SSS Iceberg tables | `athenafarm_prod_raw` |
| `--ref_database` | Transform | Glue catalog DB for PG reference Iceberg tables | `athenafarm_prod_ref` |
| `--target_database` | Ingest PG, Transform | Glue catalog DB for target Iceberg tables | `athenafarm_prod_gold` |
| `--snapshot_id_param` | Sync | SSM parameter path storing last-synced Iceberg snapshot ID | `/athenafarm/{env}/last_sync_snapshot` |
| `--snapshot_retention_hours` | Maintenance | Hours of snapshots to keep | `168` (7 days) |

## Step Functions Execution Input

| Parameter | Type | Description | Default |
|---|---|---|---|
| `full_load` | boolean/string | Retained for compatibility; tract and farm transforms run full-load only | `true` |
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

### Runtime metrics emitted by transform jobs

`Transform-Tract-Producer-Year` and `Transform-Farm-Producer-Year` emit
structured metric lines to the driver log for every run:

- `[METRIC] mode=full_load`
- `[METRIC] total_job_seconds=<float>`

`Transform-Tract-Producer-Year` also emits stage-prefixed log lines to make
CloudWatch filtering easier for the 8-stage tract flow:

- `[PP_SPINE_STAGE] ...`
- `[PP_STRUCTURE_LINK_STAGE] ...`
- `[PP_STRUCTURE_FARM_STAGE] ...`
- `[PP_CORE2_STAGE] ...`
- `[PP_PARTNER_STAGE] ...`
- `[PP_ENRICH_STAGE] ...`
- `[FINALIZE_RESOLVE_STAGE] ...`
- `[FINALIZE_PUBLISH_STAGE] ...`
- `[JOB_STAGE] ...`

Prefix convention for tract logging/metrics:

- Stage log format: `[<STAGE_PREFIX>] <message>`
- Stage metric format: `[<STAGE_PREFIX>] [METRIC] <name>=<value>`
- Examples:
  - `[PP_SPINE_STAGE] [METRIC] phase_preprocess_spine_seconds=<float>`
  - `[PP_SPINE_STAGE] [METRIC] stage_spine_row_count=<int>`
  - `[PP_STRUCTURE_LINK_STAGE] [METRIC] phase_preprocess_structure_link_seconds=<float>`
  - `[PP_STRUCTURE_LINK_STAGE] [METRIC] stage_core1_row_count=<int>`
  - `[PP_STRUCTURE_FARM_STAGE] [METRIC] phase_preprocess_structure_farm_seconds=<float>`
  - `[PP_STRUCTURE_FARM_STAGE] [METRIC] stage_structure_row_count=<int>`
  - `[PP_CORE2_STAGE] [METRIC] phase_preprocess_core2_seconds=<float>`
  - `[PP_CORE2_STAGE] [METRIC] stage_core2_row_count=<int>`
  - `[PP_PARTNER_STAGE] [METRIC] phase_preprocess_partner_seconds=<float>`
  - `[PP_PARTNER_STAGE] [METRIC] stage_base_row_count=<int>`
  - `[PP_ENRICH_STAGE] [METRIC] phase_preprocess_enrich_seconds=<float>`
  - `[PP_ENRICH_STAGE] [METRIC] stage_enrich_row_count=<int>`
  - `[FINALIZE_RESOLVE_STAGE] [METRIC] phase_finalize_resolve_seconds=<float>`
  - `[FINALIZE_PUBLISH_STAGE] [METRIC] phase_write_seconds=<float>`
  - `[JOB_STAGE] [METRIC] total_job_seconds=<float>`

Sample stage-focused filter:

```sql
fields @timestamp, @message
| filter @message like /\[PP_ENRICH_STAGE\]/
| sort @timestamp desc
| limit 200
```

Sample all-stage filter (single query for all tract stage prefixes):

```sql
fields @timestamp, @message
| filter @message like /\[(PP_SPINE_STAGE|PP_STRUCTURE_LINK_STAGE|PP_STRUCTURE_FARM_STAGE|PP_CORE2_STAGE|PP_PARTNER_STAGE|PP_ENRICH_STAGE|FINALIZE_RESOLVE_STAGE|FINALIZE_PUBLISH_STAGE|JOB_STAGE)\]/
| sort @timestamp desc
| limit 200
```

Sample all-stage parsed view (extract stage for grouping):

```sql
fields @timestamp, @message
| parse @message /\[(?<stage>PP_SPINE_STAGE|PP_STRUCTURE_LINK_STAGE|PP_STRUCTURE_FARM_STAGE|PP_CORE2_STAGE|PP_PARTNER_STAGE|PP_ENRICH_STAGE|FINALIZE_RESOLVE_STAGE|FINALIZE_PUBLISH_STAGE|JOB_STAGE)\]/
| filter ispresent(stage)
| stats count(*) as lines by stage
| sort lines desc
```

Sample CloudWatch Logs Insights query (replace log group as needed):

```sql
fields @timestamp, @message
| filter @message like /\[METRIC\]/
| sort @timestamp desc
| limit 200
```

---

## Regression Tests

The contract/regression suite for deployment + transform safety checks is:

- `deploy/projects/athenafarm/checks/test_config_contract.py`

Run from repository root:

```bash
python3 -m unittest deploy.projects.athenafarm.checks.test_config_contract -v
```

This suite validates:
- config key and default contracts (including environment-specific DB names)
- optional Glue arg parsing safety (`_opt` behavior)
- first-run table creation and full-load write-path expectations
- transform optimization guardrails (no debug cache/count regressions)
- adaptive Spark settings across all Glue scripts
- distributed sync write contract (`foreachPartition`, no driver `collect()`)

For targeted CloudShell reruns of the two transform jobs updated during
`errors-14` troubleshooting:

```bash
chmod +x deploy/projects/athenafarm/tests/run_errors14_glue_jobs.sh
deploy/projects/athenafarm/tests/run_errors14_glue_jobs.sh --env PROD --region us-east-1 --full-load true
```

The script starts both jobs, polls status, and exits non-zero if either fails.

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
