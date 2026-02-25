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
        ├─ [Parallel]
        │    ├─ Ingest-SSS-Farmrecords          (SSS Athena → sss.* Iceberg)
        │    ├─ Ingest-PG-Reference-Tables      (PG refs → farm_ref.* Iceberg)
        │    └─ Ingest-PG-CDC-Targets           (PG targets → farm_records_reporting.* Iceberg)
        │
        ├─ Transform-Tract-Producer-Year        (CTE + MERGE INTO tract_producer_year)
        ├─ Transform-Farm-Producer-Year         (CTE + MERGE INTO farm_producer_year)
        └─ Sync-Iceberg-To-RDS                  (delta → RDS PostgreSQL)

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

Pass `full_load: "true"` in the Step Functions input to force a bulk insert
(skips `WHEN MATCHED` — equivalent to `INSERT_NO_COMPARE`):

```json
{ "full_load": "true" }
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

# ------- Main ETL pipeline (incremental) -------
aws stepfunctions start-execution \
  --region "${REGION}" \
  --state-machine-arn "arn:aws:states:${REGION}:${ACCOUNT}:stateMachine:FSA-${ENV}-ATHENAFARM-Main" \
  --name "manual-$(date +%Y%m%d-%H%M%S)" \
  --input '{"full_load": "false"}'

# ------- Main ETL pipeline (full / bulk-insert only) -------
aws stepfunctions start-execution \
  --region "${REGION}" \
  --state-machine-arn "arn:aws:states:${REGION}:${ACCOUNT}:stateMachine:FSA-${ENV}-ATHENAFARM-Main" \
  --name "fullload-$(date +%Y%m%d-%H%M%S)" \
  --input '{"full_load": "true"}'

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
   - `{ "full_load": "false" }` — incremental MERGE (normal run)
   - `{ "full_load": "true" }` — full/bulk insert only
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

## Glue Job Arguments

| Argument | Description | Default |
|---|---|---|
| `--env` | Deployment environment | (required) |
| `--iceberg_warehouse` | S3 URI for Iceberg warehouse root | (required) |
| `--full_load` | `true` = bulk insert only, no MATCHED update | `false` |
| `--connection_name` | Glue connection name for RDS PostgreSQL | from config |
| `--sss_database` | Glue catalog DB for SSS Iceberg tables | `sss` |
| `--ref_database` | Glue catalog DB for PG reference Iceberg tables | `farm_ref` |
| `--target_database` | Glue catalog DB for target Iceberg tables | `farm_records_reporting` |
| `--snapshot_retention_hours` | Maintenance: hours of snapshots to keep | `168` (7 days) |

---

## Required Glue Catalog Databases

Create these in the AWS Glue Data Catalog before first run:

| Database | Contains |
|---|---|
| `sss` | Materialised SSS/SAP farmrecords Iceberg tables |
| `farm_ref` | Materialised PG reference Iceberg tables |
| `farm_records_reporting` | CDC target Iceberg tables (merge target) |

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
