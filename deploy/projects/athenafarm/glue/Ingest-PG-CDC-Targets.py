"""
================================================================================
AWS Glue Job: Ingest-PG-CDC-Targets
================================================================================

PURPOSE:
    Snapshots the two CDC target tables from FSA-PROD-rds-pg-edv into S3
    Iceberg so the downstream Transform job can run the MERGE INTO comparison
    logic entirely inside Spark without any federated JDBC reads.

    By keeping a local Iceberg copy of the current target state, the Transform
    job's WHEN MATCHED / WHEN NOT MATCHED determination becomes a pure S3 join
    rather than a serialised JDBC lookup.

TABLES PROCESSED:
    farm_records_reporting.tract_producer_year  → farm_records_reporting.tract_producer_year
    farm_records_reporting.farm_producer_year   → farm_records_reporting.farm_producer_year

GLUE JOB ARGUMENTS:
    --JOB_NAME          : Glue job name
    --env               : Deployment environment
    --iceberg_warehouse : s3:// URI for Iceberg warehouse root
    --connection_name   : Glue connection name for RDS PostgreSQL
    --target_database   : Glue catalog database (default: farm_records_reporting)
    --full_load         : "true" to force full snapshot (default: incremental)

VERSION HISTORY:
    v1.0.0 - 2026-02-25 - Initial implementation (athenafarm project)

================================================================================
"""

import sys
import logging
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

required_args = ["JOB_NAME", "env", "iceberg_warehouse", "connection_name"]
args = getResolvedOptions(sys.argv, required_args)

JOB_NAME          = args["JOB_NAME"]
ENV               = args["env"]
ICEBERG_WAREHOUSE = args["iceberg_warehouse"]
CONNECTION_NAME   = args["connection_name"]
TARGET_DATABASE   = args.get("target_database", "farm_records_reporting")
FULL_LOAD         = args.get("full_load", "false").strip().lower() == "true"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, args)

spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", ICEBERG_WAREHOUSE)

# ---------------------------------------------------------------------------
# Table specs: (pg_table, iceberg_target, partition_col, bookmark_key)
# ---------------------------------------------------------------------------
TABLE_SPECS = [
    (
        "farm_records_reporting",
        "tract_producer_year",
        "tract_producer_year",
        "state_fsa_code",
        "core_customer_identifier",           # bookmark key — monotonically increasing surrogate
    ),
    (
        "farm_records_reporting",
        "farm_producer_year",
        "farm_producer_year",
        "state_fsa_code",
        "core_customer_identifier",
    ),
]


def ingest_cdc_target(pg_schema: str, pg_table: str, tgt_table: str, partition_col: str, bookmark_key: str):
    target_fqn = f"glue_catalog.{TARGET_DATABASE}.{tgt_table}"
    source_label = f"{pg_schema}.{pg_table}"
    log.info(f"[{source_label}] Snapshotting CDC target via {CONNECTION_NAME}")

    dyf = glueContext.create_dynamic_frame.from_catalog(
        database=CONNECTION_NAME,
        table_name=f"{pg_schema}_{pg_table}",
        additional_options={
            "jobBookmarkKeys": [bookmark_key],
            "jobBookmarkKeysSortOrder": "asc",
        } if not FULL_LOAD else {},
    )

    df = dyf.toDF()
    row_count = df.count()
    log.info(f"[{source_label}] Read {row_count:,} rows")

    if row_count == 0:
        log.info(f"[{source_label}] No new rows — skipping write")
        return

    writer = df.writeTo(target_fqn).using("iceberg") \
        .tableProperty("write.format.default", "parquet") \
        .tableProperty("write.parquet.compression-codec", "snappy") \
        .tableProperty("write.distribution-mode", "range") \
        .tableProperty("write.merge.mode", "merge-on-read")

    if partition_col and partition_col in df.columns:
        writer = writer.partitionedBy(partition_col)

    if FULL_LOAD:
        log.info(f"[{source_label}] Full load — createOrReplace")
        writer.createOrReplace()
    else:
        try:
            writer.append()
        except Exception:
            log.warning(f"[{source_label}] Table does not exist — creating")
            writer.create()

    log.info(f"[{source_label}] Done → {target_fqn}")


errors = []
for pg_schema, pg_table, tgt_tbl, part_col, bk_key in TABLE_SPECS:
    try:
        ingest_cdc_target(pg_schema, pg_table, tgt_tbl, part_col, bk_key)
    except Exception as exc:
        log.error(f"[{pg_schema}.{pg_table}] FAILED: {exc}", exc_info=True)
        errors.append((f"{pg_schema}.{pg_table}", str(exc)))

job.commit()

if errors:
    msgs = "; ".join(f"{t}: {e}" for t, e in errors)
    raise RuntimeError(f"Ingest-PG-CDC-Targets completed with errors: {msgs}")

log.info("Ingest-PG-CDC-Targets: all tables ingested successfully")
