"""
================================================================================
AWS Glue Job: Sync-Iceberg-To-RDS
================================================================================

PURPOSE:
    Reads the delta rows from the Iceberg target tables (tract_producer_year
    and farm_producer_year) that were written or updated in the current pipeline
    run, then upserts them back to the authoritative RDS PostgreSQL instance
    (FSA-PROD-rds-pg-edv) via the Glue JDBC connection.

    Because Iceberg tracks every snapshot, we can use the incremental scan
    (snapshot diff) to fetch only the rows that changed, making this sync
    sub-second for typical incremental runs.

GLUE JOB ARGUMENTS:
    --JOB_NAME            : Glue job name
    --env                 : Deployment environment
    --iceberg_warehouse   : s3:// URI for Iceberg warehouse root
    --connection_name     : Glue connection name for RDS PostgreSQL
    --rds_database        : PostgreSQL database name (default: fpac_farm_records)
    --target_database     : Glue catalog db for Iceberg tables (default: farm_records_reporting)
    --snapshot_id_param   : SSM parameter storing the last-synced Iceberg snapshot ID
    --full_load           : "true" to sync full table (default: incremental delta)

VERSION HISTORY:
    v1.0.0 - 2026-02-25 - Initial implementation (athenafarm project)

================================================================================
"""

import sys
import logging
import boto3
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

required_args = ["JOB_NAME", "env", "iceberg_warehouse", "connection_name"]
args = getResolvedOptions(sys.argv, required_args)

JOB_NAME            = args["JOB_NAME"]
ENV                 = args["env"]
ICEBERG_WAREHOUSE   = args["iceberg_warehouse"]
CONNECTION_NAME     = args["connection_name"]
RDS_DATABASE        = args.get("rds_database", "fpac_farm_records")
TGT_DB              = args.get("target_database", "farm_records_reporting")
SNAPSHOT_SSM_PARAM  = args.get("snapshot_id_param", f"/athenafarm/{ENV}/last_sync_snapshot")
FULL_LOAD           = args.get("full_load", "false").strip().lower() == "true"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, args)

spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", ICEBERG_WAREHOUSE)

ssm = boto3.client("ssm")

# ---------------------------------------------------------------------------
# Table sync specs: (iceberg_table, pg_schema, pg_table, pk_cols)
# ---------------------------------------------------------------------------
SYNC_SPECS = [
    (
        "tract_producer_year",
        "farm_records_reporting",
        "tract_producer_year",
        ["core_customer_identifier", "tract_year_identifier", "producer_involvement_code"],
    ),
    (
        "farm_producer_year",
        "farm_records_reporting",
        "farm_producer_year",
        ["core_customer_identifier", "farm_year_identifier", "producer_involvement_code"],
    ),
]


def get_last_snapshot(table_name: str) -> str | None:
    """Read the last-synced snapshot ID from SSM; return None if not set."""
    param_name = f"{SNAPSHOT_SSM_PARAM}/{table_name}"
    try:
        resp = ssm.get_parameter(Name=param_name)
        return resp["Parameter"]["Value"]
    except ssm.exceptions.ParameterNotFound:
        return None


def put_snapshot(table_name: str, snapshot_id: str):
    """Persist the current snapshot ID to SSM for next run's incremental scan."""
    param_name = f"{SNAPSHOT_SSM_PARAM}/{table_name}"
    ssm.put_parameter(Name=param_name, Value=str(snapshot_id), Type="String", Overwrite=True)
    log.info(f"[{table_name}] Stored snapshot {snapshot_id} → {param_name}")


def sync_table(iceberg_table: str, pg_schema: str, pg_table: str, pk_cols: list):
    iceberg_fqn = f"glue_catalog.{TGT_DB}.{iceberg_table}"
    log.info(f"[{iceberg_table}] Starting sync → {pg_schema}.{pg_table}")

    # ── Determine rows to sync ────────────────────────────────────────────
    if FULL_LOAD:
        log.info(f"[{iceberg_table}] Full sync mode — reading entire table")
        delta_df = spark.read.format("iceberg").load(iceberg_fqn)
    else:
        last_snapshot = get_last_snapshot(iceberg_table)
        if last_snapshot:
            log.info(f"[{iceberg_table}] Incremental scan from snapshot {last_snapshot}")
            delta_df = spark.read.format("iceberg") \
                .option("start-snapshot-id", last_snapshot) \
                .load(f"{iceberg_fqn}#changes")
            # Filter to only INSERTED and UPDATED rows (exclude deletes)
            if "_change_type" in delta_df.columns:
                delta_df = delta_df.filter(F.col("_change_type").isin("INSERT", "UPDATE_AFTER")) \
                                   .drop("_change_type", "_commit_version", "_commit_timestamp")
        else:
            log.info(f"[{iceberg_table}] No previous snapshot found — full sync")
            delta_df = spark.read.format("iceberg").load(iceberg_fqn)

    row_count = delta_df.count()
    log.info(f"[{iceberg_table}] {row_count:,} rows to sync")

    if row_count == 0:
        log.info(f"[{iceberg_table}] No changed rows — skipping JDBC write")
        return

    # ── Upsert delta to RDS via Glue postgresql connection ──────────────────
    # Uses the same connection_type="postgresql" + connectionName pattern as
    # the ingest jobs.  preactions creates a temp staging table; postactions
    # runs INSERT ... ON CONFLICT and drops staging.
    staging_table  = f"_athenafarm_stage_{iceberg_table}"
    target_table   = f"{pg_schema}.{pg_table}"
    all_cols       = delta_df.columns
    pk_cols_str    = ", ".join(pk_cols)
    insert_cols_str = ", ".join(all_cols)
    update_set     = ", ".join([f"{c} = EXCLUDED.{c}" for c in all_cols if c not in pk_cols])

    preactions  = (
        f"DROP TABLE IF EXISTS {staging_table}; "
        f"CREATE TABLE {staging_table} AS SELECT * FROM {target_table} WHERE 1=0"
    )
    postactions = (
        f"INSERT INTO {target_table} ({insert_cols_str}) "
        f"SELECT {insert_cols_str} FROM {staging_table} "
        f"ON CONFLICT ({pk_cols_str}) DO UPDATE SET {update_set}; "
        f"DROP TABLE IF EXISTS {staging_table}"
    )

    dyf = DynamicFrame.fromDF(delta_df, glueContext, f"sync_{iceberg_table}")
    glueContext.write_dynamic_frame.from_options(
        frame=dyf,
        connection_type="postgresql",
        connection_options={
            "useConnectionProperties": "true",
            "connectionName": CONNECTION_NAME,
            "dbtable": staging_table,
            "preactions":  preactions,
            "postactions": postactions,
        },
    )
    log.info(f"[{iceberg_table}] Upsert complete → {target_table}")

    # ── Store current snapshot for next incremental run ───────────────────
    current_snapshot_df = spark.sql(
        f"SELECT snapshot_id FROM glue_catalog.{TGT_DB}.{iceberg_table}.snapshots ORDER BY committed_at DESC LIMIT 1"
    )
    if current_snapshot_df.count() > 0:
        current_snapshot = str(current_snapshot_df.collect()[0]["snapshot_id"])
        put_snapshot(iceberg_table, current_snapshot)


errors = []
for ice_tbl, pg_schema, pg_tbl, pks in SYNC_SPECS:
    try:
        sync_table(ice_tbl, pg_schema, pg_tbl, pks)
    except Exception as exc:
        log.error(f"[{ice_tbl}] FAILED: {exc}", exc_info=True)
        errors.append((ice_tbl, str(exc)))

job.commit()

if errors:
    msgs = "; ".join(f"{t}: {e}" for t, e in errors)
    raise RuntimeError(f"Sync-Iceberg-To-RDS completed with errors: {msgs}")

log.info("Sync-Iceberg-To-RDS: completed successfully")
