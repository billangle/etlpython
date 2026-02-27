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
    --secret_id           : AWS Secrets Manager secret ID holding PG credentials
    --rds_database        : PostgreSQL database name (default: fpac_farm_records)
    --target_database     : Glue catalog db for Iceberg tables (default: farm_records_reporting)
    --snapshot_id_param   : SSM parameter storing the last-synced Iceberg snapshot ID
    --full_load           : "true" to sync full table (default: incremental delta)
    --debug               : "true" to enable DEBUG-level CloudWatch logging (default: false)

VERSION HISTORY:
    v1.0.0 - 2026-02-25 - Initial implementation (athenafarm project)

================================================================================
"""

import sys
import json
import logging
import boto3
import psycopg2
import psycopg2.extras
from awsglue.utils import getResolvedOptions
from pyspark import SparkConf
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

_root_log = logging.getLogger()
if not _root_log.handlers:
    _h = logging.StreamHandler(sys.stderr)
    _h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    _root_log.addHandler(_h)
_root_log.setLevel(logging.INFO)
log = logging.getLogger(__name__)

required_args = ["JOB_NAME", "env", "iceberg_warehouse", "secret_id"]
args = getResolvedOptions(sys.argv, required_args)

JOB_NAME            = args["JOB_NAME"]
ENV                 = args["env"]
ICEBERG_WAREHOUSE   = args["iceberg_warehouse"]
SECRET_ID           = args["secret_id"]
RDS_DATABASE        = args.get("rds_database", "fpac_farm_records")
TGT_DB              = args.get("target_database", "farm_records_reporting")
SNAPSHOT_SSM_PARAM  = args.get("snapshot_id_param", f"/athenafarm/{ENV}/last_sync_snapshot")
FULL_LOAD           = args.get("full_load", "false").strip().lower() == "true"
DEBUG               = args.get("debug", "false").strip().lower() == "true"

if DEBUG:
    logging.getLogger().setLevel(logging.DEBUG)
    log.setLevel(logging.DEBUG)
    log.debug("DEBUG logging enabled")

_conf = SparkConf()
_conf.set("spark.sql.catalog.glue_catalog",              "org.apache.iceberg.spark.SparkCatalog")
_conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
_conf.set("spark.sql.catalog.glue_catalog.io-impl",      "org.apache.iceberg.aws.s3.S3FileIO")
_conf.set("spark.sql.catalog.glue_catalog.warehouse",    ICEBERG_WAREHOUSE)
sc = SparkContext(conf=_conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, args)

log.info("=" * 70)
log.info(f"Job            : {JOB_NAME}")
log.info(f"Env            : {ENV}")
log.info(f"Warehouse      : {ICEBERG_WAREHOUSE}")
log.info(f"Secret ID      : {SECRET_ID}")
log.info(f"Target DB      : {TGT_DB}")
log.info(f"RDS Database   : {RDS_DATABASE}")
log.info(f"Snapshot Param : {SNAPSHOT_SSM_PARAM}")
log.info(f"Full Load      : {FULL_LOAD}")
log.info(f"Debug          : {DEBUG}")
log.info("=" * 70)

# ---------------------------------------------------------------------------
# Resolve PostgreSQL credentials from Secrets Manager (same pattern as the
# ingest jobs — the Glue Connection handles VPC routing, credentials and
# the full database name come from Secrets Manager).
# ---------------------------------------------------------------------------
_sm     = boto3.client("secretsmanager")
_secret = json.loads(_sm.get_secret_value(SecretId=SECRET_ID)["SecretString"])

PG_HOST = _secret["edv_postgres_hostname"]
PG_PORT = int(_secret["postgres_port"])
PG_DB   = _secret["edv_postgres_database_name"]
PG_USER = _secret["edv_postgres_username"]
PG_PASS = _secret["edv_postgres_password"]

log.info(f"PG host     : {PG_HOST}:{PG_PORT}")
log.info(f"PG DB       : {PG_DB}")
if DEBUG:
    log.debug(f"PG user     : {PG_USER}")

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

    # Cache so count() and collect() share one Spark scan.
    delta_df.cache()
    try:
        row_count = delta_df.count()
        log.info(f"[{iceberg_table}] {row_count:,} rows to sync")

        if row_count == 0:
            log.info(f"[{iceberg_table}] No changed rows — skipping write")
            return

        all_cols    = delta_df.columns
        pk_set      = set(pk_cols)
        non_pk_cols = [c for c in all_cols if c not in pk_set]
        pk_cols_str = ", ".join(pk_cols)
        insert_cols_str = ", ".join(all_cols)
        values_placeholder = ", ".join(["%s"] * len(all_cols))
        update_set  = ", ".join([f"{c} = EXCLUDED.{c}" for c in non_pk_cols])

        upsert_sql = (
            f"INSERT INTO {pg_schema}.{pg_table} ({insert_cols_str}) "
            f"VALUES ({values_placeholder}) "
            f"ON CONFLICT ({pk_cols_str}) DO UPDATE SET {update_set}"
        )

        rows = [tuple(row[c] for c in all_cols) for row in delta_df.collect()]
    finally:
        delta_df.unpersist()

    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASS, sslmode="require",
    )
    try:
        with conn:
            with conn.cursor() as cur:
                if FULL_LOAD:
                    # Full load: truncate first so deleted-from-source rows
                    # don't silently persist as stale orphans in RDS.
                    cur.execute(f"TRUNCATE TABLE {pg_schema}.{pg_table}")
                    log.info(f"[{iceberg_table}] Truncated {pg_schema}.{pg_table} for full load")
                psycopg2.extras.execute_batch(cur, upsert_sql, rows, page_size=1000)
        log.info(f"[{iceberg_table}] {'Full load' if FULL_LOAD else 'Upserted'} {row_count:,} rows → {pg_schema}.{pg_table}")
    finally:
        conn.close()

    # ── Store current snapshot for next incremental run ───────────────────
    # Use .first() instead of .count() + .collect() — avoids evaluating the
    # same metadata query twice.
    snap_row = spark.sql(
        f"SELECT snapshot_id FROM glue_catalog.{TGT_DB}.{iceberg_table}.snapshots "
        f"ORDER BY committed_at DESC LIMIT 1"
    ).first()
    if snap_row:
        put_snapshot(iceberg_table, str(snap_row["snapshot_id"]))


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
