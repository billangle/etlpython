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
    --secret_id         : AWS Secrets Manager secret ID holding PG credentials
    --target_database   : Glue catalog database (default: farm_records_reporting)
    --debug             : "true" to enable DEBUG-level CloudWatch logging (default: false)

VERSION HISTORY:
    v1.0.0 - 2026-02-25 - Initial implementation (athenafarm project)

================================================================================
"""

import sys
import json
import logging
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark import SparkConf
from awsglue.context import GlueContext
from awsglue.job import Job

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

required_args = ["JOB_NAME", "env", "iceberg_warehouse", "secret_id"]
args = getResolvedOptions(sys.argv, required_args)


def _opt(key: str, default: str = "") -> str:
    """Resolve an optional Glue job argument from sys.argv.

    getResolvedOptions only returns keys that are in its `keys` list, so
    args.get() on optional params always hits the default.  This helper calls
    getResolvedOptions again for a single optional key so it is properly read
    from DefaultArguments / runtime overrides.
    """
    try:
        return getResolvedOptions(sys.argv, [key])[key]
    except Exception:
        return default


JOB_NAME          = args["JOB_NAME"]
ENV               = args["env"]
ICEBERG_WAREHOUSE = args["iceberg_warehouse"]
SECRET_ID         = args["secret_id"]
TARGET_DATABASE   = _opt("target_database", "farm_records_reporting")
FULL_LOAD         = _opt("full_load", "false").strip().lower() == "true"
DEBUG             = _opt("debug", "false").strip().lower() == "true"

if DEBUG:
    logging.getLogger().setLevel(logging.DEBUG)
    log.setLevel(logging.DEBUG)
    log.debug("DEBUG logging enabled")

_conf = SparkConf()
_conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
_conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
_conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
_conf.set("spark.sql.catalog.glue_catalog.warehouse", ICEBERG_WAREHOUSE)
sc = SparkContext(conf=_conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, args)

log.info("=" * 70)
log.info(f"Job         : {JOB_NAME}")
log.info(f"Env         : {ENV}")
log.info(f"Warehouse   : {ICEBERG_WAREHOUSE}")
log.info(f"Secret ID   : {SECRET_ID}")
log.info(f"Target DB   : {TARGET_DATABASE}")
log.info(f"Debug       : {DEBUG}")
log.info("=" * 70)

if DEBUG:
    safe_args = {k: ("***" if "pass" in k.lower() or "secret" in k.lower() else v)
                 for k, v in args.items()}
    log.debug(f"Resolved args: {safe_args}")

# ---------------------------------------------------------------------------
# Resolve PostgreSQL credentials from Secrets Manager.
# All CDC target tables are in the farm_records_reporting schema of the main
# EDV database.  The Glue Connection handles VPC routing; credentials and the
# full database name come from Secrets Manager.
# ---------------------------------------------------------------------------
_sm     = boto3.client("secretsmanager")
_secret = json.loads(_sm.get_secret_value(SecretId=SECRET_ID)["SecretString"])

PG_HOST    = _secret["edv_postgres_hostname"]
PG_PORT    = str(_secret["postgres_port"])
PG_DB      = _secret["edv_postgres_database_name"]
PG_USER    = _secret["edv_postgres_username"]
PG_PASS    = _secret["edv_postgres_password"]
JDBC_URL   = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}?sslmode=require"

log.info(f"PG host     : {PG_HOST}:{PG_PORT}")
log.info(f"PG DB       : {PG_DB}")
log.info(f"JDBC URL    : {JDBC_URL}")
if DEBUG:
    log.debug(f"PG user     : {PG_USER}")

# ---------------------------------------------------------------------------
# Table specs: (pg_table, iceberg_target, partition_col, bookmark_key)
# ---------------------------------------------------------------------------
TABLE_SPECS = [
    (
        "farm_records_reporting",
        "tract_producer_year",
        "tract_producer_year",
        "state_fsa_code",
    ),
    (
        "farm_records_reporting",
        "farm_producer_year",
        "farm_producer_year",
        "state_fsa_code",
    ),
]


def ingest_cdc_target(pg_schema: str, pg_table: str, tgt_table: str, partition_col: str):
    target_fqn = f"glue_catalog.{TARGET_DATABASE}.{tgt_table}"
    source_label = f"{pg_schema}.{pg_table}"
    log.info(f"[{source_label}] --- START ---")
    log.info(f"[{source_label}] Target FQN  : {target_fqn}")
    log.info(f"[{source_label}] Partition   : {partition_col}")
    log.info(f"[{source_label}] JDBC URL    : {JDBC_URL}")

    try:
        df = spark.read.format("jdbc") \
            .option("url",      JDBC_URL) \
            .option("dbtable",  f"{pg_schema}.{pg_table}") \
            .option("user",     PG_USER) \
            .option("password", PG_PASS) \
            .option("driver",   "org.postgresql.Driver") \
            .option("fetchsize", "10000") \
            .load()
    except Exception:
        log.exception(f"[{source_label}] JDBC READ FAILED — full traceback:")
        raise

    row_count = df.count()
    log.info(f"[{source_label}] Rows read   : {row_count:,}")

    if DEBUG:
        log.debug(f"[{source_label}] Schema:")
        for field in df.schema.fields:
            log.debug(f"  {field.name}: {field.dataType}")

    writer = df.writeTo(target_fqn).using("iceberg") \
        .tableProperty("write.format.default", "parquet") \
        .tableProperty("write.parquet.compression-codec", "snappy") \
        .tableProperty("write.distribution-mode", "range") \
        .tableProperty("write.merge.mode", "merge-on-read")

    if partition_col and partition_col in df.columns:
        writer = writer.partitionedBy(partition_col)
    elif partition_col and partition_col not in df.columns:
        log.warning(f"[{source_label}] Partition column '{partition_col}' not in DataFrame. Columns: {df.columns}")

    log.info(f"[{source_label}] Writing createOrReplace → {target_fqn}")
    try:
        writer.createOrReplace()
    except Exception:
        log.exception(f"[{source_label}] ICEBERG WRITE FAILED — full traceback:")
        raise
    log.info(f"[{source_label}] --- DONE ---")


errors = []
for pg_schema, pg_table, tgt_tbl, part_col in TABLE_SPECS:
    try:
        ingest_cdc_target(pg_schema, pg_table, tgt_tbl, part_col)
    except Exception as exc:
        log.error(f"[{pg_schema}.{pg_table}] FAILED: {exc}", exc_info=True)
        errors.append((f"{pg_schema}.{pg_table}", str(exc)))

job.commit()

if errors:
    msgs = "; ".join(f"{t}: {e}" for t, e in errors)
    raise RuntimeError(f"Ingest-PG-CDC-Targets completed with errors: {msgs}")

log.info("Ingest-PG-CDC-Targets: all tables ingested successfully")
