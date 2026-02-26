"""
================================================================================
AWS Glue Job: Ingest-PG-Reference-Tables
================================================================================

PURPOSE:
    Reads the six PostgreSQL reference tables from FSA-PROD-rds-pg-edv
    (farm_records_reporting schema) and the CRM business-partner table
    (crm_ods.but000) via the Glue JDBC connection and materialises each
    as an Apache Iceberg table in the final-zone S3 bucket.

    Once local to S3, the downstream Transform job can join them with zero
    JDBC round-trips and full Spark parallelism.

TABLES PROCESSED:
    farm_records_reporting.time_period              → farm_ref.time_period
    farm_records_reporting.county_office_control    → farm_ref.county_office_control
    farm_records_reporting.farm                     → farm_ref.farm
    farm_records_reporting.tract                    → farm_ref.tract
    farm_records_reporting.farm_year                → farm_ref.farm_year
    farm_records_reporting.tract_year               → farm_ref.tract_year
    crm_ods.but000                                  → farm_ref.but000

GLUE JOB ARGUMENTS:
    --JOB_NAME          : Glue job name
    --env               : Deployment environment
    --iceberg_warehouse : s3:// URI for Iceberg warehouse root
    --secret_id         : AWS Secrets Manager secret ID holding PG credentials
    --target_database   : Glue catalog database for Iceberg tables (default: farm_ref)
    --debug             : "true" to enable DEBUG-level CloudWatch logging (default: false)

PARTITION STRATEGY:
    time_period            → no partition (small, rarely changes)
    county_office_control  → partitioned by state_fsa_code
    farm                   → partitioned by state_fsa_code
    tract                  → partitioned by state_fsa_code
    farm_year              → partitioned by state_fsa_code
    tract_year             → partitioned by state_fsa_code
    but000                 → no partition

VERSION HISTORY:
    v1.0.0 - 2026-02-25 - Initial implementation (athenafarm project)

================================================================================
"""

import sys
import json
import logging
import traceback
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
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
TARGET_DATABASE   = _opt("target_database", "farm_ref")
DEBUG             = _opt("debug", "false").strip().lower() == "true"

if DEBUG:
    logging.getLogger().setLevel(logging.DEBUG)
    log.setLevel(logging.DEBUG)
    log.debug("DEBUG logging enabled")

sc = SparkContext()
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

spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", ICEBERG_WAREHOUSE)

# ---------------------------------------------------------------------------
# Resolve PostgreSQL credentials from Secrets Manager.
# Build two JDBC URLs — the main EDV database (farm_records_reporting schema)
# and the CRM database (crm_ods schema lives in a separate PG database).
# The Glue Connection attached to this job handles VPC / subnet routing;
# the credentials come from Secrets Manager rather than extract_jdbc_conf so
# the full database name is always present in the JDBC URL.
# ---------------------------------------------------------------------------
_sm     = boto3.client("secretsmanager")
_secret = json.loads(_sm.get_secret_value(SecretId=SECRET_ID)["SecretString"])

PG_HOST = _secret["edv_postgres_hostname"]
PG_PORT = str(_secret["postgres_port"])
PG_DB   = _secret["edv_postgres_database_name"]   # EDV database
PG_USER = _secret["edv_postgres_username"]
PG_PASS = _secret["edv_postgres_password"]

# farm_records_reporting tables live in the main EDV PostgreSQL database
JDBC_URL_MAIN = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}?sslmode=require"
# crm_ods.but000 lives in a separate PostgreSQL database on the same server
JDBC_URL_CRM  = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/CRM_ODS?sslmode=require"

log.info(f"PG host     : {PG_HOST}:{PG_PORT}")
log.info(f"PG main DB  : {PG_DB}")
log.info(f"JDBC main   : {JDBC_URL_MAIN}")
log.info(f"JDBC CRM    : {JDBC_URL_CRM}")
if DEBUG:
    log.debug(f"PG user     : {PG_USER}")

# ---------------------------------------------------------------------------
# Table specs: (pg_schema, pg_table, iceberg_target, partition_col, bookmark_key)
# ---------------------------------------------------------------------------
TABLE_SPECS = [
    # (pg_schema, pg_table, iceberg_target, partition_col)
    ("farm_records_reporting", "time_period",             "time_period",             None),
    ("farm_records_reporting", "county_office_control",   "county_office_control",   "state_fsa_code"),
    ("farm_records_reporting", "farm",                    "farm",                    "state_fsa_code"),
    ("farm_records_reporting", "tract",                   "tract",                   "state_fsa_code"),
    ("farm_records_reporting", "farm_year",               "farm_year",               "state_fsa_code"),
    ("farm_records_reporting", "tract_year",              "tract_year",              "state_fsa_code"),
    ("crm_ods",                "but000",                  "but000",                  None),
]


def ingest_pg_table(pg_schema: str, pg_table: str, target_table: str, partition_col):
    target_fqn = f"glue_catalog.{TARGET_DATABASE}.{target_table}"
    source_label = f"{pg_schema}.{pg_table}"
    log.info(f"[{source_label}] --- START ---")
    log.info(f"[{source_label}] Target FQN  : {target_fqn}")
    log.info(f"[{source_label}] Partition   : {partition_col}")

    jdbc_url = JDBC_URL_CRM if pg_schema.lower() == "crm_ods" else JDBC_URL_MAIN
    log.info(f"[{source_label}] JDBC URL    : {jdbc_url}")

    try:
        df = spark.read.format("jdbc") \
            .option("url",      jdbc_url) \
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

    if row_count == 0:
        log.info(f"[{source_label}] No rows — skipping write")
        return

    writer = df.writeTo(target_fqn).using("iceberg") \
        .tableProperty("write.format.default", "parquet") \
        .tableProperty("write.parquet.compression-codec", "snappy") \
        .tableProperty("write.distribution-mode", "range")

    if partition_col and partition_col in df.columns:
        writer = writer.partitionedBy(partition_col)
    elif partition_col and partition_col not in df.columns:
        log.warning(f"[{source_label}] Partition column '{partition_col}' not found in DataFrame — writing unpartitioned. Columns: {df.columns}")

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
        ingest_pg_table(pg_schema, pg_table, tgt_tbl, part_col)
    except Exception as exc:
        log.error(f"[{pg_schema}.{pg_table}] FAILED: {exc}", exc_info=True)
        errors.append((f"{pg_schema}.{pg_table}", str(exc)))

job.commit()

if errors:
    msgs = "; ".join(f"{t}: {e}" for t, e in errors)
    raise RuntimeError(f"Ingest-PG-Reference-Tables completed with errors: {msgs}")

log.info("Ingest-PG-Reference-Tables: all tables ingested successfully")
