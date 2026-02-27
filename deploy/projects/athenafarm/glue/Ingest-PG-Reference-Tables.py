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
from pyspark import SparkConf
from awsglue.context import GlueContext
from awsglue.job import Job

# ---------------------------------------------------------------------------
# Logging — explicit StreamHandler so output reaches CloudWatch regardless
# of whether Glue's runtime has already initialised the root logger.
# Glue captures sys.stderr → /aws-glue/jobs/error log stream in CloudWatch.
# ---------------------------------------------------------------------------
_root_log = logging.getLogger()
if not _root_log.handlers:
    _handler = logging.StreamHandler(sys.stderr)
    _handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    _root_log.addHandler(_handler)
_root_log.setLevel(logging.INFO)
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

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

_conf = SparkConf()
_conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
_conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
_conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
_conf.set("spark.sql.catalog.glue_catalog.warehouse", ICEBERG_WAREHOUSE)
# Force Iceberg FanoutWriter for all writes via this catalog.  The default
# ClusteredWriter requires incoming records to be physically sorted by
# partition spec — JDBC reads do not guarantee this ordering, so partitioned
# createOrReplace calls repeatedly fail with:
#   "Incoming records violate the writer assumption that records are clustered
#    by spec and by partition within each spec."
# Setting fanout at the SparkConf level ensures it applies to createOrReplace
# as well as append/create, regardless of per-table write properties.
_conf.set("spark.sql.catalog.glue_catalog.write.spark.fanout.enabled", "true")
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
    # (pg_schema, pg_table, iceberg_target, partition_col, pk_col)
    # pk_col = sequential integer PK column used for lowerBound/upperBound range
    #   partitioning via the PK index (numPartitions=20 parallel JDBC tasks).
    # pk_col = None  → single-partition read (small table or non-numeric PK).
    ("farm_records_reporting", "time_period",           "time_period",           None,              None),                             # ~30 rows
    ("farm_records_reporting", "county_office_control", "county_office_control", "state_fsa_code",  "county_office_control_identifier"),
    ("farm_records_reporting", "farm",                  "farm",                  "state_fsa_code",  "farm_identifier"),
    ("farm_records_reporting", "tract",                 "tract",                 "state_fsa_code",  "tract_identifier"),
    ("farm_records_reporting", "farm_year",             "farm_year",             "state_fsa_code",  "farm_year_identifier"),
    ("farm_records_reporting", "tract_year",            "tract_year",            "state_fsa_code",  "tract_year_identifier"),
    ("crm_ods",                "but000",                "but000",                None,              None),                             # partner_guid is a UUID string
]


def ingest_pg_table(pg_schema: str, pg_table: str, target_table: str, partition_col, pk_col: str):
    target_fqn = f"glue_catalog.{TARGET_DATABASE}.{target_table}"
    source_label = f"{pg_schema}.{pg_table}"
    log.info(f"[{source_label}] --- START ---")
    log.info(f"[{source_label}] Target FQN  : {target_fqn}")
    log.info(f"[{source_label}] Partition   : {partition_col}")
    log.info(f"[{source_label}] PK col      : {pk_col}")

    jdbc_url = JDBC_URL_CRM if pg_schema.lower() == "crm_ods" else JDBC_URL_MAIN
    log.info(f"[{source_label}] JDBC URL    : {jdbc_url}")

    # JDBC connection properties shared by all reads.
    _jdbc_props = {
        "user":           PG_USER,
        "password":       PG_PASS,
        "driver":         "org.postgresql.Driver",
        "fetchsize":      "50000",
        "connectTimeout": "120",
        "socketTimeout":  "600",
    }

    try:
        if pk_col:
            # Parallel read: split [MIN(pk), MAX(pk)] into numPartitions ranges.
            # Uses the PK btree index — guaranteed fast regardless of whether
            # secondary indexes (e.g. on state_fsa_code) exist in PostgreSQL.
            _NUM_PARTITIONS = 20
            bounds_row = spark.read.jdbc(
                url=jdbc_url,
                table=f"(SELECT MIN({pk_col}) AS lo, MAX({pk_col}) AS hi FROM {pg_schema}.{pg_table}) q",
                properties=_jdbc_props,
            ).collect()[0]
            lo = int(bounds_row["lo"] or 0)
            hi = int(bounds_row["hi"] or 0)
            log.info(f"[{source_label}] PK range {lo:,} – {hi:,}, numPartitions={_NUM_PARTITIONS}")

            if hi == 0:
                log.info(f"[{source_label}] Table is empty — skipping")
                return

            df = spark.read.jdbc(
                url=jdbc_url,
                table=f"{pg_schema}.{pg_table}",
                column=pk_col,
                lowerBound=lo,
                upperBound=hi + 1,   # +1 so last row falls inside last partition
                numPartitions=_NUM_PARTITIONS,
                properties=_jdbc_props,
            )
        else:
            # Single-partition read — acceptable for small tables (time_period
            # has ~30 rows; but000 uses a UUID PK incompatible with numeric ranges).
            log.info(f"[{source_label}] Single-partition JDBC read (small table or non-numeric PK)")
            df = spark.read.jdbc(
                url=jdbc_url,
                table=f"{pg_schema}.{pg_table}",
                properties=_jdbc_props,
            )
    except Exception:
        log.exception(f"[{source_label}] JDBC READ FAILED — full traceback:")
        raise

    # Cache before count so Spark does not re-execute the JDBC query a second
    # time during the Iceberg write.  Without cache(), df.count() triggers a
    # full table fetch over JDBC, then createOrReplace() triggers another
    # full fetch — doubling runtime for every large table (e.g. tract_year).
    df.cache()

    row_count = df.count()
    log.info(f"[{source_label}] Rows read   : {row_count:,}")

    if DEBUG:
        log.debug(f"[{source_label}] Schema:")
        for field in df.schema.fields:
            log.debug(f"  {field.name}: {field.dataType}")

    if row_count == 0:
        log.info(f"[{source_label}] No rows — skipping write")
        df.unpersist()
        return

    # Hold a reference to the cached JDBC df so we can unpersist it after
    # the repartition rebinds the local `df` variable to a new DataFrame.
    df_cached = df

    try:
        # Sort by partition column before writing.  Even though fanout mode is
        # enabled at the catalog level, sorting ensures optimal data layout and
        # avoids the ClusteredWriter error if fanout is ever not picked up:
        #   "Incoming records violate the writer assumption that records are
        #    clustered by spec and by partition within each spec."
        # repartition() colocates all rows with the same partition value into
        # the same Spark task; sortWithinPartitions() then orders within each task.
        if partition_col and partition_col in df.columns:
            log.info(f"[{source_label}] Repartitioning + sorting by '{partition_col}' for Iceberg write ordering")
            df = df.repartition(partition_col).sortWithinPartitions(partition_col)
        elif partition_col:
            log.warning(f"[{source_label}] Partition column '{partition_col}' not in DataFrame — skipping sort. Columns: {df.columns}")

        writer = df.writeTo(target_fqn).using("iceberg") \
            .tableProperty("write.format.default", "parquet") \
            .tableProperty("write.parquet.compression-codec", "snappy") \
            .tableProperty("write.distribution-mode", "none") \
            .tableProperty("write.spark.fanout.enabled", "true")

        if partition_col and partition_col in df.columns:
            writer = writer.partitionedBy(partition_col)

        log.info(f"[{source_label}] Writing createOrReplace → {target_fqn}")
        try:
            writer.createOrReplace()
        except Exception:
            log.exception(f"[{source_label}] ICEBERG WRITE FAILED — full traceback:")
            raise
        log.info(f"[{source_label}] --- DONE ---")
    finally:
        df_cached.unpersist()


errors = []
for pg_schema, pg_table, tgt_tbl, part_col, pk_col in TABLE_SPECS:
    try:
        ingest_pg_table(pg_schema, pg_table, tgt_tbl, part_col, pk_col)
    except Exception as exc:
        log.error(f"[{pg_schema}.{pg_table}] FAILED: {exc}", exc_info=True)
        errors.append((f"{pg_schema}.{pg_table}", str(exc)))

job.commit()

if errors:
    msgs = "; ".join(f"{t}: {e}" for t, e in errors)
    raise RuntimeError(f"Ingest-PG-Reference-Tables completed with errors: {msgs}")

log.info("Ingest-PG-Reference-Tables: all tables ingested successfully")
