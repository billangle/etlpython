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
    --full_load         : "true" to force full re-load (default: incremental)

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
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

required_args = ["JOB_NAME", "env", "iceberg_warehouse", "secret_id"]
args = getResolvedOptions(sys.argv, required_args)

JOB_NAME          = args["JOB_NAME"]
ENV               = args["env"]
ICEBERG_WAREHOUSE = args["iceberg_warehouse"]
SECRET_ID         = args["secret_id"]
TARGET_DATABASE   = args.get("target_database", "farm_ref")
FULL_LOAD         = args.get("full_load", "false").strip().lower() == "true"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, args)

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
    log.info(f"[{source_label}] Reading via Secrets Manager JDBC credentials")

    # crm_ods tables live in a separate PostgreSQL database (CRM_ODS);
    # everything else is in the main EDV database.
    jdbc_url = JDBC_URL_CRM if pg_schema.lower() == "crm_ods" else JDBC_URL_MAIN

    df = spark.read.format("jdbc") \
        .option("url",      jdbc_url) \
        .option("dbtable",  f"{pg_schema}.{pg_table}") \
        .option("user",     PG_USER) \
        .option("password", PG_PASS) \
        .option("driver",   "org.postgresql.Driver") \
        .option("fetchsize", "10000") \
        .load()
    row_count = df.count()
    log.info(f"[{source_label}] Read {row_count:,} rows")

    if row_count == 0:
        log.info(f"[{source_label}] No new rows — skipping write")
        return

    writer = df.writeTo(target_fqn).using("iceberg") \
        .tableProperty("write.format.default", "parquet") \
        .tableProperty("write.parquet.compression-codec", "snappy") \
        .tableProperty("write.distribution-mode", "range")

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
