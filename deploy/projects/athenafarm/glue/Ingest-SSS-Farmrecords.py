"""
================================================================================
AWS Glue Job: Ingest-SSS-Farmrecords
================================================================================

PURPOSE:
    Reads the eleven SAP/SSS source tables from the sss-farmrecords Athena
    catalog and re-materialises them as partitioned Apache Iceberg tables in
    the final-zone S3 bucket.  By keeping all SSS data local to S3/Iceberg,
    the downstream Transform job eliminates every federated catalogue hop and
    achieves full Spark-parallel scans with partition pruning.

TABLES PROCESSED (all from sss-farmrecords Athena catalog):
    ibib              - Farm IBase (farm no., state/county, status)
    ibsp              - Tract IBase Component (tract no., admin state/county)
    ibst              - IBase Structure (component → IBase link)
    ibin              - IBase Instance (instance → IBase)
    ibpart            - IBase Partner Segments (segment=2 → farm level)
    crmd_partner      - CRM Partner Roles (ZFARMONR=162 / ZOTNT=163)
    z_ibase_comp_detail  - IBase component detail
    comm_pr_frg_rel   - Product → Fragment relationship
    zmi_farm_partn    - ZMI Farm Partner (exception codes, involve dates)
    zmi_field_excp    - ZMI Field Exceptions (HEL/CW/PCW codes + dates)
    tbl_fr_bp_relationship - Pre-built FR/BP lookup

ADDITIONAL TABLE (from awsdatacatalog.fsa-prod-farm-records):
    fsa_farm_records_farm - farm table from fsa-prod-farm-records Athena catalog
                            (source for Transform-Farm-Producer-Year)

PARTITION / SORT STRATEGY (from arch.md §3.4):
    ibib   → partitioned by ZZFLD000002 (state FSA code), sorted by ZZFLD000003, ibase
    ibsp   → partitioned by admin_state, sorted by admin_county, instance
    all others sorted on their primary join key(s) for data-skip efficiency

GLUE JOB ARGUMENTS:
    --JOB_NAME          : Glue job name
    --env               : Deployment environment (e.g. prod, certdev)
    --iceberg_warehouse : s3:// URI for Iceberg warehouse root
    --source_catalog    : Athena catalog name (default: sss-farmrecords)
    --source_database   : Athena database name (default: sss)
    --target_database   : Glue catalog database for Iceberg tables (default: sss)
    --full_load         : "true" to force full re-load; default incremental via bookmark

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

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Args
# ---------------------------------------------------------------------------
required_args = ["JOB_NAME", "env", "iceberg_warehouse"]
args = getResolvedOptions(sys.argv, required_args)

JOB_NAME         = args["JOB_NAME"]
ENV              = args["env"]
ICEBERG_WAREHOUSE = args["iceberg_warehouse"]
SOURCE_CATALOG   = args.get("source_catalog", "sss-farmrecords")
SOURCE_DATABASE  = args.get("source_database", "sss")
TARGET_DATABASE  = args.get("target_database", "sss")
FULL_LOAD        = args.get("full_load", "false").strip().lower() == "true"

# ---------------------------------------------------------------------------
# Spark / Glue context (Iceberg extensions enabled via --conf in job spec)
# ---------------------------------------------------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, args)

spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", ICEBERG_WAREHOUSE)

# ---------------------------------------------------------------------------
# Table definitions: (source_table, partition_col, sort_cols)
#   partition_col = None → no partitioning; table is small enough for sort-only
# ---------------------------------------------------------------------------
TABLE_SPECS = [
    # (source_table_name, iceberg_target_name, partition_col, sort_cols)
    ("ibib",                  "ibib",                  "ZZFLD000002",  ["ZZFLD000003", "ibase"]),
    ("ibsp",                  "ibsp",                  "admin_state",   ["admin_county", "instance"]),
    ("ibst",                  "ibst",                  None,            ["ibase", "instance"]),
    ("ibin",                  "ibin",                  None,            ["ibase", "instance"]),
    ("ibpart",                "ibpart",                None,            ["segment_recno", "segment"]),
    ("crmd_partner",          "crmd_partner",          None,            ["guid"]),
    ("z_ibase_comp_detail",   "z_ibase_comp_detail",   None,            ["instance", "ibase"]),
    ("comm_pr_frg_rel",       "comm_pr_frg_rel",       None,            ["product_guid"]),
    ("zmi_farm_partn",        "zmi_farm_partn",        None,            ["frg_guid"]),
    ("zmi_field_excp",        "zmi_field_excp",        None,            ["frg_guid"]),
    ("tbl_fr_bp_relationship","tbl_fr_bp_relationship",None,            ["partner_guid"]),
]


def ingest_table(source_table: str, target_table: str, partition_col, sort_cols):
    """Read one SSS table from Athena catalog and write/merge to Iceberg."""
    target_fqn = f"glue_catalog.{TARGET_DATABASE}.{target_table}"
    log.info(f"[{source_table}] Reading from {SOURCE_CATALOG}.{SOURCE_DATABASE}.{source_table}")

    df = spark.read.format("awsdatacatalog").options(
        catalog=SOURCE_CATALOG,
        database=SOURCE_DATABASE,
        tableName=source_table,
    ).load()

    row_count = df.count()
    log.info(f"[{source_table}] Read {row_count:,} rows")

    # Sort within partitions for data-skipping
    if sort_cols:
        df = df.sortWithinPartitions(*[c for c in sort_cols if c in df.columns])

    writer = df.writeTo(target_fqn).using("iceberg") \
        .tableProperty("write.format.default", "parquet") \
        .tableProperty("write.parquet.compression-codec", "snappy") \
        .tableProperty("write.distribution-mode", "range") \
        .tableProperty("write.merge.mode", "merge-on-read")

    if partition_col and partition_col in df.columns:
        writer = writer.partitionedBy(partition_col)

    if FULL_LOAD:
        log.info(f"[{source_table}] Full load — createOrReplace")
        writer.createOrReplace()
    else:
        try:
            log.info(f"[{source_table}] Incremental — appendOrCreate")
            writer.append()
        except Exception:
            log.warning(f"[{source_table}] Table does not exist yet — creating")
            writer.create()

    log.info(f"[{source_table}] Done → {target_fqn}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
errors = []
for src_tbl, tgt_tbl, part_col, srt_cols in TABLE_SPECS:
    try:
        ingest_table(src_tbl, tgt_tbl, part_col, srt_cols)
    except Exception as exc:
        log.error(f"[{src_tbl}] FAILED: {exc}", exc_info=True)
        errors.append((src_tbl, str(exc)))

# ---------------------------------------------------------------------------
# Additional ingest: fsa-prod-farm-records.farm
# This table lives in a different Athena catalog from the sss-farmrecords
# tables above.  It is the source for Transform-Farm-Producer-Year.
# Stored in Iceberg as:  {TARGET_DATABASE}.fsa_farm_records_farm
# (Note: Iceberg table names cannot contain hyphens.)
# ---------------------------------------------------------------------------
FSA_FARM_CATALOG  = "awsdatacatalog"
FSA_FARM_DATABASE = "fsa-prod-farm-records"
FSA_FARM_SOURCE   = "farm"
FSA_FARM_TARGET   = "fsa_farm_records_farm"

try:
    fsa_farm_target_fqn = f"glue_catalog.{TARGET_DATABASE}.{FSA_FARM_TARGET}"
    log.info(f"[{FSA_FARM_SOURCE}] Reading from {FSA_FARM_CATALOG}.{FSA_FARM_DATABASE}.{FSA_FARM_SOURCE}")

    df_fsa = spark.read.format("awsdatacatalog").options(
        catalog=FSA_FARM_CATALOG,
        database=FSA_FARM_DATABASE,
        tableName=FSA_FARM_SOURCE,
    ).load()

    log.info(f"[{FSA_FARM_SOURCE}] Read {df_fsa.count():,} rows")

    writer = df_fsa.writeTo(fsa_farm_target_fqn).using("iceberg") \
        .tableProperty("write.format.default", "parquet") \
        .tableProperty("write.parquet.compression-codec", "snappy") \
        .tableProperty("write.distribution-mode", "range") \
        .tableProperty("write.merge.mode", "merge-on-read")

    # Partition by administrative_state for state-level pruning (arch.md §3.4 strategy)
    if "administrative_state" in df_fsa.columns:
        writer = writer.partitionedBy("administrative_state")

    if FULL_LOAD:
        log.info(f"[{FSA_FARM_SOURCE}] Full load — createOrReplace → {fsa_farm_target_fqn}")
        writer.createOrReplace()
    else:
        try:
            log.info(f"[{FSA_FARM_SOURCE}] Incremental — append → {fsa_farm_target_fqn}")
            writer.append()
        except Exception:
            log.warning(f"[{FSA_FARM_SOURCE}] Table does not exist yet — creating")
            writer.create()

    log.info(f"[{FSA_FARM_SOURCE}] Done → {fsa_farm_target_fqn}")
except Exception as exc:
    log.error(f"[{FSA_FARM_SOURCE}] FAILED: {exc}", exc_info=True)
    errors.append((FSA_FARM_SOURCE, str(exc)))

job.commit()

if errors:
    msgs = "; ".join(f"{t}: {e}" for t, e in errors)
    raise RuntimeError(f"Ingest-SSS-Farmrecords completed with errors: {msgs}")

log.info("Ingest-SSS-Farmrecords: all tables ingested successfully")
