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
    --source_database   : Glue catalog database for SSS source tables (default: sss-farmrecords)
    --target_database   : Glue catalog database for Iceberg targets (default: sss)
    --full_load         : "true" to force full re-load; default incremental via bookmark
    --debug             : "true" to enable DEBUG-level CloudWatch logging (default: false)

VERSION HISTORY:
    v1.0.0 - 2026-02-25 - Initial implementation (athenafarm project)

================================================================================
"""

import sys
import logging
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark import SparkConf
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
SOURCE_CATALOG    = _opt("source_catalog", "sss-farmrecords")
SOURCE_DATABASE   = _opt("source_database", "sss-farmrecords")
TARGET_DATABASE   = _opt("target_database", "sss")
FULL_LOAD         = _opt("full_load", "false").strip().lower() == "true"
DEBUG             = _opt("debug", "false").strip().lower() == "true"

if DEBUG:
    logging.getLogger().setLevel(logging.DEBUG)
    log.setLevel(logging.DEBUG)
    log.debug("DEBUG logging enabled")

# ---------------------------------------------------------------------------
# Spark / Glue context — Iceberg catalog hardcoded in SparkConf so it is
# always registered regardless of DefaultArguments
# ---------------------------------------------------------------------------
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
log.info(f"Job            : {JOB_NAME}")
log.info(f"Env            : {ENV}")
log.info(f"Warehouse      : {ICEBERG_WAREHOUSE}")
log.info(f"Source catalog : {SOURCE_CATALOG}")
log.info(f"Source DB      : {SOURCE_DATABASE}")
log.info(f"Target DB      : {TARGET_DATABASE}")
log.info(f"Full load      : {FULL_LOAD}")
log.info(f"Debug          : {DEBUG}")
log.info("=" * 70)

# List all tables in the source database so any missing-table error is
# immediately diagnosable from CloudWatch without needing to check the console.
available_tables: list = []
try:
    available_tables = [t.name for t in spark.catalog.listTables(SOURCE_DATABASE)]
    log.info(f"Tables in '{SOURCE_DATABASE}': {sorted(available_tables)}")
except Exception:
    log.exception(f"WARNING: could not list tables in catalog database '{SOURCE_DATABASE}' — full traceback:")
    log.warning("available_tables unknown — will attempt all TABLE_SPECS regardless")

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
    log.info(f"[{source_table}] --- START ---")
    log.info(f"[{source_table}] Source DB   : {SOURCE_DATABASE}")
    log.info(f"[{source_table}] Target FQN  : {target_fqn}")
    log.info(f"[{source_table}] Partition   : {partition_col}  Sort: {sort_cols}")

    try:
        dyf = glueContext.create_dynamic_frame.from_catalog(
            database=SOURCE_DATABASE,
            table_name=source_table,
        )
    except Exception:
        log.exception(f"[{source_table}] CATALOG READ FAILED — full traceback:")
        raise

    df = dyf.toDF()
    row_count = df.count()
    log.info(f"[{source_table}] Rows read   : {row_count:,}")

    if DEBUG:
        log.debug(f"[{source_table}] Schema:")
        for field in df.schema.fields:
            log.debug(f"  {field.name}: {field.dataType}")

    # Sort within partitions for data-skipping
    if sort_cols:
        valid_sort = [c for c in sort_cols if c in df.columns]
        if valid_sort:
            df = df.sortWithinPartitions(*valid_sort)
        else:
            log.warning(
                f"[{source_table}] SKIP sort — none of {sort_cols} found in actual columns: "
                f"{sorted(df.columns)}"
            )

    writer = df.writeTo(target_fqn).using("iceberg") \
        .tableProperty("write.format.default", "parquet") \
        .tableProperty("write.parquet.compression-codec", "snappy") \
        .tableProperty("write.distribution-mode", "none") \
        .tableProperty("write.spark.fanout.enabled", "true") \
        .tableProperty("write.merge.mode", "merge-on-read")

    if partition_col and partition_col in df.columns:
        writer = writer.partitionedBy(partition_col)
    elif partition_col and partition_col not in df.columns:
        log.warning(f"[{source_table}] Partition column '{partition_col}' not in DataFrame. Columns: {df.columns}")

    if FULL_LOAD:
        log.info(f"[{source_table}] Writing createOrReplace → {target_fqn}")
        try:
            writer.createOrReplace()
        except Exception:
            log.exception(f"[{source_table}] ICEBERG WRITE (createOrReplace) FAILED — full traceback:")
            raise
    else:
        try:
            log.info(f"[{source_table}] Writing append → {target_fqn}")
            writer.append()
        except Exception:
            log.warning(f"[{source_table}] append failed (table may not exist yet) — retrying with create")
            try:
                writer.create()
            except Exception:
                log.exception(f"[{source_table}] ICEBERG WRITE (create) FAILED — full traceback:")
                raise

    log.info(f"[{source_table}] --- DONE ---")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
errors = []
for src_tbl, tgt_tbl, part_col, srt_cols in TABLE_SPECS:
    # Skip tables that are confirmed absent from the Glue catalog — they may
    # not have been crawled yet.  When available_tables is empty (discovery
    # failed) we still attempt every table so we don't silently skip everything.
    if available_tables and src_tbl not in available_tables:
        log.warning(f"[{src_tbl}] NOT FOUND in '{SOURCE_DATABASE}' catalog — skipping (not crawled yet)")
        continue
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
    log.info(f"[{FSA_FARM_SOURCE}] Reading from Glue catalog database '{FSA_FARM_DATABASE}' table '{FSA_FARM_SOURCE}'")

    dyf_fsa = glueContext.create_dynamic_frame.from_catalog(
        database=FSA_FARM_DATABASE,
        table_name=FSA_FARM_SOURCE,
    )
    df_fsa = dyf_fsa.toDF()

    log.info(f"[{FSA_FARM_SOURCE}] Read {df_fsa.count():,} rows")

    writer = df_fsa.writeTo(fsa_farm_target_fqn).using("iceberg") \
        .tableProperty("write.format.default", "parquet") \
        .tableProperty("write.parquet.compression-codec", "snappy") \
        .tableProperty("write.distribution-mode", "none") \
        .tableProperty("write.spark.fanout.enabled", "true") \
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
