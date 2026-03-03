"""
================================================================================
AWS Glue Job: Transform-Farm-Producer-Year
================================================================================

PURPOSE:
    Builds farm-producer-year records with a Spark DataFrame pipeline over
    pre-materialised S3 Iceberg sources (zero federated hops), then writes a
    full snapshot to the target Iceberg table in overwrite mode.

    Source farm data is ingested into Iceberg (sss.fsa_farm_records_farm)
    before this transform runs.

    Several output fields remain intentionally NULL because source-to-target
    mapping has not been defined yet.

GLUE JOB ARGUMENTS:
    --JOB_NAME            : Glue job name
    --env                 : Deployment environment
    --iceberg_warehouse   : s3:// URI for Iceberg warehouse root
    --full_load           : accepted for compatibility; job always runs full-load
    --sss_database        : Glue catalog db where fsa_farm_records_farm resides (default: athenafarm_prod_raw)
    --ref_database        : Glue catalog db for PG ref Iceberg tables (default: athenafarm_prod_ref)
    --target_database     : Glue catalog db for target table (default: athenafarm_prod_gold)
    --target_table        : Target Iceberg table name (default: farm_producer_year)
    --debug               : "true" to enable DEBUG-level CloudWatch logging (default: false)

KEY SOURCE TABLE COLUMNS (fsa-prod-farm-records.farm, stored as fsa_farm_records_farm):
    hel_exception, cw_exception, pcw_exception   → farm_producer_*_exception_code
    rma_hel_exceptions, rma_cw_exceptions, rma_pcw_exceptions
    USER_STATUS  → data_status_code (ACTV/INCR/PEND/DRFT/X → A/I/P/D/I)
    crtim, UPTIM → creation_date, last_change_date
    administrative_state, administrative_count → state_fsa_code, county_fsa_code
    farm_number  → joined to farm_records_reporting.farm for farm_identifier

VERSION HISTORY:
    v1.0.0 - 2026-02-25 - Initial implementation (athenafarm project)
              Initial Spark DataFrame transform for farm_producer_year.

================================================================================
"""

import sys
import logging
import traceback
import time
from awsglue.utils import getResolvedOptions
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

_root_log = logging.getLogger()
if not _root_log.handlers:
    _h = logging.StreamHandler(sys.stderr)
    _h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    _root_log.addHandler(_h)
_root_log.setLevel(logging.INFO)
log = logging.getLogger(__name__)

required_args = ["JOB_NAME", "env", "iceberg_warehouse"]
args = getResolvedOptions(sys.argv, required_args)

def _opt(name: str, default: str) -> str:
    flag = f"--{name}"
    try:
        idx = sys.argv.index(flag)
    except ValueError:
        return default
    if idx + 1 >= len(sys.argv):
        return default
    value = sys.argv[idx + 1]
    if value.startswith("--"):
        return default
    return value

def _opt_int(name: str, default: int) -> int:
    value = _opt(name, str(default)).strip()
    try:
        parsed = int(value)
        return parsed if parsed >= 0 else default
    except Exception:
        return default

JOB_NAME          = args["JOB_NAME"]
ENV               = args["env"]
ICEBERG_WAREHOUSE = args["iceberg_warehouse"]
_requested_full_load = _opt("full_load", "true").strip().lower() == "true"
FULL_LOAD         = True
SSS_DB            = _opt("sss_database", "athenafarm_prod_raw")
REF_DB            = _opt("ref_database", "athenafarm_prod_ref")
TGT_DB            = _opt("target_database", "athenafarm_prod_gold")
TGT_TABLE         = _opt("target_table", "farm_producer_year")
SHUFFLE_PARTITIONS = _opt_int("shuffle_partitions", 0)
ADVISORY_PARTITION_SIZE_MB = _opt_int("advisory_partition_size_mb", 128)
DEBUG             = _opt("debug", "false").strip().lower() == "true"

if DEBUG:
    logging.getLogger().setLevel(logging.DEBUG)
    log.setLevel(logging.DEBUG)
    log.debug("DEBUG logging enabled")

if not _requested_full_load:
    log.warning("Incremental mode is disabled for this job; forcing full-load execution")

_conf = SparkConf()
_conf.set("spark.sql.catalog.glue_catalog",              "org.apache.iceberg.spark.SparkCatalog")
_conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
_conf.set("spark.sql.catalog.glue_catalog.io-impl",      "org.apache.iceberg.aws.s3.S3FileIO")
_conf.set("spark.sql.catalog.glue_catalog.warehouse",    ICEBERG_WAREHOUSE)
_conf.set("spark.sql.autoBroadcastJoinThreshold",        str(50 * 1024 * 1024))
_conf.set("spark.sql.adaptive.enabled",                  "true")
_conf.set("spark.sql.adaptive.skewJoin.enabled",         "true")
_conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
_conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
_conf.set(
    "spark.sql.adaptive.advisoryPartitionSizeInBytes",
    str(ADVISORY_PARTITION_SIZE_MB * 1024 * 1024),
)
if SHUFFLE_PARTITIONS > 0:
    _conf.set("spark.sql.shuffle.partitions", str(SHUFFLE_PARTITIONS))
sc = SparkContext(conf=_conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, args)

log.info("=" * 70)

job_t0 = time.perf_counter()
log.info(f"Job            : {JOB_NAME}")
log.info(f"Env            : {ENV}")
log.info(f"Warehouse      : {ICEBERG_WAREHOUSE}")
log.info(f"SSS DB         : {SSS_DB}")
log.info(f"Ref DB         : {REF_DB}")
log.info(f"Target DB      : {TGT_DB}")
log.info(f"Target Table   : {TGT_TABLE}")
if SHUFFLE_PARTITIONS > 0:
    log.info(f"Shuffle Parts  : {SHUFFLE_PARTITIONS} (manual override)")
else:
    log.info("Shuffle Parts  : adaptive (AQE-managed)")
log.info(f"Advisory Part MB: {ADVISORY_PARTITION_SIZE_MB}")
log.info(f"Full Load      : {FULL_LOAD}")
log.info(f"[METRIC] mode={'full_load' if FULL_LOAD else 'incremental'}")
log.info(f"Debug          : {DEBUG}")
log.info("=" * 70)

try:
    farm_producer_year_existing = spark.table(f"glue_catalog.{TGT_DB}.{TGT_TABLE}").select(
        "farm_producer_year_identifier", "farm_year_identifier"
    )
except Exception:
    farm_producer_year_existing = spark.createDataFrame(
        [],
        T.StructType(
            [
                T.StructField("farm_producer_year_identifier", T.LongType(), True),
                T.StructField("farm_year_identifier", T.LongType(), True),
            ]
        ),
    )

log.info("Running farm_producer_year transform (DataFrame API)")
phase_t0 = time.perf_counter()

f = spark.table(f"glue_catalog.{SSS_DB}.fsa_farm_records_farm").alias("f")
coc = spark.table(f"glue_catalog.{REF_DB}.county_office_control").alias("coc")
fr = spark.table(f"glue_catalog.{REF_DB}.farm").alias("fr")
fyr = spark.table(f"glue_catalog.{REF_DB}.farm_year").alias("fyr")

status_expr = (
    F.when(F.col("f.USER_STATUS") == F.lit("ACTV"), F.lit("A"))
    .when(F.col("f.USER_STATUS") == F.lit("INCR"), F.lit("I"))
    .when(F.col("f.USER_STATUS") == F.lit("PEND"), F.lit("P"))
    .when(F.col("f.USER_STATUS") == F.lit("DRFT"), F.lit("D"))
    .when(F.col("f.USER_STATUS") == F.lit("X"), F.lit("I"))
    .otherwise(F.lit("A"))
)

farm_projection = (
    f.join(
        coc,
        F.concat(F.lpad(F.col("f.administrative_state"), 2, "0"), F.lpad(F.col("f.administrative_count"), 3, "0"))
        == F.concat(F.col("coc.state_fsa_code"), F.col("coc.county_fsa_code")),
        "inner",
    )
    .join(
        fr,
        F.concat(F.col("f.farm_number").cast("string"), F.col("coc.county_office_control_identifier").cast("string"))
        == F.concat(F.col("fr.farm_number").cast("string"), F.col("fr.county_office_control_identifier").cast("string")),
        "inner",
    )
    .join(fyr, F.col("fyr.farm_identifier") == F.col("fr.farm_identifier"), "inner")
    .select(
        F.lit(None).cast("bigint").alias("core_customer_identifier"),
        F.col("fyr.farm_year_identifier").alias("farm_year_identifier"),
        F.lit(None).cast("int").alias("producer_involvement_code"),
        F.lit(None).cast("string").alias("producer_involvement_interrupted_indicator"),
        F.lit(None).cast("date").alias("producer_involvement_start_date"),
        F.lit(None).cast("date").alias("producer_involvement_end_date"),
        F.col("f.hel_exception").cast("int").alias("farm_producer_hel_exception_code"),
        F.col("f.cw_exception").cast("int").alias("farm_producer_cw_exception_code"),
        F.col("f.pcw_exception").cast("int").alias("farm_producer_pcw_exception_code"),
        status_expr.alias("data_status_code"),
        F.when(F.col("f.crtim").isNotNull(), F.to_date(F.date_format(F.col("f.crtim"), "yyyy-MM-dd"))).otherwise(
            F.to_date(F.lit("9999-12-31"))
        ).alias("creation_date"),
        F.when(F.col("f.UPTIM").isNotNull(), F.to_date(F.date_format(F.col("f.UPTIM"), "yyyy-MM-dd"))).otherwise(
            F.to_date(F.lit("9999-12-31"))
        ).alias("last_change_date"),
        F.coalesce(F.col("f.UPNAM"), F.col("f.crnam")).alias("last_change_user_name"),
        (F.year(F.col("f.crtim")) - F.lit(1998)).alias("time_period_identifier"),
        F.lpad(F.col("f.administrative_state"), 2, "0").alias("state_fsa_code"),
        F.lpad(F.col("f.administrative_count"), 3, "0").alias("county_fsa_code"),
        F.col("fr.farm_identifier").alias("farm_identifier"),
        F.col("f.farm_number").alias("farm_number"),
        F.col("f.hel_appeals_exhausted_date").alias("hel_appeals_exhausted_date"),
        F.col("f.cw_appeals_exhausted_date").alias("cw_appeals_exhausted_date"),
        F.col("f.pcw_appeals_exhausted_date").alias("pcw_appeals_exhausted_date"),
        F.col("f.rma_hel_exceptions").cast("int").alias("farm_producer_rma_hel_exception_code"),
        F.col("f.rma_cw_exceptions").cast("int").alias("farm_producer_rma_cw_exception_code"),
        F.col("f.rma_pcw_exceptions").cast("int").alias("farm_producer_rma_pcw_exception_code"),
    )
)

source_df = (
    farm_projection.alias("fpy")
    .join(
        farm_producer_year_existing.alias("fpyr"),
        F.col("fpy.farm_year_identifier") == F.col("fpyr.farm_year_identifier"),
        "left",
    )
    .select(
        F.col("fpyr.farm_producer_year_identifier").alias("farm_producer_year_identifier"),
        F.col("fpy.core_customer_identifier").alias("core_customer_identifier"),
        F.col("fpy.farm_year_identifier").alias("farm_year_identifier"),
        F.col("fpy.producer_involvement_code").alias("producer_involvement_code"),
        F.col("fpy.producer_involvement_interrupted_indicator").alias("producer_involvement_interrupted_indicator"),
        F.col("fpy.producer_involvement_start_date").alias("producer_involvement_start_date"),
        F.col("fpy.producer_involvement_end_date").alias("producer_involvement_end_date"),
        F.col("fpy.farm_producer_hel_exception_code").alias("farm_producer_hel_exception_code"),
        F.col("fpy.farm_producer_cw_exception_code").alias("farm_producer_cw_exception_code"),
        F.col("fpy.farm_producer_pcw_exception_code").alias("farm_producer_pcw_exception_code"),
        F.col("fpy.data_status_code").alias("data_status_code"),
        F.col("fpy.creation_date").alias("creation_date"),
        F.col("fpy.last_change_date").alias("last_change_date"),
        F.col("fpy.last_change_user_name").alias("last_change_user_name"),
        F.col("fpy.time_period_identifier").alias("time_period_identifier"),
        F.col("fpy.state_fsa_code").alias("state_fsa_code"),
        F.col("fpy.county_fsa_code").alias("county_fsa_code"),
        F.col("fpy.farm_identifier").alias("farm_identifier"),
        F.col("fpy.farm_number").alias("farm_number"),
        F.col("fpy.hel_appeals_exhausted_date").alias("hel_appeals_exhausted_date"),
        F.col("fpy.cw_appeals_exhausted_date").alias("cw_appeals_exhausted_date"),
        F.col("fpy.pcw_appeals_exhausted_date").alias("pcw_appeals_exhausted_date"),
        F.col("fpy.farm_producer_rma_hel_exception_code").alias("farm_producer_rma_hel_exception_code"),
        F.col("fpy.farm_producer_rma_cw_exception_code").alias("farm_producer_rma_cw_exception_code"),
        F.col("fpy.farm_producer_rma_pcw_exception_code").alias("farm_producer_rma_pcw_exception_code"),
    )
)

log.info("Transform DataFrame build complete")
log.info(f"[METRIC] phase_transform_seconds={time.perf_counter() - phase_t0:.3f}")

TARGET_FQN = f"glue_catalog.{TGT_DB}.{TGT_TABLE}"

# First-run safety with DataFrame API: create table only if missing.
source_df.limit(0).write.format("iceberg").mode("ignore").saveAsTable(TARGET_FQN)

write_t0 = time.perf_counter()
log.info(f"Executing full-load overwrite for {TARGET_FQN} (DataFrameWriter + Iceberg)")
source_df.write.format("iceberg").mode("overwrite").saveAsTable(TARGET_FQN)
log.info(f"[METRIC] phase_write_seconds={time.perf_counter() - write_t0:.3f}")

try:
    snap_t0 = time.perf_counter()
    snapshots_df = spark.table(f"glue_catalog.{TGT_DB}.{TGT_TABLE}.snapshots")
    snap_row = snapshots_df.orderBy(snapshots_df.committed_at.desc()).select("summary").first()
    summary = snap_row["summary"] if snap_row else None
    final_count = int(summary.get("total-records", -1)) if summary else -1
except Exception:
    final_count = -1
log.info(f"[METRIC] phase_snapshot_metric_seconds={time.perf_counter() - snap_t0:.3f}")
log.info(f"[METRIC] {TGT_TABLE}_row_count={final_count}")

job.commit()
log.info(f"[METRIC] total_job_seconds={time.perf_counter() - job_t0:.3f}")
log.info("Transform-Farm-Producer-Year: completed successfully")
