"""
================================================================================
AWS Glue Job: Transform-Tract-Producer-Year
================================================================================

PURPOSE:
        Builds tract_producer_year in Step Functions-manageable modes.

                29-step optimized flow:
            1) preprocess_spine                : ibsp projection + normalization
            2) preprocess_structure_link       : spine + ibst
            3) preprocess_structure_farm_filter: structure_link key-filter + ibib projection
            4) preprocess_structure_farm_b0    : structure_farm bucket 0 write
            5) preprocess_structure_farm_b1    : structure_farm bucket 1 append
            6) preprocess_structure_farm_b2    : structure_farm bucket 2 append
            7) preprocess_structure_farm_b3_s0 : structure_farm bucket 3 shard 0 write
            8) preprocess_structure_farm_b3_s1 : structure_farm bucket 3 shard 1 append
            9) preprocess_structure_farm_b3_s2 : structure_farm bucket 3 shard 2 append
            10) preprocess_structure_farm_b3_s3: structure_farm bucket 3 shard 3 append
            11) preprocess_structure_farm_b4   : structure_farm bucket 4 append
            12) preprocess_structure_farm_b5   : structure_farm bucket 5 append
            13) preprocess_structure_farm_b6   : structure_farm bucket 6 append
            14) preprocess_structure_farm_b7   : structure_farm bucket 7 append
            15) preprocess_structure_farm_b8   : structure_farm bucket 8 append
            16) preprocess_structure_farm_b9   : structure_farm bucket 9 append
            17) preprocess_structure_farm_b10  : structure_farm bucket 10 append
            18) preprocess_structure_farm_b11  : structure_farm bucket 11 append
            19) preprocess_structure_farm_b12  : structure_farm bucket 12 append
            20) preprocess_structure_farm_b13  : structure_farm bucket 13 append
            21) preprocess_structure_farm_b14  : structure_farm bucket 14 append
            22) preprocess_structure_farm_b15  : structure_farm bucket 15 append
            23) preprocess_structure_merge     : merge structure_farm bucket tables
            24) preprocess_core2_extract       : ibin key projection/materialization
            25) preprocess_core2               : structure_farm + core2_extract
            26) preprocess_partner             : core2 + ibpart + crmd_partner
            27) preprocess_enrich              : partner + zmi + time/coc/but000
            28) finalize_resolve               : enriched stage + farm/tract/farm_year/tract_year
            29) finalize_publish               : write final target snapshot

        Compatibility wrappers:
        - preprocess_base   : runs steps 1..26
        - preprocess        : runs steps 1..27
        - finalize          : runs steps 28+29
        - single            : runs all 29 steps

GLUE JOB ARGUMENTS:
    --JOB_NAME            : Glue job name
    --env                 : Deployment environment
    --iceberg_warehouse   : s3:// URI for Iceberg warehouse root
    --task_mode           : single | preprocess | preprocess_base |
                            single_fast | single_pass |
                            preprocess_spine | preprocess_structure_link |
                            preprocess_structure_farm_filter | preprocess_structure_farm |
                            preprocess_structure_farm_b0 | preprocess_structure_farm_b1 |
                            preprocess_structure_farm_b2 |
                            preprocess_structure_farm_b3 | preprocess_structure_farm_b3_s0 |
                            preprocess_structure_farm_b3_s1 | preprocess_structure_farm_b3_s2 |
                            preprocess_structure_farm_b3_s3 |
                            preprocess_structure_farm_b4 | preprocess_structure_farm_b5 |
                            preprocess_structure_farm_b6 | preprocess_structure_farm_b7 |
                            preprocess_structure_farm_b8 | preprocess_structure_farm_b9 |
                            preprocess_structure_farm_b10 | preprocess_structure_farm_b11 |
                            preprocess_structure_farm_b12 | preprocess_structure_farm_b13 |
                            preprocess_structure_farm_b14 | preprocess_structure_farm_b15 |
                            preprocess_structure_merge |
                            preprocess_core2_extract |
                            preprocess_core2 |
                            preprocess_partner | preprocess_enrich |
                            finalize_resolve | finalize_publish | finalize
    --stage_spine_table   : Stage table for step 1 (default: tract_producer_year_stage_spine)
    --stage_core1_table   : Stage table for step 2 (default: tract_producer_year_stage_core1)
    --stage_structure_filter_table : Stage table for step 3 (default: tract_producer_year_stage_structure_filter)
    --stage_structure_table : Stage table for step 20 consolidated output (default: tract_producer_year_stage_structure)
    --stage_core2_source_table : Stage table for step 21 (default: tract_producer_year_stage_core2_source)
    --stage_core2_table   : Stage table for step 22 (default: tract_producer_year_stage_core2)
    --stage_base_table    : Stage table for step 23 (default: tract_producer_year_stage_base)
    --stage_table         : Stage table for step 24 (default: tract_producer_year_stage)
    --stage_resolve_table : Stage table for step 25 (default: tract_producer_year_stage_resolve)
    --full_load           : accepted for compatibility; job always full-load
    --sss_database        : SSS Iceberg db (default: athenafarm_prod_raw)
    --ref_database        : reference Iceberg db (default: athenafarm_prod_ref)
    --target_database     : target Iceberg db (default: athenafarm_prod_gold)
    --target_table        : target Iceberg table (default: tract_producer_year)
    --shuffle_partitions  : Spark shuffle partitions (default: 800)
    --max_job_seconds     : fail-fast job cap seconds (default: 1100)
    --debug               : DEBUG logging flag (default: false)

VERSION HISTORY:
    v4.4.2 - 2026-03-06 - Add minimal progress markers for single_fast milestones across all environments.
    v4.4.1 - 2026-03-06 - Move static Spark resiliency configs to SparkConf initialization to avoid runtime AnalysisException on immutable keys.
    v4.4.0 - 2026-03-06 - Remove legacy staged runtime dispatch; enforce single-pass execution modes only (single_fast/single_pass, with single alias).
    v4.3.0 - 2026-03-06 - Make single_fast the script default mode and add single_pass alias for explicit single-pass usage.
    v4.2.0 - 2026-03-06 - Add large-data single_fast optimizations: AQE tuning, targeted repartitioning, and lineage checkpoints.
    v4.1.0 - 2026-03-06 - Tune core2_extract by key-filtering ibin with structure keys to reduce full-scan/shuffle cost.
    v4.0.0 - 2026-03-06 - Add architecture-aligned single_fast path: single-pass in-memory transform with direct publish.
    v3.12.0 - 2026-03-05 - Split hot structure bucket b3 into 4 shard stages (b3_s0..b3_s3) to resolve phase-7 timeout.
    v3.11.0 - 2026-03-05 - Isolate structure_farm writes into per-bucket stage tables and add dedicated structure merge stage.
    v3.10.0 - 2026-03-05 - Reworked structure_farm bucket SQL to bucket-filter core1 first, then key-filter ibf per bucket before join.
    v3.9.0 - 2026-03-05 - Use salted composite-key bucketing for structure_farm buckets to reduce hot-key skew in phase-4.
    v3.8.0 - 2026-03-05 - Split structure_farm into 16 hash buckets (b0-b15) to further reduce phase-4 runtime.
    v3.7.0 - 2026-03-05 - Split structure_farm into 8 hash buckets (b0-b7) to further reduce phase-4 runtime.
    v3.6.0 - 2026-03-05 - Split structure_farm into 4 hash buckets (b0-b3) to reduce phase-4 runtime.
    v3.5.0 - 2026-03-05 - Split structure_farm into even/odd bucket stages plus merge to reduce phase-4 runtime.
    v3.4.0 - 2026-03-05 - Split structure_farm into filter+join stages to bring phase-3 under time window.
    v3.3.0 - 2026-03-04 - Split core2 into extract+join stages to reduce long phase-3 runtime.
    v3.2.0 - 2026-03-04 - Split structure preprocessing into link/farm stages to shrink phase-2 runtime window.
    v3.1.1 - 2026-03-04 - Remove sys.exit-based early completion; use explicit mode dispatch so stage-only runs complete as SUCCESS in Glue.
    v3.1.0 - 2026-03-04 - Split heavy preprocessing further into 7 stages and enforce sub-20-minute orchestration profile.
    v3.0.0 - 2026-03-04 - Split tract flow into 6 stages to address >37 minute phase-1 runtime.
    v2.2.0 - 2026-03-04 - Split preprocess_base into preprocess_base_core + preprocess_base_partner.
    v2.1.0 - 2026-03-04 - Split tract flow into three Step Functions-manageable jobs.

================================================================================
"""

import sys
import logging
import time
from awsglue.utils import getResolvedOptions
from pyspark import SparkConf
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


JOB_NAME          = args["JOB_NAME"]
ENV               = args["env"]
ICEBERG_WAREHOUSE = args["iceberg_warehouse"]
_requested_full_load = _opt("full_load", "true").strip().lower() == "true"
FULL_LOAD         = True
SSS_DB            = _opt("sss_database", "athenafarm_prod_raw")
REF_DB            = _opt("ref_database", "athenafarm_prod_ref")
TGT_DB            = _opt("target_database", "athenafarm_prod_gold")
TGT_TABLE         = _opt("target_table", "tract_producer_year")
STAGE_SPINE_TABLE = _opt("stage_spine_table", "tract_producer_year_stage_spine")
STAGE_CORE1_TABLE = _opt("stage_core1_table", "tract_producer_year_stage_core1")
STAGE_STRUCTURE_FILTER_TABLE = _opt("stage_structure_filter_table", "tract_producer_year_stage_structure_filter")
STAGE_STRUCTURE_TABLE = _opt("stage_structure_table", "tract_producer_year_stage_structure")
STAGE_CORE2_SOURCE_TABLE = _opt("stage_core2_source_table", "tract_producer_year_stage_core2_source")
STAGE_CORE2_TABLE = _opt("stage_core2_table", "tract_producer_year_stage_core2")
STAGE_BASE_TABLE  = _opt("stage_base_table", "tract_producer_year_stage_base")
STAGE_TABLE       = _opt("stage_table", "tract_producer_year_stage")
STAGE_RESOLVE_TABLE = _opt("stage_resolve_table", "tract_producer_year_stage_resolve")
TASK_MODE         = _opt("task_mode", "single_fast").strip().lower()
SHUFFLE_PARTITIONS = _opt("shuffle_partitions", "800")
MAX_JOB_SECONDS   = int(_opt("max_job_seconds", "1100"))
DEBUG             = _opt("debug", "false").strip().lower() == "true"

VALID_MODES = {
    "single_fast",
    "single_pass",
    "single",
}

if TASK_MODE not in VALID_MODES:
    raise ValueError(f"Unsupported --task_mode '{TASK_MODE}'.")

if DEBUG:
    logging.getLogger().setLevel(logging.DEBUG)
    log.setLevel(logging.DEBUG)

if not _requested_full_load:
    log.warning("Incremental mode is disabled for this job; forcing full-load execution")

_conf = SparkConf()
_conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
_conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
_conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
_conf.set("spark.sql.catalog.glue_catalog.warehouse", ICEBERG_WAREHOUSE)
_conf.set("spark.sql.autoBroadcastJoinThreshold", str(50 * 1024 * 1024))
_conf.set("spark.sql.adaptive.enabled", "true")
_conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
_conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
_conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
_conf.set("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
_conf.set("spark.shuffle.io.maxRetries", "10")
_conf.set("spark.shuffle.io.retryWait", "10s")
_conf.set("spark.network.timeout", "800s")
_conf.set("spark.executor.heartbeatInterval", "60s")
_conf.set("spark.stage.maxConsecutiveAttempts", "8")
_conf.set("spark.task.maxFailures", "8")

sc = SparkContext(conf=_conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, args)

job_t0 = time.perf_counter()
STAGE_SPINE_FQN = f"glue_catalog.{TGT_DB}.{STAGE_SPINE_TABLE}"
STAGE_CORE1_FQN = f"glue_catalog.{TGT_DB}.{STAGE_CORE1_TABLE}"
STAGE_STRUCTURE_FILTER_FQN = f"glue_catalog.{TGT_DB}.{STAGE_STRUCTURE_FILTER_TABLE}"
STAGE_STRUCTURE_FQN = f"glue_catalog.{TGT_DB}.{STAGE_STRUCTURE_TABLE}"
STAGE_CORE2_SOURCE_FQN = f"glue_catalog.{TGT_DB}.{STAGE_CORE2_SOURCE_TABLE}"
STAGE_CORE2_FQN = f"glue_catalog.{TGT_DB}.{STAGE_CORE2_TABLE}"
STAGE_BASE_FQN = f"glue_catalog.{TGT_DB}.{STAGE_BASE_TABLE}"
STAGE_FQN = f"glue_catalog.{TGT_DB}.{STAGE_TABLE}"
STAGE_RESOLVE_FQN = f"glue_catalog.{TGT_DB}.{STAGE_RESOLVE_TABLE}"
TARGET_FQN = f"glue_catalog.{TGT_DB}.{TGT_TABLE}"
STRUCTURE_BUCKET_COUNT = 16
HOT_STRUCTURE_BUCKET = 3
HOT_STRUCTURE_BUCKET_SHARD_COUNT = 4

log.info("=" * 70)
log.info(f"Job            : {JOB_NAME}")
log.info(f"Env            : {ENV}")
log.info(f"Warehouse      : {ICEBERG_WAREHOUSE}")
log.info(f"Task Mode      : {TASK_MODE}")
log.info(f"SSS DB         : {SSS_DB}")
log.info(f"Ref DB         : {REF_DB}")
log.info(f"Target DB      : {TGT_DB}")
log.info(f"Target Table   : {TGT_TABLE}")
log.info(f"Stage Spine Tbl: {STAGE_SPINE_TABLE}")
log.info(f"Stage Core1 Tbl: {STAGE_CORE1_TABLE}")
log.info(f"Stage SFilterTb: {STAGE_STRUCTURE_FILTER_TABLE}")
log.info(f"Stage StructTbl: {STAGE_STRUCTURE_TABLE}")
log.info(f"Stage C2Src Tbl: {STAGE_CORE2_SOURCE_TABLE}")
log.info(f"Stage Core2 Tbl: {STAGE_CORE2_TABLE}")
log.info(f"Stage Base Tbl : {STAGE_BASE_TABLE}")
log.info(f"Stage Tbl      : {STAGE_TABLE}")
log.info(f"Stage Resolve  : {STAGE_RESOLVE_TABLE}")
log.info(f"Shuffle Parts  : {SHUFFLE_PARTITIONS}")
log.info(f"Max Job Sec    : {MAX_JOB_SECONDS}")
log.info(f"Full Load      : {FULL_LOAD}")
log.info(f"[METRIC] mode={'full_load' if FULL_LOAD else 'incremental'}")
log.info(f"Debug          : {DEBUG}")
log.info("=" * 70)


def register_view(catalog_db: str, table: str, view_name: str = None):
    fqn = f"glue_catalog.{catalog_db}.{table}"
    vn = view_name or table
    spark.table(fqn).createOrReplaceTempView(vn)


def stage_log(stage_prefix: str, message: str):
    log.info(f"[{stage_prefix}] {message}")


def progress_log(progress_pct: int, milestone: str, phase_t0: float):
    elapsed = time.perf_counter() - phase_t0
    log.info(f"[PROGRESS] progress_pct={progress_pct} milestone={milestone} elapsed_seconds={elapsed:.3f}")


def latest_snapshot_row_count(table_fqn: str) -> int:
    try:
        snapshots_df = spark.table(f"{table_fqn}.snapshots")
        snap_row = snapshots_df.orderBy(snapshots_df.committed_at.desc()).select("summary").first()
        summary = snap_row["summary"] if snap_row else None
        return int(summary.get("total-records", -1)) if summary else -1
    except Exception:
        return -1


def structure_bucket_fqn(bucket: int) -> str:
    return f"glue_catalog.{TGT_DB}.{STAGE_STRUCTURE_TABLE}_b{bucket}"


def enforce_job_timeout():
    total_job_seconds = time.perf_counter() - job_t0
    log.info(f"[JOB_STAGE] [METRIC] total_job_seconds={total_job_seconds:.3f}")
    if MAX_JOB_SECONDS > 0 and total_job_seconds > float(MAX_JOB_SECONDS):
        raise TimeoutError(
            f"job_total exceeded timeout: elapsed={total_job_seconds:.3f}s, max={MAX_JOB_SECONDS}s"
        )


def finish_and_exit(message: str):
    job.commit()
    enforce_job_timeout()
    log.info(message)


PREPROCESS_SPINE_SQL = """
SELECT
    CAST(sp.instance AS STRING) AS instance,
    CAST(sp.client AS STRING) AS client,
    CASE
        WHEN sp.ZZFLD00000T IS NOT NULL AND sp.ZZFLD00000T <> '0' THEN LPAD(sp.ZZFLD00000T, 7, '0')
        WHEN sp.ZZFLD00001O IS NOT NULL AND TRIM(sp.ZZFLD00001O) <> ' ' THEN LPAD(SPLIT(sp.ZZFLD00001O, '-')[3], 7, '0')
        ELSE '0000000'
    END AS tract_number,
    LPAD(sp.ZZFLD00001Z, 2, '0') AS admin_state,
    LPAD(sp.ZZFLD000020, 3, '0') AS admin_county,
    CASE sp.ZZFLD0000B8 WHEN 'ACTV' THEN 'A' WHEN 'IACT' THEN 'I' WHEN 'DELE' THEN 'D' WHEN 'PEND' THEN 'P' ELSE 'A' END AS t_data_status_code,
    TO_DATE(date_format(COALESCE(sp.crtim, CURRENT_TIMESTAMP), 'yyyy-MM-dd')) AS t_creation_date,
    TO_DATE(date_format(COALESCE(sp.UPTIM, CURRENT_TIMESTAMP), 'yyyy-MM-dd')) AS t_last_change_date,
    CASE
        WHEN sp.upnam IS NOT NULL AND TRIM(sp.upnam) NOT IN ('0', '', ')') THEN TRIM(sp.upnam)
        WHEN sp.crnam IS NOT NULL AND TRIM(sp.crnam) NOT IN ('0', '', ')') THEN TRIM(sp.crnam)
        ELSE 'BLANK'
    END AS t_last_change_user_name
FROM ibsp sp
"""

PREPROCESS_STRUCTURE_LINK_SQL = """
SELECT
    st.ibase AS f_ibase,
    spine.tract_number,
    spine.admin_state,
    spine.admin_county,
    spine.t_data_status_code,
    spine.t_creation_date,
    spine.t_last_change_date,
    spine.t_last_change_user_name,
    spine.instance,
    spine.client
FROM sss_spine_stage spine
JOIN ibst st
  ON st.instance = spine.instance
 AND st.parent = '0'
"""

PREPROCESS_STRUCTURE_FARM_SQL = """
SELECT
        core1.f_ibase,
    ibf.farm_number,
        core1.tract_number,
        core1.admin_state,
        core1.admin_county,
        core1.t_data_status_code,
        core1.t_creation_date,
        core1.t_last_change_date,
        core1.t_last_change_user_name,
        core1.instance,
        core1.client
FROM sss_details_core1_stage core1
JOIN sss_details_structure_filter_stage ibf
    ON CAST(core1.f_ibase AS STRING) = ibf.f_ibase
"""

PREPROCESS_STRUCTURE_FARM_BUCKET_SQL_TEMPLATE = """
WITH core1_bucket AS (
    SELECT
        core1.f_ibase,
        core1.tract_number,
        core1.admin_state,
        core1.admin_county,
        core1.t_data_status_code,
        core1.t_creation_date,
        core1.t_last_change_date,
        core1.t_last_change_user_name,
        core1.instance,
        core1.client
    FROM sss_details_core1_stage core1
    WHERE pmod(
            abs(
                hash(
                    concat_ws(
                        '|',
                        CAST(core1.f_ibase AS STRING),
                        CAST(core1.tract_number AS STRING),
                        CAST(core1.admin_state AS STRING),
                        CAST(core1.admin_county AS STRING),
                        CAST(core1.instance AS STRING),
                        CAST(core1.client AS STRING)
                    )
                )
            ),
            16
          ) = {bucket}
        {hot_bucket_shard_predicate}
),
bucket_keys AS (
    SELECT DISTINCT CAST(f_ibase AS STRING) AS f_ibase
    FROM core1_bucket
),
ibf_bucket AS (
    SELECT
        ibf.f_ibase,
        ibf.farm_number
    FROM sss_details_structure_filter_stage ibf
    JOIN bucket_keys keys
      ON ibf.f_ibase = keys.f_ibase
)
SELECT
    core1.f_ibase,
    ibf.farm_number,
    core1.tract_number,
    core1.admin_state,
    core1.admin_county,
    core1.t_data_status_code,
    core1.t_creation_date,
    core1.t_last_change_date,
    core1.t_last_change_user_name,
    core1.instance,
    core1.client
FROM core1_bucket core1
JOIN ibf_bucket ibf
  ON CAST(core1.f_ibase AS STRING) = ibf.f_ibase
"""

PREPROCESS_STRUCTURE_FARM_FILTER_SQL = """
WITH core_keys AS (
    SELECT DISTINCT CAST(f_ibase AS STRING) AS f_ibase
    FROM sss_details_core1_stage
)
SELECT
    CAST(ib.ibase AS STRING) AS f_ibase,
    ib.ZZFLD000000 AS farm_number
FROM ibib ib
JOIN core_keys keys
  ON CAST(ib.ibase AS STRING) = keys.f_ibase
"""

PREPROCESS_CORE2_EXTRACT_SQL = """
WITH structure_keys AS (
    SELECT DISTINCT
        CAST(instance AS STRING) AS i_instance,
        CAST(client AS STRING) AS i_client,
        CAST(f_ibase AS STRING) AS r_ibase
    FROM sss_details_structure_stage
)
SELECT DISTINCT
    CAST(ni.instance AS STRING) AS i_instance,
    CAST(ni.client AS STRING) AS i_client,
    CAST(ni.ibase AS STRING) AS r_ibase,
    CAST(ni.in_guid AS STRING) AS in_guid
FROM ibin ni
JOIN structure_keys sk
  ON CAST(ni.instance AS STRING) = sk.i_instance
 AND CAST(ni.client AS STRING) = sk.i_client
 AND CAST(ni.ibase AS STRING) = sk.r_ibase
"""

PREPROCESS_CORE2_SQL = """
SELECT
    core1.f_ibase,
    core1.farm_number,
    core1.tract_number,
    core1.admin_state,
    core1.admin_county,
    core1.t_data_status_code,
    core1.t_creation_date,
    core1.t_last_change_date,
    core1.t_last_change_user_name,
        src.i_instance,
        src.r_ibase,
        src.in_guid
FROM sss_details_structure_stage core1
JOIN sss_details_core2_source_stage src
    ON src.i_instance = core1.instance
 AND src.i_client = core1.client
 AND src.r_ibase = CAST(core1.f_ibase AS STRING)
"""

PREPROCESS_PARTNER_SQL = """
SELECT
    core2.f_ibase,
    core2.farm_number,
    core2.tract_number,
    core2.admin_state,
    core2.admin_county,
    core2.t_data_status_code,
    core2.t_creation_date,
    core2.t_last_change_date,
    core2.t_last_change_user_name,
    core2.i_instance,
    core2.r_ibase,
    cr.partner_fct,
    cr.partner_no
FROM sss_details_core2_stage core2
JOIN ibpart pt
  ON CAST(pt.segment_recno AS STRING) = core2.in_guid
 AND pt.segment = 2
 AND pt.valto = 99991231235959
JOIN crmd_partner cr
  ON cr.guid = pt.partnerset
"""

PREPROCESS_ENRICH_SQL = """
WITH time_pd AS (
    SELECT time_period_identifier, time_period_name
    FROM time_period
    WHERE data_status_code = 'A'
),
zmi_details AS (
    SELECT
        CAST(zd.instance AS STRING) AS instance,
        CAST(zd.ibase AS STRING) AS ibase,
        zm.ZZK0011,
        CASE TRIM(zm.ZZ0011)
            WHEN 'AE' THEN 41 WHEN 'AR' THEN 40 WHEN 'HA' THEN 40
            WHEN 'NP' THEN 45 WHEN 'NW' THEN 45 WHEN 'TP' THEN 44
            WHEN 'TR' THEN 44 ELSE NULL
        END AS tract_producer_cw_exception_code,
        CASE TRIM(zm.ZZ0010)
            WHEN 'GF' THEN 31 WHEN 'EH' THEN 34 WHEN 'AE' THEN 33
            WHEN 'NAR' THEN 33 WHEN 'AR' THEN 32 WHEN 'HA' THEN 32
            WHEN 'LT' THEN 30 ELSE NULL
        END AS tract_producer_hel_exception_code,
        CASE TRIM(zm.ZZ0012)
            WHEN 'AR' THEN 52 WHEN 'HA' THEN 52 WHEN 'NAR' THEN 50
            WHEN 'AE' THEN 50 WHEN 'GF' THEN 51 ELSE NULL
        END AS tract_producer_pcw_exception_code,
        CASE TRIM(zm.ZZ0017)
            WHEN 'WR' THEN 43 WHEN 'GF' THEN 42 WHEN 'NAR' THEN 41
            WHEN 'AR' THEN 40 WHEN 'NP' THEN 45 WHEN 'TP' THEN 44 ELSE NULL
        END AS rma_cw_exception_code,
        CASE TRIM(zm.ZZ0016)
            WHEN 'GF' THEN 31 WHEN 'EH' THEN 34 WHEN 'NAR' THEN 33
            WHEN 'AR' THEN 32 WHEN 'LT' THEN 30 ELSE NULL
        END AS rma_hel_exception_code,
        CASE TRIM(zm.ZZ0018)
            WHEN 'AR' THEN 52 WHEN 'HA' THEN 52 WHEN 'NAR' THEN 50
            WHEN 'AE' THEN 50 WHEN 'GF' THEN 51 ELSE NULL
        END AS rma_pcw_exception_code,
        CASE WHEN zm.ZZ0013 IS NOT NULL THEN TO_DATE(CAST(zm.ZZ0013 AS STRING), 'yyyyMMddHHmmss') ELSE NULL END AS hel_appeals_exhausted_date,
        CASE WHEN zm.ZZ0014 IS NOT NULL THEN TO_DATE(CAST(zm.ZZ0014 AS STRING), 'yyyyMMddHHmmss') ELSE NULL END AS cw_appeals_exhausted_date,
        CASE WHEN zm.ZZ0015 IS NOT NULL THEN TO_DATE(CAST(zm.ZZ0015 AS STRING), 'yyyyMMddHHmmss') ELSE NULL END AS pcw_appeals_exhausted_date,
        CASE WHEN zm.VALID_FROM IS NOT NULL AND zm.VALID_FROM <> 0 THEN TO_DATE(CAST(zm.VALID_FROM AS STRING), 'yyyyMMddHHmmss') ELSE NULL END AS producer_involvement_start_date,
        CASE WHEN zm.VALID_TO IS NOT NULL AND zm.VALID_TO <> 0 THEN TO_DATE(CAST(zm.VALID_TO AS STRING), 'yyyyMMddHHmmss') ELSE NULL END AS producer_involvement_end_date
    FROM z_ibase_comp_detail zd
    JOIN comm_pr_frg_rel cf ON cf.product_guid = zd.prod_objnr
    JOIN zmi_farm_partn zm ON zm.frg_guid = cf.fragment_guid
)
SELECT
    coc.county_office_control_identifier,
    timepd.time_period_identifier,
    CAST(c.bpext AS INT) AS core_customer_identifier,
    z_dets.producer_involvement_start_date,
    z_dets.producer_involvement_end_date,
    CAST(NULL AS STRING) AS producer_involvement_interrupted_indicator,
    z_dets.tract_producer_hel_exception_code,
    z_dets.tract_producer_cw_exception_code,
    z_dets.tract_producer_pcw_exception_code,
    sss_dets.t_data_status_code AS data_status_code,
    sss_dets.t_creation_date AS creation_date,
    sss_dets.t_last_change_date AS last_change_date,
    sss_dets.t_last_change_user_name AS last_change_user_name,
    CASE sss_dets.partner_fct WHEN 'ZFARMONR' THEN 162 WHEN 'ZOTNT' THEN 163 END AS producer_involvement_code,
    sss_dets.admin_state AS state_fsa_code,
    sss_dets.admin_county AS county_fsa_code,
    LPAD(sss_dets.farm_number, 7, '0') AS farm_number,
    sss_dets.tract_number,
    z_dets.hel_appeals_exhausted_date,
    z_dets.cw_appeals_exhausted_date,
    z_dets.pcw_appeals_exhausted_date,
    z_dets.rma_hel_exception_code AS tract_producer_rma_hel_exception_code,
    z_dets.rma_cw_exception_code AS tract_producer_rma_cw_exception_code,
    z_dets.rma_pcw_exception_code AS tract_producer_rma_pcw_exception_code
FROM sss_details_base_stage sss_dets
JOIN time_pd timepd
  ON CAST(CASE WHEN MONTH(CURRENT_DATE) >= 10 THEN YEAR(CURRENT_DATE) + 1 ELSE YEAR(CURRENT_DATE) END AS STRING)
   = TRIM(timepd.time_period_name)
JOIN zmi_details z_dets
  ON z_dets.instance = sss_dets.i_instance
 AND z_dets.ibase = sss_dets.r_ibase
JOIN county_office_control coc
  ON LPAD(sss_dets.admin_state, 2, '0') || LPAD(sss_dets.admin_county, 3, '0')
   = LPAD(coc.state_fsa_code, 2, '0') || LPAD(coc.county_fsa_code, 3, '0')
 AND timepd.time_period_identifier = coc.time_period_identifier
LEFT JOIN but000 c
  ON sss_dets.partner_no = c.partner_guid
 AND z_dets.ZZK0011 = c.partner
WHERE timepd.time_period_name >= '2014'
  AND c.bpext NOT IN ('DUPLICATE', '11876423_D')
"""

FINALIZE_RESOLVE_SQL = """
WITH tract_producer_year_tbl AS (
    SELECT
        farmpg.farm_identifier,
        ty.tract_year_identifier,
        tprdryr.*,
        ROW_NUMBER() OVER (
            PARTITION BY tprdryr.core_customer_identifier,
                         ty.tract_year_identifier,
                         tprdryr.producer_involvement_code
            ORDER BY tprdryr.creation_date DESC, tprdryr.last_change_date DESC
        ) AS rownum
    FROM tract_current_stage tprdryr
    JOIN farm farmpg
      ON farmpg.county_office_control_identifier = tprdryr.county_office_control_identifier
     AND LPAD(CAST(farmpg.farm_number AS STRING), 7, '0') = LPAD(tprdryr.farm_number, 7, '0')
    JOIN tract tractpg
      ON tractpg.county_office_control_identifier = tprdryr.county_office_control_identifier
     AND LPAD(CAST(tractpg.tract_number AS STRING), 7, '0') = LPAD(tprdryr.tract_number, 7, '0')
    JOIN farm_year fy
      ON farmpg.farm_identifier = fy.farm_identifier
     AND tprdryr.time_period_identifier = fy.time_period_identifier
    JOIN tract_year ty
      ON fy.farm_year_identifier = ty.farm_year_identifier
     AND tractpg.tract_identifier = ty.tract_identifier
)
SELECT
    core_customer_identifier,
    tract_year_identifier,
    producer_involvement_start_date,
    producer_involvement_end_date,
    producer_involvement_interrupted_indicator,
    tract_producer_hel_exception_code,
    tract_producer_cw_exception_code,
    tract_producer_pcw_exception_code,
    data_status_code,
    creation_date,
    last_change_date,
    last_change_user_name,
    producer_involvement_code,
    time_period_identifier,
    state_fsa_code,
    county_fsa_code,
    farm_identifier,
    farm_number,
    tract_number,
    hel_appeals_exhausted_date,
    cw_appeals_exhausted_date,
    pcw_appeals_exhausted_date,
    tract_producer_rma_hel_exception_code,
    tract_producer_rma_cw_exception_code,
    tract_producer_rma_pcw_exception_code
FROM tract_producer_year_tbl
WHERE rownum = 1
"""


def run_preprocess_spine():
    register_view(SSS_DB, "ibsp")
    phase_t0 = time.perf_counter()
    stage_log("PP_SPINE_STAGE", "Running preprocess_spine stage")
    spine_df = spark.sql(PREPROCESS_SPINE_SQL)
    spine_df.limit(0).write.format("iceberg").mode("ignore").saveAsTable(STAGE_SPINE_FQN)
    spine_df.write.format("iceberg").mode("overwrite").saveAsTable(STAGE_SPINE_FQN)
    stage_log("PP_SPINE_STAGE", f"[METRIC] phase_preprocess_spine_seconds={time.perf_counter() - phase_t0:.3f}")
    stage_log("PP_SPINE_STAGE", f"[METRIC] stage_spine_row_count={latest_snapshot_row_count(STAGE_SPINE_FQN)}")


def run_preprocess_structure_link():
    register_view(SSS_DB, "ibst")
    spark.table(STAGE_SPINE_FQN).createOrReplaceTempView("sss_spine_stage")
    phase_t0 = time.perf_counter()
    stage_log("PP_STRUCTURE_LINK_STAGE", "Running preprocess_structure_link stage")
    core1_df = spark.sql(PREPROCESS_STRUCTURE_LINK_SQL)
    core1_df.limit(0).write.format("iceberg").mode("ignore").saveAsTable(STAGE_CORE1_FQN)
    core1_df.write.format("iceberg").mode("overwrite").saveAsTable(STAGE_CORE1_FQN)
    stage_log("PP_STRUCTURE_LINK_STAGE", f"[METRIC] phase_preprocess_structure_link_seconds={time.perf_counter() - phase_t0:.3f}")
    stage_log("PP_STRUCTURE_LINK_STAGE", f"[METRIC] stage_core1_row_count={latest_snapshot_row_count(STAGE_CORE1_FQN)}")


def run_preprocess_structure_farm():
    run_preprocess_structure_farm_b0()
    run_preprocess_structure_farm_b1()
    run_preprocess_structure_farm_b2()
    run_preprocess_structure_farm_b3_s0()
    run_preprocess_structure_farm_b3_s1()
    run_preprocess_structure_farm_b3_s2()
    run_preprocess_structure_farm_b3_s3()
    run_preprocess_structure_farm_b4()
    run_preprocess_structure_farm_b5()
    run_preprocess_structure_farm_b6()
    run_preprocess_structure_farm_b7()
    run_preprocess_structure_farm_b8()
    run_preprocess_structure_farm_b9()
    run_preprocess_structure_farm_b10()
    run_preprocess_structure_farm_b11()
    run_preprocess_structure_farm_b12()
    run_preprocess_structure_farm_b13()
    run_preprocess_structure_farm_b14()
    run_preprocess_structure_farm_b15()
    run_preprocess_structure_merge()


def _run_preprocess_structure_farm_bucket(
    stage_prefix: str,
    bucket: int,
    write_mode: str,
    metric_name: str,
    shard_index: int = None,
    shard_count: int = 1,
):
    spark.table(STAGE_CORE1_FQN).createOrReplaceTempView("sss_details_core1_stage")
    spark.table(STAGE_STRUCTURE_FILTER_FQN).createOrReplaceTempView("sss_details_structure_filter_stage")
    phase_t0 = time.perf_counter()
    stage_log(stage_prefix, f"Running {metric_name} stage")
    shard_predicate = ""
    if shard_index is not None and shard_count > 1:
        shard_predicate = f"""
      AND pmod(
            abs(
                hash(
                    concat_ws(
                        '|',
                        CAST(core1.f_ibase AS STRING),
                        CAST(core1.tract_number AS STRING),
                        CAST(core1.admin_state AS STRING),
                        CAST(core1.admin_county AS STRING),
                        CAST(core1.instance AS STRING),
                        CAST(core1.client AS STRING)
                    )
                )
            ),
            {shard_count}
          ) = {shard_index}
"""
    structure_df = spark.sql(
        PREPROCESS_STRUCTURE_FARM_BUCKET_SQL_TEMPLATE.format(
            bucket=bucket,
            hot_bucket_shard_predicate=shard_predicate,
        )
    )
    bucket_fqn = structure_bucket_fqn(bucket)
    structure_df.limit(0).write.format("iceberg").mode("ignore").saveAsTable(bucket_fqn)
    structure_df.write.format("iceberg").mode(write_mode).saveAsTable(bucket_fqn)
    stage_log(stage_prefix, f"[METRIC] phase_{metric_name}_seconds={time.perf_counter() - phase_t0:.3f}")
    stage_log(stage_prefix, f"[METRIC] stage_structure_bucket_row_count={latest_snapshot_row_count(bucket_fqn)}")


def run_preprocess_structure_farm_b0():
    _run_preprocess_structure_farm_bucket(
        "PP_STRUCTURE_FARM_B0_STAGE",
        0,
        "overwrite",
        "preprocess_structure_farm_b0",
    )


def run_preprocess_structure_farm_b1():
    _run_preprocess_structure_farm_bucket(
        "PP_STRUCTURE_FARM_B1_STAGE",
        1,
        "append",
        "preprocess_structure_farm_b1",
    )


def run_preprocess_structure_farm_b2():
    _run_preprocess_structure_farm_bucket(
        "PP_STRUCTURE_FARM_B2_STAGE",
        2,
        "append",
        "preprocess_structure_farm_b2",
    )


def run_preprocess_structure_farm_b3():
    run_preprocess_structure_farm_b3_s0()
    run_preprocess_structure_farm_b3_s1()
    run_preprocess_structure_farm_b3_s2()
    run_preprocess_structure_farm_b3_s3()


def run_preprocess_structure_farm_b3_s0():
    _run_preprocess_structure_farm_bucket(
        "PP_STRUCTURE_FARM_B3_S0_STAGE",
        HOT_STRUCTURE_BUCKET,
        "overwrite",
        "preprocess_structure_farm_b3_s0",
        shard_index=0,
        shard_count=HOT_STRUCTURE_BUCKET_SHARD_COUNT,
    )


def run_preprocess_structure_farm_b3_s1():
    _run_preprocess_structure_farm_bucket(
        "PP_STRUCTURE_FARM_B3_S1_STAGE",
        HOT_STRUCTURE_BUCKET,
        "append",
        "preprocess_structure_farm_b3_s1",
        shard_index=1,
        shard_count=HOT_STRUCTURE_BUCKET_SHARD_COUNT,
    )


def run_preprocess_structure_farm_b3_s2():
    _run_preprocess_structure_farm_bucket(
        "PP_STRUCTURE_FARM_B3_S2_STAGE",
        HOT_STRUCTURE_BUCKET,
        "append",
        "preprocess_structure_farm_b3_s2",
        shard_index=2,
        shard_count=HOT_STRUCTURE_BUCKET_SHARD_COUNT,
    )


def run_preprocess_structure_farm_b3_s3():
    _run_preprocess_structure_farm_bucket(
        "PP_STRUCTURE_FARM_B3_S3_STAGE",
        HOT_STRUCTURE_BUCKET,
        "append",
        "preprocess_structure_farm_b3_s3",
        shard_index=3,
        shard_count=HOT_STRUCTURE_BUCKET_SHARD_COUNT,
    )


def run_preprocess_structure_farm_b4():
    _run_preprocess_structure_farm_bucket(
        "PP_STRUCTURE_FARM_B4_STAGE",
        4,
        "append",
        "preprocess_structure_farm_b4",
    )


def run_preprocess_structure_farm_b5():
    _run_preprocess_structure_farm_bucket(
        "PP_STRUCTURE_FARM_B5_STAGE",
        5,
        "append",
        "preprocess_structure_farm_b5",
    )


def run_preprocess_structure_farm_b6():
    _run_preprocess_structure_farm_bucket(
        "PP_STRUCTURE_FARM_B6_STAGE",
        6,
        "append",
        "preprocess_structure_farm_b6",
    )


def run_preprocess_structure_farm_b7():
    _run_preprocess_structure_farm_bucket(
        "PP_STRUCTURE_FARM_B7_STAGE",
        7,
        "append",
        "preprocess_structure_farm_b7",
    )


def run_preprocess_structure_farm_b8():
    _run_preprocess_structure_farm_bucket(
        "PP_STRUCTURE_FARM_B8_STAGE",
        8,
        "append",
        "preprocess_structure_farm_b8",
    )


def run_preprocess_structure_farm_b9():
    _run_preprocess_structure_farm_bucket(
        "PP_STRUCTURE_FARM_B9_STAGE",
        9,
        "append",
        "preprocess_structure_farm_b9",
    )


def run_preprocess_structure_farm_b10():
    _run_preprocess_structure_farm_bucket(
        "PP_STRUCTURE_FARM_B10_STAGE",
        10,
        "append",
        "preprocess_structure_farm_b10",
    )


def run_preprocess_structure_farm_b11():
    _run_preprocess_structure_farm_bucket(
        "PP_STRUCTURE_FARM_B11_STAGE",
        11,
        "append",
        "preprocess_structure_farm_b11",
    )


def run_preprocess_structure_farm_b12():
    _run_preprocess_structure_farm_bucket(
        "PP_STRUCTURE_FARM_B12_STAGE",
        12,
        "append",
        "preprocess_structure_farm_b12",
    )


def run_preprocess_structure_farm_b13():
    _run_preprocess_structure_farm_bucket(
        "PP_STRUCTURE_FARM_B13_STAGE",
        13,
        "append",
        "preprocess_structure_farm_b13",
    )


def run_preprocess_structure_farm_b14():
    _run_preprocess_structure_farm_bucket(
        "PP_STRUCTURE_FARM_B14_STAGE",
        14,
        "append",
        "preprocess_structure_farm_b14",
    )


def run_preprocess_structure_farm_b15():
    _run_preprocess_structure_farm_bucket(
        "PP_STRUCTURE_FARM_B15_STAGE",
        15,
        "append",
        "preprocess_structure_farm_b15",
    )


def run_preprocess_structure_merge():
    phase_t0 = time.perf_counter()
    stage_log("PP_STRUCTURE_MERGE_STAGE", "Running preprocess_structure_merge stage")
    merged_df = None
    for bucket in range(STRUCTURE_BUCKET_COUNT):
        bucket_df = spark.table(structure_bucket_fqn(bucket))
        merged_df = bucket_df if merged_df is None else merged_df.unionByName(bucket_df)
    merged_df.limit(0).write.format("iceberg").mode("ignore").saveAsTable(STAGE_STRUCTURE_FQN)
    merged_df.write.format("iceberg").mode("overwrite").saveAsTable(STAGE_STRUCTURE_FQN)
    stage_log("PP_STRUCTURE_MERGE_STAGE", f"[METRIC] phase_preprocess_structure_merge_seconds={time.perf_counter() - phase_t0:.3f}")
    stage_log("PP_STRUCTURE_MERGE_STAGE", f"[METRIC] stage_structure_row_count={latest_snapshot_row_count(STAGE_STRUCTURE_FQN)}")


def run_preprocess_structure_farm_filter():
    register_view(SSS_DB, "ibib")
    spark.table(STAGE_CORE1_FQN).createOrReplaceTempView("sss_details_core1_stage")
    phase_t0 = time.perf_counter()
    stage_log("PP_STRUCTURE_FARM_FILTER_STAGE", "Running preprocess_structure_farm_filter stage")
    structure_filter_df = spark.sql(PREPROCESS_STRUCTURE_FARM_FILTER_SQL)
    structure_filter_df.limit(0).write.format("iceberg").mode("ignore").saveAsTable(STAGE_STRUCTURE_FILTER_FQN)
    structure_filter_df.write.format("iceberg").mode("overwrite").saveAsTable(STAGE_STRUCTURE_FILTER_FQN)
    stage_log("PP_STRUCTURE_FARM_FILTER_STAGE", f"[METRIC] phase_preprocess_structure_farm_filter_seconds={time.perf_counter() - phase_t0:.3f}")
    stage_log("PP_STRUCTURE_FARM_FILTER_STAGE", f"[METRIC] stage_structure_filter_row_count={latest_snapshot_row_count(STAGE_STRUCTURE_FILTER_FQN)}")


def run_preprocess_core2_extract():
    register_view(SSS_DB, "ibin")
    spark.table(STAGE_STRUCTURE_FQN).createOrReplaceTempView("sss_details_structure_stage")
    phase_t0 = time.perf_counter()
    stage_log("PP_CORE2_EXTRACT_STAGE", "Running preprocess_core2_extract stage")
    core2_source_df = spark.sql(PREPROCESS_CORE2_EXTRACT_SQL)
    core2_source_df.limit(0).write.format("iceberg").mode("ignore").saveAsTable(STAGE_CORE2_SOURCE_FQN)
    core2_source_df.write.format("iceberg").mode("overwrite").saveAsTable(STAGE_CORE2_SOURCE_FQN)
    stage_log("PP_CORE2_EXTRACT_STAGE", f"[METRIC] phase_preprocess_core2_extract_seconds={time.perf_counter() - phase_t0:.3f}")
    stage_log("PP_CORE2_EXTRACT_STAGE", f"[METRIC] stage_core2_source_row_count={latest_snapshot_row_count(STAGE_CORE2_SOURCE_FQN)}")


def run_preprocess_core2():
    spark.table(STAGE_STRUCTURE_FQN).createOrReplaceTempView("sss_details_structure_stage")
    spark.table(STAGE_CORE2_SOURCE_FQN).createOrReplaceTempView("sss_details_core2_source_stage")
    phase_t0 = time.perf_counter()
    stage_log("PP_CORE2_STAGE", "Running preprocess_core2 stage")
    core2_df = spark.sql(PREPROCESS_CORE2_SQL)
    core2_df.limit(0).write.format("iceberg").mode("ignore").saveAsTable(STAGE_CORE2_FQN)
    core2_df.write.format("iceberg").mode("overwrite").saveAsTable(STAGE_CORE2_FQN)
    stage_log("PP_CORE2_STAGE", f"[METRIC] phase_preprocess_core2_seconds={time.perf_counter() - phase_t0:.3f}")
    stage_log("PP_CORE2_STAGE", f"[METRIC] stage_core2_row_count={latest_snapshot_row_count(STAGE_CORE2_FQN)}")


def run_preprocess_partner():
    for tbl in ["ibpart", "crmd_partner"]:
        register_view(SSS_DB, tbl)
    spark.table(STAGE_CORE2_FQN).createOrReplaceTempView("sss_details_core2_stage")
    phase_t0 = time.perf_counter()
    stage_log("PP_PARTNER_STAGE", "Running preprocess_partner stage")
    base_df = spark.sql(PREPROCESS_PARTNER_SQL)
    base_df.limit(0).write.format("iceberg").mode("ignore").saveAsTable(STAGE_BASE_FQN)
    base_df.write.format("iceberg").mode("overwrite").saveAsTable(STAGE_BASE_FQN)
    stage_log("PP_PARTNER_STAGE", f"[METRIC] phase_preprocess_partner_seconds={time.perf_counter() - phase_t0:.3f}")
    stage_log("PP_PARTNER_STAGE", f"[METRIC] stage_base_row_count={latest_snapshot_row_count(STAGE_BASE_FQN)}")


def run_preprocess_enrich():
    for tbl in ["z_ibase_comp_detail", "comm_pr_frg_rel", "zmi_farm_partn"]:
        register_view(SSS_DB, tbl)
    for tbl in ["time_period", "county_office_control", "but000"]:
        register_view(REF_DB, tbl)
    spark.table(STAGE_BASE_FQN).createOrReplaceTempView("sss_details_base_stage")
    phase_t0 = time.perf_counter()
    stage_log("PP_ENRICH_STAGE", "Running preprocess_enrich stage")
    stage_df = spark.sql(PREPROCESS_ENRICH_SQL)
    stage_df.limit(0).write.format("iceberg").mode("ignore").saveAsTable(STAGE_FQN)
    stage_df.write.format("iceberg").mode("overwrite").saveAsTable(STAGE_FQN)
    stage_log("PP_ENRICH_STAGE", f"[METRIC] phase_preprocess_enrich_seconds={time.perf_counter() - phase_t0:.3f}")
    stage_log("PP_ENRICH_STAGE", f"[METRIC] stage_enrich_row_count={latest_snapshot_row_count(STAGE_FQN)}")


def run_finalize_resolve():
    for tbl in ["farm", "tract", "farm_year", "tract_year"]:
        register_view(REF_DB, tbl)
    spark.table(STAGE_FQN).createOrReplaceTempView("tract_current_stage")
    phase_t0 = time.perf_counter()
    stage_log("FINALIZE_RESOLVE_STAGE", "Running finalize_resolve stage")
    resolve_df = spark.sql(FINALIZE_RESOLVE_SQL)
    resolve_df.limit(0).write.format("iceberg").mode("ignore").saveAsTable(STAGE_RESOLVE_FQN)
    resolve_df.write.format("iceberg").mode("overwrite").saveAsTable(STAGE_RESOLVE_FQN)
    stage_log("FINALIZE_RESOLVE_STAGE", f"[METRIC] phase_finalize_resolve_seconds={time.perf_counter() - phase_t0:.3f}")
    stage_log("FINALIZE_RESOLVE_STAGE", f"[METRIC] stage_resolve_row_count={latest_snapshot_row_count(STAGE_RESOLVE_FQN)}")


def run_finalize_publish():
    phase_t0 = time.perf_counter()
    stage_log("FINALIZE_PUBLISH_STAGE", "Running finalize_publish stage")
    source_df = spark.table(STAGE_RESOLVE_FQN)
    source_df.limit(0).write.format("iceberg").mode("ignore").saveAsTable(TARGET_FQN)
    write_t0 = time.perf_counter()
    stage_log("FINALIZE_PUBLISH_STAGE", f"Executing full-load overwrite for {TARGET_FQN} (DataFrameWriter + Iceberg)")
    source_df.write.format("iceberg").mode("overwrite").saveAsTable(TARGET_FQN)
    stage_log("FINALIZE_PUBLISH_STAGE", f"[METRIC] phase_write_seconds={time.perf_counter() - write_t0:.3f}")
    stage_log("FINALIZE_PUBLISH_STAGE", f"[METRIC] phase_finalize_publish_seconds={time.perf_counter() - phase_t0:.3f}")
    stage_log("FINALIZE_PUBLISH_STAGE", f"[METRIC] {TGT_TABLE}_row_count={latest_snapshot_row_count(TARGET_FQN)}")


def run_single_fast():
    phase_t0 = time.perf_counter()
    stage_log("SINGLE_FAST_STAGE", "Running single_fast architecture-aligned in-memory transform")
    progress_log(5, "single_fast_started", phase_t0)

    base_shuffle_parts = max(1200, int(SHUFFLE_PARTITIONS))
    wide_shuffle_parts = max(1600, base_shuffle_parts)
    final_shuffle_parts = max(1200, int(base_shuffle_parts * 0.85))

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(128 * 1024 * 1024))
    spark.conf.set("spark.sql.broadcastTimeout", "1200")
    spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "3")
    spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", str(64 * 1024 * 1024))
    spark.conf.set("spark.sql.adaptive.forceOptimizeSkewedJoin", "true")
    spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
    spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", str(64 * 1024 * 1024))
    spark.conf.set("spark.sql.adaptive.coalescePartitions.initialPartitionNum", str(wide_shuffle_parts))
    spark.conf.set("spark.sql.files.maxPartitionBytes", str(128 * 1024 * 1024))
    checkpoint_dir = f"{ICEBERG_WAREHOUSE.rstrip('/')}/_spark_checkpoints/{TGT_TABLE}"
    spark.sparkContext.setCheckpointDir(checkpoint_dir)
    stage_log("SINGLE_FAST_STAGE", f"Checkpoint dir set: {checkpoint_dir}")
    stage_log("SINGLE_FAST_STAGE", "Applied single_fast Spark tuning profile")

    for tbl in [
        "ibsp",
        "ibst",
        "ibib",
        "ibin",
        "ibpart",
        "crmd_partner",
        "z_ibase_comp_detail",
        "comm_pr_frg_rel",
        "zmi_farm_partn",
    ]:
        register_view(SSS_DB, tbl)

    for tbl in [
        "time_period",
        "county_office_control",
        "but000",
        "farm",
        "tract",
        "farm_year",
        "tract_year",
    ]:
        register_view(REF_DB, tbl)
    progress_log(15, "views_registered", phase_t0)

    spine_df = spark.sql(PREPROCESS_SPINE_SQL).repartition(base_shuffle_parts, "instance", "client")
    spine_df.createOrReplaceTempView("sss_spine_stage")
    progress_log(30, "spine_ready", phase_t0)

    core1_df = spark.sql(PREPROCESS_STRUCTURE_LINK_SQL).repartition(wide_shuffle_parts, "instance", "client", "f_ibase").checkpoint(eager=True)
    core1_df.createOrReplaceTempView("sss_details_core1_stage")

    structure_filter_df = spark.sql(PREPROCESS_STRUCTURE_FARM_FILTER_SQL).repartition(wide_shuffle_parts, "f_ibase").checkpoint(eager=True)
    structure_filter_df.createOrReplaceTempView("sss_details_structure_filter_stage")

    structure_df = spark.sql(PREPROCESS_STRUCTURE_FARM_SQL).repartition(wide_shuffle_parts, "instance", "client", "f_ibase").checkpoint(eager=True)
    structure_df.createOrReplaceTempView("sss_details_structure_stage")
    progress_log(50, "structure_ready", phase_t0)

    core2_source_df = spark.sql(PREPROCESS_CORE2_EXTRACT_SQL).repartition(wide_shuffle_parts, "i_instance", "i_client", "r_ibase")
    core2_source_df.createOrReplaceTempView("sss_details_core2_source_stage")

    core2_df = spark.sql(PREPROCESS_CORE2_SQL).repartition(wide_shuffle_parts, "i_instance", "r_ibase").checkpoint(eager=True)
    core2_df.createOrReplaceTempView("sss_details_core2_stage")

    base_df = spark.sql(PREPROCESS_PARTNER_SQL).repartition(wide_shuffle_parts, "i_instance", "r_ibase").checkpoint(eager=True)
    base_df.createOrReplaceTempView("sss_details_base_stage")
    progress_log(65, "partner_ready", phase_t0)

    stage_df = spark.sql(PREPROCESS_ENRICH_SQL).repartition(base_shuffle_parts, "county_office_control_identifier", "time_period_identifier").checkpoint(eager=True)
    stage_df.createOrReplaceTempView("tract_current_stage")
    progress_log(80, "enrich_ready", phase_t0)

    resolve_df = spark.sql(FINALIZE_RESOLVE_SQL).repartition(final_shuffle_parts, "tract_year_identifier")
    progress_log(90, "resolve_ready", phase_t0)

    resolve_df.limit(0).write.format("iceberg").mode("ignore").saveAsTable(TARGET_FQN)
    write_t0 = time.perf_counter()
    resolve_df.write.format("iceberg").mode("overwrite").saveAsTable(TARGET_FQN)
    progress_log(100, "single_fast_completed", phase_t0)

    stage_log("SINGLE_FAST_STAGE", f"[METRIC] phase_write_seconds={time.perf_counter() - write_t0:.3f}")
    stage_log("SINGLE_FAST_STAGE", f"[METRIC] phase_single_fast_seconds={time.perf_counter() - phase_t0:.3f}")
    stage_log("SINGLE_FAST_STAGE", f"[METRIC] {TGT_TABLE}_row_count={latest_snapshot_row_count(TARGET_FQN)}")


if TASK_MODE in {"single_fast", "single_pass", "single"}:
    run_single_fast()
    job.commit()
    enforce_job_timeout()
    log.info(f"[SINGLE_FAST_STAGE] Transform-Tract-Producer-Year {TASK_MODE}: completed successfully")
else:
    raise ValueError(
        f"Unsupported --task_mode '{TASK_MODE}' for current single-pass-only runtime. "
        "Use one of: single_fast, single_pass, single"
    )
