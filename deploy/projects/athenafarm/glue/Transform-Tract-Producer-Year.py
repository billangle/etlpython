"""
================================================================================
AWS Glue Job: Transform-Tract-Producer-Year
================================================================================

PURPOSE:
    Builds tract_producer_year in Step Functions-manageable modes.

    7-step optimized flow:
      1) preprocess_spine        : ibsp projection + normalization
      2) preprocess_structure    : spine + ibst + ibib
      3) preprocess_core2        : structure + ibin
      4) preprocess_partner      : core2 + ibpart + crmd_partner
      5) preprocess_enrich       : partner + zmi + time/coc/but000
      6) finalize_resolve        : enriched stage + farm/tract/farm_year/tract_year
      7) finalize_publish        : write final target snapshot

    Compatibility wrappers:
      - preprocess_base   : runs steps 1+2+3+4
      - preprocess        : runs steps 1+2+3+4+5
      - finalize          : runs steps 6+7
      - single            : runs all 7 steps

GLUE JOB ARGUMENTS:
    --JOB_NAME            : Glue job name
    --env                 : Deployment environment
    --iceberg_warehouse   : s3:// URI for Iceberg warehouse root
    --task_mode           : single | preprocess | preprocess_base |
                            preprocess_spine | preprocess_structure | preprocess_core2 |
                            preprocess_partner | preprocess_enrich |
                            finalize_resolve | finalize_publish | finalize
    --stage_spine_table   : Stage table for step 1 (default: tract_producer_year_stage_spine)
    --stage_core1_table   : Stage table for step 2 (default: tract_producer_year_stage_core1)
    --stage_core2_table   : Stage table for step 3 (default: tract_producer_year_stage_core2)
    --stage_base_table    : Stage table for step 4 (default: tract_producer_year_stage_base)
    --stage_table         : Stage table for step 5 (default: tract_producer_year_stage)
    --stage_resolve_table : Stage table for step 6 (default: tract_producer_year_stage_resolve)
    --full_load           : accepted for compatibility; job always full-load
    --sss_database        : SSS Iceberg db (default: athenafarm_prod_raw)
    --ref_database        : reference Iceberg db (default: athenafarm_prod_ref)
    --target_database     : target Iceberg db (default: athenafarm_prod_gold)
    --target_table        : target Iceberg table (default: tract_producer_year)
    --shuffle_partitions  : Spark shuffle partitions (default: 800)
    --max_job_seconds     : fail-fast job cap seconds (default: 1100)
    --debug               : DEBUG logging flag (default: false)

VERSION HISTORY:
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
STAGE_CORE2_TABLE = _opt("stage_core2_table", "tract_producer_year_stage_core2")
STAGE_BASE_TABLE  = _opt("stage_base_table", "tract_producer_year_stage_base")
STAGE_TABLE       = _opt("stage_table", "tract_producer_year_stage")
STAGE_RESOLVE_TABLE = _opt("stage_resolve_table", "tract_producer_year_stage_resolve")
TASK_MODE         = _opt("task_mode", "single").strip().lower()
SHUFFLE_PARTITIONS = _opt("shuffle_partitions", "800")
MAX_JOB_SECONDS   = int(_opt("max_job_seconds", "1100"))
DEBUG             = _opt("debug", "false").strip().lower() == "true"

VALID_MODES = {
    "single",
    "preprocess",
    "preprocess_base",
    "preprocess_spine",
    "preprocess_structure",
    "preprocess_core2",
    "preprocess_partner",
    "preprocess_enrich",
    "finalize_resolve",
    "finalize_publish",
    "finalize",
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

sc = SparkContext(conf=_conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, args)

job_t0 = time.perf_counter()
STAGE_SPINE_FQN = f"glue_catalog.{TGT_DB}.{STAGE_SPINE_TABLE}"
STAGE_CORE1_FQN = f"glue_catalog.{TGT_DB}.{STAGE_CORE1_TABLE}"
STAGE_CORE2_FQN = f"glue_catalog.{TGT_DB}.{STAGE_CORE2_TABLE}"
STAGE_BASE_FQN = f"glue_catalog.{TGT_DB}.{STAGE_BASE_TABLE}"
STAGE_FQN = f"glue_catalog.{TGT_DB}.{STAGE_TABLE}"
STAGE_RESOLVE_FQN = f"glue_catalog.{TGT_DB}.{STAGE_RESOLVE_TABLE}"
TARGET_FQN = f"glue_catalog.{TGT_DB}.{TGT_TABLE}"

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


def latest_snapshot_row_count(table_fqn: str) -> int:
    try:
        snapshots_df = spark.table(f"{table_fqn}.snapshots")
        snap_row = snapshots_df.orderBy(snapshots_df.committed_at.desc()).select("summary").first()
        summary = snap_row["summary"] if snap_row else None
        return int(summary.get("total-records", -1)) if summary else -1
    except Exception:
        return -1


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
    sys.exit(0)


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

PREPROCESS_STRUCTURE_SQL = """
SELECT
    ib.ibase AS f_ibase,
    ib.ZZFLD000000 AS farm_number,
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
JOIN ibib ib
  ON ib.ibase = st.ibase
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
    CAST(ni.instance AS STRING) AS i_instance,
    CAST(ni.ibase AS STRING) AS r_ibase,
    CAST(ni.in_guid AS STRING) AS in_guid
FROM sss_details_core1_stage core1
JOIN ibin ni
  ON CAST(ni.instance AS STRING) = core1.instance
 AND CAST(ni.client AS STRING) = core1.client
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


def run_preprocess_structure():
    for tbl in ["ibst", "ibib"]:
        register_view(SSS_DB, tbl)
    spark.table(STAGE_SPINE_FQN).createOrReplaceTempView("sss_spine_stage")
    phase_t0 = time.perf_counter()
    stage_log("PP_STRUCTURE_STAGE", "Running preprocess_structure stage")
    core1_df = spark.sql(PREPROCESS_STRUCTURE_SQL)
    core1_df.limit(0).write.format("iceberg").mode("ignore").saveAsTable(STAGE_CORE1_FQN)
    core1_df.write.format("iceberg").mode("overwrite").saveAsTable(STAGE_CORE1_FQN)
    stage_log("PP_STRUCTURE_STAGE", f"[METRIC] phase_preprocess_structure_seconds={time.perf_counter() - phase_t0:.3f}")
    stage_log("PP_STRUCTURE_STAGE", f"[METRIC] stage_core1_row_count={latest_snapshot_row_count(STAGE_CORE1_FQN)}")


def run_preprocess_core2():
    register_view(SSS_DB, "ibin")
    spark.table(STAGE_CORE1_FQN).createOrReplaceTempView("sss_details_core1_stage")
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


if TASK_MODE in ("single", "preprocess", "preprocess_base", "preprocess_spine"):
    run_preprocess_spine()
    if TASK_MODE == "preprocess_spine":
        finish_and_exit("[PP_SPINE_STAGE] Transform-Tract-Producer-Year preprocess_spine: completed successfully")

if TASK_MODE in ("single", "preprocess", "preprocess_base", "preprocess_structure"):
    run_preprocess_structure()
    if TASK_MODE == "preprocess_structure":
        finish_and_exit("[PP_STRUCTURE_STAGE] Transform-Tract-Producer-Year preprocess_structure: completed successfully")

if TASK_MODE in ("single", "preprocess", "preprocess_base", "preprocess_core2"):
    run_preprocess_core2()
    if TASK_MODE == "preprocess_core2":
        finish_and_exit("[PP_CORE2_STAGE] Transform-Tract-Producer-Year preprocess_core2: completed successfully")

if TASK_MODE in ("single", "preprocess", "preprocess_base", "preprocess_partner"):
    run_preprocess_partner()
    if TASK_MODE in ("preprocess_base", "preprocess_partner"):
        finish_and_exit(f"[PP_PARTNER_STAGE] Transform-Tract-Producer-Year {TASK_MODE}: completed successfully")

if TASK_MODE in ("single", "preprocess", "preprocess_enrich"):
    run_preprocess_enrich()
    if TASK_MODE in ("preprocess", "preprocess_enrich"):
        finish_and_exit(f"[PP_ENRICH_STAGE] Transform-Tract-Producer-Year {TASK_MODE}: completed successfully")

if TASK_MODE in ("single", "finalize", "finalize_resolve"):
    run_finalize_resolve()
    if TASK_MODE == "finalize_resolve":
        finish_and_exit("[FINALIZE_RESOLVE_STAGE] Transform-Tract-Producer-Year finalize_resolve: completed successfully")

run_finalize_publish()
job.commit()
enforce_job_timeout()
log.info("[FINALIZE_PUBLISH_STAGE] Transform-Tract-Producer-Year: completed successfully")
