"""
================================================================================
AWS Glue Job: Transform-Tract-Producer-Year
================================================================================

PURPOSE:
    Replaces three Athena SQL files:
        - TRACT_PRODUCER_YEAR_INSERT_ATHENA.sql
        - TRACT_PRODUCER_YEAR_INSERT_NO_COMPARE_ATHENA.sql
        - TRACT_PRODUCER_YEAR_UPDATE_ATHENA.sql

    Executes the same IBase → ZMI → PG resolution CTE chain entirely inside
    Spark (all sources are pre-materialised S3 Iceberg — zero federated hops)
    and applies the result via a single Iceberg MERGE INTO statement that
    handles INSERT (WHEN NOT MATCHED) and UPDATE (WHEN MATCHED AND changed)
    in one atomic S3 pass.

    Expected runtime: 30–90 seconds vs. hours with the original Athena queries.

GLUE JOB ARGUMENTS:
    --JOB_NAME            : Glue job name
    --env                 : Deployment environment
    --iceberg_warehouse   : s3:// URI for Iceberg warehouse root
    --full_load           : "true" skips WHEN MATCHED clause (bulk-load mode)
    --sss_database        : Glue catalog db for SSS Iceberg tables  (default: athenafarm_prod_raw)
    --ref_database        : Glue catalog db for PG ref Iceberg tables (default: athenafarm_prod_ref)
    --target_database     : Glue catalog db for target table (default: athenafarm_prod_gold)
    --target_table        : Target Iceberg table name (default: tract_producer_year)
    --debug               : "true" to enable DEBUG-level CloudWatch logging (default: false)

KEY SAP COLUMN NAMES (raw, as stored in S3 Iceberg after ingest):
    ibib.ZZFLD000000 = farm_number
    ibib.ZZFLD000002 = state_fsa_code
    ibib.ZZFLD000003 = county_fsa_code
    ibib.ZZFLD0000AI = data_status_code (ACTV/IACT/INCR/PEND/DRFT/X)
    ibsp.ZZFLD00000T = tract_number (primary); ZZFLD00001O = tract fallback
    ibsp.ZZFLD00001Z = admin_state_fsa_code
    ibsp.ZZFLD000020 = admin_county_fsa_code
    ibsp.ZZFLD0000B8 = tract_data_status_code
    zmi_farm_partn.ZZ0010/ZZ0011/ZZ0012 = HEL/CW/PCW exception codes
    zmi_farm_partn.ZZ0016/ZZ0017/ZZ0018 = RMA HEL/CW/PCW exception codes
    zmi_farm_partn.ZZ0013/ZZ0014/ZZ0015 = appeal exhaustion dates
    zmi_farm_partn.VALID_FROM/VALID_TO  = producer involvement date range
    crmd_partner.partner_fct            = ZFARMONR→162, ZOTNT→163

ICEBERG CONFIGURATION (set via --conf in Glue job definition):
    spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
    spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog
    spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
    spark.sql.catalog.glue_catalog.warehouse=<iceberg_warehouse>
    spark.sql.autoBroadcastJoinThreshold=52428800   (50 MB — broadcasts small ref tables)

VERSION HISTORY:
    v1.0.0 - 2026-02-25 - Initial implementation (athenafarm project)
              Replaces TRACT_PRODUCER_YEAR_INSERT/UPDATE Athena SQL files.

================================================================================
"""

import sys
import logging
import traceback
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

JOB_NAME          = args["JOB_NAME"]
ENV               = args["env"]
ICEBERG_WAREHOUSE = args["iceberg_warehouse"]
FULL_LOAD         = args.get("full_load", "false").strip().lower() == "true"
SSS_DB            = args.get("sss_database", "athenafarm_prod_raw")
REF_DB            = args.get("ref_database", "athenafarm_prod_ref")
TGT_DB            = args.get("target_database", "athenafarm_prod_gold")
TGT_TABLE         = args.get("target_table", "tract_producer_year")
DEBUG             = args.get("debug", "false").strip().lower() == "true"

if DEBUG:
    logging.getLogger().setLevel(logging.DEBUG)
    log.setLevel(logging.DEBUG)
    log.debug("DEBUG logging enabled")

# All catalog properties must be set in SparkConf BEFORE SparkContext is
# created.  Setting them after-the-fact via spark.conf.set() registers the
# warehouse path but does NOT register glue_catalog as a SparkCatalog, causing:
#   java.util.NoSuchElementException: None.get  (DataSourceV2Utils.loadV2Source)
_conf = SparkConf()
_conf.set("spark.sql.catalog.glue_catalog",              "org.apache.iceberg.spark.SparkCatalog")
_conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
_conf.set("spark.sql.catalog.glue_catalog.io-impl",      "org.apache.iceberg.aws.s3.S3FileIO")
_conf.set("spark.sql.catalog.glue_catalog.warehouse",    ICEBERG_WAREHOUSE)
# Broadcast small reference tables (time_period, county_office_control) rather
# than shuffle-joining them.  Default threshold in Spark 3.x is 10 MB; 50 MB
# allows Iceberg's table statistics to push these into broadcast.
_conf.set("spark.sql.autoBroadcastJoinThreshold",        str(50 * 1024 * 1024))
sc = SparkContext(conf=_conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, args)

log.info("=" * 70)
log.info(f"Job            : {JOB_NAME}")
log.info(f"Env            : {ENV}")
log.info(f"Warehouse      : {ICEBERG_WAREHOUSE}")
log.info(f"SSS DB         : {SSS_DB}")
log.info(f"Ref DB         : {REF_DB}")
log.info(f"Target DB      : {TGT_DB}")
log.info(f"Target Table   : {TGT_TABLE}")
log.info(f"Full Load      : {FULL_LOAD}")
log.info(f"Debug          : {DEBUG}")
log.info("=" * 70)

# ---------------------------------------------------------------------------
# Register all Iceberg source tables as temporary views so the CTE SQL
# matches the structure of the original Athena queries as closely as possible.
# ---------------------------------------------------------------------------

def register_view(catalog_db: str, table: str, view_name: str = None):
    fqn = f"glue_catalog.{catalog_db}.{table}"
    vn = view_name or table
    spark.table(fqn).createOrReplaceTempView(vn)
    log.info(f"Registered view [{vn}] → {fqn}")

# SSS tables — raw SAP column names are preserved; CTE SQL references them directly
for tbl in ["ibib", "ibsp", "ibst", "ibin", "ibpart", "crmd_partner",
            "z_ibase_comp_detail", "comm_pr_frg_rel", "zmi_farm_partn"]:
    register_view(SSS_DB, tbl)

# PG reference tables
for tbl in ["time_period", "county_office_control", "farm", "tract", "farm_year", "tract_year", "but000"]:
    register_view(REF_DB, tbl)

log.info("All source views registered — executing CTE transform")

# ---------------------------------------------------------------------------
# CTE transform query
# Mirrors the join chain from TRACT_PRODUCER_YEAR_INSERT_ATHENA.sql /
# TRACT_PRODUCER_YEAR_UPDATE_ATHENA.sql with no federated hops.
# ---------------------------------------------------------------------------
TRANSFORM_SQL = f"""
-- ==========================================================================
-- Faithful Spark translation of TRACT_PRODUCER_YEAR_INSERT_ATHENA.sql and
-- TRACT_PRODUCER_YEAR_UPDATE_ATHENA.sql.  All source tables are pre-
-- materialised S3 Iceberg (no federated hops).  SAP raw column names (ZZFLD*)
-- are preserved exactly as they exist in the Athena/Iceberg catalog.
-- ==========================================================================

WITH time_pd AS (
    SELECT time_period_identifier,
           time_period_name,
           time_period_start_date,
           time_period_end_date
    FROM   time_period
    WHERE  data_status_code = 'A'
),

-- ── IBase chain: ibsp → ibst (parent='0') → ibib ──────────────────────────
-- plus ibin, ibpart (segment=2, non-expired), crmd_partner
-- Mirrors the sss_details CTE in the original SQL.
sss_details AS (
    SELECT
        -- Farm header (ibib) fields
        ib.ibase                                        AS f_ibase,
        TO_DATE(date_format(
            COALESCE(ib.crtim, CURRENT_TIMESTAMP), 'yyyy-MM-dd'))  AS f_creation_date,
        TO_DATE(date_format(
            COALESCE(ib.UPTIM, CURRENT_TIMESTAMP), 'yyyy-MM-dd'))  AS f_last_change_date,
        LPAD(ib.ZZFLD000002, 2, '0')                   AS state_fsa_code,
        LPAD(ib.ZZFLD000003, 3, '0')                   AS county_fsa_code,
        ib.ZZFLD000000                                  AS farm_number,   -- farm number (raw, no LPAD yet)

        -- Tract component (ibsp) fields
        CASE
            WHEN sp.ZZFLD00000T IS NOT NULL AND sp.ZZFLD00000T <> '0'
                THEN LPAD(sp.ZZFLD00000T, 7, '0')
            WHEN sp.ZZFLD00001O IS NOT NULL AND TRIM(sp.ZZFLD00001O) <> ' '
                THEN LPAD(SPLIT(sp.ZZFLD00001O, '-')[3], 7, '0')
            ELSE '0000000'
        END                                             AS tract_number,
        sp.instance                                     AS t_instance,
        LPAD(sp.ZZFLD00001Z, 2, '0')                   AS admin_state,    -- ibsp admin state FSA code
        LPAD(sp.ZZFLD000020, 3, '0')                   AS admin_county,   -- ibsp admin county FSA code
        CASE sp.ZZFLD0000B8
            WHEN 'ACTV' THEN 'A'  WHEN 'IACT' THEN 'I'
            WHEN 'DELE' THEN 'D'  WHEN 'PEND' THEN 'P'  ELSE 'A'
        END                                             AS t_data_status_code,
        TO_DATE(date_format(
            COALESCE(sp.crtim, CURRENT_TIMESTAMP), 'yyyy-MM-dd'))  AS t_creation_date,
        TO_DATE(date_format(
            COALESCE(sp.UPTIM, CURRENT_TIMESTAMP), 'yyyy-MM-dd'))  AS t_last_change_date,
        CASE
            WHEN sp.upnam IS NOT NULL AND TRIM(sp.upnam) NOT IN ('0', '', ')')
                THEN TRIM(sp.upnam)
            WHEN sp.crnam IS NOT NULL AND TRIM(sp.crnam) NOT IN ('0', '', ')')
                THEN TRIM(sp.crnam)
            ELSE 'BLANK'
        END                                             AS t_last_change_user_name,

        -- IBase instance link (ibin)
        ni.instance                                     AS i_instance,
        ni.ibase                                        AS r_ibase,

        -- CRM partner role fields
        cr.partner_fct,
        cr.partner_no

    FROM ibsp sp
    JOIN ibst         st ON st.instance  = sp.instance AND st.parent = '0'
    JOIN ibib         ib ON ib.ibase     = st.ibase
    JOIN ibin         ni ON ni.instance  = sp.instance AND ni.client = sp.client
    JOIN ibpart       pt ON pt.segment_recno = ni.in_guid
                         AND pt.segment       = 2
                         AND pt.valto         = 99991231235959
    JOIN crmd_partner cr ON cr.guid = pt.partnerset
),

-- ── ZMI chain: z_ibase_comp_detail → comm_pr_frg_rel → zmi_farm_partn ─────
-- All exception codes and involvement dates come from zmi_farm_partn (ZZ* fields).
-- Mirrors the zmi_details CTE in the original SQL.
zmi_details AS (
    SELECT
        CAST(zd.instance AS STRING)           AS instance,
        CAST(zd.ibase    AS STRING)           AS ibase,
        zm.ZZK0011,

        -- Tract-level exception codes (CASE mappings identical to original SQL)
        CASE TRIM(zm.ZZ0011)
            WHEN 'AE' THEN 41  WHEN 'AR' THEN 40  WHEN 'HA' THEN 40
            WHEN 'NP' THEN 45  WHEN 'NW' THEN 45  WHEN 'TP' THEN 44
            WHEN 'TR' THEN 44  ELSE NULL
        END                                   AS tract_producer_cw_exception_code,
        CASE TRIM(zm.ZZ0010)
            WHEN 'GF'  THEN 31  WHEN 'EH'  THEN 34  WHEN 'AE'  THEN 33
            WHEN 'NAR' THEN 33  WHEN 'AR'  THEN 32  WHEN 'HA'  THEN 32
            WHEN 'LT'  THEN 30  ELSE NULL
        END                                   AS tract_producer_hel_exception_code,
        CASE TRIM(zm.ZZ0012)
            WHEN 'AR'  THEN 52  WHEN 'HA'  THEN 52  WHEN 'NAR' THEN 50
            WHEN 'AE'  THEN 50  WHEN 'GF'  THEN 51  ELSE NULL
        END                                   AS tract_producer_pcw_exception_code,

        -- RMA exception codes
        CASE TRIM(zm.ZZ0017)
            WHEN 'WR'  THEN 43  WHEN 'GF'  THEN 42  WHEN 'NAR' THEN 41
            WHEN 'AR'  THEN 40  WHEN 'NP'  THEN 45  WHEN 'TP'  THEN 44
            ELSE NULL
        END                                   AS rma_cw_exception_code,
        CASE TRIM(zm.ZZ0016)
            WHEN 'GF'  THEN 31  WHEN 'EH'  THEN 34  WHEN 'NAR' THEN 33
            WHEN 'AR'  THEN 32  WHEN 'LT'  THEN 30  ELSE NULL
        END                                   AS rma_hel_exception_code,
        CASE TRIM(zm.ZZ0018)
            WHEN 'AR'  THEN 52  WHEN 'HA'  THEN 52  WHEN 'NAR' THEN 50
            WHEN 'AE'  THEN 50  WHEN 'GF'  THEN 51  ELSE NULL
        END                                   AS rma_pcw_exception_code,

        -- Appeal exhaustion dates (Athena date_parse → Spark TO_DATE)
        CASE WHEN zm.ZZ0013 IS NOT NULL
             THEN TO_DATE(CAST(zm.ZZ0013 AS STRING), 'yyyyMMddHHmmss')
             ELSE NULL END                    AS hel_appeals_exhausted_date,
        CASE WHEN zm.ZZ0014 IS NOT NULL
             THEN TO_DATE(CAST(zm.ZZ0014 AS STRING), 'yyyyMMddHHmmss')
             ELSE NULL END                    AS cw_appeals_exhausted_date,
        CASE WHEN zm.ZZ0015 IS NOT NULL
             THEN TO_DATE(CAST(zm.ZZ0015 AS STRING), 'yyyyMMddHHmmss')
             ELSE NULL END                    AS pcw_appeals_exhausted_date,

        -- Involvement dates (Athena date_parse with %Y%m%d%H%i%S → Spark TO_DATE)
        CASE WHEN zm.VALID_FROM IS NOT NULL AND zm.VALID_FROM <> 0
             THEN TO_DATE(CAST(zm.VALID_FROM AS STRING), 'yyyyMMddHHmmss')
             ELSE NULL END                    AS producer_involvement_start_date,
        CASE WHEN zm.VALID_TO IS NOT NULL AND zm.VALID_TO <> 0
             THEN TO_DATE(CAST(zm.VALID_TO AS STRING), 'yyyyMMddHHmmss')
             ELSE NULL END                    AS producer_involvement_end_date

    FROM z_ibase_comp_detail zd
    JOIN comm_pr_frg_rel     cf ON cf.product_guid = zd.prod_objnr
    JOIN zmi_farm_partn      zm ON zm.frg_guid     = cf.fragment_guid
),

-- ── Resolve current-program-year time period & RDS surrogate keys ──────────
-- Mirrors tract_producer_year_current CTE in original SQL.
-- Note: program year is year+1 when month >= October (FSA fiscal year).
tract_producer_year_current AS (
    SELECT
        coc.county_office_control_identifier,
        timepd.time_period_identifier,
        CAST(c.bpext AS INT)                            AS core_customer_identifier,
        z_dets.producer_involvement_start_date,
        z_dets.producer_involvement_end_date,
        CAST(NULL AS STRING)                            AS producer_involvement_interrupted_indicator,
        z_dets.tract_producer_hel_exception_code,
        z_dets.tract_producer_cw_exception_code,
        z_dets.tract_producer_pcw_exception_code,
        sss_dets.t_data_status_code                     AS data_status_code,
        sss_dets.t_creation_date                        AS creation_date,
        sss_dets.t_last_change_date                     AS last_change_date,
        sss_dets.t_last_change_user_name                AS last_change_user_name,
        -- producer_involvement_code: partner_fct maps to integer (ZFARMONR=162, ZOTNT=163)
        CASE sss_dets.partner_fct
            WHEN 'ZFARMONR' THEN 162
            WHEN 'ZOTNT'    THEN 163
        END                                             AS producer_involvement_code,
        sss_dets.admin_state                            AS state_fsa_code,
        sss_dets.admin_county                           AS county_fsa_code,
        LPAD(sss_dets.farm_number, 7, '0')              AS farm_number,
        sss_dets.tract_number,
        z_dets.hel_appeals_exhausted_date,
        z_dets.cw_appeals_exhausted_date,
        z_dets.pcw_appeals_exhausted_date,
        z_dets.rma_hel_exception_code                   AS tract_producer_rma_hel_exception_code,
        z_dets.rma_cw_exception_code                    AS tract_producer_rma_cw_exception_code,
        z_dets.rma_pcw_exception_code                   AS tract_producer_rma_pcw_exception_code

    FROM sss_details sss_dets
    -- Current program year: month >= 10 → use year+1 (FSA fiscal year boundary)
    JOIN time_pd timepd
        ON CAST(
               CASE WHEN MONTH(CURRENT_DATE) >= 10
                    THEN YEAR(CURRENT_DATE) + 1
                    ELSE YEAR(CURRENT_DATE)
               END AS STRING
           ) = TRIM(timepd.time_period_name)
    -- ZMI join on ibase instance (matches z_ibase_comp_detail.instance/ibase)
    JOIN zmi_details z_dets
        ON z_dets.instance = sss_dets.i_instance
        AND z_dets.ibase   = sss_dets.r_ibase
    -- County office control resolution by admin state/county + time window
    JOIN county_office_control coc
        ON LPAD(sss_dets.admin_state,  2, '0') || LPAD(sss_dets.admin_county, 3, '0')
         = LPAD(coc.state_fsa_code,    2, '0') || LPAD(coc.county_fsa_code,   3, '0')
        AND timepd.time_period_identifier = coc.time_period_identifier
    -- Business partner (but000): two-field join matches original SQL exactly
    LEFT JOIN but000 c
        ON sss_dets.partner_no = c.partner_guid
        AND z_dets.ZZK0011     = c.partner
    WHERE timepd.time_period_name >= '2014'
      AND c.bpext NOT IN ('DUPLICATE', '11876423_D')
),

-- ── Resolve farm_records_reporting surrogate keys + deduplicate ────────────
-- Mirrors tract_producer_year_tbl CTE in original SQL.
tract_producer_year_tbl AS (
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
    FROM tract_producer_year_current tprdryr
    JOIN farm     farmpg ON farmpg.county_office_control_identifier = tprdryr.county_office_control_identifier
                         AND LPAD(CAST(farmpg.farm_number AS STRING), 7, '0')
                           = LPAD(tprdryr.farm_number, 7, '0')
    JOIN tract    tractpg ON tractpg.county_office_control_identifier = tprdryr.county_office_control_identifier
                          AND LPAD(CAST(tractpg.tract_number AS STRING), 7, '0')
                            = LPAD(tprdryr.tract_number, 7, '0')
    JOIN farm_year  fy   ON farmpg.farm_identifier       = fy.farm_identifier
                         AND tprdryr.time_period_identifier = fy.time_period_identifier
    JOIN tract_year ty   ON fy.farm_year_identifier      = ty.farm_year_identifier
                         AND tractpg.tract_identifier     = ty.tract_identifier
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

log.info("Running CTE transform query")
source_df = spark.sql(TRANSFORM_SQL)
# Cache so .count() and the subsequent MERGE INTO share one materialisation.
source_df.cache()
source_df.createOrReplaceTempView("new_tract_producer_year")
log.info(f"Transform produced {source_df.count():,} source rows")

# ---------------------------------------------------------------------------
# Iceberg MERGE INTO  (arch.md §3.3)
# Replaces separate INSERT + UPDATE Athena queries in one atomic S3 pass.
# ---------------------------------------------------------------------------
TARGET_FQN = f"glue_catalog.{TGT_DB}.{TGT_TABLE}"

# First run safety: create empty Iceberg target table if it does not exist yet.
# This prevents AnalysisException on initial deploy while still allowing MERGE
# to perform inserts/updates against a stable schema.
spark.sql(
    f"CREATE TABLE IF NOT EXISTS {TARGET_FQN} "
    f"USING iceberg AS SELECT * FROM new_tract_producer_year WHERE 1 = 0"
)

if FULL_LOAD:
    # --full_load flag: skip WHEN MATCHED, only insert (replaces INSERT_NO_COMPARE)
    MERGE_SQL = f"""
    MERGE INTO {TARGET_FQN} t
    USING new_tract_producer_year s
    ON  t.core_customer_identifier  = s.core_customer_identifier
    AND t.tract_year_identifier     = s.tract_year_identifier
    AND t.producer_involvement_code = s.producer_involvement_code

    WHEN NOT MATCHED THEN INSERT (
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
    ) VALUES (
        s.core_customer_identifier,
        s.tract_year_identifier,
        s.producer_involvement_start_date,
        s.producer_involvement_end_date,
        s.producer_involvement_interrupted_indicator,
        s.tract_producer_hel_exception_code,
        s.tract_producer_cw_exception_code,
        s.tract_producer_pcw_exception_code,
        s.data_status_code,
        s.creation_date,
        s.last_change_date,
        s.last_change_user_name,
        s.producer_involvement_code,
        s.time_period_identifier,
        s.state_fsa_code,
        s.county_fsa_code,
        s.farm_identifier,
        s.farm_number,
        s.tract_number,
        s.hel_appeals_exhausted_date,
        s.cw_appeals_exhausted_date,
        s.pcw_appeals_exhausted_date,
        s.tract_producer_rma_hel_exception_code,
        s.tract_producer_rma_cw_exception_code,
        s.tract_producer_rma_pcw_exception_code
    )
    """
    log.info("Full-load mode: MERGE INTO — WHEN NOT MATCHED only")
else:
    # Incremental: INSERT new rows + UPDATE changed rows (replaces INSERT + UPDATE Athena files)
    MERGE_SQL = f"""
    MERGE INTO {TARGET_FQN} t
    USING new_tract_producer_year s
    ON  t.core_customer_identifier  = s.core_customer_identifier
    AND t.tract_year_identifier     = s.tract_year_identifier
    AND t.producer_involvement_code = s.producer_involvement_code

    WHEN MATCHED AND (
            -- NOT (a <=> b) is Spark SQL's null-safe not-equal (≡ IS DISTINCT FROM).
            -- Using <> would silently skip updates where either side is NULL, e.g.
            -- when an exception code is removed (changed from a value to NULL).
            NOT (t.producer_involvement_start_date   <=> s.producer_involvement_start_date)  OR
            NOT (t.producer_involvement_end_date     <=> s.producer_involvement_end_date)    OR
            NOT (t.tract_producer_hel_exception_code <=> s.tract_producer_hel_exception_code) OR
            NOT (t.tract_producer_cw_exception_code  <=> s.tract_producer_cw_exception_code)  OR
            NOT (t.tract_producer_pcw_exception_code <=> s.tract_producer_pcw_exception_code) OR
            NOT (t.data_status_code                  <=> s.data_status_code)
    ) THEN UPDATE SET
        producer_involvement_start_date       = s.producer_involvement_start_date,
        producer_involvement_end_date         = s.producer_involvement_end_date,
        tract_producer_hel_exception_code     = s.tract_producer_hel_exception_code,
        tract_producer_cw_exception_code      = s.tract_producer_cw_exception_code,
        tract_producer_pcw_exception_code     = s.tract_producer_pcw_exception_code,
        data_status_code                      = s.data_status_code,
        last_change_date                      = s.last_change_date,
        last_change_user_name                 = s.last_change_user_name,
        hel_appeals_exhausted_date            = s.hel_appeals_exhausted_date,
        cw_appeals_exhausted_date             = s.cw_appeals_exhausted_date,
        pcw_appeals_exhausted_date            = s.pcw_appeals_exhausted_date,
        tract_producer_rma_hel_exception_code = s.tract_producer_rma_hel_exception_code,
        tract_producer_rma_cw_exception_code  = s.tract_producer_rma_cw_exception_code,
        tract_producer_rma_pcw_exception_code = s.tract_producer_rma_pcw_exception_code

    WHEN NOT MATCHED THEN INSERT (
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
    ) VALUES (
        s.core_customer_identifier,
        s.tract_year_identifier,
        s.producer_involvement_start_date,
        s.producer_involvement_end_date,
        s.producer_involvement_interrupted_indicator,
        s.tract_producer_hel_exception_code,
        s.tract_producer_cw_exception_code,
        s.tract_producer_pcw_exception_code,
        s.data_status_code,
        s.creation_date,
        s.last_change_date,
        s.last_change_user_name,
        s.producer_involvement_code,
        s.time_period_identifier,
        s.state_fsa_code,
        s.county_fsa_code,
        s.farm_identifier,
        s.farm_number,
        s.tract_number,
        s.hel_appeals_exhausted_date,
        s.cw_appeals_exhausted_date,
        s.pcw_appeals_exhausted_date,
        s.tract_producer_rma_hel_exception_code,
        s.tract_producer_rma_cw_exception_code,
        s.tract_producer_rma_pcw_exception_code
    )
    """
    log.info("Incremental mode: MERGE INTO — WHEN MATCHED UPDATE + WHEN NOT MATCHED INSERT")

log.info(f"Executing MERGE INTO {TARGET_FQN}")
spark.sql(MERGE_SQL)
source_df.unpersist()
log.info("MERGE INTO complete")

# ---------------------------------------------------------------------------
# Post-merge row-count metric from Iceberg snapshot metadata — reads one
# small JSON manifest file, not the full Parquet data files.
# ---------------------------------------------------------------------------
try:
    snap_row = spark.sql(
        f"SELECT summary['total-records'] AS n "
        f"FROM glue_catalog.{TGT_DB}.{TGT_TABLE}.snapshots "
        f"ORDER BY committed_at DESC LIMIT 1"
    ).first()
    final_count = int(snap_row["n"]) if snap_row else -1
except Exception:
    final_count = -1  # metadata unavailable — non-fatal
log.info(f"[METRIC] {TGT_TABLE}_row_count={final_count}")

job.commit()
log.info("Transform-Tract-Producer-Year: completed successfully")
