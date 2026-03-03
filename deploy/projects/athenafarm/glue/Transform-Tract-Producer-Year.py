"""
================================================================================
AWS Glue Job: Transform-Tract-Producer-Year
================================================================================

PURPOSE:
    Builds tract-producer-year records with a Spark DataFrame pipeline over
    pre-materialised S3 Iceberg sources (zero federated hops), then writes a
    full snapshot to the target Iceberg table in overwrite mode.

GLUE JOB ARGUMENTS:
    --JOB_NAME            : Glue job name
    --env                 : Deployment environment
    --iceberg_warehouse   : s3:// URI for Iceberg warehouse root
    --full_load           : accepted for compatibility; job always runs full-load
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
              Initial Spark DataFrame transform for tract_producer_year.

================================================================================
"""

import sys
import logging
import traceback
import time
from awsglue.utils import getResolvedOptions
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql.window import Window
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
SHUFFLE_PARTITIONS = _opt("shuffle_partitions", "200")
DEBUG             = _opt("debug", "false").strip().lower() == "true"

if DEBUG:
    logging.getLogger().setLevel(logging.DEBUG)
    log.setLevel(logging.DEBUG)
    log.debug("DEBUG logging enabled")

if not _requested_full_load:
    log.warning("Incremental mode is disabled for this job; forcing full-load execution")

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
_conf.set("spark.sql.adaptive.enabled",                  "true")
_conf.set("spark.sql.adaptive.skewJoin.enabled",         "true")
_conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
_conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
_conf.set("spark.sql.shuffle.partitions",                SHUFFLE_PARTITIONS)
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
log.info(f"Shuffle Parts  : {SHUFFLE_PARTITIONS}")
log.info(f"Full Load      : {FULL_LOAD}")
log.info(f"[METRIC] mode={'full_load' if FULL_LOAD else 'incremental'}")
log.info(f"Debug          : {DEBUG}")
log.info("=" * 70)

log.info("Running tract transform (DataFrame API)")
phase_t0 = time.perf_counter()

ibsp = spark.table(f"glue_catalog.{SSS_DB}.ibsp").alias("sp")
ibst = spark.table(f"glue_catalog.{SSS_DB}.ibst").alias("st")
ibib = spark.table(f"glue_catalog.{SSS_DB}.ibib").alias("ib")
ibin = spark.table(f"glue_catalog.{SSS_DB}.ibin").alias("ni")
ibpart = spark.table(f"glue_catalog.{SSS_DB}.ibpart").alias("pt")
crmd_partner = spark.table(f"glue_catalog.{SSS_DB}.crmd_partner").alias("cr")
z_ibase_comp_detail = spark.table(f"glue_catalog.{SSS_DB}.z_ibase_comp_detail").alias("zd")
comm_pr_frg_rel = spark.table(f"glue_catalog.{SSS_DB}.comm_pr_frg_rel").alias("cf")
zmi_farm_partn = spark.table(f"glue_catalog.{SSS_DB}.zmi_farm_partn").alias("zm")

time_period = spark.table(f"glue_catalog.{REF_DB}.time_period").alias("timepd")
county_office_control = spark.table(f"glue_catalog.{REF_DB}.county_office_control").alias("coc")
farm = spark.table(f"glue_catalog.{REF_DB}.farm").alias("farmpg")
tract = spark.table(f"glue_catalog.{REF_DB}.tract").alias("tractpg")
farm_year = spark.table(f"glue_catalog.{REF_DB}.farm_year").alias("fy")
tract_year = spark.table(f"glue_catalog.{REF_DB}.tract_year").alias("ty")
but000 = spark.table(f"glue_catalog.{REF_DB}.but000").alias("c")

time_pd = time_period.where(F.col("data_status_code") == F.lit("A")).select(
    "time_period_identifier", "time_period_name", "time_period_start_date", "time_period_end_date"
)

tract_number_expr = (
    F.when(
        F.col("sp.ZZFLD00000T").isNotNull() & (F.col("sp.ZZFLD00000T") != F.lit("0")),
        F.lpad(F.col("sp.ZZFLD00000T").cast("string"), 7, "0"),
    )
    .when(
        F.col("sp.ZZFLD00001O").isNotNull() & (F.trim(F.col("sp.ZZFLD00001O")) != F.lit("")),
        F.lpad(F.split(F.col("sp.ZZFLD00001O"), "-").getItem(3), 7, "0"),
    )
    .otherwise(F.lit("0000000"))
)

t_status_expr = (
    F.when(F.col("sp.ZZFLD0000B8") == F.lit("ACTV"), F.lit("A"))
    .when(F.col("sp.ZZFLD0000B8") == F.lit("IACT"), F.lit("I"))
    .when(F.col("sp.ZZFLD0000B8") == F.lit("DELE"), F.lit("D"))
    .when(F.col("sp.ZZFLD0000B8") == F.lit("PEND"), F.lit("P"))
    .otherwise(F.lit("A"))
)

upnam_clean = F.trim(F.col("sp.upnam"))
crnam_clean = F.trim(F.col("sp.crnam"))
t_last_change_user_expr = (
    F.when(
        F.col("sp.upnam").isNotNull() & (~upnam_clean.isin("0", "")),
        upnam_clean,
    )
    .when(
        F.col("sp.crnam").isNotNull() & (~crnam_clean.isin("0", "")),
        crnam_clean,
    )
    .otherwise(F.lit("BLANK"))
)

sss_details = (
    ibsp.join(ibst, (F.col("st.instance") == F.col("sp.instance")) & (F.col("st.parent") == F.lit("0")), "inner")
    .join(ibib, F.col("ib.ibase") == F.col("st.ibase"), "inner")
    .join(ibin, (F.col("ni.instance") == F.col("sp.instance")) & (F.col("ni.client") == F.col("sp.client")), "inner")
    .join(
        ibpart,
        (F.col("pt.segment_recno") == F.col("ni.in_guid"))
        & (F.col("pt.segment") == F.lit(2))
        & (F.col("pt.valto") == F.lit(99991231235959)),
        "inner",
    )
    .join(crmd_partner, F.col("cr.guid") == F.col("pt.partnerset"), "inner")
    .select(
        F.col("ib.ibase").alias("f_ibase"),
        F.to_date(F.date_format(F.coalesce(F.col("ib.crtim"), F.current_timestamp()), "yyyy-MM-dd")).alias("f_creation_date"),
        F.to_date(F.date_format(F.coalesce(F.col("ib.UPTIM"), F.current_timestamp()), "yyyy-MM-dd")).alias("f_last_change_date"),
        F.lpad(F.col("ib.ZZFLD000002").cast("string"), 2, "0").alias("state_fsa_code"),
        F.lpad(F.col("ib.ZZFLD000003").cast("string"), 3, "0").alias("county_fsa_code"),
        F.col("ib.ZZFLD000000").alias("farm_number"),
        tract_number_expr.alias("tract_number"),
        F.col("sp.instance").alias("t_instance"),
        F.lpad(F.col("sp.ZZFLD00001Z").cast("string"), 2, "0").alias("admin_state"),
        F.lpad(F.col("sp.ZZFLD000020").cast("string"), 3, "0").alias("admin_county"),
        t_status_expr.alias("t_data_status_code"),
        F.to_date(F.date_format(F.coalesce(F.col("sp.crtim"), F.current_timestamp()), "yyyy-MM-dd")).alias("t_creation_date"),
        F.to_date(F.date_format(F.coalesce(F.col("sp.UPTIM"), F.current_timestamp()), "yyyy-MM-dd")).alias("t_last_change_date"),
        t_last_change_user_expr.alias("t_last_change_user_name"),
        F.col("ni.instance").alias("i_instance"),
        F.col("ni.ibase").alias("r_ibase"),
        F.col("cr.partner_fct").alias("partner_fct"),
        F.col("cr.partner_no").alias("partner_no"),
    )
)

cw_exc_expr = (
    F.when(F.trim(F.col("zm.ZZ0011")) == F.lit("AE"), F.lit(41))
    .when(F.trim(F.col("zm.ZZ0011")).isin("AR", "HA"), F.lit(40))
    .when(F.trim(F.col("zm.ZZ0011")).isin("NP", "NW"), F.lit(45))
    .when(F.trim(F.col("zm.ZZ0011")).isin("TP", "TR"), F.lit(44))
)
hel_exc_expr = (
    F.when(F.trim(F.col("zm.ZZ0010")) == F.lit("GF"), F.lit(31))
    .when(F.trim(F.col("zm.ZZ0010")) == F.lit("EH"), F.lit(34))
    .when(F.trim(F.col("zm.ZZ0010")).isin("AE", "NAR"), F.lit(33))
    .when(F.trim(F.col("zm.ZZ0010")).isin("AR", "HA"), F.lit(32))
    .when(F.trim(F.col("zm.ZZ0010")) == F.lit("LT"), F.lit(30))
)
pcw_exc_expr = (
    F.when(F.trim(F.col("zm.ZZ0012")).isin("AR", "HA"), F.lit(52))
    .when(F.trim(F.col("zm.ZZ0012")).isin("NAR", "AE"), F.lit(50))
    .when(F.trim(F.col("zm.ZZ0012")) == F.lit("GF"), F.lit(51))
)
rma_cw_expr = (
    F.when(F.trim(F.col("zm.ZZ0017")) == F.lit("WR"), F.lit(43))
    .when(F.trim(F.col("zm.ZZ0017")) == F.lit("GF"), F.lit(42))
    .when(F.trim(F.col("zm.ZZ0017")) == F.lit("NAR"), F.lit(41))
    .when(F.trim(F.col("zm.ZZ0017")) == F.lit("AR"), F.lit(40))
    .when(F.trim(F.col("zm.ZZ0017")) == F.lit("NP"), F.lit(45))
    .when(F.trim(F.col("zm.ZZ0017")) == F.lit("TP"), F.lit(44))
)
rma_hel_expr = (
    F.when(F.trim(F.col("zm.ZZ0016")) == F.lit("GF"), F.lit(31))
    .when(F.trim(F.col("zm.ZZ0016")) == F.lit("EH"), F.lit(34))
    .when(F.trim(F.col("zm.ZZ0016")) == F.lit("NAR"), F.lit(33))
    .when(F.trim(F.col("zm.ZZ0016")) == F.lit("AR"), F.lit(32))
    .when(F.trim(F.col("zm.ZZ0016")) == F.lit("LT"), F.lit(30))
)
rma_pcw_expr = (
    F.when(F.trim(F.col("zm.ZZ0018")).isin("AR", "HA"), F.lit(52))
    .when(F.trim(F.col("zm.ZZ0018")).isin("NAR", "AE"), F.lit(50))
    .when(F.trim(F.col("zm.ZZ0018")) == F.lit("GF"), F.lit(51))
)

zmi_details = (
    z_ibase_comp_detail.join(comm_pr_frg_rel, F.col("cf.product_guid") == F.col("zd.prod_objnr"), "inner")
    .join(zmi_farm_partn, F.col("zm.frg_guid") == F.col("cf.fragment_guid"), "inner")
    .select(
        F.col("zd.instance").cast("string").alias("instance"),
        F.col("zd.ibase").cast("string").alias("ibase"),
        F.col("zm.ZZK0011").alias("ZZK0011"),
        hel_exc_expr.alias("tract_producer_hel_exception_code"),
        cw_exc_expr.alias("tract_producer_cw_exception_code"),
        pcw_exc_expr.alias("tract_producer_pcw_exception_code"),
        rma_cw_expr.alias("rma_cw_exception_code"),
        rma_hel_expr.alias("rma_hel_exception_code"),
        rma_pcw_expr.alias("rma_pcw_exception_code"),
        F.when(F.col("zm.ZZ0013").isNotNull(), F.to_date(F.col("zm.ZZ0013").cast("string"), "yyyyMMddHHmmss")).alias("hel_appeals_exhausted_date"),
        F.when(F.col("zm.ZZ0014").isNotNull(), F.to_date(F.col("zm.ZZ0014").cast("string"), "yyyyMMddHHmmss")).alias("cw_appeals_exhausted_date"),
        F.when(F.col("zm.ZZ0015").isNotNull(), F.to_date(F.col("zm.ZZ0015").cast("string"), "yyyyMMddHHmmss")).alias("pcw_appeals_exhausted_date"),
        F.when(
            F.col("zm.VALID_FROM").isNotNull() & (F.col("zm.VALID_FROM") != F.lit(0)),
            F.to_date(F.col("zm.VALID_FROM").cast("string"), "yyyyMMddHHmmss"),
        ).alias("producer_involvement_start_date"),
        F.when(
            F.col("zm.VALID_TO").isNotNull() & (F.col("zm.VALID_TO") != F.lit(0)),
            F.to_date(F.col("zm.VALID_TO").cast("string"), "yyyyMMddHHmmss"),
        ).alias("producer_involvement_end_date"),
    )
)

program_year_name = F.when(F.month(F.current_date()) >= F.lit(10), F.year(F.current_date()) + F.lit(1)).otherwise(
    F.year(F.current_date())
).cast("string")

producer_code_expr = (
    F.when(F.col("sss_dets.partner_fct") == F.lit("ZFARMONR"), F.lit(162))
    .when(F.col("sss_dets.partner_fct") == F.lit("ZOTNT"), F.lit(163))
)

tract_current = (
    sss_details.alias("sss_dets")
    .join(time_pd.alias("timepd"), program_year_name == F.trim(F.col("timepd.time_period_name")), "inner")
    .join(
        zmi_details.alias("z_dets"),
        (F.col("z_dets.instance") == F.col("sss_dets.i_instance")) & (F.col("z_dets.ibase") == F.col("sss_dets.r_ibase")),
        "inner",
    )
    .join(
        county_office_control.alias("coc"),
        (
            F.concat(F.lpad(F.col("sss_dets.admin_state"), 2, "0"), F.lpad(F.col("sss_dets.admin_county"), 3, "0"))
            == F.concat(F.lpad(F.col("coc.state_fsa_code"), 2, "0"), F.lpad(F.col("coc.county_fsa_code"), 3, "0"))
        )
        & (F.col("timepd.time_period_identifier") == F.col("coc.time_period_identifier")),
        "inner",
    )
    .join(
        but000.alias("c"),
        (F.col("sss_dets.partner_no") == F.col("c.partner_guid")) & (F.col("z_dets.ZZK0011") == F.col("c.partner")),
        "left",
    )
    .where((F.col("timepd.time_period_name") >= F.lit("2014")) & (~F.col("c.bpext").isin("DUPLICATE", "11876423_D")))
    .select(
        F.col("coc.county_office_control_identifier").alias("county_office_control_identifier"),
        F.col("timepd.time_period_identifier").alias("time_period_identifier"),
        F.col("c.bpext").cast("int").alias("core_customer_identifier"),
        F.col("z_dets.producer_involvement_start_date").alias("producer_involvement_start_date"),
        F.col("z_dets.producer_involvement_end_date").alias("producer_involvement_end_date"),
        F.lit(None).cast("string").alias("producer_involvement_interrupted_indicator"),
        F.col("z_dets.tract_producer_hel_exception_code").alias("tract_producer_hel_exception_code"),
        F.col("z_dets.tract_producer_cw_exception_code").alias("tract_producer_cw_exception_code"),
        F.col("z_dets.tract_producer_pcw_exception_code").alias("tract_producer_pcw_exception_code"),
        F.col("sss_dets.t_data_status_code").alias("data_status_code"),
        F.col("sss_dets.t_creation_date").alias("creation_date"),
        F.col("sss_dets.t_last_change_date").alias("last_change_date"),
        F.col("sss_dets.t_last_change_user_name").alias("last_change_user_name"),
        producer_code_expr.alias("producer_involvement_code"),
        F.col("sss_dets.admin_state").alias("state_fsa_code"),
        F.col("sss_dets.admin_county").alias("county_fsa_code"),
        F.lpad(F.col("sss_dets.farm_number"), 7, "0").alias("farm_number"),
        F.col("sss_dets.tract_number").alias("tract_number"),
        F.col("z_dets.hel_appeals_exhausted_date").alias("hel_appeals_exhausted_date"),
        F.col("z_dets.cw_appeals_exhausted_date").alias("cw_appeals_exhausted_date"),
        F.col("z_dets.pcw_appeals_exhausted_date").alias("pcw_appeals_exhausted_date"),
        F.col("z_dets.rma_hel_exception_code").alias("tract_producer_rma_hel_exception_code"),
        F.col("z_dets.rma_cw_exception_code").alias("tract_producer_rma_cw_exception_code"),
        F.col("z_dets.rma_pcw_exception_code").alias("tract_producer_rma_pcw_exception_code"),
    )
)

tract_tbl = (
    tract_current.alias("tprdryr")
    .join(
        farm.alias("farmpg"),
        (F.col("farmpg.county_office_control_identifier") == F.col("tprdryr.county_office_control_identifier"))
        & (F.lpad(F.col("farmpg.farm_number").cast("string"), 7, "0") == F.lpad(F.col("tprdryr.farm_number"), 7, "0")),
        "inner",
    )
    .join(
        tract.alias("tractpg"),
        (F.col("tractpg.county_office_control_identifier") == F.col("tprdryr.county_office_control_identifier"))
        & (F.lpad(F.col("tractpg.tract_number").cast("string"), 7, "0") == F.lpad(F.col("tprdryr.tract_number"), 7, "0")),
        "inner",
    )
    .join(
        farm_year.alias("fy"),
        (F.col("farmpg.farm_identifier") == F.col("fy.farm_identifier"))
        & (F.col("tprdryr.time_period_identifier") == F.col("fy.time_period_identifier")),
        "inner",
    )
    .join(
        tract_year.alias("ty"),
        (F.col("fy.farm_year_identifier") == F.col("ty.farm_year_identifier"))
        & (F.col("tractpg.tract_identifier") == F.col("ty.tract_identifier")),
        "inner",
    )
    .select(
        F.col("farmpg.farm_identifier").alias("farm_identifier"),
        F.col("ty.tract_year_identifier").alias("tract_year_identifier"),
        F.col("tprdryr.*"),
    )
)

window_spec = Window.partitionBy(
    "core_customer_identifier", "tract_year_identifier", "producer_involvement_code"
).orderBy(F.col("creation_date").desc(), F.col("last_change_date").desc())

source_df = (
    tract_tbl.withColumn("rownum", F.row_number().over(window_spec))
    .where(F.col("rownum") == F.lit(1))
    .select(
        "core_customer_identifier",
        "tract_year_identifier",
        "producer_involvement_start_date",
        "producer_involvement_end_date",
        "producer_involvement_interrupted_indicator",
        "tract_producer_hel_exception_code",
        "tract_producer_cw_exception_code",
        "tract_producer_pcw_exception_code",
        "data_status_code",
        "creation_date",
        "last_change_date",
        "last_change_user_name",
        "producer_involvement_code",
        "time_period_identifier",
        "state_fsa_code",
        "county_fsa_code",
        "farm_identifier",
        "farm_number",
        "tract_number",
        "hel_appeals_exhausted_date",
        "cw_appeals_exhausted_date",
        "pcw_appeals_exhausted_date",
        "tract_producer_rma_hel_exception_code",
        "tract_producer_rma_cw_exception_code",
        "tract_producer_rma_pcw_exception_code",
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
log.info("Write complete")

# ---------------------------------------------------------------------------
# Post-merge row-count metric from Iceberg snapshot metadata — reads one
# small JSON manifest file, not the full Parquet data files.
# ---------------------------------------------------------------------------
try:
    snap_t0 = time.perf_counter()
    snapshots_df = spark.table(f"glue_catalog.{TGT_DB}.{TGT_TABLE}.snapshots")
    snap_row = snapshots_df.orderBy(snapshots_df.committed_at.desc()).select("summary").first()
    summary = snap_row["summary"] if snap_row else None
    final_count = int(summary.get("total-records", -1)) if summary else -1
except Exception:
    final_count = -1  # metadata unavailable — non-fatal
log.info(f"[METRIC] phase_snapshot_metric_seconds={time.perf_counter() - snap_t0:.3f}")
log.info(f"[METRIC] {TGT_TABLE}_row_count={final_count}")

job.commit()
log.info(f"[METRIC] total_job_seconds={time.perf_counter() - job_t0:.3f}")
log.info("Transform-Tract-Producer-Year: completed successfully")
