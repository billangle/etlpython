"""
================================================================================
AWS Glue Job: Transform-Farm-Producer-Year
================================================================================

PURPOSE:
    Replaces TRACT_PRODUCER_YEAR_SELECT.sql which targeted farm_producer_year.

    Executes the farm-level IBase → ZMI → PG resolution CTE chain using
    pre-materialised S3 Iceberg sources (zero federated hops) and applies
    the result via a single Iceberg MERGE INTO on farm_producer_year.

    The original SELECT file used the native Athena catalog fsa-prod-farm-records.farm
    as its entry point; this is pre-ingested into Iceberg (sss.fsa_farm_records_farm)
    by Ingest-SSS-Farmrecords so the transform has zero federated hops.

    NOTE — several fields in TRACT_PRODUCER_YEAR_SELECT.sql are explicitly
    marked "NEED TO FIGURE OUT MAPPING FOR THIS FIELD" and are NULL in the
    original.  Those remain NULL here with matching comments.  The MERGE ON
    key uses farm_year_identifier only because core_customer_identifier and
    producer_involvement_code are NULL in the original SQL.

GLUE JOB ARGUMENTS:
    --JOB_NAME            : Glue job name
    --env                 : Deployment environment
    --iceberg_warehouse   : s3:// URI for Iceberg warehouse root
    --full_load           : "true" inserts only; no WHEN MATCHED (default: false)
    --sss_database        : Glue catalog db where fsa_farm_records_farm resides (default: sss)
    --ref_database        : Glue catalog db for PG ref Iceberg tables (default: farm_ref)
    --target_database     : Glue catalog db for target table (default: farm_records_reporting)
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
              Replaces TRACT_PRODUCER_YEAR_SELECT.sql Athena query.

================================================================================
"""

import sys
import logging
import traceback
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

required_args = ["JOB_NAME", "env", "iceberg_warehouse"]
args = getResolvedOptions(sys.argv, required_args)

JOB_NAME          = args["JOB_NAME"]
ENV               = args["env"]
ICEBERG_WAREHOUSE = args["iceberg_warehouse"]
FULL_LOAD         = args.get("full_load", "false").strip().lower() == "true"
SSS_DB            = args.get("sss_database", "sss")
REF_DB            = args.get("ref_database", "farm_ref")
TGT_DB            = args.get("target_database", "farm_records_reporting")
TGT_TABLE         = args.get("target_table", "farm_producer_year")
DEBUG             = args.get("debug", "false").strip().lower() == "true"

if DEBUG:
    logging.getLogger().setLevel(logging.DEBUG)
    log.setLevel(logging.DEBUG)
    log.debug("DEBUG logging enabled")

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, args)

spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", ICEBERG_WAREHOUSE)

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
# Register source views
# ---------------------------------------------------------------------------

def register_view(catalog_db: str, table: str, view_name: str = None):
    fqn = f"glue_catalog.{catalog_db}.{table}"
    vn = view_name or table
    spark.read.format("iceberg").load(fqn).createOrReplaceTempView(vn)
    log.info(f"Registered view [{vn}] → {fqn}")

# fsa-prod-farm-records.farm — pre-ingested as fsa_farm_records_farm by Ingest-SSS-Farmrecords
register_view(SSS_DB, "fsa_farm_records_farm")

# PG reference tables from farm_records_reporting (via Ingest-PG-Reference-Tables)
register_view(REF_DB, "county_office_control")
register_view(REF_DB, "farm",      "farm_rds")   # PG farm table — different from fsa_farm_records_farm
register_view(REF_DB, "farm_year")

register_view(TGT_DB, TGT_TABLE, "farm_producer_year_existing")

log.info("All source views registered — executing farm_producer_year CTE transform")

# ---------------------------------------------------------------------------
# CTE transform — faithful Spark translation of TRACT_PRODUCER_YEAR_SELECT.sql
#
# Original source: "awsdatacatalog"."fsa-prod-farm-records"."farm"
# Pre-materialised as: sss.fsa_farm_records_farm  (view: fsa_farm_records_farm)
#
# Fields marked NULL match the original SQL "NEED TO FIGURE OUT MAPPING"
# comments — do not invent mappings without business sign-off.
# ---------------------------------------------------------------------------
TRANSFORM_SQL = f"""
WITH farm_producer_year_tbl AS (
    SELECT
        -- NULL: mapping not yet resolved (see original SQL comments)
        CAST(NULL AS INT)                               AS core_customer_identifier,
        fyr.farm_year_identifier                        AS farm_year_identifier,
        CAST(NULL AS INT)                               AS producer_involvement_code,
        CAST(NULL AS STRING)                            AS producer_involvement_interrupted_indicator,
        CAST(NULL AS DATE)                              AS producer_involvement_start_date,
        CAST(NULL AS DATE)                              AS producer_involvement_end_date,

        -- Exception codes come directly from fsa-prod-farm-records.farm
        CAST(f.hel_exception  AS INT)                  AS farm_producer_hel_exception_code,
        CAST(f.cw_exception   AS INT)                  AS farm_producer_cw_exception_code,
        CAST(f.pcw_exception  AS INT)                  AS farm_producer_pcw_exception_code,

        -- Status mapping matches original SQL CASE block
        CASE f.USER_STATUS
            WHEN 'ACTV' THEN 'A'  WHEN 'INCR' THEN 'I'
            WHEN 'PEND' THEN 'P'  WHEN 'DRFT' THEN 'D'
            WHEN 'X'    THEN 'I'  ELSE 'A'
        END                                             AS data_status_code,

        -- Dates: original SQL uses date_format → Spark uses date_format with ISO pattern
        CASE WHEN f.crtim IS NOT NULL
             THEN TO_DATE(date_format(f.crtim, 'yyyy-MM-dd'))
             ELSE CAST('9999-12-31' AS DATE)
        END                                             AS creation_date,
        CASE WHEN f.UPTIM IS NOT NULL
             THEN TO_DATE(date_format(f.UPTIM, 'yyyy-MM-dd'))
             ELSE CAST('9999-12-31' AS DATE)
        END                                             AS last_change_date,
        COALESCE(f.UPNAM, f.crnam)                      AS last_change_user_name,

        -- time_period_identifier: YEAR(crtim) - 1998 (original SQL formula)
        YEAR(f.crtim) - 1998                            AS time_period_identifier,

        -- State / county from administrative fields
        LPAD(f.administrative_state, 2, '0')            AS state_fsa_code,
        LPAD(f.administrative_count, 3, '0')            AS county_fsa_code,

        -- Farm surrogate key from farm_records_reporting.farm
        fr.farm_identifier                              AS farm_identifier,
        f.farm_number                                   AS farm_number,

        -- Appeal exhaustion dates
        f.hel_appeals_exhausted_date,
        f.cw_appeals_exhausted_date,
        f.pcw_appeals_exhausted_date,

        -- RMA exception codes
        CAST(f.rma_hel_exceptions AS INT)               AS farm_producer_rma_hel_exception_code,
        CAST(f.rma_cw_exceptions  AS INT)               AS farm_producer_rma_cw_exception_code,
        CAST(f.rma_pcw_exceptions AS INT)               AS farm_producer_rma_pcw_exception_code

    FROM fsa_farm_records_farm f
    -- county_office_control: join on padded state+county composite key
    JOIN county_office_control coc
        ON LPAD(f.administrative_state, 2, '0') || LPAD(f.administrative_count, 3, '0')
         = coc.state_fsa_code || coc.county_fsa_code
    -- farm_records_reporting.farm for surrogate farm_identifier
    -- Original SQL join: f.farm_number || coc_id = fr.farm_number || fr.coc_id
    JOIN farm_rds fr
        ON CAST(f.farm_number AS STRING) || CAST(coc.county_office_control_identifier AS STRING)
         = CAST(fr.farm_number AS STRING) || CAST(fr.county_office_control_identifier AS STRING)
    -- farm_year for farm_year_identifier
    JOIN farm_year fyr
        ON fyr.farm_identifier = fr.farm_identifier
)

SELECT
    farm_producer_year_identifier,    -- provided by MERGE source — NULL for new rows
    core_customer_identifier,
    farm_year_identifier,
    producer_involvement_code,
    producer_involvement_interrupted_indicator,
    producer_involvement_start_date,
    producer_involvement_end_date,
    farm_producer_hel_exception_code,
    farm_producer_cw_exception_code,
    farm_producer_pcw_exception_code,
    data_status_code,
    creation_date,
    last_change_date,
    last_change_user_name,
    time_period_identifier,
    state_fsa_code,
    county_fsa_code,
    farm_identifier,
    farm_number,
    hel_appeals_exhausted_date,
    cw_appeals_exhausted_date,
    pcw_appeals_exhausted_date,
    farm_producer_rma_hel_exception_code,
    farm_producer_rma_cw_exception_code,
    farm_producer_rma_pcw_exception_code
FROM (
    SELECT
        fpy.*,
        fpyr.farm_producer_year_identifier              AS farm_producer_year_identifier,
        CASE WHEN fpyr.farm_producer_year_identifier IS NULL THEN 'I' ELSE 'U'
        END                                             AS cdc_op
    FROM farm_producer_year_tbl fpy
    LEFT JOIN farm_producer_year_existing fpyr
        ON fpy.farm_year_identifier = fpyr.farm_year_identifier
) a
"""

# When core_customer_identifier and producer_involvement_code mappings are resolved,
# extend the LEFT JOIN condition above to include those fields as well.

log.info("Running farm_producer_year CTE transform")
source_df = spark.sql(TRANSFORM_SQL)
source_df.createOrReplaceTempView("new_farm_producer_year")
log.info(f"Transform produced {source_df.count():,} source rows")

TARGET_FQN = f"glue_catalog.{TGT_DB}.{TGT_TABLE}"

# MERGE ON: farm_year_identifier only — core_customer_identifier and
# producer_involvement_code are NULL in the original SQL (pending mapping).
# Update the ON clause when those mappings are resolved.
if FULL_LOAD:
    MERGE_SQL = f"""
    MERGE INTO {TARGET_FQN} t
    USING new_farm_producer_year s
    ON  t.farm_year_identifier = s.farm_year_identifier

    WHEN NOT MATCHED THEN INSERT (
        core_customer_identifier, farm_year_identifier,
        producer_involvement_start_date, producer_involvement_end_date,
        producer_involvement_interrupted_indicator,
        farm_producer_hel_exception_code, farm_producer_cw_exception_code,
        farm_producer_pcw_exception_code, data_status_code,
        creation_date, last_change_date, last_change_user_name,
        producer_involvement_code, time_period_identifier,
        state_fsa_code, county_fsa_code, farm_identifier, farm_number,
        hel_appeals_exhausted_date, cw_appeals_exhausted_date, pcw_appeals_exhausted_date,
        farm_producer_rma_hel_exception_code, farm_producer_rma_cw_exception_code,
        farm_producer_rma_pcw_exception_code
    ) VALUES (
        s.core_customer_identifier, s.farm_year_identifier,
        s.producer_involvement_start_date, s.producer_involvement_end_date,
        s.producer_involvement_interrupted_indicator,
        s.farm_producer_hel_exception_code, s.farm_producer_cw_exception_code,
        s.farm_producer_pcw_exception_code, s.data_status_code,
        s.creation_date, s.last_change_date, s.last_change_user_name,
        s.producer_involvement_code, s.time_period_identifier,
        s.state_fsa_code, s.county_fsa_code, s.farm_identifier, s.farm_number,
        s.hel_appeals_exhausted_date, s.cw_appeals_exhausted_date, s.pcw_appeals_exhausted_date,
        s.farm_producer_rma_hel_exception_code, s.farm_producer_rma_cw_exception_code,
        s.farm_producer_rma_pcw_exception_code
    )
    """
    log.info("Full-load mode: MERGE INTO — WHEN NOT MATCHED only")
else:
    MERGE_SQL = f"""
    MERGE INTO {TARGET_FQN} t
    USING new_farm_producer_year s
    ON  t.farm_year_identifier = s.farm_year_identifier

    WHEN MATCHED AND (
            t.farm_producer_hel_exception_code   <> s.farm_producer_hel_exception_code OR
            t.farm_producer_cw_exception_code    <> s.farm_producer_cw_exception_code  OR
            t.farm_producer_pcw_exception_code   <> s.farm_producer_pcw_exception_code OR
            t.data_status_code                   <> s.data_status_code
    ) THEN UPDATE SET
        farm_producer_hel_exception_code    = s.farm_producer_hel_exception_code,
        farm_producer_cw_exception_code     = s.farm_producer_cw_exception_code,
        farm_producer_pcw_exception_code    = s.farm_producer_pcw_exception_code,
        data_status_code                    = s.data_status_code,
        last_change_date                    = s.last_change_date,
        last_change_user_name               = s.last_change_user_name,
        hel_appeals_exhausted_date          = s.hel_appeals_exhausted_date,
        cw_appeals_exhausted_date           = s.cw_appeals_exhausted_date,
        pcw_appeals_exhausted_date          = s.pcw_appeals_exhausted_date,
        farm_producer_rma_hel_exception_code = s.farm_producer_rma_hel_exception_code,
        farm_producer_rma_cw_exception_code  = s.farm_producer_rma_cw_exception_code,
        farm_producer_rma_pcw_exception_code = s.farm_producer_rma_pcw_exception_code

    WHEN NOT MATCHED THEN INSERT (
        core_customer_identifier, farm_year_identifier,
        producer_involvement_start_date, producer_involvement_end_date,
        producer_involvement_interrupted_indicator,
        farm_producer_hel_exception_code, farm_producer_cw_exception_code,
        farm_producer_pcw_exception_code, data_status_code,
        creation_date, last_change_date, last_change_user_name,
        producer_involvement_code, time_period_identifier,
        state_fsa_code, county_fsa_code, farm_identifier, farm_number,
        hel_appeals_exhausted_date, cw_appeals_exhausted_date, pcw_appeals_exhausted_date,
        farm_producer_rma_hel_exception_code, farm_producer_rma_cw_exception_code,
        farm_producer_rma_pcw_exception_code
    ) VALUES (
        s.core_customer_identifier, s.farm_year_identifier,
        s.producer_involvement_start_date, s.producer_involvement_end_date,
        s.producer_involvement_interrupted_indicator,
        s.farm_producer_hel_exception_code, s.farm_producer_cw_exception_code,
        s.farm_producer_pcw_exception_code, s.data_status_code,
        s.creation_date, s.last_change_date, s.last_change_user_name,
        s.producer_involvement_code, s.time_period_identifier,
        s.state_fsa_code, s.county_fsa_code, s.farm_identifier, s.farm_number,
        s.hel_appeals_exhausted_date, s.cw_appeals_exhausted_date, s.pcw_appeals_exhausted_date,
        s.farm_producer_rma_hel_exception_code, s.farm_producer_rma_cw_exception_code,
        s.farm_producer_rma_pcw_exception_code
    )
    """
    log.info("Incremental mode: MERGE INTO — WHEN MATCHED UPDATE + WHEN NOT MATCHED INSERT")

log.info(f"Executing MERGE INTO {TARGET_FQN}")
spark.sql(MERGE_SQL)

final_count = spark.read.format("iceberg").load(TARGET_FQN).count()
log.info(f"[METRIC] {TGT_TABLE}_row_count={final_count}")

job.commit()
log.info("Transform-Farm-Producer-Year: completed successfully")
