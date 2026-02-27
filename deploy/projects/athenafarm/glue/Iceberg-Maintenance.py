"""
================================================================================
AWS Glue Job: Iceberg-Maintenance
================================================================================

PURPOSE:
    Weekly maintenance job that runs OPTIMIZE and EXPIRE SNAPSHOTS on all
    Iceberg tables in the athenafarm project to:
        - Compact small files produced by frequent incremental writes (OPTIMIZE)
        - Remove old snapshot metadata exceeding the retention window (EXPIRE)
        - Rewrite the table's manifest list for faster future metadata lookups

    Recommended schedule: weekly, off-peak hours (Sunday 02:00 UTC).

    From arch.md §7:
        "Schedule a weekly OPTIMIZE and EXPIRE SNAPSHOTS job to compact small
         files and remove old snapshots, keeping scan performance consistent
         as data grows."

GLUE JOB ARGUMENTS:
    --JOB_NAME              : Glue job name
    --env                   : Deployment environment
    --iceberg_warehouse     : s3:// URI for Iceberg warehouse root
    --snapshot_retention_hours : Number of hours of snapshots to retain (default: 168 = 7 days)
    --sss_database          : Glue catalog db for SSS Iceberg tables  (default: athenafarm_prod_raw)
    --ref_database          : Glue catalog db for PG ref tables (default: athenafarm_prod_ref)
    --target_database       : Glue catalog db for gold tables (default: athenafarm_prod_gold)
    --debug                 : "true" to enable DEBUG-level CloudWatch logging (default: false)

VERSION HISTORY:
    v1.0.0 - 2026-02-25 - Initial implementation (athenafarm project)

================================================================================
"""

import sys
import logging
import datetime
from awsglue.utils import getResolvedOptions
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

JOB_NAME                  = args["JOB_NAME"]
ENV                       = args["env"]
ICEBERG_WAREHOUSE         = args["iceberg_warehouse"]
RETENTION_HOURS           = int(args.get("snapshot_retention_hours", "168"))
SSS_DB                    = args.get("sss_database", "athenafarm_prod_raw")
REF_DB                    = args.get("ref_database", "athenafarm_prod_ref")
TGT_DB                    = args.get("target_database", "athenafarm_prod_gold")
DEBUG                     = args.get("debug", "false").strip().lower() == "true"

RETENTION_MS = RETENTION_HOURS * 3600 * 1000   # Iceberg expects milliseconds

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
log.info(f"Job                : {JOB_NAME}")
log.info(f"Env                : {ENV}")
log.info(f"Warehouse          : {ICEBERG_WAREHOUSE}")
log.info(f"Retention (hours)  : {RETENTION_HOURS}")
log.info(f"SSS DB             : {SSS_DB}")
log.info(f"Ref DB             : {REF_DB}")
log.info(f"Target DB          : {TGT_DB}")
log.info(f"Debug              : {DEBUG}")
log.info("=" * 70)

# ---------------------------------------------------------------------------
# All Iceberg tables to maintain
# ---------------------------------------------------------------------------
ALL_TABLES = [
    # (glue_catalog_database, table_name)
    # ── SSS source materialised tables ──
    (SSS_DB, "ibib"),
    (SSS_DB, "ibsp"),
    (SSS_DB, "ibst"),
    (SSS_DB, "ibin"),
    (SSS_DB, "ibpart"),
    (SSS_DB, "crmd_partner"),
    (SSS_DB, "z_ibase_comp_detail"),
    (SSS_DB, "comm_pr_frg_rel"),
    (SSS_DB, "zmi_farm_partn"),
    (SSS_DB, "zmi_field_excp"),
    (SSS_DB, "tbl_fr_bp_relationship"),
    # ── PG reference tables ──
    (REF_DB, "time_period"),
    (REF_DB, "county_office_control"),
    (REF_DB, "farm"),
    (REF_DB, "tract"),
    (REF_DB, "farm_year"),
    (REF_DB, "tract_year"),
    (REF_DB, "but000"),
    # ── CDC target tables ──
    (TGT_DB, "tract_producer_year"),
    (TGT_DB, "farm_producer_year"),
]


def maintain_table(catalog_db: str, table: str):
    fqn = f"glue_catalog.{catalog_db}.{table}"
    log.info(f"[{fqn}] Running maintenance")

    # ── 1. OPTIMIZE: compact small files ────────────────────────────────────
    try:
        result = spark.sql(f"CALL glue_catalog.system.rewrite_data_files(table => '{catalog_db}.{table}')")
        rows = result.collect()
        if rows:
            r = rows[0]
            log.info(
                f"[{fqn}] OPTIMIZE: rewritten_data_files={r['rewritten-data-files-count']}, "
                f"added_data_files={r['added-data-files-count']}, "
                f"rewritten_bytes={r['rewritten-bytes-count']:,}"
            )
    except Exception as exc:
        log.warning(f"[{fqn}] OPTIMIZE failed (table may not exist yet): {exc}")
        return

    # ── 2. EXPIRE SNAPSHOTS: remove snapshots older than retention window ────
    try:
        _cutoff = (datetime.datetime.utcnow() - datetime.timedelta(hours=RETENTION_HOURS)).strftime("%Y-%m-%d %H:%M:%S")
        expire_result = spark.sql(f"""
            CALL glue_catalog.system.expire_snapshots(
                table              => '{catalog_db}.{table}',
                older_than         => TIMESTAMP '{_cutoff}',
                retain_last        => 5,
                max_concurrent_deletes => 10
            )
        """)
        rows = expire_result.collect()
        if rows:
            r = rows[0]
            log.info(
                f"[{fqn}] EXPIRE SNAPSHOTS: deleted_data_files={r['deleted-data-files-count']}, "
                f"deleted_manifest_files={r['deleted-manifest-files-count']}"
            )
    except Exception as exc:
        log.warning(f"[{fqn}] EXPIRE SNAPSHOTS failed: {exc}")

    # ── 3. Rewrite manifests: improve manifest list efficiency ───────────────
    try:
        spark.sql(f"CALL glue_catalog.system.rewrite_manifests('{catalog_db}.{table}')")
        log.info(f"[{fqn}] Manifests rewritten")
    except Exception as exc:
        log.warning(f"[{fqn}] REWRITE MANIFESTS failed: {exc}")

    log.info(f"[{fqn}] Maintenance complete")


errors = []
for db, tbl in ALL_TABLES:
    try:
        maintain_table(db, tbl)
    except Exception as exc:
        log.error(f"[{db}.{tbl}] FAILED: {exc}", exc_info=True)
        errors.append((f"{db}.{tbl}", str(exc)))

job.commit()

if errors:
    msgs = "; ".join(f"{t}: {e}" for t, e in errors)
    raise RuntimeError(f"Iceberg-Maintenance completed with errors: {msgs}")

log.info("Iceberg-Maintenance: all tables maintained successfully")
