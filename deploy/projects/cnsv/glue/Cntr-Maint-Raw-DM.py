"""
raw_dm.py — Publish final parquet datasets as parallel part-*.parquet

What this does:
  - Reads the dataset folders written by the producer job
    (.../Datasets/<TABLE>-LOAD/part-*.parquet for initial; .../Datasets/<TABLE>/part-*.parquet for incrementals).
  - Applies your schema typing, PK-based dedupe and merge (preserves target rows not in source; adds/updates non-deletes).
  - Writes the final target to: s3://<target_bucket>/<target_prefix>/<table>/part-*.parquet
  - Writes CDC data (including deletes) to: s3://<target_bucket>/<target_prefix>/_cdc/<table>/dart_filedate=YYYY-MM-DD/

Architecture:
  - Final Zone ({table}/)     : Like dbo schema in SQL Server - NO deletes, permanent data
  - CDC Zone (_cdc/{table}/)  : Like fnet_changes in SQL Server - ALL ops including D, 10-day retention

Performance Options:
  - SKIP_DQ_TABLES: Comma-separated list of tables to skip ALL DQ metrics
  - SKIP_COUNT_TABLES: Comma-separated list of tables to skip only expensive count operations
  - PARTITION_CONFIG: Tables configured here will be partitioned by specified column
"""

import sys, os, re, json, math, hashlib, logging, s3fs, uuid, urllib.parse, boto3
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from urllib.parse import urlparse

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import SparkSession, Row, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StringType, IntegerType, LongType, ShortType, ByteType, DoubleType,
    BooleanType, DateType, TimestampType, DecimalType, StructType, StructField
)

# ------------------------------
# Args & Spark session
# ------------------------------
# Required arguments
required_args = [
    "JOB_NAME", "env", "TableName", "JobId",
    "postgres_prcs_ctrl_dbname", "target_bucket",
    "target_prefix", "source_prefix"
]

# Get required arguments first
args = getResolvedOptions(sys.argv, required_args)

# Handle optional arguments with defaults
def get_optional_arg(arg_name: str, default_value: str) -> str:
    """
    Get optional Glue argument with a default value.

    Args:
        arg_name: Name of the argument (without --)
        default_value: Default value if argument not provided

    Returns:
        Argument value or default
    """
    try:
        optional_args = getResolvedOptions(sys.argv, [arg_name])
        return optional_args.get(arg_name, default_value)
    except:
        return default_value

# Get optional performance arguments from Glue job args
ENABLE_DQ_METRICS_ARG = get_optional_arg("ENABLE_DQ_METRICS", "true")
SKIP_DQ_TABLES_ARG = get_optional_arg("SKIP_DQ_TABLES", "")
SKIP_COUNT_TABLES_ARG = get_optional_arg("SKIP_COUNT_TABLES", "")

sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = (
    glueContext.spark_session
    .builder.appName("MergeParquetFiles")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .config("spark.sql.parquet.mergeSchema", "false")
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    .config("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
    .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .config("spark.sql.files.maxPartitionBytes", "256m")
    .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
    .config("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
    .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
    .config("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
    .getOrCreate()
)

job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s raw_dm: %(message)s")
log = logging.getLogger("raw_dm")

# ------------------------------
# Basic config
# ------------------------------
JOB_NAME   = args["JOB_NAME"]
ENV        = args["env"]
JOB_ID     = args["JobId"]
TABLE      = args["TableName"]
PCDB_NAME  = args["postgres_prcs_ctrl_dbname"]
TARGET_BUCKET = args["target_bucket"]
TARGET_PREFIX = args["target_prefix"]
SOURCE_PREFIX = args["source_prefix"]  # where schema JSON lives (landing bucket path)

# Final Zone directory (like dbo schema - no deletes)
TARGET_DATASET_DIR = f"s3://{TARGET_BUCKET}/{TARGET_PREFIX}/{TABLE}/"
TARGET_PART_BYTES  = 1024 * 1024 * 1024  # ~1 GiB
MAX_OUTPUT_PARTS   = 2048

# CDC Zone directory (like fnet_changes - includes deletes, partitioned by dart_filedate)
TARGET_CDC_DIR = f"s3://{TARGET_BUCKET}/{TARGET_PREFIX}/_cdc/{TABLE}/"
CDC_RETENTION_COUNT = 10

# ------------------------------
# Partition Configuration
# ------------------------------
# Maps table name (lowercase) to partition column name
# Tables listed here will be written with .partitionBy(column)
PARTITION_CONFIG = {
    # Add more tables as needed:
    # "your_table_name": "your_partition_column",
}

# Get partition column for current table (if configured)
PARTITION_COL = PARTITION_CONFIG.get(TABLE.lower())

# ------------------------------
# Performance Configuration
# ------------------------------
# Global toggle for DQ metrics
ENABLE_DQ_METRICS = ENABLE_DQ_METRICS_ARG.lower() == "true"

# Tables to skip ALL DQ metrics (comma-separated, case-insensitive)
SKIP_DQ_TABLES = [t.strip().upper() for t in SKIP_DQ_TABLES_ARG.split(",") if t.strip()]

# Tables to skip only expensive count operations (comma-separated, case-insensitive)
# This is the FASTER option - skips counts but still collects basic metrics
SKIP_COUNT_TABLES = [t.strip().upper() for t in SKIP_COUNT_TABLES_ARG.split(",") if t.strip()]

# Check if current table should skip operations
CURRENT_TABLE_UPPER = TABLE.upper()
SKIP_DQ_FOR_TABLE = CURRENT_TABLE_UPPER in SKIP_DQ_TABLES
SKIP_COUNTS_FOR_TABLE = CURRENT_TABLE_UPPER in SKIP_COUNT_TABLES

# ------------------------------
# PROMINENT PERFORMANCE CONFIG LOGGING
# ------------------------------
log.info("="*80)
log.info(f"[PERFORMANCE CONFIG] Current Table: {TABLE} (normalized: {CURRENT_TABLE_UPPER})")
log.info("="*80)

# Log partition configuration
if PARTITION_COL:
    log.info(f"[PERFORMANCE CONFIG] Table will be PARTITIONED by '{PARTITION_COL}'")
else:
    log.info(f"[PERFORMANCE CONFIG] Table will NOT be partitioned (no repartition overhead)")

# Log global DQ setting
log.info(f"[PERFORMANCE CONFIG] Global DQ Metrics Enabled: {ENABLE_DQ_METRICS}")

# Log DQ skip configuration
if SKIP_DQ_TABLES_ARG:
    log.info(f"[PERFORMANCE CONFIG] DQ Skip List (raw): '{SKIP_DQ_TABLES_ARG}'")
    log.info(f"[PERFORMANCE CONFIG] DQ Skip List (parsed): {SKIP_DQ_TABLES}")
    if SKIP_DQ_FOR_TABLE:
        log.warning(f"[PERFORMANCE CONFIG] ALL DQ METRICS WILL BE SKIPPED FOR '{TABLE}'")
    else:
        log.info(f"[PERFORMANCE CONFIG] DQ metrics enabled for '{TABLE}' (not in DQ skip list)")
else:
    log.info(f"[PERFORMANCE CONFIG] DQ Skip List: EMPTY - DQ metrics enabled for all tables")

# Log count skip configuration
if SKIP_COUNT_TABLES_ARG:
    log.info(f"[PERFORMANCE CONFIG] Count Skip List (raw): '{SKIP_COUNT_TABLES_ARG}'")
    log.info(f"[PERFORMANCE CONFIG] Count Skip List (parsed): {SKIP_COUNT_TABLES}")
    if SKIP_COUNTS_FOR_TABLE:
        log.warning(f"[PERFORMANCE CONFIG] EXPENSIVE COUNTS WILL BE SKIPPED FOR '{TABLE}'")
        log.warning(f"[PERFORMANCE CONFIG] (Basic metrics still collected, only .count() operations skipped)")
    else:
        log.info(f"[PERFORMANCE CONFIG] Detailed counts enabled for '{TABLE}' (not in count skip list)")
else:
    log.info(f"[PERFORMANCE CONFIG] Count Skip List: EMPTY - Detailed counts enabled for all tables")

log.info("="*80)

# Schema JSON path (same convention as before)
SOURCE_BUCKET = TARGET_BUCKET.replace("final", "landing")
SCHEMA_S3_PATH = f"s3://{SOURCE_BUCKET}/{SOURCE_PREFIX}/etl-jobs/{TARGET_PREFIX}_target_schema.json"

s3_client = boto3.client("s3")
log.info(f"[CONFIG] Table={TABLE}, JobId={JOB_ID}, Env={ENV}")
log.info(f"[CONFIG] Schema JSON: {SCHEMA_S3_PATH}")
log.info(f"[CONFIG] Final Zone: {TARGET_DATASET_DIR}")
log.info(f"[CONFIG] CDC Zone: {TARGET_CDC_DIR}")
log.info(f"[CONFIG] CDC Retention: {CDC_RETENTION_COUNT} days")

# ------------------------------
# DB connection (unchanged details)
# ------------------------------
from py4j.java_gateway import java_import
java_import(sc._gateway.jvm, "java.sql.DriverManager")

GLUE_CONN_NAME = f"FSA-{ENV.upper()}-PG-DART114" if ENV == "dev" else f"FSA-{ENV.upper()}-PG-DART115"

def pcdb_connect():
    """
    Establish connection to the process control database using Glue connection.

    Returns:
        JDBC Connection object with autocommit disabled
    """
    conf = glueContext.extract_jdbc_conf(GLUE_CONN_NAME)
    conn = sc._gateway.jvm.DriverManager.getConnection(
        conf.get("url") + "/" + PCDB_NAME,
        conf.get("user"),
        conf.get("password")
    )
    conn.setAutoCommit(False)
    return conn

pcdb_conn = pcdb_connect()
log.info("[DB] Process Control DB connected successfully")

def get_load_oper_tgt_id_s3(pcdb_conn, target_bucket: str, target_prefix: str) -> int:
    """
    Retrieve or create load_oper_tgt_id for the given bucket and prefix.

    Args:
        pcdb_conn: JDBC connection to process control DB
        target_bucket: S3 bucket name
        target_prefix: S3 prefix path

    Returns:
        load_oper_tgt_id: Integer ID for this load operation target
    """
    log.info(f"[DB] Getting load_oper_tgt_id for bucket={target_bucket}, prefix={target_prefix}")
    sel = f"""
    SELECT load_oper_tgt_id
      FROM dart_process_control.load_oper_tgt
     WHERE data_bkt_nm = '{target_bucket}'
       AND data_bkt_pfx_nm = '{target_prefix}'
    """
    st = pcdb_conn.createStatement(); rs = st.executeQuery(sel)
    if rs.next():
        lot_id = rs.getInt(1)
        log.info(f"[DB] Found existing load_oper_tgt_id={lot_id}")
        return lot_id

    step = f"""
    SELECT dps.data_ppln_step_id
      FROM dart_process_control.data_ppln_job dpj
      JOIN dart_process_control.data_ppln_step dps
        ON dpj.data_ppln_id = dps.data_ppln_id
     WHERE dpj.data_ppln_job_id = {JOB_ID}
       AND step_seq_nbr = 2
    """
    rs = pcdb_conn.createStatement().executeQuery(step)
    if not rs.next():
        raise RuntimeError(f"[DB] data_ppln_step record not found for JobId={JOB_ID}")
    data_ppln_step_id = rs.getInt(1)
    log.info(f"[DB] Found data_ppln_step_id={data_ppln_step_id}")

    ins = f"""
    INSERT INTO dart_process_control.load_oper_tgt (data_ppln_step_id, data_bkt_nm, data_bkt_pfx_nm)
    VALUES ({data_ppln_step_id}, '{target_bucket}', '{target_prefix}')
    ON CONFLICT DO NOTHING
    RETURNING load_oper_tgt_id
    """
    rs = pcdb_conn.createStatement().executeQuery(ins); pcdb_conn.commit()
    if rs.next():
        lot_id = rs.getInt(1)
        log.info(f"[DB] Created new load_oper_tgt_id={lot_id}")
        return lot_id
    rs = pcdb_conn.createStatement().executeQuery(sel)
    if rs.next():
        lot_id = rs.getInt(1)
        log.info(f"[DB] Retrieved load_oper_tgt_id={lot_id} after insert conflict")
        return lot_id
    raise RuntimeError("[DB] load_oper_tgt_id not found or insert failed")

def log_to_db(pcdb_conn, load_oper_tgt_id: int, job_id: str, data_obj_nm: str, oper_status: str,
              rcd_ct: int, start_time: datetime, end_time: datetime, job_name: str, error_msg: Optional[str] = None):
    """
    Insert audit record into data_ppln_oper table.

    Args:
        pcdb_conn: JDBC connection
        load_oper_tgt_id: Load operation target ID
        job_id: Job ID
        data_obj_nm: Data object name (S3 path)
        oper_status: Operation status (Complete/Failed)
        rcd_ct: Record count
        start_time: Operation start timestamp
        end_time: Operation end timestamp
        job_name: Job name
        error_msg: Optional error message
    """
    msg = 'NULL' if error_msg is None else f"'{error_msg.replace(chr(39), chr(39)+chr(39))}'"
    ins = f"""
    INSERT INTO dart_process_control.data_ppln_oper
    (load_oper_tgt_id, data_ppln_job_id, cre_dt, last_chg_dt,
     data_stat_cd, data_eng_oper_nm, oper_strt_dt, oper_end_dt,
     data_obj_type_nm, data_obj_nm, data_obj_xtrc_strt_dt, data_obj_xtrc_end_dt,
     rcd_ct, data_obj_proc_rtn_nm, data_ppln_oper_stat_nm, err_msg_txt)
    VALUES ({load_oper_tgt_id}, {job_id}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'A',
            'Glue', '{start_time}', '{end_time}', 'Parquet', '{data_obj_nm}',
            '{start_time}', '{end_time}', {rcd_ct}, '{job_name}', '{oper_status}', {msg})
    """
    st = pcdb_conn.createStatement(); st.executeUpdate(ins); pcdb_conn.commit()
    log.info(f"[AUDIT] data_ppln_oper written: status={oper_status}, records={rcd_ct}")

def update_process_control_job_with_error(pcdb_conn, job_id: str, error_msg: str):
    """
    Update the job record with failure status and error message.

    Args:
        pcdb_conn: JDBC connection
        job_id: Job ID to update
        error_msg: Error message to record
    """
    error_msg_clean = error_msg.replace(chr(39), chr(39)+chr(39))[:500]
    upd = f"""
    UPDATE dart_process_control.data_ppln_job
       SET job_end_dt = '{datetime.utcnow():%Y-%m-%d %H:%M:%S}',
           job_stat_nm = 'Failed',
           err_msg_txt = COALESCE(err_msg_txt, '') || '{error_msg_clean}'
     WHERE data_ppln_job_id = {job_id}
    """
    try:
        st = pcdb_conn.createStatement(); st.executeUpdate(upd); pcdb_conn.commit()
        log.info(f"[AUDIT] Job {job_id} marked as Failed in process control DB")
    except Exception as e:
        log.error(f"[AUDIT] Error updating job with failure: {e}"); pcdb_conn.rollback()

LOAD_OPER_TGT_ID = get_load_oper_tgt_id_s3(pcdb_conn, TARGET_BUCKET, TARGET_PREFIX)
OPER_STRT_DT = datetime.utcnow()

# -------------------------------------------------------------
# CDC Functions
# -------------------------------------------------------------
def cleanup_old_cdc_partitions(cdc_base_path: str, retention_count: int = 10,
                                exclude_dates: List[str] = None):
    """
    Remove CDC partitions keeping only the most recent N dart_filedate folders.

    Args:
        cdc_base_path: Base S3 path for CDC data
        retention_count: Number of most recent dart_filedate partitions to keep
        exclude_dates: List of date strings (YYYY-MM-DD) to exclude from cleanup
    """
    log.info(f"[CDC_CLEANUP] Starting cleanup for {cdc_base_path}, keeping last {retention_count} partitions")

    if exclude_dates:
        log.info(f"[CDC_CLEANUP] Excluding dates from cleanup: {exclude_dates}")

    parsed = urlparse(cdc_base_path)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")

    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        all_partitions = []

        # Collect all dart_filedate partitions
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/'):
            for common_prefix in page.get('CommonPrefixes', []):
                partition_path = common_prefix['Prefix']
                if 'dart_filedate=' in partition_path:
                    date_str = partition_path.split('dart_filedate=')[1].rstrip('/')
                    all_partitions.append((date_str, partition_path))

        if not all_partitions:
            log.info("[CDC_CLEANUP] No partitions found")
            return

        # Sort by date descending (newest first)
        all_partitions.sort(key=lambda x: x[0], reverse=True)

        log.info(f"[CDC_CLEANUP] Found {len(all_partitions)} partitions: {[p[0] for p in all_partitions]}")

        # Keep the most recent N partitions
        partitions_to_keep = set(p[0] for p in all_partitions[:retention_count])

        # Add excluded dates to keep set
        if exclude_dates:
            partitions_to_keep.update(exclude_dates)

        log.info(f"[CDC_CLEANUP] Keeping partitions: {sorted(partitions_to_keep, reverse=True)}")

        # Identify partitions to delete
        partitions_to_delete = [
            (date_str, path) for date_str, path in all_partitions
            if date_str not in partitions_to_keep
        ]

        if not partitions_to_delete:
            log.info("[CDC_CLEANUP] No partitions to delete")
            return

        log.info(f"[CDC_CLEANUP] Deleting {len(partitions_to_delete)} old partitions: {[p[0] for p in partitions_to_delete]}")

        for date_str, partition_prefix in partitions_to_delete:
            objects_to_delete = []
            for page in paginator.paginate(Bucket=bucket, Prefix=partition_prefix):
                for obj in page.get('Contents', []):
                    objects_to_delete.append({'Key': obj['Key']})

            if objects_to_delete:
                for i in range(0, len(objects_to_delete), 1000):
                    batch = objects_to_delete[i:i+1000]
                    s3_client.delete_objects(Bucket=bucket, Delete={'Objects': batch})
                log.info(f"[CDC_CLEANUP] Deleted partition: dart_filedate={date_str} ({len(objects_to_delete)} files)")

        log.info(f"[CDC_CLEANUP] Cleanup complete")

    except Exception as e:
        log.warning(f"[CDC_CLEANUP] Error during cleanup (non-fatal): {e}")

def extract_filedate_from_data(df: DataFrame) -> Optional[str]:
    """
    Extract dart_filedate from the first row of the DataFrame.

    Args:
        df: Source DataFrame containing dart_filedate column

    Returns:
        Date string in YYYY-MM-DD format, or None if not found
    """
    if "dart_filedate" not in df.columns:
        log.warning("[FILEDATE] dart_filedate column not found in data")
        return None

    try:
        # Get first non-null dart_filedate value
        first_row = df.select("dart_filedate").filter(F.col("dart_filedate").isNotNull()).first()

        if first_row is None or first_row["dart_filedate"] is None:
            log.warning("[FILEDATE] No non-null dart_filedate found in data")
            return None

        value = first_row["dart_filedate"]

        # Handle different types (date, timestamp, string)
        if hasattr(value, 'strftime'):
            # datetime.date or datetime.datetime
            formatted_date = value.strftime("%Y-%m-%d")
        else:
            # String - try to parse YYYYMMDD or YYYY-MM-DD format
            value_str = str(value).strip()
            if len(value_str) == 8 and value_str.isdigit():
                # YYYYMMDD format
                formatted_date = f"{value_str[:4]}-{value_str[4:6]}-{value_str[6:8]}"
            elif len(value_str) >= 10:
                # YYYY-MM-DD or longer timestamp format
                formatted_date = value_str[:10]
            else:
                log.warning(f"[FILEDATE] Unexpected dart_filedate format: {value_str}")
                return None

        log.info(f"[FILEDATE] Extracted dart_filedate={formatted_date} from data")
        return formatted_date

    except Exception as e:
        log.warning(f"[FILEDATE] Error extracting dart_filedate from data: {e}")
        return None

def create_empty_cdc_table(df_schema: DataFrame, cdc_dir: str, table_name: str):
    """
    Create an empty CDC table with the correct schema for Glue Crawler discovery.
    dart_filedate is stored in folder path only (not inside parquet file).
    """
    log.info(f"[CDC] Creating empty CDC table structure for {table_name}")

    try:
        # Clean up any existing CDC data for clean initial load
        parsed = urlparse(cdc_dir)
        bucket = parsed.netloc
        prefix = parsed.path.lstrip("/")

        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)

        if response.get('Contents') or response.get('CommonPrefixes'):
            log.warning(f"[CDC] Existing CDC data found - DELETING for clean initial load")

            paginator = s3_client.get_paginator('list_objects_v2')
            objects_to_delete = []

            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get('Contents', []):
                    objects_to_delete.append({'Key': obj['Key']})

            if objects_to_delete:
                for i in range(0, len(objects_to_delete), 1000):
                    batch = objects_to_delete[i:i+1000]
                    s3_client.delete_objects(Bucket=bucket, Delete={'Objects': batch})
                log.info(f"[CDC] Deleted {len(objects_to_delete)} existing CDC files")

        # Create empty DataFrame with same schema
        empty_df = df_schema.limit(0)

        # DROP dart_filedate if it exists - it will be in folder path only
        if "dart_filedate" in empty_df.columns:
            empty_df = empty_df.drop("dart_filedate")

        # Write to partition folder (dart_filedate is in folder name, not in file)
        placeholder_date = "1900-01-01"
        partition_path = f"{cdc_dir.rstrip('/')}/dart_filedate={placeholder_date}/"

        log.info(f"[CDC] Writing empty schema file to {partition_path}")

        (empty_df
            .write
            .mode("overwrite")
            .option("parquet.compression", "snappy")
            .parquet(partition_path))

        log.info(f"[CDC] Empty CDC table created at {cdc_dir}")
        log.info(f"[CDC] dart_filedate stored in folder path only (no duplicate column)")

    except Exception as e:
        log.warning(f"[CDC] Failed to create empty CDC table (non-fatal): {e}")

def write_cdc_changes(df_source: DataFrame, table_name: str, cdc_dir: str,
                      dart_filedate: Optional[str] = None):
    """
    Write ALL source changes (including deletes) to CDC location, partitioned by dart_filedate.
    If partition already exists for the same dart_filedate, it will be replaced (not appended).

    Args:
        df_source: Source DataFrame with all changes (I/U/D) - should be deduplicated
        table_name: Name of the table
        cdc_dir: Target CDC directory
        dart_filedate: Date string in YYYY-MM-DD format extracted from source data.
    """
    log.info(f"[CDC] Writing CDC changes to {cdc_dir}")

    # Use provided dart_filedate, or fallback to column/current date
    if dart_filedate:
        df_source = df_source.withColumn("dart_filedate", F.lit(dart_filedate).cast(DateType()))
        log.info(f"[CDC] Using dart_filedate={dart_filedate} extracted from source data")
    elif "dart_filedate" in df_source.columns:
        if not isinstance(df_source.schema["dart_filedate"].dataType, DateType):
            df_source = df_source.withColumn("dart_filedate", F.to_date(F.col("dart_filedate")))
        # Get the date value for partition management
        first_date = df_source.select("dart_filedate").filter(F.col("dart_filedate").isNotNull()).first()
        if first_date:
            dart_filedate = first_date["dart_filedate"].strftime("%Y-%m-%d")
        log.info("[CDC] Using existing dart_filedate column from data")
    else:
        dart_filedate = datetime.utcnow().strftime("%Y-%m-%d")
        df_source = df_source.withColumn("dart_filedate", F.lit(dart_filedate).cast(DateType()))
        log.warning(f"[CDC] No dart_filedate provided, using current date: {dart_filedate}")

    # Count - but respect SKIP_COUNTS_FOR_TABLE
    if not SKIP_COUNTS_FOR_TABLE:
        row_count = df_source.count()
        # Count by operation type
        op_counts = df_source.groupBy("op").count().collect()
        op_summary = {row["op"]: row["count"] for row in op_counts}
        log.info(f"[CDC] Writing {row_count} deduplicated records: {op_summary}")
    else:
        log.info(f"[CDC] SKIPPING CDC row count (table in SKIP_COUNT_TABLES)")
        row_count = -1  # Estimate for partition calculation

    # Get distinct dates being written
    dates_written = [row['dart_filedate'].strftime('%Y-%m-%d')
                     for row in df_source.select('dart_filedate').distinct().collect()]
    log.info(f"[CDC] Writing to partitions: {dates_written}")

    # ========== DELETE EXISTING PARTITION(S) BEFORE WRITE ==========
    for date_str in dates_written:
        partition_path = f"{cdc_dir.rstrip('/')}/dart_filedate={date_str}/"
        delete_partition_if_exists(partition_path)
    # ===============================================================

    # CDC always uses partitionBy dart_filedate - repartition by column is efficient
    log.info(f"[CDC] Writing with repartition('dart_filedate') + partitionBy('dart_filedate')")

    (df_source
        .repartition("dart_filedate")
        .write
        .mode("append")
        .partitionBy("dart_filedate")
        .option("parquet.compression", "snappy")
        .parquet(cdc_dir))

    log.info(f"[CDC] CDC changes written successfully to {cdc_dir}")

    # Cleanup old partitions - keep only last N partitions
    cleanup_old_cdc_partitions(cdc_dir, CDC_RETENTION_COUNT, exclude_dates=dates_written)
	
def delete_partition_if_exists(partition_path: str):
    """
    Delete a specific CDC partition if it exists.
    Used to prevent duplicates when re-running incremental loads.

    Args:
        partition_path: Full S3 path to partition (e.g., s3://bucket/prefix/_cdc/table/dart_filedate=2026-01-09/)
    """
    try:
        parsed = urlparse(partition_path)
        bucket = parsed.netloc
        prefix = parsed.path.lstrip("/")

        # Check if partition exists
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)

        if response.get('Contents'):
            log.info(f"[CDC] Deleting existing partition before re-write: {partition_path}")

            paginator = s3_client.get_paginator('list_objects_v2')
            objects_to_delete = []

            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get('Contents', []):
                    objects_to_delete.append({'Key': obj['Key']})

            if objects_to_delete:
                for i in range(0, len(objects_to_delete), 1000):
                    batch = objects_to_delete[i:i+1000]
                    s3_client.delete_objects(Bucket=bucket, Delete={'Objects': batch})

                log.info(f"[CDC] Deleted {len(objects_to_delete)} files from partition")
        else:
            log.info(f"[CDC] Partition does not exist (first write): {partition_path}")

    except Exception as e:
        log.warning(f"[CDC] Error checking/deleting partition (non-fatal): {e}")

# -------------------------------------------------------------
# DQ metrics
# -------------------------------------------------------------
TARGET_DQ_SUMMARY_DIR = f"s3://{TARGET_BUCKET}/{TARGET_PREFIX}/_dq_metrics_run_summary/"
TARGET_DQ_COLUMN_DIR  = f"s3://{TARGET_BUCKET}/{TARGET_PREFIX}/_dq_metrics_column_profile/"

def _is_minmax_type(dt):
    """Check if data type supports min/max aggregations."""
    return isinstance(dt, (IntegerType, LongType, ShortType, ByteType,
                           DoubleType, DecimalType, DateType, TimestampType))

def _approx_distinct_pk(df: DataFrame, primary_keys: List[str], skip_counts: bool = False) -> Optional[int]:
    """
    Calculate approximate distinct count of primary key combinations.

    Args:
        df: DataFrame to analyze
        primary_keys: List of primary key column names
        skip_counts: If True, skip the count operation

    Returns:
        Approximate distinct count or None if no primary keys or skipped
    """
    if skip_counts:
        log.info("[DQ] SKIPPING approx distinct PK count (table in SKIP_COUNT_TABLES)")
        return None
    if not primary_keys:
        log.warning("[DQ] No primary keys defined, skipping PK cardinality check")
        return None
    log.info(f"[DQ] Calculating approx distinct PK for keys: {primary_keys}")
    return df.select(F.approx_count_distinct(F.concat_ws("||", *[F.col(c).cast("string") for c in primary_keys]))\
                     .alias("acd")).first()["acd"]

def _collect_column_metrics(df: DataFrame, skip_counts: bool = False) -> List[Dict]:
    """
    Collect comprehensive column-level metrics including nulls, cardinality, min/max.

    Args:
        df: DataFrame to analyze
        skip_counts: If True, skip expensive count operations

    Returns:
        List of dictionaries with per-column metrics
    """
    log.info(f"[DQ] Collecting column metrics for {len(df.columns)} columns")

    if skip_counts:
        log.info("[DQ] SKIPPING detailed column metrics (table in SKIP_COUNT_TABLES)")
        # Return basic schema info without counts
        out = []
        for c in df.columns:
            field = df.schema[c]
            out.append({
                "column": c,
                "data_type": str(field.dataType),
                "nullable": field.nullable,
                "null_count": -1,  # Skipped
                "null_percentage": -1.0,
                "approx_cardinality": -1,
                "cardinality_ratio": -1.0,
                "min_value": None,
                "max_value": None,
            })
        return out

    # Null counts
    null_aggs = [F.sum(F.col(c).isNull().cast("bigint")).alias(c) for c in df.columns]
    null_row  = df.agg(*null_aggs).first().asDict()

    # Approximate distinct counts
    acd_aggs  = [F.approx_count_distinct(F.col(c)).alias(c) for c in df.columns]
    acd_row   = df.agg(*acd_aggs).first().asDict()

    # Min/max for numeric/date columns
    mm_cols = [f.name for f in df.schema.fields if _is_minmax_type(f.dataType)]
    if mm_cols:
        log.info(f"[DQ] Computing min/max for {len(mm_cols)} numeric/date columns")
        mins = df.agg(*[F.min(c).alias(c) for c in mm_cols]).first().asDict()
        maxs = df.agg(*[F.max(c).alias(c) for c in mm_cols]).first().asDict()
    else:
        mins, maxs = {}, {}

    # Get total row count for percentage calculations
    total_rows = df.count()
    out = []
    for c in df.columns:
        field = df.schema[c]
        null_count = int(null_row.get(c, 0) or 0)
        cardinality = int(acd_row.get(c, 0) or 0)

        out.append({
            "column": c,
            "data_type": str(field.dataType),
            "nullable": field.nullable,
            "null_count": null_count,
            "null_percentage": round(100.0 * null_count / total_rows, 2) if total_rows > 0 else 0.0,
            "approx_cardinality": cardinality,
            "cardinality_ratio": round(cardinality / total_rows, 4) if total_rows > 0 else 0.0,
            "min_value": None if c not in mins or mins[c] is None else str(mins[c]),
            "max_value": None if c not in maxs or maxs[c] is None else str(maxs[c]),
        })

    log.info(f"[DQ] Column metrics collected successfully")
    return out

def _validate_data_quality(df: DataFrame, primary_keys: List[str], table_name: str,
                           skip_counts: bool = False) -> Dict:
    """
    Perform data quality validations and return metrics.

    Args:
        df: DataFrame to validate
        primary_keys: List of primary key columns
        table_name: Name of the table
        skip_counts: If True, skip expensive count operations

    Returns:
        Dictionary with validation results
    """
    log.info(f"[DQ_VALIDATION] Starting validation for {table_name}")
    validations = {}

    if skip_counts:
        log.info(f"[DQ_VALIDATION] SKIPPING detailed validations (table in SKIP_COUNT_TABLES)")
        validations['counts_skipped'] = True
        validations['row_count'] = -1
        return validations

    # 1. Check for empty dataset
    row_count = df.count()
    validations['is_empty'] = row_count == 0
    validations['row_count'] = row_count

    if row_count == 0:
        log.warning(f"[DQ_VALIDATION] Dataset is empty for {table_name}")
        return validations

    # 2. Check for null PKs
    if primary_keys:
        pk_null_filter = " OR ".join([f"`{pk}` IS NULL" for pk in primary_keys])
        null_pk_count = df.filter(pk_null_filter).count()
        validations['null_primary_keys'] = null_pk_count
        validations['null_pk_percentage'] = round(100.0 * null_pk_count / row_count, 2)

        if null_pk_count > 0:
            log.warning(f"[DQ_VALIDATION] Found {null_pk_count} rows ({validations['null_pk_percentage']}%) with NULL primary keys")

    # 3. Check for duplicate PKs (after deduplication should be 0)
    if primary_keys:
        pk_groups = df.groupBy(*primary_keys).count().filter("count > 1")
        dup_count = pk_groups.count()
        validations['duplicate_primary_keys'] = dup_count

        if dup_count > 0:
            log.warning(f"[DQ_VALIDATION] Found {dup_count} duplicate primary key combinations")

    # 4. Check for columns with all nulls
    all_null_cols = []
    for c in df.columns:
        if df.filter(F.col(c).isNotNull()).count() == 0:
            all_null_cols.append(c)
    validations['all_null_columns'] = all_null_cols

    if all_null_cols:
        log.warning(f"[DQ_VALIDATION] Columns with all NULL values: {all_null_cols}")

    # 5. Op column distribution (if exists)
    if 'op' in df.columns:
        op_dist = df.groupBy('op').count().collect()
        validations['op_distribution'] = {row['op']: row['count'] for row in op_dist}
        log.info(f"[DQ_VALIDATION] Op distribution: {validations['op_distribution']}")

    log.info(f"[DQ_VALIDATION] Validation complete for {table_name}")
    return validations

def write_dq_metrics(df_final: DataFrame, table_name: str, primary_keys: List[str], dedupe_key: str,
                     total_input_bytes: int, target_dataset_dir: str, job_id: str,
                     load_type: str = "unknown"):
    """
    Write comprehensive data quality metrics to S3.

    Supports:
    - Global disable via ENABLE_DQ_METRICS
    - Table-level skip via SKIP_DQ_TABLES (skips ALL DQ)
    - Table-level count skip via SKIP_COUNT_TABLES (skips only expensive counts)

    Args:
        df_final: Final DataFrame after all transformations
        table_name: Name of the table
        primary_keys: List of primary key columns
        dedupe_key: Deduplication key column
        total_input_bytes: Total bytes read from source
        target_dataset_dir: Target S3 directory
        job_id: Job ID
        load_type: Type of load (initial_load/incremental_load)
    """
    # Check global disable flag
    if not ENABLE_DQ_METRICS:
        log.info(f"[DQ] Skipping ALL DQ metrics for {table_name} (globally disabled via ENABLE_DQ_METRICS=false)")
        return

    # Check table-level DQ skip
    if SKIP_DQ_FOR_TABLE:
        log.info("="*60)
        log.info(f"[DQ] SKIPPING ALL DQ METRICS FOR '{table_name}'")
        log.info(f"[DQ] Table is in SKIP_DQ_TABLES list")
        log.info("="*60)
        return

    # Check if we should skip counts but still collect basic metrics
    skip_counts = SKIP_COUNTS_FOR_TABLE
    if skip_counts:
        log.info("="*60)
        log.info(f"[DQ] SKIPPING EXPENSIVE COUNTS FOR '{table_name}'")
        log.info(f"[DQ] Table is in SKIP_COUNT_TABLES list")
        log.info(f"[DQ] Basic schema metrics will still be collected")
        log.info("="*60)

    log.info(f"[DQ] Writing metrics for {table_name}, load_type={load_type}, skip_counts={skip_counts}")
    run_date = datetime.utcnow().strftime("%Y%m%d")

    # Get row count - expensive operation
    if not skip_counts:
        row_count = df_final.count()
        log.info(f"[DQ] Row count: {row_count}")
    else:
        row_count = -1  # Indicator that count was skipped
        log.info(f"[DQ] Row count: SKIPPED (table in SKIP_COUNT_TABLES)")

    # Enhanced PK metrics - also expensive
    approx_pk = _approx_distinct_pk(df_final, primary_keys, skip_counts)
    if approx_pk is not None and row_count > 0:
        dup_pk_est = row_count - approx_pk
        pk_uniqueness_ratio = round(approx_pk / row_count, 4)
    else:
        dup_pk_est = None
        pk_uniqueness_ratio = None

    # Schema fingerprint - cheap operation, always run
    schema_fprint = hashlib.sha256(df_final.schema.simpleString().encode("utf-8")).hexdigest()

    # Run validations with skip_counts flag
    validations = _validate_data_quality(df_final, primary_keys, table_name, skip_counts)

    # Append 1-row run summary
    summary = Row(
        table=table_name.upper(),
        job_id=int(job_id),
        run_date=run_date,
        load_type=load_type,
        row_count=int(row_count),
        approx_distinct_pk=(int(approx_pk) if approx_pk is not None else None),
        duplicate_pk_est=(int(dup_pk_est) if dup_pk_est is not None else None),
        pk_uniqueness_ratio=pk_uniqueness_ratio,
        null_pk_count=validations.get('null_primary_keys', -1 if skip_counts else 0),
        null_pk_percentage=validations.get('null_pk_percentage', -1.0 if skip_counts else 0.0),
        dedupe_key=(dedupe_key or ""),
        total_input_bytes=int(total_input_bytes),
        schema_fingerprint=schema_fprint,
        all_null_columns=",".join(validations.get('all_null_columns', [])) if not skip_counts else "SKIPPED",
        op_distribution=str(validations.get('op_distribution', {})) if not skip_counts else "SKIPPED",
        target_dataset_dir=target_dataset_dir,
        processing_timestamp=datetime.utcnow().isoformat(),
        counts_skipped=skip_counts
    )

    if not skip_counts:
        log.info(f"[DQ] Writing summary metrics: rows={row_count}, distinct_pk={approx_pk}, duplicates={dup_pk_est}")
    else:
        log.info(f"[DQ] Writing summary metrics: schema_fingerprint={schema_fprint[:16]}..., counts=SKIPPED")

    # Define explicit schema to avoid inference issues
    summary_schema = StructType([
        StructField("table", StringType(), False),
        StructField("job_id", LongType(), False),
        StructField("run_date", StringType(), False),
        StructField("load_type", StringType(), False),
        StructField("row_count", LongType(), False),
        StructField("approx_distinct_pk", LongType(), True),
        StructField("duplicate_pk_est", LongType(), True),
        StructField("pk_uniqueness_ratio", DoubleType(), True),
        StructField("null_pk_count", LongType(), False),
        StructField("null_pk_percentage", DoubleType(), False),
        StructField("dedupe_key", StringType(), False),
        StructField("total_input_bytes", LongType(), False),
        StructField("schema_fingerprint", StringType(), False),
        StructField("all_null_columns", StringType(), False),
        StructField("op_distribution", StringType(), False),
        StructField("target_dataset_dir", StringType(), False),
        StructField("processing_timestamp", StringType(), False),
        StructField("counts_skipped", BooleanType(), False),
    ])

    try:
        spark.createDataFrame([summary], schema=summary_schema) \
             .write.mode("append").partitionBy("run_date","table").parquet(TARGET_DQ_SUMMARY_DIR)
        log.info(f"[DQ] Summary metrics written successfully")
    except Exception as e:
        log.error(f"[DQ] Failed to write summary metrics: {e}")
        log.warning(f"[DQ] Continuing without DQ summary metrics")

    # Column profile - pass skip_counts flag
    col_schema = StructType([
        StructField("table", StringType(), False),
        StructField("job_id", LongType(), False),
        StructField("run_date", StringType(), False),
        StructField("column", StringType(), False),
        StructField("data_type", StringType(), False),
        StructField("nullable", BooleanType(), False),
        StructField("null_count", LongType(), False),
        StructField("null_percentage", DoubleType(), False),
        StructField("approx_cardinality", LongType(), False),
        StructField("cardinality_ratio", DoubleType(), False),
        StructField("min_value", StringType(), True),
        StructField("max_value", StringType(), True),
    ])

    col_rows = _collect_column_metrics(df_final, skip_counts)

    try:
        col_df = spark.createDataFrame([Row(table=table_name.upper(), job_id=int(job_id), run_date=run_date, **r)
                                       for r in col_rows], schema=col_schema)

        log.info(f"[DQ] Writing column profile metrics for {len(col_rows)} columns")
        col_df.write.mode("append").partitionBy("run_date","table").parquet(TARGET_DQ_COLUMN_DIR)
        log.info(f"[DQ] Column profile metrics written successfully")
    except Exception as e:
        log.error(f"[DQ] Failed to write column profile metrics: {e}")
        log.warning(f"[DQ] Continuing without DQ column metrics")

    log.info(f"[DQ] DQ metrics write completed → summary: {TARGET_DQ_SUMMARY_DIR}, columns: {TARGET_DQ_COLUMN_DIR}")

# ------------------------------
# Schema & typing
# ------------------------------
def read_json_from_s3(s3_path: str) -> Dict:
    """
    Read and parse JSON file from S3.

    Args:
        s3_path: S3 URI of the JSON file

    Returns:
        Parsed JSON as dictionary
    """
    log.info(f"[SCHEMA] Reading schema from {s3_path}")
    u = urllib.parse.urlparse(s3_path)
    try:
        resp = s3_client.get_object(Bucket=u.netloc, Key=u.path.lstrip("/"))
        content = json.loads(resp["Body"].read().decode("utf-8"))
        log.info(f"[SCHEMA] Schema JSON loaded successfully, contains {len(content.get('tables', []))} tables")
        return content
    except Exception as e:
        log.error(f"[SCHEMA] Failed to read schema from S3: {e}")
        raise

def get_table_schema(schema_json: dict, table_name: str) -> Tuple[List[str], Dict[str, str], str, List[str], str]:
    """
    Extract schema information for a specific table from the schema JSON.

    Args:
        schema_json: Full schema JSON dictionary
        table_name: Name of the table to extract

    Returns:
        Tuple of (columns, data_types, table_name, primary_keys, dedupe_key)
    """
    log.info(f"[SCHEMA] Extracting schema for table: {table_name}")
    entry = next((t for t in schema_json["tables"] if t["target_table"] == table_name), None)
    if not entry:
        raise ValueError(f"[SCHEMA] Table {table_name} not found in schema JSON")

    cols = [c["target_column_name"] for c in entry["columns"]]
    raw = {c["target_column_name"]: (c.get("data_type") or "string") for c in entry["columns"]}

    # Normalize data types
    dt_map = {}
    for k, v in raw.items():
        v_l = v.lower()
        if v_l in ("datetime","timestamptz","timestamp with time zone"):
            v_l = "timestamp"
        dt_map[k] = v_l

    pks = [p.strip() for p in (entry.get("primary_key") or "").split(",") if p.strip()]
    dedupe_key = entry.get("dedupe_key", "")

    log.info(f"[SCHEMA] Table {table_name}: {len(cols)} columns, PK={pks}, dedupe_key={dedupe_key}")
    return cols, dt_map, table_name, pks, dedupe_key

def _parse_iso_timestamp(expr):
    """
    Parse various ISO timestamp formats into Spark TimestampType.

    Args:
        expr: Column expression containing timestamp string

    Returns:
        Parsed timestamp column
    """
    return F.coalesce(
        F.to_timestamp(expr, "yyyy-MM-dd'T'HH:mm:ss.SSSSSSX"),
        F.to_timestamp(expr, "yyyy-MM-dd'T'HH:mm:ss.SSSX"),
        F.to_timestamp(expr, "yyyy-MM-dd'T'HH:mm:ssX"),
        F.to_timestamp(expr, "yyyy-MM-dd HH:mm:ss.SSSSSS"),
        F.to_timestamp(expr, "yyyy-MM-dd HH:mm:ss.SSS"),
        F.to_timestamp(expr, "yyyy-MM-dd HH:mm:ss")
    )

def convert_data_types(df: DataFrame, data_types: Dict[str, str]) -> DataFrame:
    """
    Convert DataFrame columns to specified data types with robust error handling.

    Args:
        df: Input DataFrame
        data_types: Dictionary mapping column names to target data types

    Returns:
        DataFrame with converted types
    """
    log.info(f"[TYPES] Converting data types for {len(data_types)} columns")
    conversion_count = 0

    for col_name, data_type in (data_types or {}).items():
        if col_name not in df.columns:
            log.debug(f"[TYPES] Column {col_name} not in DataFrame, skipping")
            continue

        data_type = (data_type or "string").lower()
        src_type = df.schema[col_name].dataType

        try:
            converted = False
            if data_type in ("int","integer"):
                if not isinstance(src_type, IntegerType):
                    df = df.withColumn(col_name, col(col_name).cast(IntegerType()))
                    converted = True
            elif data_type in ("bigint","long"):
                if not isinstance(src_type, LongType):
                    df = df.withColumn(col_name, col(col_name).cast(LongType()))
                    converted = True
            elif data_type == "smallint":
                if not isinstance(src_type, ShortType):
                    df = df.withColumn(col_name, col(col_name).cast(ShortType()))
                    converted = True
            elif data_type == "tinyint":
                if not isinstance(src_type, ByteType):
                    df = df.withColumn(col_name, col(col_name).cast(ByteType()))
                    converted = True
            elif data_type.startswith(("decimal","numeric")):
                m = re.search(r'(decimal|numeric)\((\d+),\s*(\d+)\)', data_type)
                if m:
                    df = df.withColumn(col_name, col(col_name).cast(DecimalType(int(m.group(2)), int(m.group(3)))))
                    converted = True
                else:
                    df = df.withColumn(col_name, col(col_name).cast(StringType()))
                    converted = True
            elif data_type in ("timestamp","datetime"):
                if isinstance(src_type, TimestampType):
                    pass
                elif isinstance(src_type, StringType):
                    df = df.withColumn(col_name, _parse_iso_timestamp(col(col_name)))
                    converted = True
                else:
                    df = df.withColumn(col_name, col(col_name).cast(TimestampType()))
                    converted = True
            elif data_type == "date":
                if isinstance(src_type, DateType):
                    pass
                elif isinstance(src_type, StringType):
                    ts = _parse_iso_timestamp(col(col_name))
                    df = df.withColumn(col_name, F.to_date(F.coalesce(ts, col(col_name))))
                    converted = True
                else:
                    df = df.withColumn(col_name, col(col_name).cast(DateType()))
                    converted = True
            elif data_type.startswith(("char","varchar","string","text")):
                if not isinstance(src_type, StringType):
                    df = df.withColumn(col_name, col(col_name).cast(StringType()))
                    converted = True

            if converted:
                conversion_count += 1
                log.debug(f"[TYPES] Converted {col_name}: {src_type} → {data_type}")

        except Exception as e:
            log.error(f"[TYPES] FAILED converting {col_name}: {src_type} → {data_type}: {e}")
            raise RuntimeError(f"Type conversion failed for column {col_name}: {e}")

    log.info(f"[TYPES] Successfully converted {conversion_count} columns")
    return df

def verify_and_add_missing_columns(df: DataFrame, expected_columns: List[str],
                                   primary_keys: List[str]) -> Tuple[DataFrame, List[str], bool]:
    """
    Verify DataFrame has all expected columns and add missing ones as NULL.

    Args:
        df: Input DataFrame
        expected_columns: List of expected column names
        primary_keys: List of primary key columns

    Returns:
        Tuple of (updated DataFrame, list of missing columns, bool indicating if PK is missing)
    """
    log.info(f"[SCHEMA] Verifying {len(expected_columns)} expected columns")
    existing = set(c.lower() for c in df.columns)
    missing = []
    pk_missing = any(pk.lower() not in existing for pk in (primary_keys or []))

    if pk_missing:
        log.error(f"[SCHEMA] PRIMARY KEY MISSING: Expected {primary_keys}, found {list(df.columns)}")

    for c in expected_columns:
        if c.lower() in (pk.lower() for pk in (primary_keys or [])):
            continue
        if c.lower() not in existing:
            df = df.withColumn(c.lower(), F.lit(None))
            missing.append(c)

    if missing:
        log.warning(f"[SCHEMA] Added {len(missing)} missing columns with NULL values: {missing[:10]}")
    else:
        log.info(f"[SCHEMA] All expected columns present")

    return df, missing, pk_missing

def deduplicate_by_date_column(df: DataFrame, primary_keys: List[str], dedupe_key: str) -> DataFrame:
    """
    Deduplicate DataFrame by primary keys, keeping the row with latest dedupe_key value.

    Args:
        df: Input DataFrame
        primary_keys: List of primary key columns to group by
        dedupe_key: Column name to use for determining latest record

    Returns:
        Deduplicated DataFrame
    """
    if not dedupe_key:
        log.info("[DEDUPE] No dedupe_key specified, skipping deduplication")
        return df

    if dedupe_key not in df.columns:
        log.warning(f"[DEDUPE] Column '{dedupe_key}' not found in DataFrame, skipping deduplication")
        return df

    # Count before - but respect SKIP_COUNTS_FOR_TABLE
    if not SKIP_COUNTS_FOR_TABLE:
        initial_count = df.count()
        log.info(f"[DEDUPE] Deduplicating on PK={primary_keys}, dedupe_key={dedupe_key}, initial_rows={initial_count}")
    else:
        log.info(f"[DEDUPE] Deduplicating on PK={primary_keys}, dedupe_key={dedupe_key}")
        log.info(f"[DEDUPE] SKIPPING initial row count (table in SKIP_COUNT_TABLES)")

    w = Window.partitionBy(*primary_keys).orderBy(F.col(dedupe_key).desc())
    df_deduped = df.withColumn("row_number", F.row_number().over(w)).filter(F.col("row_number")==1).drop("row_number")

    # Count after - but respect SKIP_COUNTS_FOR_TABLE
    if not SKIP_COUNTS_FOR_TABLE:
        final_count = df_deduped.count()
        removed = initial_count - final_count
        log.info(f"[DEDUPE] Removed {removed} duplicate rows, final_rows={final_count}")
    else:
        log.info(f"[DEDUPE] SKIPPING final row count (table in SKIP_COUNT_TABLES)")
        log.info(f"[DEDUPE] Deduplication complete (counts skipped)")

    return df_deduped

# ------------------------------
# Input discovery
# ------------------------------

def cleanup_temp_directory(temp_dir: str):
    """
    Clean up temporary S3 directory.

    Args:
        temp_dir: S3 URI of temp directory to delete
    """
    try:
        log.info(f"[CLEANUP] Deleting temp directory: {temp_dir}")
        parsed = urlparse(temp_dir)
        bucket = parsed.netloc
        prefix = parsed.path.lstrip("/")

        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            objects = [{'Key': obj['Key']} for obj in page.get('Contents', [])]
            if objects:
                for i in range(0, len(objects), 1000):
                    batch = objects[i:i+1000]
                    s3_client.delete_objects(Bucket=bucket, Delete={'Objects': batch})

        log.info(f"[CLEANUP] Temp directory deleted: {temp_dir}")
    except Exception as e:
        log.warning(f"[CLEANUP] Failed to delete temp directory (non-fatal): {e}")

def fetch_potential_filepaths(pcdb_conn, job_id: str, table: str) -> List[str]:
    """
    Query process control DB for potential input file paths for this job and table.

    Args:
        pcdb_conn: JDBC connection to process control DB
        job_id: Job ID to search for
        table: Table name

    Returns:
        List of S3 paths that may contain input data
    """
    log.info(f"[INPUT] Fetching potential filepaths for JobId={job_id}, Table={table}")
    sql = f"""
        SELECT oper.data_obj_nm
          FROM dart_process_control.data_ppln_oper oper
          JOIN dart_process_control.data_ppln_job job
            ON oper.data_ppln_job_id = job.data_ppln_job_id
         WHERE job.data_ppln_job_id = '{job_id}'
           AND (
                oper.data_obj_nm LIKE '%/{job_id}/Datasets/{table.upper()}-LOAD/%'
             OR oper.data_obj_nm LIKE '%/{job_id}/Datasets/{table.upper()}/%'
           )
    """
    out = []
    st = pcdb_conn.createStatement(); rs = st.executeQuery(sql)
    while rs.next():
        url = rs.getString(1)
        if not url.endswith("/"): url += "/"
        out.append(url)

    log.info(f"[INPUT] Found {len(out)} potential file paths")
    return out

def _list_parquet_parts_under(prefix_url: str) -> List[str]:
    """
    List all .parquet files under an S3 prefix.

    Args:
        prefix_url: S3 URI prefix to search

    Returns:
        List of S3 URIs for parquet files
    """
    parsed = urlparse(prefix_url)
    bucket, prefix = parsed.netloc, parsed.path.lstrip("/")
    log.debug(f"[INPUT] Listing parquet files in s3://{bucket}/{prefix}")

    urls = []
    token = None
    while True:
        resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, ContinuationToken=token) if token else \
               s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        for obj in resp.get("Contents", []):
            k = obj["Key"]
            if k.endswith(".parquet"):
                urls.append(f"s3://{bucket}/{k}")
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break

    log.debug(f"[INPUT] Found {len(urls)} parquet files under {prefix_url}")
    return urls

def filter_filepaths(potential_filepaths: List[str]) -> List[str]:
    """
    Filter and deduplicate actual parquet file paths from potential paths.

    Args:
        potential_filepaths: List of S3 paths that may contain parquet files

    Returns:
        Deduplicated list of actual parquet file S3 URIs
    """
    log.info(f"[INPUT] Filtering {len(potential_filepaths)} potential paths for actual parquet files")
    files = []
    seen = set()

    for p in potential_filepaths:
        try:
            parts = _list_parquet_parts_under(p) if p.endswith("/") else ([p] if p.endswith(".parquet") else [])
            for f in parts:
                if f not in seen:
                    files.append(f)
                    seen.add(f)
        except Exception as e:
            log.warning(f"[INPUT] Error listing files for path {p}: {e}")

    log.info(f"[INPUT] Found {len(files)} unique parquet files")
    if files:
        log.info(f"[INPUT] Sample files: {files[:3]}")

    return files

def determine_load_type(filepaths: List[str]) -> str:
    """
    Determine if this is an initial load or incremental load based on file paths.

    Args:
        filepaths: List of input file paths

    Returns:
        'initial_load' or 'incremental_load'
    """
    load_type = "initial_load" if any("-LOAD/" in f for f in filepaths) else "incremental_load"
    log.info(f"[INPUT] Determined load type: {load_type}")
    return load_type

# ------------------------------
# Sizing
# ------------------------------
def _estimate_total_input_bytes(filepaths: List[str]) -> int:
    """
    Calculate total size in bytes of all input files.

    Args:
        filepaths: List of S3 file URIs

    Returns:
        Total size in bytes
    """
    log.info(f"[SIZING] Estimating total size for {len(filepaths)} files")
    total = 0
    errors = 0

    for u in filepaths:
        p = urlparse(u)
        b, k = p.netloc, p.path.lstrip("/")
        try:
            h = s3_client.head_object(Bucket=b, Key=k)
            total += h["ContentLength"]
        except Exception as e:
            log.warning(f"[SIZING] HEAD {u}: {e}")
            errors += 1

    log.info(f"[SIZING] Total input: {total:,} bytes ({total/1e9:.2f} GB), errors={errors}")
    return total

# ------------------------------
# Write Helper Function
# ------------------------------
def write_dataframe_to_parquet(df: DataFrame, target_path: str, partition_col: Optional[str] = None):
    """
    Write DataFrame to parquet with optional partitioning.

    - For partitioned tables: uses repartition(partition_col) + partitionBy(partition_col)
    - For non-partitioned tables: writes directly without repartition (fastest)

    Args:
        df: DataFrame to write
        target_path: S3 path to write to
        partition_col: Optional partition column name
    """
    if partition_col:
        if partition_col not in df.columns:
            log.error(f"[WRITE] Partition column '{partition_col}' not found in DataFrame!")
            log.error(f"[WRITE] Available columns: {df.columns}")
            raise ValueError(f"Partition column '{partition_col}' not found in DataFrame")

        log.info(f"[WRITE] Writing PARTITIONED by '{partition_col}' to {target_path}")
        log.info(f"[WRITE] Using repartition('{partition_col}') + partitionBy('{partition_col}')")

        (df
           .repartition(partition_col)
           .write.mode("overwrite")
           .partitionBy(partition_col)
           .option("parquet.block.size", 128 * 1024 * 1024)
           .option("parquet.page.size", 1 * 1024 * 1024)
           .option("parquet.compression", "snappy")
           .parquet(target_path))
    else:
        log.info(f"[WRITE] Writing NON-PARTITIONED to {target_path}")
        log.info(f"[WRITE] No repartition (fastest write)")

        (df
           .write.mode("overwrite")
           .option("parquet.block.size", 128 * 1024 * 1024)
           .option("parquet.page.size", 1 * 1024 * 1024)
           .option("parquet.compression", "snappy")
           .parquet(target_path))

    log.info(f"[WRITE] Write completed successfully")
    
#------------------------------
# Read Target Data Helper
# ------------------------------
def read_target_dataframe(target_path: str, partition_col: Optional[str] = None) -> Optional[DataFrame]:
    """
    Read existing target data, handling both partitioned and non-partitioned tables.

    For partitioned tables: Uses basePath option to preserve partition columns
    For non-partitioned tables: Standard parquet read

    Args:
        target_path: S3 path to read from
        partition_col: Optional partition column name (if table is partitioned)

    Returns:
        DataFrame if target exists, None otherwise
    """
    try:
        if partition_col:
            # For partitioned tables, use basePath to preserve partition column values
            log.info(f"[READ_TARGET] Reading PARTITIONED table with basePath={target_path}")
            df = spark.read \
                .option("basePath", target_path) \
                .option("pathGlobFilter", "*.parquet") \
                .parquet(target_path)

            # Verify partition column exists after read
            if partition_col not in df.columns:
                log.warning(f"[READ_TARGET] Partition column '{partition_col}' not found after read!")
                log.warning(f"[READ_TARGET] Available columns: {df.columns}")
        else:
            # For non-partitioned tables, standard read
            log.info(f"[READ_TARGET] Reading NON-PARTITIONED table from {target_path}")
            df = spark.read \
                .option("pathGlobFilter", "*.parquet") \
                .parquet(target_path)

        return df

    except Exception as e:
        log.warning(f"[READ_TARGET] Failed to read target: {e}")
        return None

# ------------------------------
# Initial load
# ------------------------------
def perform_initial_load(filepaths: List[str], target_dataset_dir: str, schema_s3_path: str,
                        table_name: str, job_id: str) -> Tuple[int, str]:
    """
    Perform initial load: read source parquet, add Op='I', apply schema, write to target.
    Also creates empty CDC table structure for Glue Crawler Discovery with 1900-01-01 partition.

    Args:
        filepaths: List of source parquet file paths
        target_dataset_dir: Target S3 directory for output
        schema_s3_path: S3 path to schema JSON
        table_name: Name of table being loaded
        job_id: Job ID

    Returns:
        Tuple of (record_count, status)
    """
    log.info(f"[INITIAL_LOAD] Starting initial load for {table_name}")
    log.info(f"[INITIAL_LOAD] Reading {len(filepaths)} source files")

    # Get partition column for this table (uses global PARTITION_COL)
    partition_col = PARTITION_COL
    if partition_col:
        log.info(f"[INITIAL_LOAD] Table will be PARTITIONED by '{partition_col}'")
    else:
        log.info(f"[INITIAL_LOAD] Table will NOT be partitioned")

    try:
        schema_json = read_json_from_s3(schema_s3_path)
        columns, data_types, table_name, primary_keys, dedupe_key = get_table_schema(schema_json, table_name)

        # Check if this is an append-only table (no PK defined)
        has_primary_key = primary_keys and len(primary_keys) > 0
        if not has_primary_key:
            log.info("[INITIAL_LOAD] No primary keys defined - treating as APPEND-ONLY table")

        # Read source files
        df_raw = spark.read.parquet(*filepaths)
        source_columns = df_raw.columns
        source_columns_lower = [c.lower() for c in source_columns]

        log.info(f"[INITIAL_LOAD] Source columns: {source_columns}")

        # Build column list for selection - exclude 'op' (we'll add it explicitly)
        columns_lower = [c.lower() for c in columns]
        columns_to_select = []

        for src_col in source_columns:
            if src_col.lower() in columns_lower and src_col.lower() != 'op':
                columns_to_select.append(src_col)

        log.info(f"[INITIAL_LOAD] Selecting columns from source: {columns_to_select}")

        if not columns_to_select:
            raise RuntimeError(f"[INITIAL_LOAD] No matching columns found between source and schema!")

        df = df_raw.select(*columns_to_select)

        # Count - respect SKIP_COUNTS_FOR_TABLE
        if not SKIP_COUNTS_FOR_TABLE:
            initial_count = df.count()
            log.info(f"[INITIAL_LOAD] Read {initial_count} rows from source")
        else:
            log.info(f"[INITIAL_LOAD] SKIPPING source row count (table in SKIP_COUNT_TABLES)")
            initial_count = -1

        # Normalize column names to lowercase
        df = df.toDF(*[c.lower() for c in df.columns])

        # Normalize keys to lowercase
        primary_keys_lower = [p.lower() for p in primary_keys] if primary_keys else []
        dedupe_key_lower = dedupe_key.lower() if dedupe_key else None
        data_types_lower = {k.lower(): v for k, v in data_types.items()}

        # Only deduplicate if we have primary keys
        if has_primary_key:
            df = deduplicate_by_date_column(df, primary_keys_lower, dedupe_key_lower)
            if not SKIP_COUNTS_FOR_TABLE:
                dedupe_count = df.count()
                log.info(f"[INITIAL_LOAD] After deduplication: {dedupe_count} rows (removed {initial_count - dedupe_count} duplicates)")
        else:
            log.info("[INITIAL_LOAD] Skipping deduplication (no primary keys)")

        # ========== CRITICAL: Always add 'op' column with 'I' for initial load ==========
        df = df.withColumn("op", F.lit("I"))
        log.info("[INITIAL_LOAD] Added 'op'='I' column for initial load")

        # Verify 'op' column exists
        if "op" not in df.columns:
            raise RuntimeError("[INITIAL_LOAD] CRITICAL: Failed to add 'op' column!")
        log.info(f"[INITIAL_LOAD] Confirmed 'op' column present. Current columns: {df.columns}")
        # ================================================================================

        # Convert data types
        df = convert_data_types(df, data_types_lower)

        # Verify and add missing columns
        columns_lower_list = [c.lower() for c in columns]
        df, missing_columns, pk_missing = verify_and_add_missing_columns(df, columns_lower_list, primary_keys_lower)

        # Only enforce PK presence if table is supposed to have PKs
        if has_primary_key and pk_missing:
            raise RuntimeError(f"[INITIAL_LOAD] Primary key(s) {primary_keys} missing in source for {table_name}")

        # Ensure 'op' is in final column list if not already in schema
        final_columns = columns_lower_list.copy()
        if 'op' not in final_columns:
            final_columns.append('op')

        # Select only the expected columns in proper order (include only columns that exist)
        available_columns = [c for c in final_columns if c in df.columns]
        df = df.select(*available_columns)

        log.info(f"[INITIAL_LOAD] Final columns to write: {df.columns}")

        # Estimate input bytes for DQ metrics
        total_in_bytes = _estimate_total_input_bytes(filepaths)

        # Write using helper function (handles partitioning)
        write_dataframe_to_parquet(df, target_dataset_dir, partition_col)
        
        # ========== CREATE EMPTY CDC TABLE FOR GLUE CRAWLER ==========
        log.info("[INITIAL_LOAD] Creating empty CDC table structure for Glue Crawler discovery")
        create_empty_cdc_table(df, TARGET_CDC_DIR, table_name)
        # =============================================================

        log.info("[INITIAL_LOAD] Write completed, collecting metrics")
        write_dq_metrics(
            df_final=df, table_name=table_name,
            primary_keys=primary_keys_lower,
            dedupe_key=(dedupe_key_lower or ""), total_input_bytes=total_in_bytes,
            target_dataset_dir=target_dataset_dir,
            job_id=job_id, load_type="initial_load"
        )

        # Final count for audit - this is required for the audit table
        if not SKIP_COUNTS_FOR_TABLE:
            final_count = df.count()
        else:
            log.info(f"[INITIAL_LOAD] SKIPPING final count for audit (table in SKIP_COUNT_TABLES)")
            final_count = -1  # Indicate count was skipped

        log.info(f"[INITIAL_LOAD] Successfully completed: {final_count} records with 'op' column")
        log.info(f"[INITIAL_LOAD] NOTE: For Postgres staging initial load, query Final Zone directly: {target_dataset_dir}")
        log.info(f"[INITIAL_LOAD] Empty CDC table created at : {TARGET_CDC_DIR}")
        return final_count, "Complete"

    except Exception as e:
        log.error(f"[INITIAL_LOAD] Failed with error: {str(e)}")
        raise

# ------------------------------
# Incremental merge
# ------------------------------
def merge_parquet_files(filepaths: List[str], target_dataset_dir: str, schema_s3_path: str,
                       table_name: str) -> Tuple[int, str]:
    """
    Perform incremental merge:
    1. Deduplicate source data
    2. Write ALL deduplicated changes (including D) to CDC location for Postgres staging
    3. Merge non-delete records into Final Zone

    Args:
        filepaths: List of source parquet file paths with changes
        target_dataset_dir: Target S3 directory containing existing data
        schema_s3_path: S3 path to schema JSON
        table_name: Name of table being merged

    Returns:
        Tuple of (final_record_count, status)
    """
    log.info(f"[MERGE] Starting incremental merge for {table_name}")
    log.info(f"[MERGE] Reading {len(filepaths)} source files")

    # Get partition column for this table (uses global PARTITION_COL)
    partition_col = PARTITION_COL
    if partition_col:
        log.info(f"[MERGE] Table will be PARTITIONED by '{partition_col}'")
    else:
        log.info(f"[MERGE] Table will NOT be partitioned")

    temp_dir = None  # Track temp directory for cleanup

    try:
        schema_json = read_json_from_s3(schema_s3_path)
        columns, data_types, table_name, primary_keys, dedupe_key = get_table_schema(schema_json, table_name)

        # Read source files
        df_raw = spark.read.parquet(*filepaths)
        source_columns = df_raw.columns
        source_columns_lower = [c.lower() for c in source_columns]
        has_op_in_source = "op" in source_columns_lower

        log.info(f"[MERGE] Source columns: {source_columns}")
        log.info(f"[MERGE] Source has 'op' column: {has_op_in_source}")

        # Build column list for selection - include 'op' if present in source
        columns_lower = [c.lower() for c in columns]
        columns_to_select = []

        for src_col in source_columns:
            src_col_lower = src_col.lower()
            # Include if it's in schema (but not 'op') OR if it's 'op' and exists in source
            if (src_col_lower in columns_lower and src_col_lower != 'op') or \
               (src_col_lower == 'op' and has_op_in_source):
                columns_to_select.append(src_col)

        log.info(f"[MERGE] Selecting columns from source: {columns_to_select}")

        df_source = df_raw.select(*columns_to_select)

        # Count source - respect SKIP_COUNTS_FOR_TABLE
        if not SKIP_COUNTS_FOR_TABLE:
            initial_count = df_source.count()
            log.info(f"[MERGE] Read {initial_count} source rows (before deduplication)")
        else:
            log.info(f"[MERGE] SKIPPING source row count (table in SKIP_COUNT_TABLES)")
            initial_count = -1

        # Normalize column names to lowercase
        df_source = df_source.toDF(*[c.lower() for c in df_source.columns])

        # Normalize keys
        primary_keys_lower = [pk.lower() for pk in primary_keys] if primary_keys else []
        dedupe_key_lower = dedupe_key.lower() if dedupe_key else None
        data_types_lower = {k.lower(): v for k, v in data_types.items()}

        has_primary_key = primary_keys_lower and len(primary_keys_lower) > 0
        if not has_primary_key:
            log.info("[MERGE] No primary keys defined - treating as APPEND-ONLY table")

        # ========== STEP 1: DEDUPLICATE SOURCE DATA ==========
        if has_primary_key and dedupe_key_lower:
            df_source = deduplicate_by_date_column(df_source, primary_keys_lower, dedupe_key_lower)
            if not SKIP_COUNTS_FOR_TABLE:
                dedupe_count = df_source.count()
                log.info(f"[MERGE] After deduplication: {dedupe_count} rows (removed {initial_count - dedupe_count} duplicates)")
        else:
            log.info("[MERGE] Skipping deduplication (no primary keys or dedupe_key)")
        # =====================================================

        # ========== STEP 2: CONVERT DATA TYPES ==========
        df_source = convert_data_types(df_source, data_types_lower)

        columns_lower_list = [c.lower() for c in columns]
        df_source, missing_columns, pk_missing = verify_and_add_missing_columns(df_source, columns_lower_list, primary_keys_lower)

        if has_primary_key and pk_missing:
            log.error(f"[MERGE] PK(s) {primary_keys} missing in source for {table_name}.")
            return 0, "Failed"

        # Add 'op' column if not present in source
        if not has_op_in_source:
            df_source = df_source.withColumn("op", F.lit("I"))
            log.info("[MERGE] Added 'op'='I' column (not present in source)")
        else:
            log.info("[MERGE] Using existing 'op' column from source")

        # Verify 'op' column exists
        if "op" not in df_source.columns:
            raise RuntimeError("[MERGE] CRITICAL: 'op' column missing after processing!")
        # ================================================

        # Check if source has data (use head instead of count for performance)
        source_head = df_source.head(1)
        if not source_head or len(source_head) == 0:
            log.info("[MERGE] No source rows after filtering, exiting")
            return 0, "Complete"

        # ========== STEP 3: CDC WRITE (DEDUPLICATED DATA INCLUDING DELETES) ==========
        log.info("[MERGE] Writing DEDUPLICATED data to CDC location (includes op='D' records)")

        # Extract dart_filedate from first row of source data
        dart_filedate = extract_filedate_from_data(df_source)
        if not dart_filedate:
            dart_filedate = datetime.utcnow().strftime("%Y-%m-%d")
            log.warning(f"[MERGE] Falling back to current date for dart_filedate: {dart_filedate}")

        # Log op distribution before CDC write
        if not SKIP_COUNTS_FOR_TABLE:
            op_dist = df_source.groupBy("op").count().collect()
            op_summary = {row["op"]: row["count"] for row in op_dist}
            log.info(f"[MERGE] CDC op distribution (deduplicated): {op_summary}")
        else:
            log.info(f"[MERGE] SKIPPING op distribution count (table in SKIP_COUNT_TABLES)")

        write_cdc_changes(df_source, table_name, TARGET_CDC_DIR, dart_filedate)
        log.info(f"[MERGE] CDC write complete. For Postgres staging incremental, query: {TARGET_CDC_DIR}")
        # =============================================================================

        # ========== STEP 4: MERGE TO FINAL ZONE (EXCLUDES DELETES) ==========
        if not has_primary_key:
            # APPEND-ONLY MODE
            log.info("[MERGE] APPEND-ONLY mode: appending non-delete records to Final Zone")

            df_source_non_deletes = df_source.filter(F.col("op") != "D")

            if not SKIP_COUNTS_FOR_TABLE:
                non_delete_count = df_source_non_deletes.count()
                log.info(f"[MERGE] Non-delete records for Final Zone: {non_delete_count}")
            else:
                log.info(f"[MERGE] SKIPPING non-delete count (table in SKIP_COUNT_TABLES)")

            # Try to read existing target using helper function
            df_target_raw = read_target_dataframe(target_dataset_dir, partition_col)

            if df_target_raw is not None:

                # CRITICAL: Write to temp, read back, then checkpoint to fully materialize
                temp_dir = f"s3://{TARGET_BUCKET}/{TARGET_PREFIX}/_temp/{table_name}_{uuid.uuid4().hex}/"
                log.info(f"[MERGE] Materializing target to temp: {temp_dir}")

                df_target_raw.write.mode("overwrite").parquet(temp_dir)

                # Read back and immediately checkpoint to break ALL lineage
                df_target = spark.read.parquet(temp_dir)
                df_target = df_target.localCheckpoint(eager=True)

                # Check if target has data
                target_head = df_target.head(1)
                target_exists = target_head is not None and len(target_head) > 0

                if not SKIP_COUNTS_FOR_TABLE:
                    target_count = df_target.count()
                    log.info(f"[MERGE] Existing Final Zone has {target_count} rows (checkpointed)")
                else:
                    log.info(f"[MERGE] SKIPPING target count (table in SKIP_COUNT_TABLES)")
                    log.info(f"[MERGE] Existing Final Zone found (count skipped)")

                if target_exists:
                    df_target = convert_data_types(df_target, data_types_lower)
                    df_final = df_target.unionByName(df_source_non_deletes, allowMissingColumns=True)
                    log.info(f"[MERGE] Appended new rows to existing data")
                else:
                    log.info("[MERGE] Final Zone is empty, using source non-deletes as baseline")
                    df_final = df_source_non_deletes

            else:
                log.warning(f"[MERGE] Final Zone not found → creating new dataset")
                df_final = df_source_non_deletes
                df_final, _, _ = verify_and_add_missing_columns(df_final, columns_lower_list, primary_keys_lower)
                df_final = convert_data_types(df_final, data_types_lower)

        else:
            # UPSERT MODE
            log.info(f"[MERGE] UPSERT mode with primary keys: {primary_keys_lower}")

            affected_keys = df_source.select(*primary_keys_lower).distinct()

            if not SKIP_COUNTS_FOR_TABLE:
                affected_count = affected_keys.count()
                log.info(f"[MERGE] Affected keys: {affected_count}")
                if affected_count == 0:
                    log.info("[MERGE] No affected keys, exiting")
                    return 0, "Complete"
            else:
                log.info(f"[MERGE] SKIPPING affected keys count (table in SKIP_COUNT_TABLES)")
                # Check if any affected keys exist
                affected_head = affected_keys.head(1)
                if not affected_head or len(affected_head) == 0:
                    log.info("[MERGE] No affected keys, exiting")
                    return 0, "Complete"

            # Try to read existing target using helper function
            df_target_raw = read_target_dataframe(target_dataset_dir, partition_col)

            if df_target_raw is not None:

                # CRITICAL: Write to temp, read back, then checkpoint to fully materialize
                temp_dir = f"s3://{TARGET_BUCKET}/{TARGET_PREFIX}/_temp/{table_name}_{uuid.uuid4().hex}/"
                log.info(f"[MERGE] Materializing target to temp: {temp_dir}")

                df_target_raw.write.mode("overwrite").parquet(temp_dir)

                # Read back and immediately checkpoint to break ALL lineage
                df_target = spark.read.parquet(temp_dir)
                df_target = df_target.localCheckpoint(eager=True)

                # Check if target has data
                target_head = df_target.head(1)
                target_exists = target_head is not None and len(target_head) > 0

                if not SKIP_COUNTS_FOR_TABLE:
                    target_count = df_target.count()
                    log.info(f"[MERGE] Existing Final Zone has {target_count} rows (checkpointed)")
                else:
                    log.info(f"[MERGE] SKIPPING target count (table in SKIP_COUNT_TABLES)")
                    log.info(f"[MERGE] Existing Final Zone found (count skipped)")

                if target_exists:
                    df_target = convert_data_types(df_target, data_types_lower)

                    # Log target columns for debugging
                    log.info(f"[MERGE] Target columns after read: {df_target.columns}")
                    if partition_col and partition_col not in df_target.columns:
                        log.error(f"[MERGE] CRITICAL: Partition column '{partition_col}' missing from target!")
                        log.error(f"[MERGE] This will cause data loss. Check read_target_dataframe logic.")

                    # Preserve rows not affected by this merge
                    df_target_preserved = df_target.join(affected_keys, on=primary_keys_lower, how="left_anti")

                    if not SKIP_COUNTS_FOR_TABLE:
                        preserved_count = df_target_preserved.count()
                        log.info(f"[MERGE] Preserved {preserved_count} existing rows (not affected by this merge)")
                    else:
                        log.info(f"[MERGE] SKIPPING preserved row count (table in SKIP_COUNT_TABLES)")

                    # Get non-delete operations from source for Final Zone
                    df_source_updates = df_source.filter(F.col("op") != "D")

                    if not SKIP_COUNTS_FOR_TABLE:
                        updates_count = df_source_updates.count()
                        log.info(f"[MERGE] Adding {updates_count} updates (excluding deletes)")
                    else:
                        log.info(f"[MERGE] SKIPPING updates count (table in SKIP_COUNT_TABLES)")

                    df_final = df_target_preserved.unionByName(df_source_updates, allowMissingColumns=True)
                else:
                    log.info("[MERGE] Final Zone is empty, using source non-deletes as baseline")
                    df_final = df_source.filter(F.col("op") != "D")
                    df_final, _, _ = verify_and_add_missing_columns(df_final, columns_lower_list, primary_keys_lower)
                    df_final = convert_data_types(df_final, data_types_lower)

            else:
                log.warning(f"[MERGE] Final Zone not found → creating new dataset")
                df_final = df_source.filter(F.col("op") != "D")
                df_final, _, _ = verify_and_add_missing_columns(df_final, columns_lower_list, primary_keys_lower)
                df_final = convert_data_types(df_final, data_types_lower)
        # ====================================================================

        # ========== STEP 5: WRITE TO FINAL ZONE ==========
        # Estimate input bytes for DQ metrics
        total_in_bytes = _estimate_total_input_bytes(filepaths)

        log.info(f"[MERGE] Writing Final Zone to {target_dataset_dir}")

        # Log final columns for debugging
        log.info(f"[MERGE] Final DataFrame columns: {df_final.columns}")
        if partition_col:
            if partition_col in df_final.columns:
                log.info(f"[MERGE] Partition column '{partition_col}' present in final DataFrame")
            else:
                log.error(f"[MERGE] Partition column '{partition_col}' MISSING from final DataFrame!")

        # Write using helper function (handles partitioning)
        write_dataframe_to_parquet(df_final, target_dataset_dir, partition_col)

        # Clean up temp directory after successful write
        if temp_dir:
            cleanup_temp_directory(temp_dir)

        log.info("[MERGE] Write completed, collecting metrics")
        write_dq_metrics(
            df_final=df_final, table_name=table_name,
            primary_keys=primary_keys_lower, dedupe_key=(dedupe_key_lower or ""),
            total_input_bytes=total_in_bytes,
            target_dataset_dir=target_dataset_dir, job_id=JOB_ID, load_type="incremental_load"
        )

        # Final count for audit - this is required
        if not SKIP_COUNTS_FOR_TABLE:
            final_count = df_final.count()
            log.info(f"[MERGE] Successfully completed: {final_count} records in Final Zone (excludes deletes)")
        else:
            log.info(f"[MERGE] SKIPPING final count for audit (table in SKIP_COUNT_TABLES)")
            final_count = -1  # Indicate count was skipped
            log.info(f"[MERGE] Successfully completed: Final Zone written (count skipped)")

        return final_count, "Complete"

    except Exception as e:
        # Clean up temp on failure too
        if temp_dir:
            cleanup_temp_directory(temp_dir)
        log.error(f"[MERGE] Failed with error: {str(e)}")
        raise

# ------------------------------
# MAIN
# ------------------------------
if __name__ == "__main__":
    try:
        log.info("="*80)
        log.info(f"[MAIN] Job started: {JOB_NAME}")
        log.info(f"[MAIN] Discovering inputs for Table={TABLE}, JobId={JOB_ID}")
        log.info("="*80)

        potential = fetch_potential_filepaths(pcdb_conn, JOB_ID, TABLE)
        if not potential:
            msg = f"No dataset prefixes found for table {TABLE} under job_id {JOB_ID}."
            log.warning(f"[MAIN] {msg}")
            log_to_db(pcdb_conn, LOAD_OPER_TGT_ID, JOB_ID, TARGET_DATASET_DIR, "Complete",
                     0, OPER_STRT_DT, datetime.utcnow(), JOB_NAME, msg)
            job.commit()
            sys.exit(0)

        filepaths = filter_filepaths(potential)
        if not filepaths:
            msg = f"No parquet parts found for table {TABLE}."
            log.warning(f"[MAIN] {msg}")
            log_to_db(pcdb_conn, LOAD_OPER_TGT_ID, JOB_ID, TARGET_DATASET_DIR, "Complete",
                     0, OPER_STRT_DT, datetime.utcnow(), JOB_NAME, msg)
            job.commit()
            sys.exit(0)

        # Debug listing
        try:
            fs = s3fs.S3FileSystem()
            parsed = urlparse(filepaths[0])
            base_dir = '/'.join(parsed.path.split('/')[:-1]) + '/'
            s3_base_dir = f"s3://{parsed.netloc}/{base_dir.lstrip('/')}"
            log.info(f"[DEBUG] Sample listing under {s3_base_dir}")
            sample_list = fs.ls(s3_base_dir)[:10]
            log.info(f"[DEBUG] Found {len(sample_list)} items")
        except Exception as e:
            log.debug(f"[DEBUG] Directory listing skipped: {e}")

        load_type = determine_load_type(filepaths)

        if load_type == "initial_load":
            log.info("[MAIN] Executing INITIAL LOAD flow")
            log.info("[MAIN] NOTE: Empty CDC table will be created for Glue Crawler discovery")
            file_count, status = perform_initial_load(filepaths, TARGET_DATASET_DIR, SCHEMA_S3_PATH, TABLE, JOB_ID)
        else:
            log.info("[MAIN] Executing INCREMENTAL MERGE flow")
            log.info(f"[MAIN] CDC will be written to: {TARGET_CDC_DIR}")
            file_count, status = merge_parquet_files(filepaths, TARGET_DATASET_DIR, SCHEMA_S3_PATH, TABLE)

        log_to_db(pcdb_conn, LOAD_OPER_TGT_ID, JOB_ID, TARGET_DATASET_DIR,
                  status, file_count, OPER_STRT_DT, datetime.utcnow(), JOB_NAME, None)

        log.info("="*80)
        log.info(f"[MAIN] Job completed successfully: status={status}, records={file_count}")
        log.info(f"[MAIN] Final Zone: {TARGET_DATASET_DIR}")
        log.info(f"[MAIN] CDC Zone: {TARGET_CDC_DIR}")
        log.info("="*80)
        job.commit()

    except Exception as e:
        error_detail = str(e)
        # Extract more detail from Spark exceptions
        if "org.apache.spark" in error_detail:
            log.error(f"[MAIN] Spark exception occurred")
            log.error(f"[MAIN] Full error: {error_detail[:1000]}")  # First 1000 chars

            # Try to extract root cause
            if "Caused by:" in error_detail:
                cause_start = error_detail.find("Caused by:")
                root_cause = error_detail[cause_start:cause_start+500]
                log.error(f"[MAIN] Root cause: {root_cause}")

        msg = error_detail[:200]
        log.error("="*80)
        log.error(f"[MAIN] FATAL ERROR: {msg}")
        log.error("="*80)

        update_process_control_job_with_error(pcdb_conn, JOB_ID, msg)
        job.commit()
        raise