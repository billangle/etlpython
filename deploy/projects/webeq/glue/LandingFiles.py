"""
================================================================================
AWS Glue Job: DMS Landing File Publisher
================================================================================

PURPOSE:
    Processes DMS (Database Migration Service) parquet files from S3, handling
    both initial LOAD files and incremental CDC (Change Data Capture) files.
    Publishes consolidated parquet files to destination datasets using Spark
    for schema alignment and data merging.

PROCESSING LOGIC:

    1. LOAD File Processing (Initial/Full Load):
       - Detects unprocessed LOAD files based on S3 LastModified timestamp
       - ONLY processes LOAD files where LastModified > previous load_timestamp
       - This prevents mixing new LOAD files with stale old LOAD files
       - Single LOAD file: copies directly to Datasets/<TABLE>-LOAD/
       - Multiple new LOAD files: merges and writes to Datasets/<TABLE>-LOAD/
       - Records max LastModified of processed LOAD files as new load_timestamp

    2. Incremental File Processing (CDC) - TIMESTAMP-BASED:
       - Tracks last_processed_timestamp per table (NOT filename date)
       - Processes ALL files where S3 LastModified > last_processed_timestamp
       - This ensures late-arriving files are NEVER skipped
       - Single file: copies to Datasets/<TABLE>/
       - Multiple files: merges and writes to Datasets/<TABLE>/
       - Updates last_processed_timestamp to max(LastModified) of processed files

    3. New LOAD Handling:
       - Detects new LOAD files arriving at any point
       - ONLY uses new LOAD files (ignores stale old LOAD files)
       - Resets cutoff timestamp to new LOAD's LastModified
       - Resumes incremental processing from new baseline

KEY FEATURES:
    - TIMESTAMP-BASED TRACKING: Never skips late-arriving files
    - SAFE LOAD HANDLING: Only processes new LOAD files, ignores stale ones
    - Exact prefix listing (prevents reading sibling folders like dbo-backup)
    - Compressed parquet output (~1GB parts by default)
    - Schema-safe union with type harmonization fallback
    - Configurable bad table skipping
    - Sequential table processing (avoids Spark task explosion)
    - State tracking via downloaded_files.json

ENVIRONMENT VARIABLES:
    PARQUET_CODEC                    : Compression codec (default: snappy)
    TARGET_PART_MB                   : Target size per part in MB (default: 1024)
    SKIP_COUNTS                      : Skip row counting (default: true)
    SKIP_BAD_TABLES                  : Continue on table errors (default: true)
    CAST_CONFLICT_TO_STRING          : Cast type conflicts to string (default: true)
    DROP_EMPTY_COLUMN_NAME_FIELDS    : Drop columns with empty names (default: true)

GLUE JOB ARGUMENTS:
    --JOB_NAME           : Glue job name
    --DestinationBucket  : S3 bucket for output
    --DestinationPrefix  : S3 prefix for output (e.g., landing/processed)
    --PipelineName       : Data pipeline identifier in control database
    --SourcePrefix       : S3 prefix for DMS source files (e.g., sbsd/dbo)
    --SecretId           : AWS Secrets Manager ID for database credentials

OUTPUT STRUCTURE:
    DestinationPrefix/
    |-- YYYYMMDD/
    |   +-- {job_id}/
    |       +-- Datasets/
    |           |-- TABLE1-LOAD/
    |           |   +-- part-{uuid}.parquet
    |           |-- TABLE1/
    |           |   +-- part-{uuid}.parquet
    |           +-- TABLE2/
    |               +-- part-{uuid}.parquet
    |-- job_id.json
    +-- downloaded_files.json

STATE FILES:
    downloaded_files.json: Processing history per table with timestamps
        {
            "file_name": "incremental_batch",
            "folder": "table_name",
            "load_timestamp": "2025-11-15T10:30:00Z",
            "last_processed_timestamp": "2025-11-16T14:30:00Z",
            "files_combined": ["20251115-001.parquet", "20251116-001.parquet"]
        }

VERSION HISTORY:
    v1.0.0 - 2025-11-15 - Initial implementation
        - Basic LOAD and incremental processing
        - Date-based tracking using filename date (yyyymmdd)
        - Parallel table processing using ThreadPoolExecutor
        - Merged file approach in source directory

    v2.0.0 - 2026-02-11 - Performance fixes
        - Fixed "threshold for consecutive task creation reached" error
        - Changed from parallel to sequential table processing
        - Added proper SKIP_COUNTS handling to avoid unnecessary Spark jobs
        - Added DataFrame caching before count+write operations
        - Increased spark.task.maxConsecutiveCreation threshold to 50000
        - Added FAIR scheduler mode for better resource management

    v3.0.0 - 2026-02-11 - Timestamp-based tracking
        - Changed from date-based to LastModified timestamp tracking
        - Late-arriving files are now processed correctly
        - Added last_processed_timestamp to state tracking
        - Files are selected based on S3 LastModified, not filename date
        - Prevents skipping files that arrive after their date was processed

    v4.0.0 - 2026-02-11 - Safe LOAD handling
        - Only processes LOAD files newer than previous load_timestamp
        - Prevents mixing new LOAD files with stale old LOAD files
        - Handles scenario where source replaces only some LOAD files
        - Added _select_new_load_files() function for safe filtering
        - Added _has_new_load_files() function for detection
        - Enhanced logging for LOAD file selection decisions
        - Added files_combined tracking to state file for incremental processing

AUTHOR:
    Steampunk Data Engineering Team

DEPENDENCIES:
    - boto3
    - psycopg2
    - pyspark
    - awsglue

================================================================================
"""

import os
import sys
import re
import json
import uuid
import math
import logging
from datetime import datetime, timezone, timedelta

import boto3
import psycopg2
from boto3.s3.transfer import TransferConfig
from botocore.config import Config

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, LongType, DoubleType, DateType, TimestampType
)
from pyspark.storagelevel import StorageLevel

# =============================================================================
# CONFIGURATION AND INITIALIZATION
# =============================================================================

# -------------------------
# Glue Job Arguments
# -------------------------
args = getResolvedOptions(sys.argv, [
    "JOB_NAME", "DestinationBucket", "DestinationPrefix",
    "PipelineName", "SourcePrefix", "SecretId"
])

DEST_BUCKET   = args["DestinationBucket"]
DEST_PREFIX   = args["DestinationPrefix"].rstrip("/")
SRC_PREFIX    = args["SourcePrefix"].rstrip("/")
PIPELINE_NAME = args["PipelineName"]
SECRET_NAME   = args["SecretId"]
SCHEMA_JSON_PATH = f"s3://{DEST_BUCKET.replace('landing','final')}/{DEST_PREFIX.split('/')[0]}/_configs/{DEST_PREFIX.split('/')[0]}_target_schema.json"
REGION        = os.environ.get("AWS_REGION", "us-east-1")

# -------------------------
# Runtime Configuration
# -------------------------
PARQUET_CODEC          = os.environ.get("PARQUET_CODEC", "snappy").lower()
TARGET_PART_MB         = int(os.environ.get("TARGET_PART_MB", "1024"))
SKIP_COUNTS            = os.environ.get("SKIP_COUNTS", "true").lower() == "true"
SKIP_BAD_TABLES        = os.environ.get("SKIP_BAD_TABLES", "true").lower() == "true"
CAST_CONFLICT_TO_STRING       = os.environ.get("CAST_CONFLICT_TO_STRING", "true").lower() == "true"
DROP_EMPTY_COLUMN_NAME_FIELDS = os.environ.get("DROP_EMPTY_COLUMN_NAME_FIELDS", "true").lower() == "true"

# -------------------------
# Spark Configuration
# -------------------------
conf = (
    SparkConf()
    # Adaptive query execution for better performance
    .set("spark.sql.adaptive.enabled", "true")
    .set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    # Parquet optimizations
    .set("spark.sql.parquet.filterPushdown", "true")
    .set("spark.sql.parquet.mergeSchema", "false")
    .set("spark.sql.parquet.compression.codec", PARQUET_CODEC)
    # Legacy timestamp handling for compatibility
    .set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
    .set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
    .set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
    .set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
    # Serialization
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    # S3 optimizations
    .set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    .set("spark.hadoop.fs.s3a.fast.upload", "true")
    .set("spark.hadoop.fs.s3a.connection.maximum", "512")
    .set("spark.hadoop.fs.s3a.multipart.size", str(128 * 1024 * 1024))
    .set("spark.sql.files.maxPartitionBytes", "256m")
    # Task creation threshold fix (v2.0.0)
    # Prevents "threshold for consecutive task creation reached" error
    .set("spark.task.maxConsecutiveCreation", "50000")
    .set("spark.scheduler.mode", "FAIR")
    .set("spark.default.parallelism", "200")
)

sc = SparkContext(conf=conf)
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session

job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

# -------------------------
# Logging Configuration
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
log = logging.getLogger("landing_files")

# -------------------------
# AWS Clients
# -------------------------
sm = boto3.client("secretsmanager", region_name=REGION)
_boto_cfg = Config(max_pool_connections=256)
s3  = boto3.client("s3", region_name=REGION, config=_boto_cfg)
s3r = boto3.resource("s3", region_name=REGION, config=_boto_cfg)

# -------------------------
# Database Connection
# -------------------------
secret_val = sm.get_secret_value(SecretId=SECRET_NAME)
secret = json.loads(secret_val["SecretString"])
db_host = secret["edv_postgres_hostname"]
db_port = secret["postgres_port"]
db_name = secret["postgres_prcs_ctrl_dbname"]
db_user = secret["edv_postgres_username"]
db_pass = secret["edv_postgres_password"]

conn = psycopg2.connect(
    host=db_host,
    port=db_port,
    user=db_user,
    password=db_pass,
    dbname=db_name
)
conn.autocommit = True
cursor = conn.cursor()

# -------------------------
# Global Variables
# -------------------------
glue_job_name = args["JOB_NAME"]
lambda_start = datetime.utcnow()

# S3 transfer configuration for multipart uploads
TRANSFER_CFG = TransferConfig(
    multipart_threshold=8 * 1024 * 1024,
    multipart_chunksize=128 * 1024 * 1024,
    max_concurrency=32,
    use_threads=True,
)

# -------------------------
# Startup Logging
# -------------------------
log.info("=" * 70)
log.info("LANDING FILES JOB CONFIGURATION (v4.0.0)")
log.info("=" * 70)
log.info("Job Name: %s", glue_job_name)
log.info("Pipeline: %s", PIPELINE_NAME)
log.info("Source Prefix: %s", SRC_PREFIX)
log.info("Destination Bucket: %s", DEST_BUCKET)
log.info("Destination Prefix: %s", DEST_PREFIX)
log.info("-" * 70)
log.info("SKIP_COUNTS: %s (row counting %s)", 
         SKIP_COUNTS, "DISABLED" if SKIP_COUNTS else "ENABLED")
log.info("SKIP_BAD_TABLES: %s", SKIP_BAD_TABLES)
log.info("PARQUET_CODEC: %s", PARQUET_CODEC)
log.info("TARGET_PART_MB: %d", TARGET_PART_MB)
log.info("CAST_CONFLICT_TO_STRING: %s", CAST_CONFLICT_TO_STRING)
log.info("DROP_EMPTY_COLUMN_NAME_FIELDS: %s", DROP_EMPTY_COLUMN_NAME_FIELDS)
log.info("-" * 70)
log.info("TRACKING MODE: S3 LastModified timestamp")
log.info("LOAD HANDLING: Only new LOAD files (stale files ignored)")
log.info("PROCESSING MODE: Sequential (prevents Spark task explosion)")
log.info("=" * 70)


# =============================================================================
# SCHEMA CONFIGURATION FUNCTIONS
# =============================================================================

def _load_schema_json(s3_path):
    """
    Load and parse the target schema JSON file from S3.

    This function reads a schema definition JSON file from S3 and extracts
    the list of tables that should be processed. Only tables defined in this
    JSON will be processed; all other tables found in the source location
    will be ignored.

    Args:
        s3_path (str): Full S3 path to the schema JSON file.
            Example: "s3://bucket/app/etl-jobs/app_target_schema.json"

    Returns:
        set: A set of lowercase table names to process, or None if s3_path
            is empty/None.

    Raises:
        ValueError: If the S3 path format is invalid or JSON is malformed.
        FileNotFoundError: If the schema JSON file does not exist in S3.
        RuntimeError: If there is an unexpected error loading the file.

    Example Schema JSON Format:
        {
            "tables": [
                {
                    "source_table": "accounting_program",
                    "target_table": "accounting_program",
                    "columns": [...],
                    "primary_key": "accounting_program_identifier",
                    "dedupe_key": "last_change_date"
                },
                ...
            ]
        }

    Version History:
        v1.0.0 - Initial implementation
        v3.1.0 - Added to support selective table processing
    """
    if not s3_path:
        log.warning("No schema JSON path provided, all tables will be processed")
        return None

    # Validate S3 path format
    if not s3_path.startswith("s3://"):
        raise ValueError(f"Invalid S3 path format. Expected 's3://bucket/key', got: {s3_path}")

    # Parse bucket and key from S3 path
    parts = s3_path[5:].split("/", 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid S3 path format. Could not parse bucket and key from: {s3_path}")

    bucket = parts[0]
    key = parts[1]

    log.info("Loading schema JSON from S3")
    log.info("  Bucket: %s", bucket)
    log.info("  Key: %s", key)

    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        schema_data = json.loads(response['Body'].read().decode('utf-8'))

        # Extract table names from schema
        table_names = set()
        if "tables" in schema_data:
            for table_def in schema_data["tables"]:
                source_table = table_def.get("source_table")
                if source_table:
                    # Convert to lowercase for case-insensitive matching
                    table_names.add(source_table.lower())

        log.info("Schema JSON loaded successfully")
        log.info("  Tables defined: %d", len(table_names))
        
        if table_names:
            log.debug("  Table list: %s", sorted(table_names))
        else:
            log.warning("  No tables found in schema JSON - this may be unintentional")

        return table_names

    except s3.exceptions.NoSuchKey:
        raise FileNotFoundError(f"Schema JSON file not found in S3: {s3_path}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON format in schema file: {e}")
    except Exception as e:
        raise RuntimeError(f"Failed to load schema JSON from S3: {e}")


def _filter_folders_by_schema(folders, allowed_tables):
    """
    Filter table folders to only those defined in the schema JSON.

    This function performs case-insensitive matching between discovered
    S3 folder names and the table names defined in the schema JSON.
    Tables not in the schema are excluded from processing.

    Args:
        folders (list): List of S3 folder prefixes discovered in the source.
            Example: ['sbsd/dbo/table1/', 'sbsd/dbo/table2/']
        allowed_tables (set): Set of lowercase table names from schema JSON.
            Example: {'table1', 'table3'}

    Returns:
        tuple: A tuple containing:
            - filtered (list): Folders that match allowed tables
            - excluded_folders (list): Folders that were excluded

    Example:
        >>> folders = ['sbsd/dbo/table1/', 'sbsd/dbo/table2/', 'sbsd/dbo/table3/']
        >>> allowed = {'table1', 'table3'}
        >>> filtered, excluded = _filter_folders_by_schema(folders, allowed)
        >>> filtered
        ['sbsd/dbo/table1/', 'sbsd/dbo/table3/']
        >>> excluded
        ['sbsd/dbo/table2/']

    Version History:
        v1.0.0 - Initial implementation
        v3.1.0 - Added detailed logging of included/excluded tables
    """
    if not allowed_tables:
        log.warning("No allowed tables specified - returning all folders unfiltered")
        return folders, []

    filtered = []
    excluded = []
    excluded_folders = []

    for folder in folders:
        # Extract table name from folder path
        # e.g., 'sbsd/dbo/accounting_program/' -> 'accounting_program'
        table_name = folder.rstrip("/").split("/")[-1].lower()

        if table_name in allowed_tables:
            filtered.append(folder)
        else:
            excluded.append(table_name)
            excluded_folders.append(folder)

    log.info("Table filtering complete")
    log.info("  Tables included: %d", len(filtered))
    log.info("  Tables excluded: %d", len(excluded))

    if filtered:
        included_names = [f.rstrip("/").split("/")[-1] for f in filtered]
        log.debug("  Included tables: %s", sorted(included_names))

    if excluded:
        # Log first 20 excluded tables to avoid log flooding
        log.debug("  Excluded tables (first 20): %s", sorted(excluded)[:20])
        if len(excluded) > 20:
            log.debug("  ... and %d more excluded tables", len(excluded) - 20)

    return filtered, excluded_folders


# =============================================================================
# S3 HELPER FUNCTIONS
# =============================================================================

def _exact_prefix(prefix):
    """
    Ensure a prefix has a trailing slash for proper S3 folder semantics.

    S3 uses prefixes rather than true folders. To ensure we only match
    exact folder contents (not sibling prefixes), we need trailing slashes.
    For example, 'dbo' without trailing slash would also match 'dbo-backup'.

    Args:
        prefix (str): S3 prefix path, with or without trailing slash.

    Returns:
        str: Prefix with guaranteed trailing slash.

    Example:
        >>> _exact_prefix('sbsd/dbo')
        'sbsd/dbo/'
        >>> _exact_prefix('sbsd/dbo/')
        'sbsd/dbo/'

    Version History:
        v1.0.0 - Initial implementation
    """
    return prefix.rstrip("/") + "/"


def _list_table_folders_exact(bucket, base_prefix):
    """
    List ONLY immediate child folders under the exact source prefix.

    Uses S3 delimiter='/' to get only direct children, preventing
    traversal into sibling prefixes. This is critical to avoid
    accidentally reading from backup or archive folders.

    Args:
        bucket (str): S3 bucket name.
        base_prefix (str): Base prefix to list children of.
            Example: 'sbsd/dbo'

    Returns:
        list: List of folder prefixes (with trailing slashes).
            Example: ['sbsd/dbo/table1/', 'sbsd/dbo/table2/']

    Example:
        Given S3 structure:
            sbsd/dbo/table1/file.parquet
            sbsd/dbo/table2/file.parquet
            sbsd/dbo-backup/table1/file.parquet

        >>> _list_table_folders_exact('bucket', 'sbsd/dbo')
        ['sbsd/dbo/table1/', 'sbsd/dbo/table2/']
        # Note: 'sbsd/dbo-backup/' is NOT included

    Version History:
        v1.0.0 - Initial implementation
        v2.0.0 - Added exact prefix matching with delimiter
    """
    base = _exact_prefix(base_prefix)
    folders = []
    
    log.info("Listing table folders under: s3://%s/%s", bucket, base)
    
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=base, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            folders.append(cp["Prefix"])
    
    log.info("Found %d table folders", len(folders))
    return folders


def _list_parquet_keys_with_metadata(bucket, prefix):
    """
    List all parquet files under a prefix with their S3 metadata.

    Returns file keys along with LastModified timestamps, which are
    the source of truth for determining file processing order in
    timestamp-based tracking.

    Args:
        bucket (str): S3 bucket name.
        prefix (str): S3 prefix to list files under.

    Returns:
        list: List of dictionaries containing file information.
            Each dict has keys:
                - 'Key' (str): Full S3 key path
                - 'LastModified' (datetime): S3 LastModified timestamp

    Example:
        >>> files = _list_parquet_keys_with_metadata('bucket', 'sbsd/dbo/table1/')
        >>> files[0]
        {'Key': 'sbsd/dbo/table1/LOAD00001.parquet',
         'LastModified': datetime(2026, 1, 15, 10, 0, 0, tzinfo=...)}

    Version History:
        v1.0.0 - Initial implementation (keys only)
        v3.0.0 - Added LastModified metadata for timestamp-based tracking
    """
    keys = []
    prefix_exact = _exact_prefix(prefix)
    
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix_exact):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append({
                    'Key': obj["Key"],
                    'LastModified': obj["LastModified"]
                })
    
    log.debug("Found %d parquet files under %s", len(keys), prefix)
    return keys


def _sum_sizes_bytes(keys):
    """
    Calculate the total size in bytes of a list of S3 objects.

    Used to determine optimal partition count for Spark processing
    based on the TARGET_PART_MB configuration.

    Args:
        keys (list): List of S3 keys to measure.

    Returns:
        int: Total size in bytes of all specified files.

    Version History:
        v1.0.0 - Initial implementation
    """
    total = 0
    for k in keys:
        h = s3.head_object(Bucket=DEST_BUCKET, Key=k)
        total += h["ContentLength"]
    
    log.debug("Total size of %d files: %d bytes (%.2f MB)", 
              len(keys), total, total / (1024 * 1024))
    return total


def _ensure_empty_prefix(bucket, prefix):
    """
    Delete all objects under a prefix to ensure clean overwrite.

    This function handles pagination and batch deletion (1000 objects
    per request as per S3 limits). Safe to call on non-existent prefixes.

    Args:
        bucket (str): S3 bucket name.
        prefix (str): S3 prefix to clear.

    Version History:
        v1.0.0 - Initial implementation
        v2.0.0 - Added batch deletion for performance (1000 per request)
    """
    pref = _exact_prefix(prefix)
    
    log.debug("Clearing existing objects under: s3://%s/%s", bucket, pref)
    
    paginator = s3.get_paginator("list_objects_v2")
    to_delete = []
    deleted_count = 0
    
    for page in paginator.paginate(Bucket=bucket, Prefix=pref):
        for obj in page.get("Contents", []):
            to_delete.append({"Key": obj["Key"]})
            if len(to_delete) == 1000:
                s3.delete_objects(Bucket=bucket, Delete={"Objects": to_delete})
                deleted_count += len(to_delete)
                to_delete = []
    
    if to_delete:
        s3.delete_objects(Bucket=bucket, Delete={"Objects": to_delete})
        deleted_count += len(to_delete)
    
    if deleted_count > 0:
        log.debug("Deleted %d existing objects", deleted_count)


def _copy_one(src_key, dst_key):
    """
    Copy a single S3 object using multipart transfer configuration.

    Uses the global TRANSFER_CFG for optimal handling of large files
    with multipart uploads and concurrent transfers.

    Args:
        src_key (str): Source S3 key (within DEST_BUCKET).
        dst_key (str): Destination S3 key (within DEST_BUCKET).

    Version History:
        v1.0.0 - Initial implementation
        v2.0.0 - Added TRANSFER_CFG for large file handling
    """
    s3r.Object(DEST_BUCKET, dst_key).copy(
        {"Bucket": DEST_BUCKET, "Key": src_key}, 
        Config=TRANSFER_CFG
    )


# =============================================================================
# FILE PATTERN DETECTION FUNCTIONS
# =============================================================================

def _get_date_from_name(filename):
    """
    Extract the date component from an incremental filename.

    Incremental files follow the pattern: yyyymmdd-NNN.parquet
    where NNN is a sequence number.

    Args:
        filename (str): Filename (not full path) to parse.

    Returns:
        str or None: Date string in 'yyyymmdd' format, or None if
            the filename doesn't match the incremental pattern.

    Example:
        >>> _get_date_from_name('20251116-001.parquet')
        '20251116'
        >>> _get_date_from_name('LOAD00001.parquet')
        None

    Version History:
        v1.0.0 - Initial implementation
    """
    m = re.match(r"(\d{8})-\d+\.parquet$", filename)
    return m.group(1) if m else None


def _is_incremental_file(filename):
    """
    Check if a filename matches the incremental CDC file pattern.

    Incremental files have the format: yyyymmdd-NNN.parquet

    Args:
        filename (str): Filename (not full path) to check.

    Returns:
        bool: True if the filename matches the incremental pattern.

    Example:
        >>> _is_incremental_file('20251116-001.parquet')
        True
        >>> _is_incremental_file('LOAD00001.parquet')
        False

    Version History:
        v3.0.0 - Added for clearer file type detection
    """
    return _get_date_from_name(filename) is not None


def _is_load_file(filename):
    """
    Check if a filename is a LOAD (initial/full load) file.

    LOAD files start with 'LOAD' (case-insensitive) and end with '.parquet'.

    Args:
        filename (str): Filename (not full path) to check.

    Returns:
        bool: True if the filename is a LOAD file.

    Example:
        >>> _is_load_file('LOAD00001.parquet')
        True
        >>> _is_load_file('load00001.parquet')
        True
        >>> _is_load_file('20251116-001.parquet')
        False

    Version History:
        v4.0.0 - Added for clearer LOAD file detection
    """
    return filename.upper().startswith("LOAD") and filename.endswith(".parquet")


# =============================================================================
# SCHEMA HANDLING FUNCTIONS
# =============================================================================

_VALID_CHAR = re.compile(r"[^A-Za-z0-9_]")


def _canon(name):
    """
    Canonicalize a column name to a valid Spark identifier.

    Replaces invalid characters with underscores and collapses
    consecutive underscores. Returns None for empty or whitespace-only names.

    Args:
        name (str or None): Original column name.

    Returns:
        str or None: Canonicalized name, or None if input is empty/None.

    Example:
        >>> _canon('column-name')
        'column_name'
        >>> _canon('column  name')
        'column_name'
        >>> _canon('  ')
        None

    Version History:
        v2.0.0 - Added for schema normalization
    """
    if name is None:
        return None
    name = name.strip()
    if not name:
        return None
    name = _VALID_CHAR.sub("_", name)
    return re.sub(r"_+", "_", name)


def _commonize_type(types):
    """
    Determine a common Spark data type for a set of conflicting types.

    When multiple parquet files have different types for the same column,
    this function determines the best common type for union operations.

    Type Resolution Strategy:
        1. If CAST_CONFLICT_TO_STRING=true and types differ: StringType
        2. All integer types (byte, short, int, bigint): LongType
        3. Any floating point (double, float, decimal): DoubleType
        4. Date only: DateType
        5. Mixed date/timestamp: TimestampType
        6. Otherwise: First type encountered

    Args:
        types (set): Set of Spark DataType objects.

    Returns:
        DataType: Target Spark DataType for the column.

    Version History:
        v2.0.0 - Initial implementation
        v3.0.0 - Added configurable string casting via CAST_CONFLICT_TO_STRING
    """
    if CAST_CONFLICT_TO_STRING and len({t.simpleString() for t in types}) > 1:
        return StringType()
    
    names = {t.simpleString() for t in types}
    
    if names <= {"byte", "short", "int", "bigint"}:
        return LongType()
    if any(n in names for n in {"double", "float", "decimal"}):
        return DoubleType()
    if names == {"date"}:
        return DateType()
    if names <= {"timestamp", "date"}:
        return TimestampType()
    
    return list(types)[0]


def _schema_safe_union(spark_session, s3_urls):
    """
    Perform a schema-safe union of multiple parquet files.

    This is a fallback mechanism when the fast mergeSchema=true approach
    fails due to incompatible schemas. It reads each file individually,
    normalizes column names, harmonizes types, and performs a union
    with missing column handling.

    Process:
        1. Read each file and canonicalize column names
        2. Collect all unique columns and their types across files
        3. Determine target type for each column using _commonize_type
        4. Align all DataFrames to the common schema
        5. Union all DataFrames

    Args:
        spark_session: Active SparkSession.
        s3_urls (list): List of S3 URLs to parquet files.
            Example: ['s3://bucket/path/file1.parquet', ...]

    Returns:
        DataFrame: Unified Spark DataFrame containing all data.

    Version History:
        v2.0.0 - Initial implementation
        v3.0.0 - Enhanced with configurable empty column handling
    """
    log.info("Performing schema-safe union for %d files", len(s3_urls))
    
    dfs = []
    all_cols = {}  # column_name -> set(DataTypes)

    for path in s3_urls:
        log.debug("Reading file for schema analysis: %s", path)
        df = spark_session.read.parquet(path)
        new_cols = []

        # Canonicalize column names
        for c in df.columns:
            nc = _canon(c)
            if nc is None and DROP_EMPTY_COLUMN_NAME_FIELDS:
                log.warning("Dropping empty/invalid column name from %s", path)
                continue
            if nc is None:
                nc = f"_unnamed_{len(new_cols)}"
                log.warning("Renaming empty column to '%s' in %s", nc, path)
            new_cols.append((c, nc))

        # Rename columns and track types
        for old, new in new_cols:
            if old != new and new in df.columns:
                uniq = f"{new}_{uuid.uuid4().hex[:6]}"
                df = df.withColumnRenamed(old, uniq)
                all_cols.setdefault(uniq, set()).add(df.schema[uniq].dataType)
            else:
                df = df.withColumnRenamed(old, new)
                all_cols.setdefault(new, set()).add(df.schema[new].dataType)

        dfs.append(df)

    # Determine target types for all columns
    target_types = {c: _commonize_type(ts) for c, ts in all_cols.items()}
    log.debug("Unified schema has %d columns", len(target_types))

    # Align all DataFrames to common schema
    aligned = []
    for df in dfs:
        for c, tgt in target_types.items():
            if c not in df.columns:
                df = df.withColumn(c, F.lit(None).cast(tgt))
            else:
                src = df.schema[c].dataType
                if src.simpleString() != tgt.simpleString():
                    df = df.withColumn(c, F.col(c).cast(tgt))
        aligned.append(df.select(*sorted(target_types.keys())))

    # Union all DataFrames
    log.debug("Performing union of %d aligned DataFrames", len(aligned))
    out = aligned[0]
    for df in aligned[1:]:
        out = out.unionByName(df, allowMissingColumns=True)

    return out


# =============================================================================
# FILE PUBLISHING FUNCTIONS
# =============================================================================

def _publish_single_file_as_part(src_key, dst_dir):
    """
    Publish a single parquet file to the destination directory.

    Clears the destination directory, then copies the single file with
    a generated part-{uuid}.parquet name. Optionally counts rows if
    SKIP_COUNTS is false.

    Performance Note (v2.0.0):
        When SKIP_COUNTS=true, this function does a pure S3 copy without
        involving Spark, which is much faster.

    Args:
        src_key (str): Source S3 key to copy.
        dst_dir (str): Destination directory (will be cleared first).

    Returns:
        tuple: (s3_url, row_count)
            - s3_url (str): Full S3 URL of the destination directory
            - row_count (int): Number of rows, or -1 if SKIP_COUNTS=true

    Example:
        >>> url, count = _publish_single_file_as_part(
        ...     'source/file.parquet',
        ...     'dest/table/'
        ... )
        >>> url
        's3://bucket/dest/table/'
        >>> count
        1000  # or -1 if SKIP_COUNTS=true

    Version History:
        v1.0.0 - Initial implementation (always counted rows)
        v2.0.0 - Added SKIP_COUNTS support to avoid unnecessary Spark jobs
    """
    log.info("Publishing single file: %s", src_key)
    
    if SKIP_COUNTS:
        row_count = -1
        log.debug("Row counting skipped (SKIP_COUNTS=true)")
    else:
        log.debug("Counting rows in source file")
        df = spark.read.parquet(f"s3://{DEST_BUCKET}/{src_key}")
        row_count = df.count()
        log.info("Source file row count: %d", row_count)

    dst_dir = _exact_prefix(dst_dir)
    _ensure_empty_prefix(DEST_BUCKET, dst_dir)

    part_name = f"part-{uuid.uuid4().hex}.parquet"
    dst_key = dst_dir + part_name
    
    log.debug("Copying to: s3://%s/%s", DEST_BUCKET, dst_key)
    _copy_one(src_key, dst_key)

    result_url = f"s3://{DEST_BUCKET}/{dst_dir}"
    log.info("Published to: %s", result_url)
    
    return result_url, row_count


def _write_merged_to_dir(file_keys, dst_dir):
    """
    Merge multiple parquet files and write to the destination directory.

    Process:
        1. Calculate optimal partition count based on total file sizes
        2. Attempt fast merge with mergeSchema=true
        3. Fall back to schema-safe union if fast merge fails
        4. Repartition and write compressed parquet

    Performance Notes (v2.0.0):
        - Only counts rows if SKIP_COUNTS=false
        - Caches DataFrame before count+write to avoid re-reading data
        - Unpersists DataFrame after completion to free memory

    Args:
        file_keys (list): List of S3 keys to merge.
        dst_dir (str): Destination directory (will be cleared first).

    Returns:
        tuple: (s3_url, row_count)
            - s3_url (str): Full S3 URL of the destination directory
            - row_count (int): Number of rows, or -1 if SKIP_COUNTS=true

    Version History:
        v1.0.0 - Initial implementation (always counted rows)
        v2.0.0 - Added SKIP_COUNTS support and DataFrame caching
    """
    log.info("Merging %d files to destination", len(file_keys))
    
    s3_urls = [f"s3://{DEST_BUCKET}/{k}" for k in file_keys]
    
    # Calculate optimal partition count
    total_bytes = _sum_sizes_bytes(file_keys)
    target_bytes = max(TARGET_PART_MB, 8) * 1024 * 1024
    num_parts = max(1, min(4096, math.ceil(total_bytes / target_bytes)))
    
    log.info("Total size: %.2f MB, target part size: %d MB, partitions: %d",
             total_bytes / (1024 * 1024), TARGET_PART_MB, num_parts)

    dst_dir = _exact_prefix(dst_dir)
    _ensure_empty_prefix(DEST_BUCKET, dst_dir)

    # Try fast merge first
    try:
        log.debug("Attempting fast merge with mergeSchema=true")
        df = (spark.read
              .option("mergeSchema", "true")
              .parquet(*s3_urls))
    except Exception as e:
        log.warning("Fast merge failed: %s", e)
        log.info("Falling back to schema-safe union")
        df = _schema_safe_union(spark, s3_urls)

    # Write with optional row counting
    output_path = f"s3://{DEST_BUCKET}/{dst_dir}"
    
    if SKIP_COUNTS:
        row_count = -1
        log.debug("Row counting skipped (SKIP_COUNTS=true)")
        log.info("Writing merged data to: %s", output_path)
        (df.repartition(num_parts)
           .write
           .mode("overwrite")
           .option("compression", PARQUET_CODEC)
           .parquet(output_path))
    else:
        log.debug("Caching DataFrame for count and write operations")
        df.cache()
        try:
            row_count = df.count()
            log.info("Merged row count: %d", row_count)
            log.info("Writing merged data to: %s", output_path)
            (df.repartition(num_parts)
               .write
               .mode("overwrite")
               .option("compression", PARQUET_CODEC)
               .parquet(output_path))
        finally:
            df.unpersist()
            log.debug("DataFrame unpersisted from cache")

    log.info("Published to: %s", output_path)
    return output_path, row_count


# =============================================================================
# STATE MANAGEMENT FUNCTIONS
# =============================================================================

def _read_prev_downloaded(bucket, root_prefix):
    """
    Read the previous processing state from downloaded_files.json.

    The state file tracks which files have been processed for each table,
    including timestamps for LOAD files and incremental processing.

    State File Structure:
        [
            {
                "file_name": "incremental_batch" or "LOAD_SINGLE",
                "folder": "table_name",
                "load_timestamp": "2025-11-15T10:30:00Z",
                "last_processed_timestamp": "2025-11-16T14:30:00Z",
                "files_combined": ["20251115-001.parquet", "20251116-001.parquet"]
            },
            ...
        ]
        
        Note: files_combined is only present for incremental processing.

    Args:
        bucket (str): S3 bucket name.
        root_prefix (str): Root prefix where state file is stored.

    Returns:
        dict: Dictionary mapping table names to their state entries.
            Returns empty dict if state file doesn't exist.

    Version History:
        v1.0.0 - Initial implementation
        v3.0.0 - Added last_processed_timestamp tracking
        v4.0.0 - Enhanced to handle both load_timestamp and last_processed_timestamp
    """
    key = f"{root_prefix}/downloaded_files.json"
    
    log.info("Reading previous state from: s3://%s/%s", bucket, key)
    
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        entries = json.loads(obj["Body"].read().decode("utf-8"))
        
        info = {}
        for e in entries:
            folder = e["folder"]
            if folder not in info:
                info[folder] = e
            else:
                # Keep entry with latest last_processed_timestamp
                existing_ts = info[folder].get("last_processed_timestamp")
                new_ts = e.get("last_processed_timestamp")
                if existing_ts and new_ts:
                    if new_ts > existing_ts:
                        info[folder] = e
                elif new_ts:
                    info[folder] = e
        
        log.info("Loaded previous state for %d tables", len(info))
        return info
        
    except Exception as e:
        if "NoSuchKey" in str(e):
            log.info("No previous state file found - this appears to be first run")
            return {}
        raise


def _record_oper(load_oper_tgt_id, data_ppln_job_id, pub_url, cdc_start, cdc_end, rcd_ct):
    """
    Record a processing operation in the control database.

    Inserts a record into the data_ppln_oper table tracking the completion
    of a table processing operation for audit and monitoring purposes.

    Args:
        load_oper_tgt_id (int): Load operation target ID.
        data_ppln_job_id (int): Data pipeline job ID.
        pub_url (str): Published S3 URL.
        cdc_start (str): CDC window start timestamp.
        cdc_end (str): CDC window end timestamp.
        rcd_ct (int): Record count (-1 if SKIP_COUNTS=true).

    Version History:
        v1.0.0 - Initial implementation
    """
    log.debug("Recording operation: pub_url=%s, rcd_ct=%d", pub_url, rcd_ct)
    
    cursor.execute("""
        INSERT INTO dart_process_control.data_ppln_oper (
            load_oper_tgt_id, data_ppln_job_id, data_eng_oper_nm,
            oper_strt_dt, oper_end_dt, data_obj_type_nm, data_obj_nm,
            data_obj_xtrc_strt_dt, data_obj_xtrc_end_dt, rcd_ct,
            data_obj_proc_rtn_nm, data_ppln_oper_stat_nm
        ) VALUES (%s, %s, 'Glue', %s, %s, 'Parquet', %s, %s, %s, %s, %s, 'Complete')
    """, (
        load_oper_tgt_id, data_ppln_job_id,
        datetime.utcnow(), datetime.utcnow(),
        pub_url, cdc_start, cdc_end, rcd_ct, glue_job_name
    ))


# =============================================================================
# LOAD FILE SELECTION FUNCTIONS
# =============================================================================

def _select_new_load_files(load_items, table_name, prev_load_ts=None):
    """
    Select only NEW LOAD files, ignoring stale old LOAD files.

    This is a critical safety function added in v4.0.0 to handle scenarios
    where the source system performs a new initial load but only replaces
    some LOAD files, leaving stale files with old timestamps.

    Logic:
        - First time (no prev_load_ts): Process ALL LOAD files
        - Subsequent runs: Only process LOAD files where LastModified > prev_load_ts

    This prevents data corruption from mixing new LOAD data with stale old data.

    Args:
        load_items (list): List of dicts with 'Key' and 'LastModified' for LOAD files.
        table_name (str): Table name (for logging).
        prev_load_ts (datetime or None): Previous load_timestamp, or None if first run.

    Returns:
        tuple: (new_load_items, max_timestamp)
            - new_load_items (list): List of LOAD file items to process
            - max_timestamp (datetime): Max LastModified among selected files, or None

    Example:
        Previous load_timestamp: 2026-01-10T10:00:00Z

        Files in S3:
            LOAD0000001.parquet (LastModified: 2026-01-20) -> PROCESS
            LOAD0000002.parquet (LastModified: 2026-01-10) -> SKIP (stale)
            LOAD0000003.parquet (LastModified: 2026-01-10) -> SKIP (stale)

        Result: Only LOAD0000001.parquet is processed

    Version History:
        v4.0.0 - Added for safe LOAD file handling
    """
    if not load_items:
        log.debug("No LOAD files provided for %s", table_name)
        return [], None

    log.info("Evaluating %d LOAD files for table: %s", len(load_items), table_name)

    if prev_load_ts is None:
        # First time processing - use ALL LOAD files
        log.info("First time processing - all %d LOAD files will be used", len(load_items))
        for item in load_items:
            log.debug("  INCLUDE: %s (LastModified: %s)", 
                     os.path.basename(item['Key']), item['LastModified'])
        new_items = load_items
    else:
        # Filter to only LOAD files newer than previous load_timestamp
        log.info("Filtering LOAD files with LastModified > %s", prev_load_ts)
        
        new_items = []
        skipped_items = []
        
        for item in load_items:
            if item['LastModified'] > prev_load_ts:
                new_items.append(item)
                log.info("  INCLUDE (new): %s (LastModified: %s)", 
                        os.path.basename(item['Key']), item['LastModified'])
            else:
                skipped_items.append(item)
                log.info("  SKIP (stale): %s (LastModified: %s)", 
                        os.path.basename(item['Key']), item['LastModified'])
        
        log.info("LOAD file selection: %d to process, %d skipped as stale", 
                len(new_items), len(skipped_items))

    if not new_items:
        return [], None

    # Get max LastModified among selected files
    max_timestamp = max(item['LastModified'] for item in new_items)
    log.debug("Max LastModified of selected LOAD files: %s", max_timestamp)
    
    return new_items, max_timestamp


def _has_new_load_files(load_items, prev_load_ts=None):
    """
    Check if there are any new LOAD files to process.

    Quick check function to determine if LOAD processing is needed
    before doing the full file selection.

    Args:
        load_items (list): List of dicts with 'Key' and 'LastModified' for LOAD files.
        prev_load_ts (datetime or None): Previous load_timestamp, or None if first run.

    Returns:
        bool: True if there are new LOAD files to process.

    Version History:
        v4.0.0 - Added for efficient LOAD detection
    """
    if not load_items:
        return False
    
    if prev_load_ts is None:
        # First time - any LOAD file means we should process
        return True
    
    # Check if any LOAD file is newer than previous timestamp
    return any(item['LastModified'] > prev_load_ts for item in load_items)


# =============================================================================
# INCREMENTAL FILE SELECTION FUNCTIONS
# =============================================================================

def _select_unprocessed_incremental_files(file_list_with_metadata, table_name,
                                          cutoff_timestamp=None):
    """
    Select ALL unprocessed incremental files based on LastModified timestamp.

    This is the key function for timestamp-based tracking introduced in v3.0.0.
    It ensures that late-arriving files are never skipped, regardless of their
    filename date.

    Previous Approach (DATE-BASED - v1.0/v2.0):
        - Grouped files by filename date (yyyymmdd)
        - Processed one date at a time
        - Files arriving AFTER their date was processed were SKIPPED

    Current Approach (TIMESTAMP-BASED - v3.0+):
        - Tracks last_processed_timestamp per table
        - Selects ALL files where LastModified > last_processed_timestamp
        - Late-arriving files are ALWAYS processed

    Args:
        file_list_with_metadata (list): List of dicts with 'Key' and 'LastModified'.
        table_name (str): Table name (for logging).
        cutoff_timestamp (datetime or None): Only process files modified AFTER this.

    Returns:
        tuple: (unprocessed_files, max_timestamp)
            - unprocessed_files (list): List of file items to process, sorted by LastModified
            - max_timestamp (datetime): Latest LastModified among selected files, or None

    Example:
        Cutoff timestamp: 2026-01-15T10:00:00Z

        Files in S3:
            20260114-001.parquet (LastModified: 2026-01-14) -> SKIP
            20260114-002.parquet (LastModified: 2026-01-16) -> PROCESS (late arrival!)
            20260116-001.parquet (LastModified: 2026-01-16) -> PROCESS

        Result: Both 20260114-002 and 20260116-001 are processed

    Version History:
        v1.0.0 - Initial implementation (date-based)
        v3.0.0 - Complete rewrite to timestamp-based tracking
    """
    # Filter to incremental files only
    inc_files = []
    for item in file_list_with_metadata:
        key = item['Key']
        filename = os.path.basename(key)
        if _is_incremental_file(filename):
            inc_files.append(item)

    if not inc_files:
        log.info("No incremental files found for %s", table_name)
        return [], None

    log.info("Found %d total incremental files for %s", len(inc_files), table_name)

    # Apply timestamp filter
    if cutoff_timestamp:
        log.info("Filtering by LastModified > %s", cutoff_timestamp)
        before_count = len(inc_files)
        unprocessed = [f for f in inc_files if f['LastModified'] > cutoff_timestamp]
        log.info("Files after filter: %d (filtered out %d already processed)", 
                len(unprocessed), before_count - len(unprocessed))
    else:
        log.info("No cutoff timestamp - all %d incremental files are unprocessed", len(inc_files))
        unprocessed = inc_files

    if not unprocessed:
        log.info("No unprocessed incremental files for %s", table_name)
        return [], None

    # Sort by LastModified ascending (process oldest first)
    unprocessed.sort(key=lambda x: x['LastModified'])
    
    # Get max LastModified for state update
    max_timestamp = max(f['LastModified'] for f in unprocessed)

    # Log details of files to process
    log.info("Selected %d unprocessed incremental files for %s", len(unprocessed), table_name)
    log.info("  Earliest: %s (LastModified: %s)", 
            os.path.basename(unprocessed[0]['Key']), unprocessed[0]['LastModified'])
    log.info("  Latest:   %s (LastModified: %s)", 
            os.path.basename(unprocessed[-1]['Key']), unprocessed[-1]['LastModified'])
    log.info("  Max LastModified (for state update): %s", max_timestamp)

    return unprocessed, max_timestamp


# =============================================================================
# TABLE PROCESSING FUNCTION
# =============================================================================

def _process_table(folder_prefix, s3_folder_path, cdc_start, cdc_end,
                   load_oper_tgt_id, data_ppln_job_id, prev_state):
    """
    Process a single table's LOAD and incremental files.

    This is the main processing function for each table. It handles both
    LOAD (initial/full) files and incremental CDC files using timestamp-based
    tracking to ensure no files are ever skipped.

    Processing Logic (v4.0.0):

        1. Check for new LOAD files (LastModified > prev_load_timestamp):
           - If YES: Process ONLY the new LOAD files (ignore stale ones)
           - Update load_timestamp to max of processed LOAD files
           - Reset last_processed_timestamp = load_timestamp

        2. Process incremental files (LastModified > last_processed_timestamp):
           - Select ALL unprocessed files regardless of filename date
           - Merge and write to output
           - Update last_processed_timestamp to max LastModified

    Args:
        folder_prefix (str): S3 prefix for table's source folder.
        s3_folder_path (str): Base path for published output.
        cdc_start (str): CDC window start timestamp (for audit).
        cdc_end (str): CDC window end timestamp (for audit).
        load_oper_tgt_id (int): Load operation target ID.
        data_ppln_job_id (int): Pipeline job ID.
        prev_state (dict): Previous processing state dictionary.

    Returns:
        dict or None: Processing result dictionary if files were processed,
            None if nothing to process. Result structure:
            {
                "file_name": "incremental_batch" or "LOAD_SINGLE",
                "folder": "table_name",
                "s3_key": "s3://bucket/path/",
                "load_timestamp": "2025-11-15T10:30:00Z",
                "last_processed_timestamp": "2025-11-16T14:30:00Z",
                "files_combined": ["20251115-001.parquet", "20251116-001.parquet"]
            }
            
            Note: files_combined is only present for incremental processing,
            not for LOAD file processing.

    Version History:
        v1.0.0 - Initial implementation
        v2.0.0 - Removed merged_* files from source
        v3.0.0 - Added timestamp-based tracking
        v4.0.0 - Added safe LOAD handling (only new LOAD files)
    """
    table_name = folder_prefix.strip("/").split("/")[-1]
    
    log.info("=" * 70)
    log.info("PROCESSING TABLE: %s", table_name)
    log.info("=" * 70)

    # Get all parquet files with metadata
    all_items = _list_parquet_keys_with_metadata(DEST_BUCKET, folder_prefix)

    # Separate LOAD and incremental files
    load_items = [item for item in all_items if _is_load_file(os.path.basename(item['Key']))]
    inc_items = [item for item in all_items if _is_incremental_file(os.path.basename(item['Key']))]

    log.info("File inventory: %d LOAD files, %d incremental files", 
            len(load_items), len(inc_items))

    # Initialize result variables
    published = None
    published_count = 0
    downloaded_tag = None
    load_timestamp = None
    last_processed_timestamp = None
    files_combined = []  # Track list of files combined for incremental processing

    # Get previous state for this table
    prev_entry = prev_state.get(table_name, {})
    prev_load_ts_str = prev_entry.get("load_timestamp")
    prev_last_processed_str = prev_entry.get("last_processed_timestamp")

    # Parse previous timestamps
    prev_load_ts = None
    if prev_load_ts_str:
        try:
            prev_load_ts = datetime.fromisoformat(prev_load_ts_str.replace('Z', '+00:00'))
            log.info("Previous load_timestamp: %s", prev_load_ts)
        except Exception as e:
            log.warning("Could not parse previous load_timestamp '%s': %s", 
                       prev_load_ts_str, e)

    prev_last_processed = None
    if prev_last_processed_str:
        try:
            prev_last_processed = datetime.fromisoformat(prev_last_processed_str.replace('Z', '+00:00'))
            log.info("Previous last_processed_timestamp: %s", prev_last_processed)
        except Exception as e:
            log.warning("Could not parse previous last_processed_timestamp '%s': %s",
                       prev_last_processed_str, e)

    # =========================================================================
    # CASE 1: Process NEW LOAD files (ignore stale ones)
    # =========================================================================
    if _has_new_load_files(load_items, prev_load_ts):
        log.info("New LOAD files detected - processing LOAD")
        
        # Get only the NEW LOAD files (v4.0.0 safe handling)
        new_load_items, new_load_max_ts = _select_new_load_files(
            load_items, table_name, prev_load_ts
        )
        
        if new_load_items:
            load_keys = [item['Key'] for item in new_load_items]
            load_timestamp = new_load_max_ts

            dst_dir = f"{s3_folder_path}Datasets/{table_name.upper()}-LOAD/"
            
            if len(load_keys) == 1:
                log.info("Publishing single LOAD file")
                published, published_count = _publish_single_file_as_part(load_keys[0], dst_dir)
                downloaded_tag = "LOAD_SINGLE"
            else:
                log.info("Merging %d LOAD files", len(load_keys))
                published, published_count = _write_merged_to_dir(load_keys, dst_dir)
                downloaded_tag = f"LOAD_MERGED_{len(load_keys)}_files"

            log.info("LOAD processing complete: %d files, timestamp: %s", 
                    len(load_keys), load_timestamp)

            # Reset last_processed_timestamp to load_timestamp after LOAD
            last_processed_timestamp = load_timestamp
        else:
            log.warning("No new LOAD files after filtering - this should not happen")

    # =========================================================================
    # CASE 2: Process incremental files
    # =========================================================================
    else:
        # Determine cutoff timestamp for incremental processing
        # Priority: last_processed_timestamp > load_timestamp > None
        cutoff_ts = prev_last_processed or prev_load_ts

        log.info("Processing incrementals with cutoff_timestamp: %s", cutoff_ts)

        # Select unprocessed files using timestamp-based filtering
        unprocessed_files, max_timestamp = _select_unprocessed_incremental_files(
            inc_items, table_name, cutoff_timestamp=cutoff_ts
        )

        if not unprocessed_files:
            log.info("No unprocessed incremental files for %s", table_name)
            return None

        keys_to_process = [f['Key'] for f in unprocessed_files]
        log.info("Processing %d incremental files", len(keys_to_process))

        dst_dir = f"{s3_folder_path}Datasets/{table_name.upper()}/"

        # Track the list of files being combined
        files_combined = [os.path.basename(f['Key']) for f in unprocessed_files]
        
        if len(keys_to_process) == 1:
            log.info("Publishing single incremental file")
            published, published_count = _publish_single_file_as_part(keys_to_process[0], dst_dir)
            downloaded_tag = f"incremental_{os.path.basename(keys_to_process[0])}"
        else:
            log.info("Merging %d incremental files", len(keys_to_process))
            published, published_count = _write_merged_to_dir(keys_to_process, dst_dir)
            downloaded_tag = f"incremental_batch_{len(keys_to_process)}_files"

        # Update timestamps
        last_processed_timestamp = max_timestamp
        load_timestamp = prev_load_ts  # Preserve from previous state

        log.info("Incremental processing complete. New last_processed_timestamp: %s", 
                last_processed_timestamp)
        log.info("Files combined: %s", files_combined)

    # =========================================================================
    # Record operation and return result
    # =========================================================================
    if published:
        _record_oper(
            load_oper_tgt_id, data_ppln_job_id, published,
            cdc_start, cdc_end,
            -1 if SKIP_COUNTS else published_count
        )

        result = {
            "file_name": downloaded_tag,
            "folder": table_name,
            "s3_key": published
        }

        # Add timestamps to result
        if load_timestamp:
            if hasattr(load_timestamp, 'isoformat'):
                result["load_timestamp"] = load_timestamp.isoformat()
            else:
                result["load_timestamp"] = str(load_timestamp)

        if last_processed_timestamp:
            if hasattr(last_processed_timestamp, 'isoformat'):
                result["last_processed_timestamp"] = last_processed_timestamp.isoformat()
            else:
                result["last_processed_timestamp"] = str(last_processed_timestamp)

        # Add list of files combined (for incremental processing)
        if files_combined:
            result["files_combined"] = files_combined

        log.info("Table processing result: %s", result)
        return result

    return None


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """
    Main execution function for the DMS landing file publisher.

    This function orchestrates the entire job execution flow:

        1. Validate pipeline configuration in control database
        2. Check for in-progress jobs and handle stale jobs
        3. Create new job record with CDC window
        4. Read previous processing state
        5. Load schema JSON to determine allowed tables
        6. Discover table folders under SourcePrefix
        7. Filter tables based on schema JSON
        8. Process tables SEQUENTIALLY (prevents Spark task explosion)
        9. Update job status and state files
        10. Publish results and summary

    Performance Note (v2.0.0):
        Tables are processed sequentially instead of in parallel.
        This prevents the "threshold for consecutive task creation reached"
        error caused by multiple Spark jobs running simultaneously.

    Exit Conditions:
        - Success: All tables processed (or skipped if SKIP_BAD_TABLES=true)
        - Failure: Pipeline not found, job already running, critical error

    Version History:
        v1.0.0 - Initial implementation
        v2.0.0 - Changed to sequential processing
        v3.0.0 - Enhanced state management with timestamps
        v4.0.0 - Added safe LOAD handling
    """
    print("=" * 70)
    print("DMS LANDING FILE PUBLISHER v4.0.0")
    print("=" * 70)
    print(f"Pipeline: {PIPELINE_NAME}")
    print(f"Parquet Codec: {PARQUET_CODEC}")
    print(f"Target Part Size: {TARGET_PART_MB} MB")
    print(f"Tracking Mode: S3 LastModified timestamp")
    print(f"LOAD Handling: Only new files (stale files ignored)")
    print("=" * 70)

    # =========================================================================
    # STEP 1: Lookup pipeline configuration
    # =========================================================================
    log.info("Step 1: Looking up pipeline configuration")
    
    cursor.execute("""
        SELECT data_ppln_id
        FROM dart_process_control.data_ppln
        WHERE data_ppln_nm = %s
    """, (PIPELINE_NAME,))
    
    row = cursor.fetchone()
    if not row:
        raise RuntimeError(f"Pipeline '{PIPELINE_NAME}' not found in data_ppln table")
    
    data_ppln_id = row[0]
    log.info("Pipeline ID: %d", data_ppln_id)

    # =========================================================================
    # STEP 2: Check for in-progress jobs
    # =========================================================================
    log.info("Step 2: Checking for in-progress jobs")
    
    cursor.execute(f"""
        SELECT data_ppln_job_id, job_stat_nm, job_strt_dt
        FROM dart_process_control.data_ppln_job
        WHERE data_ppln_id = {data_ppln_id}
          AND job_stat_nm = 'In Progress'
          AND data_stat_cd = 'A'
        ORDER BY job_strt_dt DESC
    """)
    
    inprog = cursor.fetchall()
    if inprog:
        ppln_job_id, _, job_strt_dt = inprog[0]
        corrected = job_strt_dt - timedelta(hours=5)
        now_utc = datetime.now(timezone.utc)
        mins = (now_utc - corrected).total_seconds() / 60.0
        
        if mins > 360:
            log.warning("Found stale job %d (running for %.1f minutes) - marking as Failed",
                       ppln_job_id, mins)
            cursor.execute("""
                UPDATE dart_process_control.data_ppln_job
                SET job_stat_nm = 'Failed', job_end_dt = %s
                WHERE data_ppln_job_id = %s
            """, (datetime.utcnow(), ppln_job_id))
        else:
            raise RuntimeError(f"Previous job {ppln_job_id} still in progress "
                             f"(running for {mins:.1f} minutes)")

    # =========================================================================
    # STEP 3: Get pipeline step configuration
    # =========================================================================
    log.info("Step 3: Getting pipeline step configuration")
    
    cursor.execute("""
        SELECT data_ppln_step_id, data_load_tgt_nm
        FROM dart_process_control.data_ppln_step
        WHERE data_ppln_id = %s AND step_seq_nbr = %s
    """, (data_ppln_id, 1))
    
    data_ppln_step_id, data_load_tgt_nm = cursor.fetchone()
    log.info("Pipeline step ID: %d, Target: %s", data_ppln_step_id, data_load_tgt_nm)

    # =========================================================================
    # STEP 4: Create new job record
    # =========================================================================
    log.info("Step 4: Creating new job record")
    
    cursor.execute("""
        INSERT INTO dart_process_control.data_ppln_job (
            data_ppln_id, data_eng_job_type_nm, job_strt_dt,
            job_stat_nm, last_cplt_data_load_tgt_nm
        ) VALUES (%s, %s, %s, %s, %s)
        RETURNING data_ppln_job_id
    """, (data_ppln_id, "INCREMENTAL", lambda_start, "In Progress", "DMS"))
    
    data_ppln_job_id = cursor.fetchone()[0]
    log.info("Created job record with ID: %d", data_ppln_job_id)

    # =========================================================================
    # STEP 5: Calculate CDC window and output path
    # =========================================================================
    log.info("Step 5: Calculating CDC window and output path")
    
    today = datetime.utcnow()
    day_str = today.strftime("%Y%m%d")
    s3_folder_path = f"{DEST_PREFIX}/{day_str}/{data_ppln_job_id}/"

    offset = data_ppln_job_id % 3600
    cdc_start_dt = today.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(seconds=offset)
    cdc_end_dt = today.replace(hour=23, minute=59, second=59, microsecond=999999)
    cdc_start = cdc_start_dt.strftime("%Y-%m-%d %H:%M:%S")
    cdc_end = cdc_end_dt.strftime("%Y-%m-%d %H:%M:%S")

    log.info("CDC window: %s to %s", cdc_start, cdc_end)
    log.info("Output path: s3://%s/%s", DEST_BUCKET, s3_folder_path)

    cursor.execute("""
        UPDATE dart_process_control.data_ppln_job
        SET data_xtrc_strt_dt = %s, data_xtrc_end_dt = %s
        WHERE data_ppln_job_id = %s
    """, (cdc_start, cdc_end, data_ppln_job_id))

    # =========================================================================
    # STEP 6: Get/create load operation target
    # =========================================================================
    log.info("Step 6: Getting/creating load operation target")
    
    try:
        cursor.execute("""
            INSERT INTO dart_process_control.load_oper_tgt (
                data_ppln_step_id, data_bkt_nm, data_bkt_pfx_nm
            ) VALUES (%s, %s, %s)
            ON CONFLICT (data_ppln_step_id, data_bkt_nm, data_bkt_pfx_nm)
            WHERE (data_stat_cd = 'A'::bpchar) AND (data_bkt_nm IS NOT NULL)
            DO NOTHING
            RETURNING load_oper_tgt_id
        """, (data_ppln_step_id, DEST_BUCKET, s3_folder_path))
        
        r = cursor.fetchone()
        if r:
            load_oper_tgt_id = r[0]
        else:
            cursor.execute("""
                SELECT load_oper_tgt_id
                FROM dart_process_control.load_oper_tgt
                WHERE data_ppln_step_id = %s
                  AND data_bkt_nm = %s
                  AND data_bkt_pfx_nm = %s
                  AND data_stat_cd = 'A'
            """, (data_ppln_step_id, DEST_BUCKET, s3_folder_path))
            load_oper_tgt_id = cursor.fetchone()[0]
        
        log.info("Load operation target ID: %d", load_oper_tgt_id)
        
    except Exception as e:
        log.error("Failed to get/create load operation target: %s", e)
        cursor.execute("""
            UPDATE dart_process_control.data_ppln_job
            SET job_stat_nm = 'Failed', err_msg_txt = %s
            WHERE data_ppln_job_id = %s
        """, (str(e), data_ppln_job_id))
        raise

    # =========================================================================
    # STEP 7: Load schema JSON
    # =========================================================================
    log.info("Step 7: Loading schema JSON")
    
    allowed_tables = None
    if SCHEMA_JSON_PATH:
        try:
            allowed_tables = _load_schema_json(SCHEMA_JSON_PATH)
        except Exception as e:
            log.error("Failed to load schema JSON: %s", e)
            cursor.execute("""
                UPDATE dart_process_control.data_ppln_job
                SET job_stat_nm = 'Failed', job_end_dt = %s, err_msg_txt = %s
                WHERE data_ppln_job_id = %s
            """, (datetime.utcnow(), f"Schema JSON error: {e}", data_ppln_job_id))
            raise

    # Initialize processing summary
    processing_summary = {
        "processed": [],
        "failed": [],
        "missing_in_dbo": [],
        "not_in_config": []
    }

    # =========================================================================
    # STEP 8: Read previous state
    # =========================================================================
    log.info("Step 8: Reading previous processing state")
    
    prev = _read_prev_downloaded(DEST_BUCKET, DEST_PREFIX)

    # =========================================================================
    # STEP 9: Discover and filter table folders
    # =========================================================================
    log.info("Step 9: Discovering and filtering table folders")
    
    all_folders = _list_table_folders_exact(DEST_BUCKET, SRC_PREFIX)
    log.info("Total table folders discovered: %d", len(all_folders))

    folders, excluded_folders = _filter_folders_by_schema(all_folders, allowed_tables)
    log.info("Tables after schema filter: %d", len(folders))

    # Track excluded tables
    for folder in excluded_folders:
        table_name = folder.rstrip("/").split("/")[-1]
        processing_summary["not_in_config"].append(table_name)

    # Track missing tables (in config but not in source)
    discovered_table_names = {f.rstrip("/").split("/")[-1].lower() for f in folders}
    for config_table in (allowed_tables or []):
        if config_table not in discovered_table_names:
            processing_summary["missing_in_dbo"].append(config_table)

    if processing_summary["missing_in_dbo"]:
        log.warning("Tables in config but missing in source: %s", 
                   processing_summary["missing_in_dbo"])

    if not folders:
        log.warning("No matching tables found - job will complete with no processing")

    # =========================================================================
    # STEP 10: Process tables SEQUENTIALLY
    # =========================================================================
    log.info("Step 10: Processing tables")
    log.info("=" * 70)
    log.info("PROCESSING %d TABLES SEQUENTIALLY", len(folders))
    log.info("(Sequential processing prevents Spark task explosion)")
    log.info("=" * 70)

    downloaded = []

    def _process_unit(fprefix):
        """Process a single table with error handling."""
        tbl_name = fprefix.rstrip("/").split("/")[-1]
        try:
            result = _process_table(
                fprefix, s3_folder_path, cdc_start, cdc_end,
                load_oper_tgt_id, data_ppln_job_id, prev
            )
            if result:
                processing_summary["processed"].append(tbl_name)
            return result
        except Exception as e:
            error_msg = f"{type(e).__name__}: {str(e)}"
            processing_summary["failed"].append((tbl_name, error_msg))
            if SKIP_BAD_TABLES:
                log.error("Error processing table '%s': %s", tbl_name, e, exc_info=True)
                log.warning("Skipping table due to SKIP_BAD_TABLES=true")
                return None
            raise

    # Process each table sequentially
    for i, fprefix in enumerate(folders, 1):
        table_name = fprefix.rstrip("/").split("/")[-1]
        log.info("-" * 70)
        log.info("Table %d of %d: %s", i, len(folders), table_name)
        log.info("-" * 70)
        
        try:
            res = _process_unit(fprefix)
            if res:
                downloaded.append(res)
                log.info("[PROCESSED] Table %s completed successfully", table_name)
            else:
                log.info("[SKIPPED] Table %s: no files to process", table_name)
        except Exception as e:
            log.error("[FAILED] Table %s: %s", table_name, e)
            if not SKIP_BAD_TABLES:
                raise

    log.info("=" * 70)
    log.info("TABLE PROCESSING COMPLETE: %d tables processed", len(downloaded))
    log.info("=" * 70)

    # =========================================================================
    # STEP 11: Update job status
    # =========================================================================
    log.info("Step 11: Updating job status to Complete")
    
    cursor.execute("""
        UPDATE dart_process_control.data_ppln_job
        SET job_end_dt = %s, job_stat_nm = 'Complete',
            err_msg_txt = %s, last_cplt_data_load_tgt_nm = %s
        WHERE data_ppln_job_id = %s
    """, (datetime.utcnow(), None, data_load_tgt_nm, data_ppln_job_id))

    # =========================================================================
    # STEP 12: Publish job metadata
    # =========================================================================
    log.info("Step 12: Publishing job metadata")
    
    payload = {
        "JobId": data_ppln_job_id,
        "noFiles": len(downloaded) == 0,
        "FileList": downloaded
    }
    
    s3.put_object(
        Bucket=DEST_BUCKET,
        Key=f"{DEST_PREFIX}/job_id.json",
        Body=json.dumps(payload).encode("utf-8"),
        ContentType="application/json"
    )
    log.info("Published job_id.json")

    # =========================================================================
    # STEP 13: Update state file
    # =========================================================================
    log.info("Step 13: Updating state file")
    
    latest = []
    for d in downloaded:
        entry = {
            "file_name": d["file_name"],
            "folder": d["folder"]
        }
        if "load_timestamp" in d:
            entry["load_timestamp"] = d["load_timestamp"]
        if "last_processed_timestamp" in d:
            entry["last_processed_timestamp"] = d["last_processed_timestamp"]
        if "files_combined" in d:
            entry["files_combined"] = d["files_combined"]
        latest.append(entry)

    # Preserve entries for tables not processed in this run
    for tbl, rec in prev.items():
        if tbl not in [x["folder"] for x in latest]:
            entry = {
                "file_name": rec["file_name"],
                "folder": tbl
            }
            if "load_timestamp" in rec:
                entry["load_timestamp"] = rec["load_timestamp"]
            if "last_processed_timestamp" in rec:
                entry["last_processed_timestamp"] = rec["last_processed_timestamp"]
            if "files_combined" in rec:
                entry["files_combined"] = rec["files_combined"]
            latest.append(entry)

    s3.put_object(
        Bucket=DEST_BUCKET,
        Key=f"{DEST_PREFIX}/downloaded_files.json",
        Body=json.dumps(latest, indent=2).encode("utf-8"),
        ContentType="application/json"
    )
    log.info("Published downloaded_files.json with %d entries", len(latest))

    # =========================================================================
    # STEP 14: Print Processing Summary
    # =========================================================================
    print("\n")
    print("=" * 70)
    print("PROCESSING SUMMARY")
    print("=" * 70)

    print(f"\nSuccessfully Processed: {len(processing_summary['processed'])}")
    if processing_summary["processed"]:
        for table in sorted(processing_summary["processed"]):
            print(f"  [OK] {table}")

    print(f"\nFailed to Process: {len(processing_summary['failed'])}")
    if processing_summary["failed"]:
        for table, reason in sorted(processing_summary["failed"]):
            print(f"  [FAILED] {table}")
            print(f"           Reason: {reason}")

    print(f"\nIn Config but Missing in Source: {len(processing_summary['missing_in_dbo'])}")
    if processing_summary["missing_in_dbo"]:
        for table in sorted(processing_summary["missing_in_dbo"])[:10]:
            print(f"  [MISSING] {table}")
        if len(processing_summary["missing_in_dbo"]) > 10:
            print(f"  ... and {len(processing_summary['missing_in_dbo']) - 10} more")

    print(f"\nIn Source but Not in Config (Skipped): {len(processing_summary['not_in_config'])}")
    if processing_summary["not_in_config"]:
        for table in sorted(processing_summary["not_in_config"])[:10]:
            print(f"  [NOT IN CONFIG] {table}")
        if len(processing_summary["not_in_config"]) > 10:
            print(f"  ... and {len(processing_summary['not_in_config']) - 10} more")

    print("\n" + "=" * 70)
    print("JOB CONFIGURATION SUMMARY")
    print("=" * 70)
    print(f"  Job ID: {data_ppln_job_id}")
    print(f"  Pipeline: {PIPELINE_NAME}")
    print(f"  LOAD Handling: Only new LOAD files (stale files ignored)")
    print(f"  Tracking Mode: S3 LastModified timestamp")
    print(f"  Tables in Config: {len(allowed_tables) if allowed_tables else 'N/A'}")
    print(f"  Tables in Source: {len(all_folders)}")
    print(f"  Tables Processed: {len(downloaded)}")
    print("=" * 70)

    print(json.dumps({"JobId": data_ppln_job_id}))
    print("\nLanding file publish complete.")

    job.commit()
    conn.close()


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    main()