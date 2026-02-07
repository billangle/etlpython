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
       - Single LOAD file → copies directly to Datasets/<TABLE>-LOAD/
       - Multiple LOAD files → merges and writes to Datasets/<TABLE>-LOAD/
       - Records LOAD's LastModified timestamp as cutoff for incrementals
    
    2. Incremental File Processing (CDC):
       - Two-stage filtering approach:
         a) PRIMARY: Filters by S3 LastModified > LOAD timestamp
            (Source of truth - ignores filename date)
         b) SECONDARY: Groups by date in filename (yyyymmdd-*.parquet)
            Selects earliest unprocessed date
       - Single file for date → copies to Datasets/<TABLE>/
       - Multiple files for date → merges and writes to Datasets/<TABLE>/
    
    3. New LOAD Handling:
       - Detects new LOAD files arriving at any point
       - Resets cutoff timestamp to new LOAD's LastModified
       - Resumes incremental processing from new baseline

KEY FEATURES:
    - Exact prefix listing (prevents reading sibling folders like dbo-backup)
    - Compressed parquet output (~1GB parts by default)
    - Schema-safe union with type harmonization fallback
    - Configurable bad table skipping
    - Parallel table processing
    - State tracking via downloaded_files.json

ENVIRONMENT VARIABLES:
    PARQUET_CODEC                    : Compression codec (default: snappy)
    TARGET_PART_MB                   : Target size per part in MB (default: 1024)
    MAX_CONCURRENT_FOLDERS           : Parallel table processing limit (default: 8)
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
    --SchemaJsonPath     : S3 path to schema JSON defining tables to process
                           (e.g., s3://bucket/app/etl-jobs/app_target_schema.json)

SCHEMA JSON FORMAT:
    {
        "tables": [
            {
                "source_table": "accounting_program",
                "target_table": "accounting_program",
                "columns": [...],
                "primary_key": "...",
                "dedupe_key": "..."
            },
            ...
        ]
    }
    
    Only tables with "source_table" defined in this JSON will be processed.
    All other tables in the source location will be ignored.

OUTPUT STRUCTURE:
    DestinationPrefix/
    ├── YYYYMMDD/
    │   └── {job_id}/
    │       └── Datasets/
    │           ├── TABLE1-LOAD/
    │           │   └── part-{uuid}.parquet
    │           ├── TABLE1/
    │           │   └── part-{uuid}.parquet
    │           └── TABLE2/
    │               └── part-{uuid}.parquet
    ├── job_id.json
    └── downloaded_files.json

STATE FILES:
    job_id.json: Current job metadata and processed file list
    downloaded_files.json: Processing history per table with timestamps

VERSION HISTORY:
    v1.0.0 - 2025-11-15 - Initial implementation
             - Basic LOAD and incremental processing
             - Merged file approach in source directory
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
from concurrent.futures import ThreadPoolExecutor, as_completed

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
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel

# -------------------------
# Config & Glue arguments
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
SCHEMA_JSON_PATH = f"s3://{DEST_BUCKET}/{DEST_PREFIX}/{DEST_PREFIX.split('/')[0]}_target_schema.json"  # e.g., s3://bucket/app_name/etl-jobs/app_name_target_schema.json
REGION        = os.environ.get("AWS_REGION", "us-east-1")

# Runtime configuration from environment variables
PARQUET_CODEC          = os.environ.get("PARQUET_CODEC", "snappy").lower()
TARGET_PART_MB         = int(os.environ.get("TARGET_PART_MB", "1024"))
MAX_CONCURRENT_FOLDERS = int(os.environ.get("MAX_CONCURRENT_FOLDERS", "8"))
SKIP_COUNTS            = os.environ.get("SKIP_COUNTS", "false").lower() == "true"

# Data quality and error handling configuration
SKIP_BAD_TABLES               = os.environ.get("SKIP_BAD_TABLES", "true").lower() == "true"
CAST_CONFLICT_TO_STRING       = os.environ.get("CAST_CONFLICT_TO_STRING", "true").lower() == "true"
DROP_EMPTY_COLUMN_NAME_FIELDS = os.environ.get("DROP_EMPTY_COLUMN_NAME_FIELDS", "true").lower() == "true"

# -------------------------
# Spark Configuration
# -------------------------
conf = (
    SparkConf()
    .set("spark.sql.adaptive.enabled", "true")
    .set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .set("spark.sql.parquet.filterPushdown", "true")
    .set("spark.sql.parquet.mergeSchema", "false")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    .set("spark.hadoop.fs.s3a.fast.upload", "true")
    .set("spark.hadoop.fs.s3a.connection.maximum", "512")
    .set("spark.hadoop.fs.s3a.multipart.size", str(128 * 1024 * 1024))
    .set("spark.sql.files.maxPartitionBytes", "256m")
    .set("spark.sql.parquet.compression.codec", PARQUET_CODEC)
    .set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")    
    .set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")    
    .set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")    
    .set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
)
sc = SparkContext(conf=conf)
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session

job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

# -------------------------
# AWS & DB clients
# -------------------------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("landing_files")

sm = boto3.client("secretsmanager", region_name=REGION)
_boto_cfg = Config(max_pool_connections=256)
s3  = boto3.client("s3", region_name=REGION, config=_boto_cfg)
s3r = boto3.resource("s3", region_name=REGION, config=_boto_cfg)

# Retrieve database credentials from Secrets Manager
secret_val = sm.get_secret_value(SecretId=SECRET_NAME)
secret = json.loads(secret_val["SecretString"])
db_host = secret["edv_postgres_hostname"]
db_port = secret["postgres_port"]
db_name = secret["postgres_prcs_ctrl_dbname"]
db_user = secret["edv_postgres_username"]
db_pass = secret["edv_postgres_password"]

conn = psycopg2.connect(host=db_host, port=db_port, user=db_user, password=db_pass, dbname=db_name)
conn.autocommit = True
cursor = conn.cursor()

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
# Schema Configuration Functions
# -------------------------

def _load_schema_json(s3_path: str):
    """
    Load and parse the target schema JSON file from S3.
    
    Extracts the list of tables to process from the schema definition.
    Only tables defined in this JSON will be processed; all others are ignored.
    
    Args:
        s3_path: Full S3 path to schema JSON 
                 (e.g., s3://bucket/app/etl-jobs/app_target_schema.json)
        
    Returns:
        Set of lowercase table names to process
        
    Example Schema JSON:
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
        v3.1.0 - Added to support selective table processing
    """
    if not s3_path:
        return None
        
    # Parse S3 path
    if not s3_path.startswith("s3://"):
        raise ValueError(f"Invalid S3 path: {s3_path}")
    
    parts = s3_path[5:].split("/", 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid S3 path format: {s3_path}")
    
    bucket = parts[0]
    key = parts[1]
    
    log.info("Loading schema JSON from: %s", s3_path)
    
    try:
        # Load JSON from S3
        response = s3.get_object(Bucket=bucket, Key=key)
        schema_data = json.loads(response['Body'].read().decode('utf-8'))
        
        # Extract table names (use source_table as the folder name in S3)
        table_names = set()
        if "tables" in schema_data:
            for table_def in schema_data["tables"]:
                source_table = table_def.get("source_table")
                if source_table:
                    # Convert to lowercase for case-insensitive matching
                    table_names.add(source_table.lower())
        
        log.info("Loaded schema with %d tables defined", len(table_names))
        if table_names:
            log.info("Tables to process: %s", sorted(table_names))
        else:
            log.warning("No tables found in schema JSON!")
        
        return table_names
        
    except s3.exceptions.NoSuchKey:
        raise FileNotFoundError(f"Schema JSON not found: {s3_path}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in schema file: {e}")
    except Exception as e:
        raise RuntimeError(f"Failed to load schema JSON: {e}")


def _filter_folders_by_schema(folders: list, allowed_tables: set):
    """
    Filter table folders to only those defined in schema JSON.
    
    Performs case-insensitive matching between folder names and schema table names.
    Logs both included and excluded tables for visibility.
    
    Args:
        folders: List of S3 folder prefixes (e.g., ['sbsd/dbo/table1/', ...])
        allowed_tables: Set of lowercase table names from schema JSON
        
    Returns:
        List of filtered folder prefixes
        
    Version History:
        v3.1.0 - Added to support selective table processing
    """
    if not allowed_tables:
        log.warning("No tables defined in schema - returning all folders")
        return folders, []
    
    filtered = []
    excluded = []
    excluded_folders = []
    
    for folder in folders:
        # Extract table name from folder path (last component before trailing slash)
        # e.g., 'sbsd/dbo/accounting_program/' -> 'accounting_program'
        table_name = folder.rstrip("/").split("/")[-1].lower()
        
        if table_name in allowed_tables:
            filtered.append(folder)
        else:
            excluded.append(table_name)
            excluded_folders.append(folder)
    
    log.info("Filtered tables: %d included, %d excluded", len(filtered), len(excluded))
    
    if filtered:
        log.info("Tables to process: %s", [f.rstrip("/").split("/")[-1] for f in filtered])
    
    if excluded:
        log.info("Tables excluded (not in schema): %s", sorted(excluded)[:20])  # Show first 20
        if len(excluded) > 20:
            log.info("... and %d more excluded tables", len(excluded) - 20)
    
    return filtered, excluded_folders

# -------------------------
# Helper Functions
# -------------------------

def _exact_prefix(p: str) -> str:
    """
    Ensure trailing slash for folder semantics.
    
    Args:
        p: S3 prefix path
        
    Returns:
        Prefix with trailing slash
        
    Version History:
        v1.0.0 - Initial implementation
    """
    return p.rstrip("/") + "/"


def _list_table_folders_exact(bucket: str, base_prefix: str):
    """
    List ONLY immediate children under the exact SourcePrefix.
    
    Uses S3 delimiter='/' to prevent traversing into sibling prefixes
    (e.g., prevents reading 'dbo-backup' when SourcePrefix is 'dbo').
    
    Args:
        bucket: S3 bucket name
        base_prefix: Base prefix to list (e.g., 'sbsd/dbo')
        
    Returns:
        List of folder prefixes (e.g., ['sbsd/dbo/table1/', 'sbsd/dbo/table2/'])
        
    Version History:
        v1.0.0 - Initial implementation
        v2.0.0 - Added exact prefix matching with delimiter
    """
    base = _exact_prefix(base_prefix)
    folders = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=base, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            folders.append(cp["Prefix"])
    log.info("Exact table-folder count under %s: %d", base, len(folders))
    return folders


def _list_parquet_keys_with_metadata(bucket: str, prefix: str):
    """
    List all parquet files under prefix WITH their S3 metadata.
    
    Returns file keys along with LastModified timestamps, which are used
    as the source of truth for determining file processing order.
    
    Args:
        bucket: S3 bucket name
        prefix: S3 prefix to list
        
    Returns:
        List of dicts: [{'Key': 'path/file.parquet', 'LastModified': datetime}, ...]
        
    Version History:
        v3.0.0 - Added to support timestamp-based filtering
    """
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=_exact_prefix(prefix)):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append({
                    'Key': obj["Key"],
                    'LastModified': obj["LastModified"]
                })
    return keys


def _list_parquet_keys(bucket: str, prefix: str):
    """
    List all parquet file keys under prefix (backward compatibility wrapper).
    
    Args:
        bucket: S3 bucket name
        prefix: S3 prefix to list
        
    Returns:
        List of S3 keys
        
    Version History:
        v1.0.0 - Initial implementation
        v3.0.0 - Refactored to use _list_parquet_keys_with_metadata
    """
    return [item['Key'] for item in _list_parquet_keys_with_metadata(bucket, prefix)]


def _get_date_from_name(name: str):
    """
    Extract yyyymmdd date from filename.
    
    Matches incremental file pattern: yyyymmdd-NNN.parquet
    Example: '20251116-001.parquet' → '20251116'
    
    Args:
        name: Filename (not full path)
        
    Returns:
        Date string 'yyyymmdd' or None if no match
        
    Version History:
        v1.0.0 - Initial implementation
    """
    m = re.match(r"(\d{8})-\d+\.parquet$", name)
    return m.group(1) if m else None


def _sum_sizes_bytes(keys):
    """
    Calculate total size of files in bytes.
    
    Used to determine optimal partition count for Spark processing
    based on TARGET_PART_MB configuration.
    
    Args:
        keys: List of S3 keys
        
    Returns:
        Total size in bytes
        
    Version History:
        v1.0.0 - Initial implementation
    """
    total = 0
    for k in keys:
        h = s3.head_object(Bucket=DEST_BUCKET, Key=k)
        total += h["ContentLength"]
    return total


def _ensure_empty_prefix(bucket: str, prefix: str):
    """
    Delete all objects under prefix to ensure clean overwrite.
    
    Handles pagination and batch deletion (1000 objects per request).
    Safe to call on non-existent prefixes.
    
    Args:
        bucket: S3 bucket name
        prefix: S3 prefix to clear
        
    Version History:
        v1.0.0 - Initial implementation
        v2.0.0 - Added batch deletion for performance
    """
    pref = _exact_prefix(prefix)
    paginator = s3.get_paginator("list_objects_v2")
    to_delete = []
    for page in paginator.paginate(Bucket=bucket, Prefix=pref):
        for obj in page.get("Contents", []):
            to_delete.append({"Key": obj["Key"]})
            if len(to_delete) == 1000:
                s3.delete_objects(Bucket=bucket, Delete={"Objects": to_delete})
                to_delete = []
    if to_delete:
        s3.delete_objects(Bucket=bucket, Delete={"Objects": to_delete})


def _copy_one(src_key: str, dst_key: str):
    """
    Copy a single S3 object using multipart transfer configuration.
    
    Args:
        src_key: Source S3 key (within DEST_BUCKET)
        dst_key: Destination S3 key (within DEST_BUCKET)
        
    Version History:
        v1.0.0 - Initial implementation
        v2.0.0 - Added TRANSFER_CFG for large files
    """
    s3r.Object(DEST_BUCKET, dst_key).copy(
        {"Bucket": DEST_BUCKET, "Key": src_key}, Config=TRANSFER_CFG
    )


def _publish_single_file_as_part(src_key: str, dst_dir: str):
    """
    Publish a single file to destination directory as a part file.
    
    Clears destination directory, then copies the single file with a
    generated part-{uuid}.parquet name.
    
    Args:
        src_key: Source S3 key to copy
        dst_dir: Destination directory (will be cleared first)
        
    Returns:
        Tuple of (s3_url: str, file_count: int)
        Example: ('s3://bucket/path/to/dir/', 1)
        
    Version History:
        v2.0.0 - Added to support single-file publishing
        v3.0.0 - Enhanced documentation
    """
    df = spark.read.parquet(f"s3://{DEST_BUCKET}/{src_key}") #maham changes 
    row_count = df.count()
    
    dst_dir = _exact_prefix(dst_dir)
    _ensure_empty_prefix(DEST_BUCKET, dst_dir)
    
    part_name = f"part-{uuid.uuid4().hex}.parquet"
    dst_key = dst_dir + part_name
    _copy_one(src_key, dst_key)
    
    return f"s3://{DEST_BUCKET}/{dst_dir}", row_count


# -------------------------
# Schema Handling Functions
# -------------------------

_VALID_CHAR = re.compile(r"[^A-Za-z0-9_]")

def _canon(name: str):
    """
    Canonicalize column name to valid Spark identifier.
    
    Replaces invalid characters with underscores and collapses
    consecutive underscores. Returns None for empty/whitespace names.
    
    Args:
        name: Original column name
        
    Returns:
        Canonicalized name or None
        
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
    Determine common Spark type for a set of conflicting types.
    
    Type resolution strategy:
    1. If CAST_CONFLICT_TO_STRING=true and types differ → StringType
    2. All numeric types → widest numeric type
    3. Date/Timestamp mix → TimestampType
    4. Otherwise → first type encountered
    
    Args:
        types: Set of Spark DataType objects
        
    Returns:
        Target Spark DataType
        
    Version History:
        v2.0.0 - Initial implementation
        v3.0.0 - Added configurable string casting
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


def _schema_safe_union(spark, s3_urls):
    """
    Perform schema-safe union of multiple parquet files.
    
    This is a fallback when fast mergeSchema=true fails. Reads each file
    individually, normalizes column names, harmonizes types, and performs
    a union by name with missing column handling.
    
    Process:
    1. Read each file and canonicalize column names
    2. Collect all unique columns and their types
    3. Determine target type for each column
    4. Align all DataFrames to common schema
    5. Union all DataFrames
    
    Args:
        spark: SparkSession
        s3_urls: List of S3 URLs to parquet files
        
    Returns:
        Unified Spark DataFrame
        
    Version History:
        v2.0.0 - Initial implementation
        v3.0.0 - Enhanced with configurable empty column handling
    """
    dfs = []
    all_cols = {}  # col -> set(types)
    
    for path in s3_urls:
        df = spark.read.parquet(path)
        new_cols = []
        
        # Canonicalize column names
        for c in df.columns:
            nc = _canon(c)
            if nc is None and DROP_EMPTY_COLUMN_NAME_FIELDS:
                log.warning("Dropping empty/invalid column name from %s", path)
                continue
            if nc is None:
                nc = f"_unnamed_{len(new_cols)}"
                log.warning("Renaming empty column to %s in %s", nc, path)
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
    
    # Determine target types
    target_types = {c: _commonize_type(ts) for c, ts in all_cols.items()}
    
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
    out = aligned[0]
    for df in aligned[1:]:
        out = out.unionByName(df, allowMissingColumns=True)
    
    return out


def _write_merged_to_dir(file_keys, dst_dir: str):
    """
    Merge multiple parquet files and write to destination directory.
    
    Process:
    1. Calculate optimal partition count based on file sizes
    2. Attempt fast merge with mergeSchema=true
    3. Fall back to schema-safe union on failure
    4. Repartition and write compressed parquet
    
    Args:
        file_keys: List of S3 keys to merge
        dst_dir: Destination directory (will be cleared first)
        
    Returns:
        Tuple of (s3_url: str, num_partitions: int)
        
    Version History:
        v1.0.0 - Initial implementation
        v2.0.0 - Added schema-safe union fallback
        v3.0.0 - Enhanced partition calculation
    """
    s3_urls = [f"s3://{DEST_BUCKET}/{k}" for k in file_keys]
    total_bytes = _sum_sizes_bytes(file_keys)
    target_bytes = max(TARGET_PART_MB, 8) * 1024 * 1024
    num_parts = max(1, min(4096, math.ceil(total_bytes / target_bytes)))

    dst_dir = _exact_prefix(dst_dir)
    _ensure_empty_prefix(DEST_BUCKET, dst_dir)

    try:
        log.info("Fast merge path: mergeSchema=true for %d files", len(s3_urls))
        df = (spark.read
                  .option("mergeSchema", "true")
                  .parquet(*s3_urls))
    except Exception as e:
        log.warning("Fast mergeSchema read failed: %s ; falling back to safe union", e)
        df = _schema_safe_union(spark, s3_urls)
    
    row_count = df.count() #maham changes

    (df.repartition(num_parts)
       .write
       .mode("overwrite")
       .option("compression", PARQUET_CODEC)
       .parquet(f"s3://{DEST_BUCKET}/{dst_dir}"))
    
    return f"s3://{DEST_BUCKET}/{dst_dir}", row_count


# -------------------------
# State Management Functions
# -------------------------

def _read_prev_downloaded(bucket: str, root_prefix: str):
    """
    Read processing state from downloaded_files.json.
    
    State file structure:
    [
        {
            "file_name": "merged_20251116" or "LOAD_SINGLE",
            "folder": "table_name",
            "load_timestamp": "2025-11-15T10:30:00Z"  (optional)
        },
        ...
    ]
    
    Args:
        bucket: S3 bucket name
        root_prefix: Root prefix where state file is stored
        
    Returns:
        Dict mapping table_name to latest state entry
        
    Version History:
        v1.0.0 - Initial implementation
        v3.0.0 - Added load_timestamp tracking
    """
    key = f"{root_prefix}/downloaded_files.json"
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        entries = json.loads(obj["Body"].read().decode("utf-8"))
        info = {}
        for e in entries:
            folder = e["folder"]
            # Keep the latest entry per table
            if folder not in info:
                info[folder] = e
            else:
                # Compare timestamps if available, otherwise compare file names
                existing_ts = info[folder].get("load_timestamp")
                new_ts = e.get("load_timestamp")
                if existing_ts and new_ts:
                    if new_ts > existing_ts:
                        info[folder] = e
                elif e["file_name"] > info[folder]["file_name"]:
                    info[folder] = e
        return info
    except Exception as e:
        if "NoSuchKey" in str(e):
            return {}
        raise


def _record_oper(load_oper_tgt_id, data_ppln_job_id, pub_url, cdc_start, cdc_end, rcd_ct):
    """
    Record processing operation in control database.
    
    Inserts a record into data_ppln_oper table tracking the completion
    of a table processing operation.
    
    Args:
        load_oper_tgt_id: Load operation target ID
        data_ppln_job_id: Data pipeline job ID
        pub_url: Published S3 URL
        cdc_start: CDC window start timestamp
        cdc_end: CDC window end timestamp
        rcd_ct: Record count (-1 if SKIP_COUNTS=true)
        
    Version History:
        v1.0.0 - Initial implementation
    """
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


# -------------------------
# LOAD File Processing
# -------------------------

def _get_load_file_last_modified(load_keys):
    """
    Get the latest LastModified timestamp among LOAD files.
    
    When multiple LOAD files exist, returns the timestamp of the most
    recently modified one. This timestamp becomes the cutoff for
    incremental file processing.
    
    Args:
        load_keys: List of S3 keys for LOAD files
        
    Returns:
        datetime object (timezone-aware) or None if no valid timestamps
        
    Version History:
        v3.0.0 - Added to support timestamp-based filtering
    """
    if not load_keys:
        return None
    
    latest_modified = None
    for key in load_keys:
        try:
            response = s3.head_object(Bucket=DEST_BUCKET, Key=key)
            last_modified = response['LastModified']
            if latest_modified is None or last_modified > latest_modified:
                latest_modified = last_modified
        except Exception as e:
            log.warning("Could not get LastModified for %s: %s", key, e)
    
    return latest_modified

# -------------------------
# Incremental File Selection
# -------------------------

def process_incremental_files_spark(file_list_with_metadata, table_name, spark_session, 
                                   cutoff_timestamp=None, last_processed_date=None):
    """
    Select next incremental date-group to process using two-stage filtering.
    
    TWO-STAGE FILTERING APPROACH:
    
    Stage 1 (PRIMARY - Source of Truth):
        Filters by S3 LastModified timestamp.
        Only processes files where LastModified > cutoff_timestamp.
        Ignores the date in filename at this stage.
        
        Example: A file named '20251114-001.parquet' uploaded on Nov 16
                 WILL pass this filter if cutoff is Nov 15.
    
    Stage 2 (SECONDARY - Chronological Ordering):
        Groups remaining files by date extracted from filename (yyyymmdd).
        Selects earliest unprocessed date to maintain chronological order.
        Prevents reprocessing already-completed dates.
    
    CRITICAL: This ensures that files uploaded after a LOAD are processed
             regardless of their filename date, while maintaining ordered
             processing by filename date.
    
    Args:
        file_list_with_metadata: List of dicts with 'Key' and 'LastModified'
        table_name: Table name (for logging)
        spark_session: Spark session (unused, kept for compatibility)
        cutoff_timestamp: datetime - only process files modified after this
        last_processed_date: str 'yyyymmdd' - only process dates after this
        
    Returns:
        Tuple of (target_date: str|None, date_files: List[str])
        - target_date: 'yyyymmdd' string or None if nothing to process
        - date_files: List of S3 keys for that date
        
    Example:
        Given files:
            20251114-001.parquet (LastModified: 2025-11-14 08:00:00)
            20251114-002.parquet (LastModified: 2025-11-16 09:00:00)
            20251116-001.parquet (LastModified: 2025-11-16 10:00:00)
        
        With cutoff_timestamp = 2025-11-15 10:30:00:
            Stage 1: Keeps 20251114-002.parquet and 20251116-001.parquet
            Stage 2: Groups by date → {20251114: [...], 20251116: [...]}
            Returns: ('20251114', [20251114-002.parquet])
    
    Version History:
        v1.0.0 - Initial implementation with filename-only logic
        v3.0.0 - Added two-stage filtering with LastModified timestamp
    """
    # Filter to incremental files only (those with date in filename)
    inc_files = []
    for item in file_list_with_metadata:
        key = item['Key']
        file_date = _get_date_from_name(os.path.basename(key))
        if file_date:
            inc_files.append({
                'Key': key,
                'Date': file_date,
                'LastModified': item['LastModified']
            })
    
    if not inc_files:
        return None, []
    
    # STEP 1: Apply timestamp filter (PRIMARY - source of truth)
    # Process files modified AFTER cutoff, regardless of filename date
    if cutoff_timestamp:
        log.info("Filtering incremental files for %s: cutoff timestamp %s", 
                 table_name, cutoff_timestamp)
        before_count = len(inc_files)
        inc_files = [f for f in inc_files if f['LastModified'] >= cutoff_timestamp]
        log.info("Files after timestamp filter: %d (was %d)", len(inc_files), before_count)
        
        if not inc_files:
            log.info("No incremental files after cutoff timestamp for %s", table_name)
            return None, []
    
    # STEP 2: Group by date (from filename)
    by_date = {}
    for item in inc_files:
        date = item['Date']
        by_date.setdefault(date, []).append(item['Key'])
    
    # STEP 3: Choose earliest date after last_processed_date (SECONDARY filter)
    # This prevents reprocessing already-completed dates
    candidates = sorted(d for d in by_date if (last_processed_date is None or d > last_processed_date))
    
    if not candidates:
        log.info("No new incremental dates to process for %s (all dates already processed)", table_name)
        return None, []
    
    target_date = candidates[0]
    log.info("Selected incremental date %s for %s (files: %d)", 
             target_date, table_name, len(by_date[target_date]))
    
    return target_date, by_date[target_date]


# -------------------------
# Table Processing
# -------------------------

def _process_table(folder_prefix: str,
                   s3_folder_path: str,
                   cdc_start: str, cdc_end: str,
                   load_oper_tgt_id: int, data_ppln_job_id: int,
                   prev_state: dict):
    """
    Process a single table's LOAD and incremental files.
    
    PROCESSING DECISION TREE:
    
    1. Check for unprocessed LOAD files:
       - Has LOAD files AND (no previous processing OR new LOAD detected)
       - If YES → Process LOAD files
           - Single file → copy to Datasets/<TABLE>-LOAD/
           - Multiple files → merge to Datasets/<TABLE>-LOAD/
           - Record LOAD's LastModified timestamp
       - If NO → Continue to step 2
    
    2. Process incremental files:
       - Use process_incremental_files_spark() with two-stage filtering
       - Filter 1: LastModified > LOAD timestamp (if exists)
       - Filter 2: Select earliest unprocessed date
       - Single file for date → copy to Datasets/<TABLE>/
       - Multiple files for date → merge to Datasets/<TABLE>/
    
    Args:
        folder_prefix: S3 prefix for table's source folder
        s3_folder_path: Base path for published output
        cdc_start: CDC window start (for audit)
        cdc_end: CDC window end (for audit)
        load_oper_tgt_id: Load operation target ID
        data_ppln_job_id: Pipeline job ID
        prev_state: Previous processing state dict
        
    Returns:
        Dict with processing result or None if nothing processed:
        {
            "file_name": "merged_20251116" or "LOAD_SINGLE",
            "folder": "table_name",
            "s3_key": "s3://bucket/path/",
            "load_timestamp": "2025-11-15T10:30:00Z"  (if LOAD processed)
        }
        
    Version History:
        v1.0.0 - Initial implementation
        v2.0.0 - Removed merged_* files from source
        v3.0.0 - Added timestamp-based LOAD detection and processing
    """
    table_name = folder_prefix.strip("/").split("/")[-1]
    log.info("=== TABLE %s ===", table_name)

    # Get all parquet files WITH metadata
    all_items = _list_parquet_keys_with_metadata(DEST_BUCKET, folder_prefix)
    
    # Separate LOAD and incremental files
    load_items = [item for item in all_items 
                  if os.path.basename(item['Key']).startswith("LOAD") 
                  and item['Key'].endswith(".parquet")]
    inc_items = [item for item in all_items 
                 if _get_date_from_name(os.path.basename(item['Key']))]

    published = None
    published_count = 0
    downloaded_tag = None
    load_timestamp = None

    # Get previous state for this table
    prev_entry = prev_state.get(table_name, {})
    last_file = prev_entry.get("file_name")
    last_load_ts = prev_entry.get("load_timestamp")
    
    # Convert last_load_ts to datetime if exists
    cutoff_timestamp = None
    if last_load_ts:
        try:
            cutoff_timestamp = datetime.fromisoformat(last_load_ts.replace('Z', '+00:00'))
            log.info("Table %s has previous LOAD timestamp: %s", table_name, cutoff_timestamp)
        except Exception as e:
            log.warning("Could not parse previous load_timestamp for %s: %s", table_name, e)

    # ===== CASE 1: Process unprocessed LOAD files =====
    has_unprocessed_load= False
    if load_items:
        if not cutoff_timestamp:
            # No previous LOAD timestamp = never processed LOAD before
            has_unprocessed_load = True
            log.info("No previous LOAD timestamp found for %s - will process LOAD files", table_name)
        else:
            # We have a previous LOAD timestamp - check if there's a newer LOAD file
            latest_load_modified = _get_load_file_last_modified([item['Key'] for item in load_items])
            if latest_load_modified and latest_load_modified > cutoff_timestamp:
                has_unprocessed_load = True
                log.info("Found newer LOAD file for %s (modified: %s vs previous: %s)",
                        table_name, latest_load_modified, cutoff_timestamp)
            else:
                log.info("LOAD files already processed for %s (timestamp: %s)", table_name, cutoff_timestamp)

    if has_unprocessed_load:
        log.info("Processing LOAD files for %s", table_name)
        load_keys = [item['Key'] for item in load_items]
        
        # Get the timestamp of the LOAD file(s) we're about to process
        load_timestamp = _get_load_file_last_modified(load_keys)
        
        if len(load_keys) == 1:
            dst_dir = f"{s3_folder_path}Datasets/{table_name.upper()}-LOAD/"
            published, published_count = _publish_single_file_as_part(load_keys[0], dst_dir)
            downloaded_tag = "LOAD_SINGLE"
        else:
            dst_dir = f"{s3_folder_path}Datasets/{table_name.upper()}-LOAD/"
            published, published_count = _write_merged_to_dir(load_keys, dst_dir)
            downloaded_tag = "merged_LOAD"
        
        log.info("LOAD processed for %s with timestamp %s", table_name, load_timestamp)

    # ===== CASE 2: Process incremental files =====
    else:
        # Determine what to use as cutoff
        active_cutoff = cutoff_timestamp
        
        # Determine last processed incremental date
        last_inc_date = None
        if last_file and last_file not in ["LOAD_SINGLE", "merged_LOAD"]:
            # Extract date from last processed incremental
            if last_file.startswith("merged_"):
                m = re.search(r"merged_(\d{8})", last_file)
                if m:
                    last_inc_date = m.group(1)
            else:
                last_inc_date = _get_date_from_name(last_file)
        
        log.info("Incremental processing for %s: cutoff_ts=%s, last_date=%s", 
                 table_name, active_cutoff, last_inc_date)
        
        # Select next incremental date
        target_date, keys_for_day = process_incremental_files_spark(
            inc_items, table_name, spark, 
            cutoff_timestamp=active_cutoff,
            last_processed_date=last_inc_date
        )
        
        if not target_date:
            log.info("No incremental files to process for %s", table_name)
            return None

        dst_dir = f"{s3_folder_path}Datasets/{table_name.upper()}/"
        if len(keys_for_day) == 1:
            # Single incremental file → direct copy as part
            published, published_count = _publish_single_file_as_part(keys_for_day[0], dst_dir) #maham changes
            downloaded_tag = os.path.basename(keys_for_day[0])
        else:
            # Merge that day's shards → Datasets/<TABLE>/
            published, published_count = _write_merged_to_dir(keys_for_day, dst_dir) #maham changes
            downloaded_tag = f"merged_{target_date}"

    # ===== Record operation and return =====
    if published:
        _record_oper(load_oper_tgt_id, data_ppln_job_id, published, cdc_start, cdc_end,
                     -1 if SKIP_COUNTS else published_count)
        
        result = {
            "file_name": downloaded_tag,
            "folder": table_name,
            "s3_key": published
        }
        
        # Add load_timestamp if we processed a LOAD file, or preserve previous timestamp
        if load_timestamp:
            result["load_timestamp"] = load_timestamp.isoformat()
        elif cutoff_timestamp:
            result["load_timestamp"] = cutoff_timestamp.isoformat()
        
        return result
    
    return None
	
# -------------------------
# Main Execution
# -------------------------

def main():
    """
    Main execution function for DMS landing file publisher.

    Execution Flow:
    1. Validate pipeline configuration in control database
    2. Check for in-progress jobs and handle stale jobs
    3. Create new job record with CDC window
    4. Read previous processing state
    5. Load schema JSON to determine allowed tables
    6. Discover table folders under SourcePrefix
    7. Filter tables based on schema JSON (only process defined tables)
    8. Process tables in parallel (up to MAX_CONCURRENT_FOLDERS)
    9. Update job status and state files
    10. Publish results

    Exit Conditions:
    - Success: All tables processed (or skipped if SKIP_BAD_TABLES=true)
    - Failure: Pipeline not found, job already running, schema JSON missing, or critical error

    Version History:
        v1.0.0 - Initial implementation
        v2.0.0 - Added parallel processing
        v3.0.0 - Enhanced state management with timestamps
        v3.1.0 - Added schema-based table filtering
    """
    print(f"Landing publish → codec={PARQUET_CODEC}, target_part≈{TARGET_PART_MB}MB")
    print(f"Pipeline: {PIPELINE_NAME}")

    # ===== STEP 1: Lookup pipeline configuration =====
    cursor.execute("""
        select data_ppln_id
        from dart_process_control.data_ppln
        where data_ppln_nm = %s
    """, (PIPELINE_NAME,))
    row = cursor.fetchone()
    if not row:
        raise RuntimeError(f"Pipeline '{PIPELINE_NAME}' not found in data_ppln table")
    data_ppln_id = row[0]

    # ===== STEP 2: Check for in-progress jobs =====
    cursor.execute(f"""
        select data_ppln_job_id, job_stat_nm, job_strt_dt
        from dart_process_control.data_ppln_job
        where data_ppln_id = {data_ppln_id}
          and job_stat_nm = 'In Progress'
          and data_stat_cd = 'A'
        order by job_strt_dt desc;
    """)
    inprog = cursor.fetchall()
    if inprog:
        ppln_job_id, _, job_strt_dt = inprog[0]
        corrected = job_strt_dt - timedelta(hours=5)
        now_utc = datetime.now(timezone.utc)
        mins = (now_utc - corrected).total_seconds() / 60.0
        if mins > 360:
            # Mark stale job as failed
            cursor.execute("""
                update dart_process_control.data_ppln_job
                set job_stat_nm = 'Failed', job_end_dt = %s
                where data_ppln_job_id = %s
            """, (datetime.utcnow(), ppln_job_id))
            log.warning("Marked stale job %d as Failed (running for %.1f minutes)", ppln_job_id, mins)
        else:
            raise RuntimeError(f"Previous job still in progress: {ppln_job_id}")

    # ===== STEP 3: Get pipeline step configuration =====
    cursor.execute("""
        select data_ppln_step_id, data_load_tgt_nm
        from dart_process_control.data_ppln_step
        where data_ppln_id = %s and step_seq_nbr = %s
    """, (data_ppln_id, 1))
    data_ppln_step_id, data_load_tgt_nm = cursor.fetchone()

    # ===== STEP 4: Create new job record =====
    cursor.execute("""
        insert into dart_process_control.data_ppln_job (
            data_ppln_id, data_eng_job_type_nm, job_strt_dt, job_stat_nm, last_cplt_data_load_tgt_nm
        ) values (%s, %s, %s, %s, %s)
        returning data_ppln_job_id
    """, (data_ppln_id, "INCREMENTAL", lambda_start, "In Progress", "DMS"))
    data_ppln_job_id = cursor.fetchone()[0]
    log.info("Created job record: %d", data_ppln_job_id)

    # ===== STEP 5: Calculate CDC window and output path =====
    today = datetime.utcnow()
    day_str = today.strftime("%Y%m%d")
    s3_folder_path = f"{DEST_PREFIX}/{day_str}/{data_ppln_job_id}/"

    offset = data_ppln_job_id % 3600
    cdc_start_dt = today.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(seconds=offset)
    cdc_end_dt   = today.replace(hour=23, minute=59, second=59, microsecond=999999)
    cdc_start = cdc_start_dt.strftime("%Y-%m-%d %H:%M:%S")
    cdc_end   = cdc_end_dt.strftime("%Y-%m-%d %H:%M:%S")

    cursor.execute("""
        update dart_process_control.data_ppln_job
        set data_xtrc_strt_dt = %s, data_xtrc_end_dt = %s
        where data_ppln_job_id = %s
    """, (cdc_start, cdc_end, data_ppln_job_id))

    # ===== STEP 6: Get/create load operation target =====
    try:
        cursor.execute("""
            insert into dart_process_control.load_oper_tgt (data_ppln_step_id, data_bkt_nm, data_bkt_pfx_nm)
            values (%s, %s, %s)
            on conflict (data_ppln_step_id, data_bkt_nm, data_bkt_pfx_nm)
            where (data_stat_cd = 'A'::bpchar) and (data_bkt_nm is not null)
            do nothing
            returning load_oper_tgt_id
        """, (data_ppln_step_id, DEST_BUCKET, s3_folder_path))
        r = cursor.fetchone()
        if r:
            load_oper_tgt_id = r[0]
        else:
            cursor.execute("""
                select load_oper_tgt_id
                from dart_process_control.load_oper_tgt
                where data_ppln_step_id = %s and data_bkt_nm = %s and data_bkt_pfx_nm = %s and data_stat_cd = 'A'
            """, (data_ppln_step_id, DEST_BUCKET, s3_folder_path))
            load_oper_tgt_id = cursor.fetchone()[0]
    except Exception as e:
        cursor.execute("""
            update dart_process_control.data_ppln_job
            set job_stat_nm = 'Failed', err_msg_txt = %s
            where data_ppln_job_id = %s
        """, (str(e), data_ppln_job_id))
        raise

    # ===== STEP 7: Load schema JSON and determine allowed tables =====
    allowed_tables = None
    if SCHEMA_JSON_PATH:
        try:
            allowed_tables = _load_schema_json(SCHEMA_JSON_PATH)
            if not allowed_tables:
                log.warning("No tables defined in schema JSON")
        except Exception as e:
            log.error("Failed to load schema JSON: %s", e)
            cursor.execute("""
                update dart_process_control.data_ppln_job
                set job_stat_nm = 'Failed', job_end_dt = %s, err_msg_txt = %s
                where data_ppln_job_id = %s
            """, (datetime.utcnow(), f"Schema JSON error: {e}", data_ppln_job_id))
            raise

    # Track processing results for summary
    processing_summary = {
        "processed": [],           # Successfully processed
        "failed": [],              # Failed to process (table, reason)
        "missing_in_dbo": [],      # In config but not in DBO
        "not_in_config": []        # In DBO but not in config
        }

    # ===== STEP 8: Read previous processing state =====
    prev = _read_prev_downloaded(DEST_BUCKET, DEST_PREFIX)
    log.info("Loaded previous state for %d tables", len(prev))

    # ===== STEP 9: Discover and filter table folders =====
    all_folders = _list_table_folders_exact(DEST_BUCKET, SRC_PREFIX)
    log.info("Total table folders discovered: %d", len(all_folders))

    folders, excluded_folders = _filter_folders_by_schema(all_folders, allowed_tables)
    log.info("Tables after schema filter: %d", len(folders))

    # Track tables not in config
    for folder in excluded_folders:
        table_name = folder.rstrip("/").split("/")[-1]
        processing_summary["not_in_config"].append(table_name)

    # After the filtering, check which config tables are missing
    discovered_table_names = {f.rstrip("/").split("/")[-1].lower() for f in folders}
    for config_table in (allowed_tables or []):
        if config_table not in discovered_table_names:
            processing_summary["missing_in_dbo"].append(config_table)

    if processing_summary["missing_in_dbo"]:
        log.warning("Tables in config but missing in DBO: %s", processing_summary["missing_in_dbo"])

    if not folders:
        log.warning("No matching tables found - job will complete with no processing")


    # ===== STEP 10: Process tables in parallel =====
    downloaded = []

    def _unit(fprefix):
        """Process a single table, handling errors based on SKIP_BAD_TABLES."""
        table_name = fprefix.rstrip("/").split("/")[-1]
        try:
            result = _process_table(fprefix, s3_folder_path, cdc_start, cdc_end,
                                load_oper_tgt_id, data_ppln_job_id, prev)
            if result:
                processing_summary["processed"].append(table_name)  # NEW
            return result
        except Exception as e:
            error_msg = f"{type(e).__name__}: {str(e)}"
            processing_summary["failed"].append((table_name, error_msg))  # NEW
            if SKIP_BAD_TABLES:
                log.error("Skipping table for prefix '%s' due to error: %s", fprefix, e, exc_info=True)
                return None
            raise

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_FOLDERS) as pool:
        futs = [pool.submit(_unit, f) for f in folders]
        for fut in as_completed(futs):
            try:
                res = fut.result()
                if res:
                    downloaded.append(res)
            except Exception as e:
                log.error("Table task failed: %s", e, exc_info=True)
                if not SKIP_BAD_TABLES:
                    raise

    log.info("Processing complete: %d tables processed", len(downloaded))

    # ===== STEP 11: Update job status =====
    cursor.execute("""
        update dart_process_control.data_ppln_job
        set job_end_dt = %s, job_stat_nm = 'Complete', err_msg_txt = %s, last_cplt_data_load_tgt_nm = %s
        where data_ppln_job_id = %s
    """, (datetime.utcnow(), None, data_load_tgt_nm, data_ppln_job_id))

    # ===== STEP 12: Publish job metadata =====
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

    # ===== STEP 13: Update state file =====
    latest = []
    for d in downloaded:
        entry = {
            "file_name": d["file_name"],
            "folder": d["folder"]
        }
        if "load_timestamp" in d:
            entry["load_timestamp"] = d["load_timestamp"]
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
            latest.append(entry)

    s3.put_object(
        Bucket=DEST_BUCKET,
        Key=f"{DEST_PREFIX}/downloaded_files.json",
        Body=json.dumps(latest, indent=2).encode("utf-8"),
        ContentType="application/json"
    )

    # ===== STEP 14: Print Processing Summary =====
    print("\n" + "="*80)
    print("PROCESSING SUMMARY")
    print("="*80)

    print(f"\nSuccessfully Processed: {len(processing_summary['processed'])}")
    if processing_summary["processed"]:
        for table in sorted(processing_summary["processed"]):
            print(f"  - {table}")

    print(f"\nFailed to Process: {len(processing_summary['failed'])}")
    if processing_summary["failed"]:
        for table, reason in sorted(processing_summary["failed"]):
            print(f"  - {table}")
            print(f"    Reason: {reason}")

    print(f"\nIn Config but Missing in DBO: {len(processing_summary['missing_in_dbo'])}")
    if processing_summary["missing_in_dbo"]:
        for table in sorted(processing_summary["missing_in_dbo"]):
            print(f"  - {table}")

    print(f"\nIn DBO but Not in Config (Skipped): {len(processing_summary['not_in_config'])}")
    if processing_summary["not_in_config"]:
        # Only show first 20 to avoid cluttering
        shown = sorted(processing_summary["not_in_config"])[:20]
        for table in shown:
            print(f"  - {table}")
        if len(processing_summary["not_in_config"]) > 20:
            print(f"  ... and {len(processing_summary['not_in_config']) - 20} more")

    print("\n" + "="*80)
    print(f"TOTAL: {len(allowed_tables or [])} tables in config, "
        f"{len(all_folders)} tables in DBO, "
        f"{len(folders)} processed/attempted")
    print("="*80 + "\n")

    print(json.dumps({"JobId": data_ppln_job_id}))
    print("Landing publish complete.")

    job.commit()
    conn.close()


if __name__ == "__main__":
    main()