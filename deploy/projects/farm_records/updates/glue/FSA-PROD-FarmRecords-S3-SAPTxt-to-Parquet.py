# Author:  Steampunk Staff/Dennis Sprous DG Sprous
# Purpose: Convert SAP txt files with/without headers, pipe delimited --> parquets
# Date:    2025-11-25
#
# Author:  Steampunk Staff/Emmanuel Maduka
# Change:  Added support for multiple files per table on the same day.
#          Files sharing the same base_name (e.g. XYZ_01_20250101.txt, XYZ_02_20250101.txt)
#          are now grouped, unioned into a single DataFrame, and written as one parquet
#          instead of being processed independently (which caused overwrites to lose data).
# Updated: 2026-03-15
#
# This was originally FSA-PROD-FarmRecords-S3-CSV-to-Parquet and named glue variants thereof.
# Originally, the SAP txt --> parquets was split into multiple steps.
#
# This was revised to do the process in one glue.
# This glue will automatically detect the presence or absence of header lines and correctly
# process the data irrespective of the presence/absence thereof.
# By virtue of schema stored in a json, it does not need the header line.
# Json schema includes both column names and data types.
#

import sys
import subprocess
import json
import boto3
import os
import io
import zipfile
import logging
import re
import psycopg2
import pandas as pd
import urllib.parse
import time
import glob
import csv
csv.field_size_limit(100000000)
import s3fs
import pkg_resources
import pyarrow.csv as pc
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from datetime import timezone, timedelta
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when
from pyspark.sql.types import StructType
from pyspark.sql.types import *
from pyspark.sql.functions import *
from collections import defaultdict

## @params: [JOB_NAME]
glue_start_time = datetime.now()

args = getResolvedOptions(sys.argv, ["JOB_NAME", "PipelineName", "REGION", "SecretId", "SOURCE_FILE_FOLDER",
                                     "SOURCE_FILE_BUCKET", "SOURCE_FILE_PREFIX", "TARGET_PARQUET_BUCKET",
                                     "TARGET_PARQUET_FOLDER", "PREFIX", "JSON_FILE_NAME", "JSON_FILE_PATH", "date_portion", "job_id"])

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
spark = glueContext.spark_session

SECRET_NAME = args["SecretId"]
REGION = args["REGION"]
glue_job_name = args['JOB_NAME']
lambda_start_time = datetime.now()
s3_client = boto3.client('s3', region_name=REGION)
s3_resource = boto3.resource("s3", region_name=REGION)
job_id = args["job_id"]

secrets = boto3.client("secretsmanager", region_name=REGION)
secret_value = secrets.get_secret_value(SecretId=SECRET_NAME)
secret = json.loads(secret_value["SecretString"])

SOURCE_FILE_FOLDER = args["SOURCE_FILE_FOLDER"]
SOURCE_FILE_BUCKET = args["SOURCE_FILE_BUCKET"]
SOURCE_FILE_PREFIX = args["SOURCE_FILE_PREFIX"]
TARGET_PARQUET_BUCKET = args["TARGET_PARQUET_BUCKET"]
TARGET_PARQUET_FOLDER = args["TARGET_PARQUET_FOLDER"]
PREFIX = args["PREFIX"]
JSON_FILE_NAME = args["JSON_FILE_NAME"]
JSON_FILE_PATH = args["JSON_FILE_PATH"]
pipeline_name = args["PipelineName"]
db_host = secret["edv_postgres_hostname"]
db_port = secret["postgres_port"]
db_name = secret["postgres_prcs_ctrl_dbname"]
db_user = secret["edv_postgres_username"]
db_password = secret["edv_postgres_password"]

try:
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        user=db_user,
        password=db_password,
        dbname=db_name
    )
    cursor = None
    conn.autocommit = False
    cursor = conn.cursor()
except Exception as e:
    print("Failed to connect to the process control database:", e)
    # raise e

#####################################################################
def update_process_control_job_with_error(pcdb_conn, job_id, error_msg):
    job_status = "Failed"
    job_end_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    update_sql = f"""
    UPDATE dart_process_control.data_ppln_job
    SET job_end_dt = '{job_end_time}', 
        job_stat_nm = '{job_status}', 
        data_stat_cd ='I',
        err_msg_txt = COALESCE(err_msg_txt, '') || '{error_msg}'
    WHERE data_ppln_job_id = '{job_id}'
    """

    try:
        pcdb_conn.execute(update_sql)
    except Exception as e:
        print(f"An error occurred while updating the process control job: {e}")

###############################################################################
def update_process_control_job_with_success(pcdb_conn, job_id):
    job_status = "Complete"
    job_end_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    update_sql = f"""
    UPDATE dart_process_control.data_ppln_job
    SET job_end_dt = '{job_end_time}', 
        data_stat_cd ='I',
        job_stat_nm = '{job_status}'
    WHERE data_ppln_job_id = '{job_id}'
    """

    try:
        pcdb_conn.execute(update_sql)
    except Exception as e:
        print(f"An error occurred while updating the process control job: {e}")

##################################################################################
def get_json_file(sourcefilebucket, filepath):
    print(f"source file bucket: {sourcefilebucket}")
    content_object = s3_resource.Object(sourcefilebucket, filepath)
    file_content = content_object.get()['Body'].read().decode('utf-8')
    INPUT_JSON = json.loads(file_content)
    return INPUT_JSON

#####################################################################
def get_structtype_dynamic_crosswalk(json_data_file, target_table_name):
    # Returns schema (StructType) and partition_key for a specific target_table_name.
    # Dates/timestamps are read as StringType() so formats can be applied later.
    try:
        partition_key = []
        for item in JSON_DATA_FILE:
            if item["target_table_name"] is not None and item["target_table_name"] == target_table_name:
                target_data_type_list = []
                for subitem in item["target_table_schema"]:
                    print('subitem name:', subitem["name"])
                    print('subitem type:', subitem["type"])
                    name = subitem["name"]
                    data_type = subitem["type"]

                    if 'number' in data_type:
                        data_type = FloatType()
                    elif 'int' in data_type:
                        data_type = IntegerType()
                    elif 'smallint' in data_type:
                        data_type = IntegerType()
                    elif 'bigint' in data_type:
                        data_type = FloatType()
                    elif 'decimal' in data_type:
                        data_type = FloatType()
                    elif 'string' in data_type:
                        data_type = StringType()
                    elif 'varchar' in data_type:
                        data_type = StringType()
                    elif 'char' in data_type:
                        data_type = StringType()
                    elif 'datetime' in data_type:
                        data_type = StringType()
                    elif 'date' in data_type:
                        data_type = StringType()
                    elif 'timestamp' in data_type:
                        data_type = StringType()
                    elif 'bool' in data_type:
                        data_type = BooleanType()
                    elif 'long' in data_type:
                        data_type = LongType()

                    target_data_type_list.append(StructField(name, data_type))
                partition_key = item["partition_key"]
        print(f"target_data_type_list {target_table_name}:{target_data_type_list}")
    except Exception as e:
        print(f"An error occurred: {str(e)[:200]}")
        raise e
    return StructType(target_data_type_list), partition_key

#####################################################################
def read_schema(json_file, csv_file):
    schema = {}
    schema_list = []
    for item in json_file:
        if item["target_table_name"] == csv_file:
            for subitem in item["target_table_schema"]:
                schema_list.append(subitem)
            schema[csv_file] = schema_list
    return schema

####################################################################
def get_input_file(sourcefilebucket, sourcefilefolder):
    try:
        prefix = f"{sourcefilefolder}/{date_portion}/{job_id}/"
        print(f"prefix:{prefix}")
        filenames = []
        result = s3_client.list_objects_v2(Bucket=sourcefilebucket, Prefix=prefix)
        if 'Contents' in result:
            for item in result['Contents']:
                files = item['Key']
                files = files.split("/")[-1]
                goflag = False
                if files.find('ZFRH_CCC505H') == -1:
                    goflag = True
                if (".txt" in files, goflag) == (True, True):
                    filenames.append(files)
        else:
            print("No objects returned")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise e

    return filenames

#######################################################################################
def remove_digits_from_filename(filename):
    """
    Removes all numeric digits and following characters from a filename,
    starting from the last occurrence of an underscore followed by a number.
    """
    match = re.search(r'_\d+.*', filename)
    if match:
        start_index = match.start()
        return filename[:start_index]
    return filename

######################################################################
def remove_date_from_filename(filename):
    """
    Removes date patterns from a filename.
    Returns the filename with date patterns removed and the extracted date.
    """
    date_patterns = [
        r"_\d{8}",        # Matches _YYYYMMDD
        r"\d{8}_",        # Matches YYYYMMDD_
        r"\d{4}-\d{2}-\d{2}",  # Matches YYYY-MM-DD
        r"\d{2}-\d{2}-\d{4}"   # Matches DD-MM-YYYY
    ]

    date_only = None  # initialize before loop to avoid unbound variable

    for pattern in date_patterns:
        new_filename = re.sub(pattern, "", filename)
        match_str = re.search(pattern, filename)

        if match_str:
            raw = str(match_str.group()).replace('_', '')
            # Take up to 8 digits
            digits_only = re.sub(r'\D', '', raw)[:8]
            try:
                date_object = datetime.strptime(digits_only, '%Y%m%d') - timedelta(days=1)
                date_only = date_object.date()
                print(f"date_object: {date_object}")
            except ValueError:
                print(f"Could not parse date from: {digits_only}")

        if new_filename != filename:
            return new_filename, date_only

    return filename, date_only

#####################################################################
def find_needed_json(listed_jsons, base_name):
    for sch_json in listed_jsons:
        if sch_json['source_table_name'] == base_name:
            return sch_json
    return None

def fix_timestamp_fields(df, listed_jsons, base_name):
    def find_needed_json(listed_jsons, base_name):
        for sch_json in listed_jsons:
            if sch_json['source_table_name'] == base_name:
                return sch_json
        return None

    def find_timestamp_field_json(sch_json):
        timestamp_jsons = []
        for field_json in sch_json['target_table_schema']:
            if (field_json['type'] == 'timestamp', field_json['name'] not in ['LOAD_DT', 'CDC_DT']) == (True, True):
                timestamp_jsons.append(field_json)
        return timestamp_jsons

    def find_date_field_json(sch_json):
        date_jsons = []
        for field_json in sch_json['target_table_schema']:
            if (field_json['type'] == 'datetime', field_json['name'] not in ['LOAD_DT', 'CDC_DT']) == (True, True):
                date_jsons.append(field_json)
        return date_jsons

    def recast_timestamp(df, timestamp_jsons):
        for field_json in timestamp_jsons:
            field = field_json['name']
            df = df.withColumn(field, to_timestamp(col(field), "yyyyMMddHHmmss"))
        return df

    def recast_date(df, date_jsons):
        for field_json in date_jsons:
            field = field_json['name']
            df = df.withColumn(field, to_date(col(field), "yyyy.MM.dd"))
        return df

    sch_json = find_needed_json(listed_jsons, base_name)
    if sch_json is None:
        print(f'Error: Could not find json for {base_name}')
        return None

    print(sch_json['target_table_schema'])

    timestamp_jsons = find_timestamp_field_json(sch_json)
    df = recast_timestamp(df, timestamp_jsons)

    date_jsons = find_date_field_json(sch_json)
    df = recast_date(df, date_jsons)

    return df

######################################################################
def read_single_file(csv_file, base_name, schema):
    """
    Read a single SAP txt file from S3 into a Spark DataFrame.
    Auto-detects whether the file has a header row.
    """
    bucketkey = f"{PREFIX}/{date_portion}/{job_id}/{csv_file}"
    sourcefilepath = f"s3://{SOURCE_FILE_BUCKET}/{bucketkey}"

    print(f"Reading file: {sourcefilepath}")

    # Detect header presence by reading with header=True and checking first column
    dfx = spark.read.csv(sourcefilepath, sep="|", header=True)
    sch_json = find_needed_json(JSON_DATA_FILE, base_name)
    expected_first_column = sch_json["source_table_schema"][0]["name"]

    print(f"TABLE: {base_name}  expected_first_column: {expected_first_column}  observed columns: {dfx.columns}")

    if expected_first_column in dfx.columns:
        print(f"Header detected in {csv_file}")
        df = spark.read.csv(sourcefilepath, sep="|", header=True, schema=schema)
    else:
        print(f"No header detected in {csv_file}")
        df = spark.read.csv(sourcefilepath, sep="|", header=False, schema=schema)

    return df

######################################################################
def build_table_df(base_name, file_list):
    """
    Given a base_name (table) and the list of source files that map to it,
    read each file and union them into a single DataFrame.
    All files for the same table must share the same schema.
    """
    print(f"\n--- Processing table: {base_name} ({len(file_list)} file(s)): {file_list} ---")

    schema, partition_key = get_structtype_dynamic_crosswalk(JSON_DATA_FILE, base_name)

    combined_df = None
    for csv_file in file_list:
        df = read_single_file(csv_file, base_name, schema)

        if combined_df is None:
            combined_df = df
        else:
            print(f"Unioning {csv_file} into {base_name}")
            combined_df = combined_df.unionByName(df)

    print(f"Total rows after union for {base_name}: {combined_df.count()}")
    return combined_df, partition_key

#####################################################################
# date_portion handling
date_portion = args["date_portion"]
if date_portion == "00000000":
    date_portion = datetime.now().strftime("%Y%m%d")

#####################################################################
# Load JSON schema file
DIR_PATH = f"{JSON_FILE_PATH}/{JSON_FILE_NAME}"
JSON_DATA_FILE = get_json_file(SOURCE_FILE_BUCKET, DIR_PATH)

#####################################################################
# Get all txt files to process
csv_files = get_input_file(SOURCE_FILE_BUCKET, SOURCE_FILE_FOLDER)
print(f"All files found: {csv_files}")

# ─────────────────────────────────────────────────────────────────────────────
# GROUP FILES BY BASE TABLE NAME
# e.g. XYZ_01_20250101.txt, XYZ_02_20250101.txt, XYZ_03_20250101.txt
#      all resolve to base_name = XYZ and are merged before writing.
# ─────────────────────────────────────────────────────────────────────────────
files_by_table = defaultdict(list)

for csv_file in csv_files:
    base_name_with_extension, result_cdc_date = remove_date_from_filename(csv_file)
    base_name = remove_digits_from_filename(
        os.path.splitext(os.path.basename(base_name_with_extension))[0]
    )
    files_by_table[base_name].append(csv_file)

print(f"\nFiles grouped by table:")
for tbl, files in files_by_table.items():
    print(f"  {tbl}: {files}")

# S3 folder path for landing zone parquets
s3_folder_path = f"{TARGET_PARQUET_FOLDER}/{date_portion}/{job_id}/"
s3_filesystem = s3fs.S3FileSystem()

# ─────────────────────────────────────────────────────────────────────────────
# PROCESS ONE TABLE AT A TIME — union all files for the table, then write once
# ─────────────────────────────────────────────────────────────────────────────
for base_name, file_list in files_by_table.items():
    error_msg = None
    try:
        # Read and union all files for this table
        df, partition_key = build_table_df(base_name, file_list)

        # Apply LOAD_DT / CDC_DT
        for col_name in ["LOAD_DT", "CDC_DT"]:
            if col_name in df.columns:
                df = df.withColumn(col_name, lit(current_date()).cast(TimestampType()))

        # Set standard audit columns
        df = df.withColumn("DATA_SRC_NM", lit("SAP/CRM"))
        df = df.withColumn("CDC_OPER_CD", lit("I"))

        print(f"Schema after standard column processing for {base_name}:")
        df.printSchema()

        # Fix timestamp/date fields per JSON schema
        df = fix_timestamp_fields(df, JSON_DATA_FILE, base_name)
        print(f"Schema after timestamp correction for {base_name}:")
        df.printSchema()

        # ── Write to cleansed zone ──────────────────────────────────────────
        cleansed_parquet_path = f"s3://c108-prod-fpacfsa-cleansed-zone/farm_records/RAW_SAP_PARQUETS/{base_name}"

        overwrite_tables = [
            "ibib", "ibsp", "zmi_base_acre", "zmi_ccc505", "zfrh_ibib", "zfrh_ibsp",
            "ztab001fl", "zfr_hip_elig_det", "zfr_crop_mast_yr", "zmi_crp_mast",
            "zmi_program_year", "zcrm_crm_ext_mapping", "zmiribcansit",
            "comm_pr_frg_rel", "z_ibase_comp_detail"
        ]

        if base_name.lower() in overwrite_tables:
            # Overwrite — all files for this table have been unioned above,
            # so a single overwrite write captures the full day's data correctly.
            df = df.repartition(20, *partition_key)
            df.write.mode("overwrite").parquet(cleansed_parquet_path)
            print(f"Overwrite parquet completed to cleansed: {cleansed_parquet_path}")
        else:
            # Append — union already handled multi-file merging for today's batch.
            df = df.repartition(1, *partition_key)
            df.write.mode("append").parquet(cleansed_parquet_path)
            print(f"Append parquet completed to cleansed: {cleansed_parquet_path}")

        # ── Write to landing zone (always append across job runs) ───────────
        s3_key = f"{s3_folder_path}{base_name}"
        parquet_file_path = f"s3://{TARGET_PARQUET_BUCKET}/{s3_key}"
        print(f"Writing to landing zone: {parquet_file_path}")

        df.write.mode("append").parquet(parquet_file_path)
        print(f"Landing zone write completed for {base_name} ({len(file_list)} source file(s) merged)")

    except Exception as e:
        error_msg = str(e)[:200]
        print(f"Error processing table {base_name} (files: {file_list}): {error_msg}")
        # update_process_control_job_with_error(cursor, job_id, error_msg)
        raise e

output = {
    "JobId": job_id
}

job.commit()