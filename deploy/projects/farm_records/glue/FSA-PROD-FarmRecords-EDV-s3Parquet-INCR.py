import sys
import json
import time
import boto3
import shutil
import urllib.parse
import s3fs
import glob
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import pyarrow.csv as csv
from datetime import datetime, timedelta
import psycopg2
from datetime import timezone, timedelta
import os
from pathlib import Path
from io import StringIO
# import awswrangler as wr
from pyspark.sql.types import StructType
from pyspark.sql.types import *
from pyspark.sql.functions import *
from py4j.java_gateway import java_import
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import threading
      
## @params: [JOB_NAME]
glue_start_time = datetime.now()

args = getResolvedOptions(sys.argv, ["JOB_NAME", "JobId", "date_portion", "TableName", "load_type", "cdc_files_path", "SecretId", "SOURCE_FILE_FOLDER","SOURCE_FILE_BUCKET", "SOURCE_FILE_PREFIX", "JSON_FILE_NAME", "JSON_FILE_PATH", "REGION", "ppln_nm", "step_nm", "db_schm_nm", "db_nm", "application", "redshift_schema_name", "DESTINATION_FILE_BUCKET","DESTINATION_FILE_FOLDER", "redshift_raw_folder", "loadzone" ])

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
spark = glueContext.spark_session
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .config("spark.sql.parquet.timestampNTZ.enabled", "true") \
    .getOrCreate()
    
spark.conf.set("spark.sql.parquet.enableVectorizedReader","false") 
# Enable Arrow optimization
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
REGION = args['REGION']
SECRET_NAME = args["SecretId"]
secrets = boto3.client("secretsmanager", region_name=REGION)
secret_value = secrets.get_secret_value(SecretId=SECRET_NAME)
secret = json.loads(secret_value["SecretString"])
load_type = args['load_type']
glue_job_name = args['JOB_NAME']
lambda_start_time = datetime.now()

s3_client = boto3.client('s3', region_name=REGION)
s3_resource = boto3.resource("s3", region_name=REGION)
job_id = args["JobId"]
# job_id = "17187"
SOURCE_FILE_FOLDER = args["SOURCE_FILE_FOLDER"]
SOURCE_FILE_BUCKET = args["SOURCE_FILE_BUCKET"]
SOURCE_FILE_PREFIX = args["SOURCE_FILE_PREFIX"]

DESTINATION_FILE_FOLDER = args["DESTINATION_FILE_FOLDER"]
DESTINATION_FILE_BUCKET = args["DESTINATION_FILE_BUCKET"]
redshift_raw_folder = args["redshift_raw_folder"]
cdc_files_path = args["cdc_files_path"]
JSON_FILE_NAME = args["JSON_FILE_NAME"]
JSON_FILE_PATH = args["JSON_FILE_PATH"]
pipeline_name = args["ppln_nm"]
loadzone = args["loadzone"]
# Input parameters
env = args["JOB_NAME"].split("-")[1]
ENVIRON = env.upper()
application = args["application"]
tmstmp = datetime.now().strftime("%Y%m%d")
# datetime.now().strftime("%Y%m%d%H%M")
schema = args["db_schm_nm"]
db_schm_nm = args["db_schm_nm"]
db_nm = args["db_nm"]

system_date = datetime.now()
# datetime.now().strftime("%Y-%m-%d") 
print(f"system_date:{system_date}")

field_list = []
#####################################################################
# Connect to postgres dart_process_control DB
keepalive_kwargs = {
    "keepalives": 1,
    "keepalives_idle": 30,
    "keepalives_interval": 5,
    "keepalives_count": 5,
}

db_host = secret["edv_postgres_hostname"]
db_port = secret["postgres_port"]
db_name = secret["postgres_prcs_ctrl_dbname"]
db_user = secret["edv_postgres_username"]
db_password = secret["edv_postgres_password"]

conn = psycopg2.connect(
    host=db_host,
    port=db_port,
    user=db_user,
    password=db_password,
    dbname=db_name,
    **keepalive_kwargs
)
cursor = None
conn.autocommit=True
cursor = conn.cursor()
#####################################################################
# Connect to postgres EDV DB

edv_db_host = secret["edv_postgres_hostname"]
edv_db_port = secret["postgres_port"]
edv_db_name = secret["edv_postgres_database_name"]
edv_db_user = secret["edv_postgres_username"]
edv_db_password = secret["edv_postgres_password"]

edv_postgres_conn = psycopg2.connect(
    host=edv_db_host,
    port=edv_db_port,
    user=edv_db_user,
    password=edv_db_password,
    dbname=edv_db_name,
    **keepalive_kwargs
)
postgres_client = None
edv_postgres_conn.autocommit=True
postgres_client = edv_postgres_conn.cursor()
#####################################################################
# Connect to Redshift DB
redshift_db_host = secret["redshift_host"]
redshift_db_port = secret["redshift_port"]
redshift_db_user = secret['user_db_redshift']
redshift_db_password = secret['pass_db_redshift']
redshift_db_name = secret["redshift_db_name"]
redshift_schema_name = args["redshift_schema_name"]

# Environmental-specific settings
if env.lower() == 'dev':
    rs_catalog_connection = "fsa{}_redshift_db".format(env.lower())
    arnrole = "arn:aws-us-gov:iam::662519022378:role/FSA_Redshift_Role"
    secret_name = "FSA-{}-secrets".format(ENVIRON)
    glue_catalog_db = "fsa-dev-redshift-db"
    cluster_endpoint = 'fsa-dev-redshift.cldrls4ljqk0.us-gov-west-1.redshift.amazonaws.com'
    jdbc_url = "jdbc:redshift://fsa-{}-redshift.cldrls4ljqk0.us-gov-west-1.redshift.amazonaws.com:8002/redshift_db".format(env.lower())
    
elif env.lower() == 'cert':
    rs_catalog_connection = "FSA-{}-redshift-conn".format(ENVIRON)
    arnrole = "arn:aws-us-gov:iam::662519022378:role/FSA_Redshift_Role"
    secret_name = "FSA-{}-Secrets".format(ENVIRON)
    glue_catalog_db = "fsa-cert-redshift-db"
    cluster_endpoint = 'fsa-cert-redshift.cldrls4ljqk0.us-gov-west-1.redshift.amazonaws.com'
    jdbc_url = "jdbc:redshift://fsa-{}-redshift.cldrls4ljqk0.us-gov-west-1.redshift.amazonaws.com:9002/redshift_db".format(env.lower())
elif env.lower() == 'prod':
    rs_catalog_connection = "FSA-{}-redshift-conn".format(ENVIRON)
    arnrole = "arn:aws:iam::253490756794:role/FSA_Redshift_Role"
    secret_name = "FSA-{}-secrets".format(ENVIRON)
    glue_catalog_db = "fsa-prod-redshift-db"
    cluster_endpoint = 'disc-fsa-prod-redshift.co7jv5kzm7ac.us-east-1.redshift.amazonaws.com:5439/dev'
    jdbc_url = "jdbc:redshift://disc-fsa-prod-redshift.co7jv5kzm7ac.us-east-1.redshift.amazonaws.com:5439/dev".format(env.lower())
    
s3bucket_srcode_util = 'c108-{}-fpacfsa-landing-zone'.format(env.lower())
key_name = 'dmart/fwadm_utils/utils.py'

# Block to fetch Redshift credentials from Secrets Manager
secrets_manager_client = boto3.client('secretsmanager')
get_secret_value_response = secrets_manager_client.get_secret_value(SecretId=secret_name)
# logger.info(f"FSA-{ENVIRON}-secrets: {get_secret_value_response}")
secret = json.loads(get_secret_value_response['SecretString'])
username = secret['user_db_redshift']
password = secret['pass_db_redshift']
properties = {
    "user": username,
    "password": password,
    "aws_iam_role": arnrole,
    "driver": "com.amazon.redshift.jdbc.Driver"
}

redshift_conn = psycopg2.connect(
    host=redshift_db_host,
    port=redshift_db_port,
    user=redshift_db_user,
    password=redshift_db_password,
    # host=cluster_endpoint,
    dbname=redshift_db_name,
    **keepalive_kwargs
)
redshift_client = None
redshift_conn.autocommit=True
redshift_client = redshift_conn.cursor()
#####################################################################
# Ingest EDV_Tables_list .json file and return datafram
def get_json_file(bucket_name = '', bucket_key = ''):
    # READ INPUT DICTIONARY FILES: Opening JSON file
    content_object = s3_resource.Object(bucket_name, bucket_key)
    file_content = content_object.get()['Body'].read().decode('utf-8')
    INPUT_JSON = json.loads(file_content)
    JSON_DATA = INPUT_JSON
    # returns JSON object as a dictionary
    return JSON_DATA
#####################################################################
targetColumn_crosswalk = {}
target_data_type_list = []
def get_structtype_dynamic_crosswalk(json_data_file, target_table_name):
    try:
        for item in json_data_file:
            if item["target_table_name"] != None and item["target_table_name"] == target_table_name:
                # declare a dict to hold crosswalk data
                table_keys = []
                field_to_cast_list = []
                for subitem in item["target_table_schema"]:
                    # print('subitem type:', subitem["type"])
                    name = subitem["name"]
                    data_type = subitem["type"]
                    
                    if 'number' in data_type:
                        data_type = IntegerType()
                    elif 'int' in data_type:
                        data_type = IntegerType()
                    elif 'smallint' in data_type:
                        data_type = IntegerType()
                    elif 'bigint' in data_type:
                        data_type = IntegerType()
                    elif 'decimal' in data_type:
                        data_type = DecimalType()
                    elif 'string' in data_type:
                        data_type = StringType()
                    elif 'varchar' in data_type:
                        data_type = StringType()
                    elif 'char' in data_type:
                        data_type = StringType()
                    elif 'datetime' in data_type:
                        data_type = TimestampType()
                    elif 'date' in data_type:
                        data_type = TimestampType()
                    elif 'timestamp' in data_type:
                        data_type = TimestampType()
                    elif 'bool' in data_type:
                        data_type = BooleanType()
                    elif 'float' in data_type:
                        data_type = FloatType()
                    elif 'numeric' in data_type:
                        data_type = IntegerType()
                    elif 'numeric' in data_type:
                        data_type = IntegerType(4)
                    else:
                        data_type = StringType() 
                    
                    target_data_type_list.append(StructField(name, data_type, True))
                for subitem in item["target_table_keys"]:
                    table_keys.append(subitem)
                for subitem in item["field_to_cast_list"]:
                    field_to_cast_list.append(subitem)
    except Exception as e:
        print(f"An error occurred: {str(e)[:200]}")
        raise
    return StructType(target_data_type_list), table_keys, field_to_cast_list


#####################################################################
def get_pa_schema_dynamic(json_data_file, target_table_name):
    try:
        for item in json_data_file:
            if item["target_table_name"] != None and item["target_table_name"] == target_table_name:
                target_data_type_list = []
                for subitem in item["target_table_schema"]:
                    # print('subitem type:', subitem["type"])
                    name = subitem["name"]
                    data_type = subitem["type"]
                    
                    if 'number' in data_type:
                        data_type = pa.float64()
                    elif 'int' in data_type:
                        data_type = pa.float64()
                    elif 'smallint' in data_type:
                        data_type = pa.float64()
                    elif 'bigint' in data_type:
                        data_type = pa.float64()
                    elif 'decimal' in data_type:
                        data_type = pa.float64()
                    elif 'string' in data_type:
                        data_type = pa.string()
                    elif 'varchar' in data_type:
                        data_type = pa.string()
                    elif 'char' in data_type:
                        data_type = pa.string()
                    elif 'datetime' in data_type:
                        data_type = pa.string()
                    elif 'date' in data_type:
                        data_type = pa.string()
                    elif 'timestamp' in data_type:
                        data_type = pa.string()
                    elif 'bool' in data_type:
                        data_type = pa.bool()
                    
                    target_data_type_list.append((name, data_type))
        
        schema = pa.schema(target_data_type_list)
        
    except Exception as e:
        print(f"An error occurred: {str(e)[:200]}")
        raise
    return schema
    # pa.schema(target_data_type_list)

#####################################################################
def get_target_table_list(tgt_tbl_lst = [], Json_dataframe = None):
    try:
        for item in Json_dataframe:
            if item["target_table_name"] != None:
                # append the target table name to a variable
                tgt_tbl_lst.append(item["target_table_name"])
        table_count = len(tgt_tbl_lst)
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise
    return tgt_tbl_lst

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
        # pcdb_conn.commit()
    except Exception as e:
        print(f"An error occurred while updating the process control job: {e}")
        
#####################################################################    
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
        # pcdb_conn.commit()
    except Exception as e:
        print(f"An error occurred while updating the process control job: {e}")

##################################################################### 
def list_scripts(env, application, load_type, schema, table):
    bucket = SOURCE_FILE_BUCKET
    if load_type == "INCR" or load_type == "FULL":
        prefix = "farm_records/scripts/sql/{application}/{schema}/{load_type}/{table}/".format(
            application=application,
            schema=schema,
            load_type=load_type,
            table=table
        )
    else:
        prefix = "farm_records/scripts/sql/{application}/{schema}/{table}/".format(
            application=application,
            schema=schema,
            table=table
        )       
    print(f"Reading: s3://{bucket}/{prefix}")
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    objs = response.get("Contents", [])
    if len(objs) == 0:
        raise Exception(f"No files found at: s3://{bucket}/{prefix}")
    return {Path(obj["Key"]).stem: obj["Key"] for obj in objs}
######################################################################
def read_script(key, date_parameters):
    bucket = SOURCE_FILE_BUCKET
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return populate_dates(obj["Body"].read().decode(), date_parameters)

def populate_dates(query, date_parameters):
    return query.format(**date_parameters)
######################################################################
def read_scripts(schema, table, date_portion):
    date_parameters = Date(new_date_string).parameters
    print(f"date_parameters: {date_parameters}")
    scripts = [
        {
            "position": fname.split("_", 1)[0] if fname[0].isdigit() else 0,
            "operation": fname.split("_", 1)[1] if fname[0].isdigit() else fname,
            "query": read_script(key=key, date_parameters=date_parameters),
        }
        for fname, key in list_scripts(env=env, application=application, load_type=load_type, schema=schema, table=table).items()
    ]
    # scripts = sorted(scripts, key=lambda x: x["position"])
    # scripts = sorted(scripts, key:script.get("operation") for script in scripts)
    # for script in scripts:
    #    print("{}\n\n".format(script.get("operation")))
    #    print("{}\n\n".format(script.get("query")))
    return scripts
######################################################################

oper_status = "Failed"
s3_filesystem = s3fs.S3FileSystem() 

def file_exists(target_path, s3_filesystem):
    return s3_filesystem.exists(target_path)
################################################################################################# 
def stringify_timestamps(df:DataFrame) -> DataFrame:
    return df.select(*[
        F.col(c).cast("string").alias(c) if t == "timestamp" else F.col(c).alias(c)
        for c, t in df.dtypes
    ])
################################################################################################# 
def read_and_deduplicate_parquet_chunks(source_file_path, target_file_path, output_path, keys, chunksize=10000):
    s3_filesystem = s3fs.S3FileSystem()
    file_paths = []
    all_tables = []
    
    srce_files = s3_filesystem.glob(source_file_path)
    print(f"srce_files:{srce_files}")
    target_files = s3_filesystem.glob(target_file_path)
    print(f"target_files:{target_files}")
    tgt_file_path = ''
    srce_file_path = ''
    
    if len(srce_files) != 0:
        for files in srce_files:
            print(f"file : {files}")
            file = files.split("/")[-1]
            if ".parquet" in file:
                srce_file_path = f"s3://{files}"
                file_paths.append(srce_file_path)
                print(f"srce file path:{srce_file_path}")
            else:
                print(f"No parquet file find!")
    file_exist = False
    if len(target_files) != 0:
        for files in target_files:
            print(f"target file : {files}")
            file = files.split("/")[-1]
            if ".parquet" in file:
                file_exist = True
                tgt_file_path = f"s3://{files}"
                file_paths.append(tgt_file_path)
                print(f"tgt file path:{tgt_file_path}")
            else:
                print(f"No parquet file find!")
                
    s3 = s3fs.S3FileSystem()
    all_chunks = []
    
    for file_path in file_paths:
        for chunk in pd.read_parquet(file_path, chunksize=chunksize, storage_options={'s3': s3}):
            all_chunks.append(chunk)
        combined_df = pd.concat(all_chunks, ignore_index=True)
        final_df = combined_df.drop_duplicates(subset=keys, keep='first')
    
    # combined_df = pd.concat(all_chunks, ignore_index=True)
    # final_df = combined_df.drop_duplicates(subset=keys, keep='first')
    
    return final_df

###############################################################################################
def convert_and_cast(dynamic_frame, field_list):
    # Convert DynamicFrame to PySpark DataFrame
    spark_df = dynamic_frame.toDF()
    try:
        for field_name in field_list:
            # Check if the field exists 
            if field_name in spark_df.columns:
                # and spark_df.schema[field_name].dataType.typeName() == "string":
                # Cast the field to numeric(1) - precision 1, scale 0
                spark_df = spark_df.withColumn(field_name, spark_df[field_name].cast(IntegerType()))
                print(f"spark_df: {spark_df}")
    except Exception as err:
        error_msg = str(err)[:500]
        print(f"Error converting and casting dynamic frame: {error_msg}")
        raise err
            
    # Convert back to DynamicFrame
    new_dynamic_frame = DynamicFrame.fromDF(spark_df, dynamic_frame.glue_ctx, "new_dynamic_frame")
    return new_dynamic_frame
###########################################################################################
def read_in_chunks(iterable, chunk_size):
    
    """
    Yields data in chunks from an iterable.

    Args:
        iterable: The iterable to process.
        chunk_size: The size of each chunk.
    """
    
    it = iter(iterable)
    while True:
        chunk = []
        try:
            for _ in range(chunk_size):
                chunk.append(next(it))
        except StopIteration:
            if chunk:
                yield chunk
            break
        yield chunk
        
###########################################################################################

def combined_cdc_files(s3_bucket, field_list, parquet_path):
    print(f"start combining cdc files:")
    
    try:
        s3_filesystem = s3fs.S3FileSystem()
        all_tables = []
        s3_path = f"{s3_bucket}/{parquet_path}"
        parquet_srce_files = s3_filesystem.glob(s3_path)
        print(f"parquet_srce_files:{parquet_srce_files}")
        parquet_srce_file_path = ''
        if len(parquet_srce_files) != 0:
            for files in parquet_srce_files:
                print(f"file : {files}")
                file = files.split("/")[-1]
                if ".parquet" in file:
                    parquet_srce_file_path = f"s3://{files}"
                    print(f"parquet srce file path:{parquet_srce_file_path}")
                    df_source = pq.ParquetDataset(parquet_srce_file_path, filesystem=s3_filesystem).read_pandas().to_pandas()
                    all_tables.append(df_source)
                else:
                    print(f"No parquet file find!")
                    
            # combined_table = pa.concat_tables(all_tables)
            df_combined = pd.concat(all_tables)
                    
            # deduplicated_df = df_combined.drop_duplicates(subset=field_list, keep='first')
            # deduplicated_df = df_combined.drop_duplicates(subset=keys)
                    
            final_table = pa.Table.from_pandas(df_combined)
                    
            # Construct the Parquet file name using the 'date_portion' variable
            parquet_file_name = f"{table}.parquet"
                    
            # Construct the key for the Parquet file
            final_output_path = f"{parquet_path}Finals/"
            # "{loadzone}{load_type}/{table}/".format(loadzone=loadzone, load_type=load_type, table=table)
            
            s3_key = f"{final_output_path}{parquet_file_name}"
            print(f"s3_key:{s3_key}")    
                    
            parquet_file_path = f"s3://{SOURCE_FILE_BUCKET}/{s3_key}"
            print(f"parquet file path: {parquet_file_path}")
                        
            # Write the PyArrow Table to a Parquet file
            pq.write_table(final_table, parquet_file_path)
            print("write table completed!")
            print(f"completed combining cdc files:")
        
    except Exception as e:
        error_msg = str(e)[:200]  # Truncate error message if necessary
        print(error_msg)
        raise    
###########################################################################################
def delete_file_from_s3(bucket_name, file_key):
    # s3_client = boto3.client('s3')  # Initialize the S3 client
    try:
        s3_filesystem = s3fs.S3FileSystem()
        all_tables = []
        s3_path = f"{bucket_name}/{file_key}"
        parquet_srce_files = s3_filesystem.glob(s3_path)
        print(f"parquet_srce_files:{parquet_srce_files}")
        parquet_srce_file_path = ''
        if len(parquet_srce_files) != 0:
            for files in parquet_srce_files:
                print(f"file : {files}")
                file = files.split("/")[-1]
                if ".parquet" in file:
                    parquet_srce_file_path = f"s3://{files}"
                    print(f"parquet srce file path:{parquet_srce_file_path}")
                    final_file_key = f"{file_key}/{file}"
                    
                    response = s3_client.delete_object(Bucket=bucket_name, Key=final_file_key)
                    print(f"File '{final_file_key}' deleted successfully from bucket '{bucket_name}'")
                else:
                    print(f"No parquet file find!")
        return True
    except Exception as e:
        print(f"Error deleting file: {e}")
        return False
        
###########################################################################################        
def Create_source_parquet_from_postgresql(sql_script, edv_postgres_conn, table):
    try:
        print(f"sql script: {sql_script}")
        position = sql_script["position"]
        operation = script.get("operation")
        print(f"position: {position}")
        if (int(position) != 0 and int(position) != 4):
            # postgres_client.execute(query=script.get("query"))
            print("Executed: {}".format(script.get("operation")))
            
            df_list = []
            chunk_size = 2000000
            
            # Iterate through chunks
            for chunk in pd.read_sql_query(script.get("query"), edv_postgres_conn, chunksize=chunk_size):
                pandas_df = pd.DataFrame(chunk)
                # pandas_df.columns = column_names
                print(f"pandas df: {pandas_df}")
                df_list.append(pandas_df)
                
                '''
                df_table = pa.Table.from_pandas(pandas_df)
                print(f"df_table: {df_table}")
                
                df_list.append(df_table)
                '''
            # Combine all results
            Combined_df_final = pd.concat(df_list, ignore_index=True)
            df_table_final = pa.Table.from_pandas(Combined_df_final, preserve_index=False)
            
            # df_table_final = pa.concat_tables(df_list)    
            print('constructing parquet load path')
            
            if df_table_final.num_rows != 0:
                print(f"df_table_final is not empty!")
                # Convert all columns to string type
                for col_name in df_table_final.column_names:
                    df_table_final = df_table_final.set_column(df_table_final.column_names.index(col_name), col_name, df_table_final.column(col_name).cast(pa.string()))
                    
                # Construct the Parquet file name using the 'date_portion' variable
                parquet_file_name = f"{operation}.parquet"
                
                # transfert to S3 bucket
                s3_bucket = SOURCE_FILE_BUCKET
                # "{cdc_files_path}{load_type}/{table}/".format(
                
                prefix = "{cdc_files_path}{load_type}/{date_portion}/{table}/".format(
                    cdc_files_path=cdc_files_path,
                    load_type=load_type,
                    date_portion=date_portion,
                    table=table
                )
                        
                # Construct the key for the Parquet file
                s3_key = f"{prefix}{parquet_file_name}"
                print(f"s3_key:{s3_key}")
                    
                print(f"s3_key:{s3_key}")    
                
                parquet_file_path = f"s3://{s3_bucket}/{s3_key}"
                print(f"parquet file path: {parquet_file_path}")
                
                # Write the PyArrow Table to a Parquet file
                pq.write_table(df_table_final, parquet_file_path)
                print("write table completed!")
                
                print(f"Parquet File copied successfully to final folder {parquet_file_path} ")
                
            else:
                print(f"df_table_final is empty!")
            
    except Exception as e:
        error_msg = str(e)[:200]  # Truncate error message if necessary
        print(error_msg)
        raise e
    
###########################################################################################
def Create_target_parquet_from_Redshift(redshift_conn, redshift_schema_name, table, target_path):
    try:
        # Define source path and destination path
        # target_path = redshift_tmp_raw_dir
        target_table_name = f"{table}"
        print(f"target table name: {target_table_name.lower()}")
        
        if target_table_name in ["FARM_DIM", "FLD_YR_DIM","TR_DIM"]:
            redshift_schema_name = "cmn_dim_dm"
        else:
            redshift_schema_name = f"{redshift_schema_name}"
        
        query = f""" SELECT *
                     FROM {redshift_schema_name}.{target_table_name}
                """
        
        target_df_list = []
        chunk_size = 2000000
        
        # Iterate through chunks
        for chunk in pd.read_sql_query(query, redshift_conn, chunksize=chunk_size):
            pandas_df = pd.DataFrame(chunk)
            # print(f"Redshift pandas df: {pandas_df}")
            target_df_list.append(pandas_df)
            ''' 
            # df_table = pa.Table.from_pandas(pandas_df, pa_table_schema, preserve_index=False)
            df_table = pa.Table.from_pandas(pandas_df, preserve_index=False)
            print(f"Redshift df table: {df_table}")
                
            target_df_list.append(df_table)
            
            # target_df = spark.createDataFrame(pandas_df)
            '''
        # Combine all results
        Combined_df_final = pd.concat(target_df_list, ignore_index=True)
        target_df_final = pa.Table.from_pandas(Combined_df_final, preserve_index=False)
        # target_df_final = pa.concat_tables(target_df_list)
        # print(f"Redshift target df final: {target_df_final}")
        
        if target_df_final.num_rows != 0:
            print(f"target_df_final is not empty!")
            # Convert all columns to string type
            for col_name in target_df_final.column_names:
                target_df_final = target_df_final.set_column(target_df_final.column_names.index(col_name), col_name, target_df_final.column(col_name).cast(pa.string()))
        
            tgt_parquet_file_name = f"{table}.parquet"
            
            print('constructing parquet load path')
            tgt_parquet_file_path = f"{target_path}/{tgt_parquet_file_name}"
            
            # Write the PyArrow Table to a Parquet file
            pq.write_table(target_df_final, tgt_parquet_file_path)
            
            # target_df_final.coalesce(1).write.mode("overwrite").parquet(tgt_parquet_file_path)
            # target_df_final.coalesce(1).write.format("parquet").mode("overwrite").save(tgt_parquet_file_path)
            
            print("Redshift write table completed!")
            
            # Read the schema of the Parquet file
            parquet_schema = pq.read_schema(tgt_parquet_file_path)
            print("Redshift read_schema completed!")
                        
            # Print the schema of the Parquet file
            print(f"Redshift Schema of the Parquet file for {tgt_parquet_file_name}:")
            print(parquet_schema)
            
            print(f"Parquet File copied successfully to final folder! {tgt_parquet_file_name}")
            
        else:
            print(f"target_df_final is empty!")
            
    except Exception as e:
        error_msg = str(e)[:200]  # Truncate error message if necessary
        print(error_msg)
        raise e
######################################################################
class Date:
    def __init__(self, date: str):
        self.system_date = datetime.now()
        # datetime.strptime(date, "%Y-%m-%d")
        self.parameters = {
            "V_BATCH_LOAD_DT": self.V_BATCH_LOAD_DT,
            "V_CDC_DT": self.V_CDC_DT,
        }
    
    @property
    def V_BATCH_LOAD_DT(self):
        return self.system_date.date()
    
    @property
    def V_CDC_DT(self):
        return f"{new_date_string}"
        # self.system_date.date() - timedelta(days=1)
        
######################################################################
# Execute get json file function and return spark datafram
dir_Path = f"{JSON_FILE_PATH}/{JSON_FILE_NAME}"
print(f"dir_Path:{dir_Path}")
Json_df = get_json_file(SOURCE_FILE_BUCKET, dir_Path)
# print('Json_dataframe:', Json_df)
tgt_tbl_lst = []
target_table_list = get_target_table_list(tgt_tbl_lst, Json_df)
# print('target_table_list:', target_table_list)
#####################################################################
# date_portion = datetime.now().strftime("%Y%m%d")
date_portion = args["date_portion"]
tgt_tb_nm = args["TableName"] 
######################################################################
# Parse the string into a datetime object using the original format
# %Y for full year, %m for zero-padded month, %d for zero-padded day
date_object = datetime.strptime(f"{date_portion}", "%Y%m%d")
print(f"date_object: {date_object}")
    
# Format the datetime object into the desired string format
new_date_string = date_object.strftime("%Y-%m-%d")
print(f"new_date_string: {new_date_string}")
######################################################################
try:
    table = tgt_tb_nm
    # redshift_tmp_raw_dir = f"s3://{SOURCE_FILE_BUCKET}/{redshift_raw_folder}{date_portion}/{table}".format(SOURCE_FILE_BUCKET, redshift_raw_folder, date_portion, table)
    redshift_tmp_raw_dir = f"s3://{SOURCE_FILE_BUCKET}/{redshift_raw_folder}{table}".format(SOURCE_FILE_BUCKET, redshift_raw_folder, table)        
    # Read the corresponding JSON schema file
    tgt_table_schema, tgt_table_keys, field_list = get_structtype_dynamic_crosswalk(Json_df, table)
    print(f"tgt_table_schema: {tgt_table_schema}")
    print(f"tgt_table_keys: {tgt_table_keys}")
    print(f"field_list: {field_list}")
    pa_table_schema = get_pa_schema_dynamic(Json_df, table)
    
    # Create target parquet from Redshift
    print("Start Create target parquet from Redshift")
    Create_target_parquet_from_Redshift(redshift_conn, redshift_schema_name, table, redshift_tmp_raw_dir)
    
    # Read SQL scripts
    scripts = read_scripts(schema, table, date_portion)
    print(f"scripts: {scripts}")
        
    for script in scripts:
        print("start process Create source parquet from postgresql")
        Create_source_parquet_from_postgresql(script, edv_postgres_conn, table)
        print("completed process Create source parquet from postgresql")
    
    
    # construct the S3 bucket
    s3_bucket = SOURCE_FILE_BUCKET
    # "{cdc_files_path}{load_type}/{table}/".format(
    
    prefix = "{cdc_files_path}{load_type}/{date_portion}/{table}/".format(    
        cdc_files_path=cdc_files_path,
        load_type=load_type,
        date_portion = date_portion,
        table=table
    )
                    
    # Construct the key for the Parquet file
    s3_key = f"{prefix}"
    print(f"s3_key:{s3_key}")
    
    # Combine cdc files into one with the table name
    combined_cdc_files(s3_bucket, field_list, s3_key)   
    
    # Delete files after combining them
    # file_key = f"{s3_key}/*.parquet"
    # delete_file_from_s3(s3_bucket, s3_key)
    
    update_process_control_job_with_success(cursor, job_id)
    
except Exception as e:
    error_msg = str(e)[:300]
    print(f"Error processing Table {table}: {error_msg}")
    update_process_control_job_with_error(cursor, job_id, error_msg)
    raise e 

output = {
     "JobId": job_id
}

job.commit()