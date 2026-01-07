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
# from sqlalchemy import create_engine
# from signal import signal, SIGPIPE, SIG_DFL
# signal(SIGPIPE,SIG_DFL) 

        
## @params: [JOB_NAME]
glue_start_time = datetime.now()

args = getResolvedOptions(sys.argv, ["JOB_NAME", "JobId", "TableName", "SecretId", "SOURCE_FILE_FOLDER","SOURCE_FILE_BUCKET", "SOURCE_FILE_PREFIX", "JSON_FILE_NAME", "JSON_FILE_PATH", "REGION", "ppln_nm",  "db_schm_nm", "db_nm", "application" ])

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

glue_job_name = args['JOB_NAME']
lambda_start_time = datetime.now()
# s3 = boto3.client("s3", region_name=REGION)
s3_client = boto3.client('s3', region_name=REGION)
s3_resource = boto3.resource("s3", region_name=REGION)

SOURCE_FILE_FOLDER = args["SOURCE_FILE_FOLDER"]
SOURCE_FILE_BUCKET = args["SOURCE_FILE_BUCKET"]
SOURCE_FILE_PREFIX = args["SOURCE_FILE_PREFIX"]

JSON_FILE_NAME = args["JSON_FILE_NAME"]
JSON_FILE_PATH = args["JSON_FILE_PATH"]
pipeline_name = args["ppln_nm"]

# Input parameters
env = args["JOB_NAME"].split("-")[1]
ENVIRON = env.upper()
application = args["application"]
tmstmp = datetime.now().strftime("%Y%m%d")
# datetime.now().strftime("%Y%m%d%H%M")
schema = args["db_schm_nm"]
db_schm_nm = args["db_schm_nm"]
db_nm = args["db_nm"]
job_id = args["JobId"]

system_date = datetime.now()
# datetime.now().strftime("%Y-%m-%d") 
print(f"system_date:{system_date}")

V_BATCH_LOAD_DT = system_date
V_BATCH_LOAD_DT = f"{V_BATCH_LOAD_DT}"
print(f"V_BATCH_LOAD_DT:{V_BATCH_LOAD_DT}")

# Subtract one day to get yesterday's date
# V_CDC_DT = system_date
V_CDC_DT = system_date - timedelta(days=1)
print(f"V_CDC_DT: {V_CDC_DT}")
java_import(sc._gateway.jvm,"java.sql.DriverManager")

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
# ================================
# Insert initial job audit records
# ================================

#print(pipeline_name)
# Get the ID of the pipeline
get_data_ppln_id_sql = f"""
select data_ppln_id
from dart_process_control.data_ppln
where data_ppln_nm = '{pipeline_name}'
"""
try:
   
   #print(get_data_ppln_id_sql)
   # Execute the SQL query
   cursor.execute(get_data_ppln_id_sql)

   # Fetch the result
   result = cursor.fetchone()

   # Check if the result is None
   if result is None:
       raise Exception("No results returned from the SQL query")
   else:
       # Assign the first element of the result to data_ppln_id
       data_ppln_id = result[0]

except Exception as e:
   print(f"An error occurred: {e}")
   # Additional error handling logic can be added here
   raise e # Re-raise the exception to be handled by the calling process or to halt the script

# Insert into data_ppln_job

job_status = "In Progress"
job_failure = False

# Get the Pipeline Name and ID
get_data_ppln_step_data_sql = """
select data_ppln_step_id, data_load_tgt_nm
from dart_process_control.data_ppln_step
where data_ppln_id = %s
and step_seq_nbr = %s;
"""

cursor.execute(get_data_ppln_step_data_sql, (data_ppln_id, 1))

data_ppln_step_record = cursor.fetchone()
data_ppln_step_id = data_ppln_step_record[0]
data_load_tgt_nm = data_ppln_step_record[1]

data_ppln_job_insert_sql = f"""
insert into dart_process_control.data_ppln_job (
    data_ppln_id,
    data_eng_job_type_nm,
    job_strt_dt,
    job_stat_nm,
    last_cplt_data_load_tgt_nm)
values (%s, %s, %s, %s, %s)
returning data_ppln_job_id;
"""

cursor.execute(data_ppln_job_insert_sql, (data_ppln_id,
                                         "INCREMENTAL", 
                                         lambda_start_time, 
                                         job_status, 
                                         "ECHO SERVER"))

data_ppln_job_id = cursor.fetchone()[0]
# job_id = f"{data_ppln_job_id}"
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
def list_scripts(env, application, schema, table):
    bucket = SOURCE_FILE_BUCKET
    prefix = "farm_records/scripts/sql/{application}/{schema}/{table}".format(
        application=application,
        schema=schema,
        table=table
    )

    print(f'prefix line 291: {prefix}')
    #print(f"Reading line 289: s3://{bucket}/{prefix}")
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    #print(response)
    objs = response.get("Contents", [])
    #print(f"line 292")
    #print(objs)
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
def read_scripts(schema, table, system_date):
    print(f'read_scripts line 305')
    date_parameters = Date(system_date).parameters
    #print(f'line 307 {date_parameters}')
    scripts = [
        {
            "position": fname.split("_", 1)[0] if fname[0].isdigit() else 0,
            "operation": fname.split("_", 1)[1] if fname[0].isdigit() else fname,
            "query": read_script(key=key, date_parameters=date_parameters),
        }
        for fname, key in list_scripts(env=env, application=application, schema=schema, table=table).items()
    ]
    #print(f"line 316 ")
    print(scripts)
    # scripts = sorted(scripts, key=lambda x: x["position"])
    # scripts = sorted(scripts, key:script.get("operation") for script in scripts)
    #for script in scripts:
    #    print("{}\n\n".format(script.get("operation")))
    #    print("{}\n\n".format(script.get("query")))
    return scripts

###########################################################################################        
def Execute_sql_scripts_postgresql(sql_script, postgres_client, table):
    try:
        print(f"sql script: {sql_script} line ")
        position = sql_script["position"]
        operation = script.get("operation")
        print(f"position: {position}")
        if (int(position) != 0 and int(position) != 4):
            # Execute SQL operation on PostgreSQL
            print(f"script:{script}")
            postgres_client.execute(query=script.get("query"))
            print("Executed: {}".format(script.get("operation")))
        
    except Exception as e:
        error_msg = str(e)[:200]  # Truncate error message if necessary
        print(error_msg)
        raise

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
        # return '2025-09-25'
        return self.system_date.date() - timedelta(days=1)
        
######################################################################
# Execute get json file function and return spark datafram
# dir_Path = f"{JSON_FILE_PATH}/{JSON_FILE_NAME}"
# print(f"dir_Path:{dir_Path}")
# Json_df = get_json_file(SOURCE_FILE_BUCKET, dir_Path)
# print('Json_dataframe:', Json_df)
# tgt_tbl_lst = []
# target_table_list = []
# target_table_list = get_target_table_list(tgt_tbl_lst, Json_df)
# print('target_table_list:', target_table_list)

#####################################################################
tgt_tb_nm = args["TableName"] 
print('START line 372')
try:
    #print(f'line 373 {tgt_tb_nm}')
    table = tgt_tb_nm
    #print(f'line 375 {table}')
    
    # Read SQL scripts
    scripts = read_scripts(schema, table, system_date)
    # print(f'line 377 {schema} {table}')
        
    for script in scripts:
        print("start process Execute sql scripts postgresql")
        Execute_sql_scripts_postgresql(script, postgres_client, table)
        print("completed process Execute sql scripts postgresql")
      
    update_process_control_job_with_success(cursor, job_id)
    
except Exception as e:
    error_msg = str(e)[:300]
    # print(f"Error processing Table {table}: {error_msg}")
    update_process_control_job_with_error(cursor, job_id, error_msg)
    raise e 

# Write the job_id to an S3 bucket
bucket_name = args["SOURCE_FILE_BUCKET"]
key = f"{args['JSON_FILE_PATH']}/job_id.json"
s3_client.put_object(
    Bucket=bucket_name,
    Key=key,
    Body=json.dumps(str(job_id))
    # Body = json.dumps({"JobId": str(job_id)})
)

output = {
     "JobId": job_id
}

print(json.dumps(output))

# Commit the Glue job
job.commit()