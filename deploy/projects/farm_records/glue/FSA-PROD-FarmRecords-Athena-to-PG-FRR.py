import sys
import os
import io
import time
import boto3
import random
import json
import logging
from pathlib import Path
import s3fs
from io import BytesIO
from datetime import datetime, timezone, timedelta
from pandas.api.types import is_datetime64_any_dtype
import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import psycopg2
from psycopg2 import extras
import pyarrow as pa
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import awswrangler as wr
from sqlalchemy import create_engine
import pg8000.native

args = getResolvedOptions(sys.argv, ["JOB_NAME", "region", "source_bucket", "pipeline_name", "edv_table_name", "JSON_FILE_NAME", "SecretId", "sql_scripts_prefix", "athena_output_location", "athena_database", "pandas_datatype", "run_type", "JobId"])

JOB_NAME = args["JOB_NAME"]
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
spark = glueContext.spark_session

REGION = args["region"]
SOURCE_BUCKET = args["source_bucket"]
sql_scripts_prefix = args["sql_scripts_prefix"]
athena_output_location = args["athena_output_location"]
athena_database = args["athena_database"]
PIPELINE_NAME = args["pipeline_name"]
JSON_FILE_NAME = args["JSON_FILE_NAME"]
pandas_datatype = args["pandas_datatype"]
run_type = args["run_type"]
job_id = args["JobId"]

SECRET_NAME = args["SecretId"]
secrets = boto3.client("secretsmanager", region_name=REGION)
secret_value = secrets.get_secret_value(SecretId=SECRET_NAME)
secret = json.loads(secret_value["SecretString"])

edv_table_name = args["edv_table_name"]
date_portion = datetime.now().strftime("%Y%m%d")

TODAY = datetime.now().date()
# job_id = '120027'

job_id_threshold = f"{job_id}" # I just put this in for now but future enhancement is to grab it from the database
s3_client = boto3.client('s3', region_name=REGION)
s3_resource = boto3.resource("s3", region_name=REGION)
#####################################################################
# Connect to postgres dart_process_control DB
keepalive_kwargs = {
    "keepalives": 1,
    "keepalives_idle": 30,
    "keepalives_interval": 5,
    "keepalives_count": 5,
}
#####################################################################
# Connect to postgres EDV DB
edv_db_host = secret["edv_postgres_hostname"]
edv_db_port = secret["postgres_port"]
edv_db_name = secret["edv_postgres_database_name"]
# edv_db_name_crm_ods = secret["edv_postgres_database_CRM_ODS"]
edv_db_user = secret["edv_postgres_username"]
edv_db_password = secret["edv_postgres_password"]


crmods_connection_dict = {
    "dbname": "CRM_ODS",
    "user": secret["edv_postgres_username"],
    "password": secret["edv_postgres_password"],
    "port": secret["postgres_port"],
    "host": secret["edv_postgres_hostname"],
    "sslmode":"require"
}

print(f"crmods_connection_dict:{crmods_connection_dict}")

edv_postgres_conn_crm_ods = psycopg2.connect(**crmods_connection_dict)
print(f"edv_postgres_conn_crm_ods:{edv_postgres_conn_crm_ods}")
print('connection successful')

postgres_client_crm_ods = edv_postgres_conn_crm_ods.cursor()

#####################################################################

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
s3_S3FileSystem = s3fs.S3FileSystem()
def file_exists(target_path, s3_S3FileSystem):
    return s3_S3FileSystem.exists(target_path)
#####################################################################

#####################################################################
# Upsert Primary Keys
#####################################################################
upsert_pk = {
    "CLU_PRODUCER_YEAR": ['clu_producer_year_identifier'],
    "CLU_YEAR": ['clu_year_identifier'],
    "CLU_YEAR_ADJUSTMENT": ['clu_year_adjustment_identifier'],
    "COMMON_LAND_UNIT": ['clu_identifier'],
    "COUNTY_OFFICE_CONTROL": ['county_office_control_identifier'],
    "CROP": ['crop_identifier'],
    "CROP_FARM_YEAR_DCP": ['crop_farm_year_dcp_identifier'],
    "CROP_FARM_YEAR_MANAGED_VALUES_ADJUSTMENT": ['crop_farm_year_managed_values_adjustment_identifier'],
    "CROP_TRACT_CONTRACT": ['crop_tract_contract_identifier'],
    "CROP_TRACT_YEAR_DCP": ['crop_tract_year_dcp_identifier'],
    "CROP_TRACT_YEAR_DCP_ADJUSTMENT": ['crop_tract_year_dcp_adjustment_identifier'],
    "DOMAIN": ['domain_identifier'],
    "DOMAIN_VALUE": ['domain_value_identifier'],
    "FARM": ['farm_identifier'],
    "FARM_PRODUCER_YEAR": ['farm_producer_year_identifier'],
    "FARM_RECONSTITUTION": ['farm_reconstitution_identifier'],
    "FARM_YEAR": ['farm_year_identifier'],
    "FARM_YEAR_CROP_IRRIGATION_HISTORY": ['farm_year_crop_irrigation_history_identifier'],
    "FARM_YEAR_DCP": ['farm_year_dcp_identifier'],
    "IRRIGATION_COUNTY_CROP": ['irrigation_county_crop_identifier'],
    "MIDAS_FARM_TRACT_CHANGE": ['midas_farm_tract_change_identifier'],
    "RECONSTITUTION": ['reconstitution_identifier'],
    "RECONSTITUTION_CROP": ['reconstitution_crop_identifier'],
    "TIME_PERIOD": ['time_period_identifier'],
    "TRACT": ['tract_identifier'],
    "TRACT_PRODUCER_YEAR": ['tract_producer_year_identifier'],
    "TRACT_RECONSTITUTION": ['tract_reconstitution_identifier'],
    "TRACT_YEAR": ['tract_year_identifier'],
    "TRACT_YEAR_ADJUSTMENT": ['tract_year_adjustment_identifier'],
    "TRACT_YEAR_DCP": ['tract_year_dcp_identifier'],
    "TRACT_YEAR_WETLAND_VIOLATION": ['tract_year_wetland_violation_identifier'],
    "YEAR_ARC_PLC_PARTICIPATION_ELECTION": ['year_arc_plc_participation_election_identifier']
}
    
######################################################################
# Read the script from S3 Cleansed Zone
######################################################################
def read_script(bucket, key):
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read().decode()
    
######################################################################
# Read the json datatypes from S3 Cleansed Zone
######################################################################
def read_json(bucket, key, edv_table_name):
    print('168: read_json')
    print(f'bucket, key, edv_table_name: \n{bucket} \n{key} \n{edv_table_name}')
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    print('171')
    file_content = obj['Body'].read().decode('utf-8')
    print('173')
    INPUT_JSON = json.loads(file_content)
    print('175')
    JSON_DATA = INPUT_JSON
    return JSON_DATA[edv_table_name.lower()]

######################################################################
# Apply the mapping of the datatype fields from the csv that is outputted from Athena
######################################################################
def apply_mapping(df, mapping):
    for source_col, source_type, target_col, target_type in mapping:
        # Add column if it doesn't exist
        if target_col not in df.columns:
            df[target_col] = None
        
        # Type conversion
        if target_type == "int":
            df[target_col] = pd.to_numeric(df[target_col], errors='coerce').fillna(0).astype('int')
        elif target_type == "Timestamp":
            try:
                df[target_col] = pd.to_datetime(df[target_col], errors='coerce')
                # .fillna('9999-12-31').astype('datetime')
            except Exception as e:
                df[target_col] = '9999-12-31'
        elif target_type == "varchar":
            df[target_col] = df[target_col].astype(str)
        elif target_type == "string":
            df[target_col] = df[target_col].astype(str)
    
    # Replace "None" with empty strings in all columns
    df = df.replace("None", None)
    
    return df
  
######################################################################
# Run the script on athena and load the new data to postgres (Insert)
######################################################################
def run_athena_insert_query (bucket_name, edv_table_name):
    
    database_name = athena_database
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(bucket_name)
    
    s3_athena_output_location = f"s3://{bucket_name}/{athena_output_location}/{date_portion}/{job_id}/{edv_table_name.upper()}/INSERT/"
    print(f"s3_athena_output_location:{s3_athena_output_location}")
    
    athena_insert_query = f"{sql_scripts_prefix}/{edv_table_name.upper()}/{edv_table_name.upper()}_INSERT_ATHENA.sql"
    # Get primary keys for the table
    # primary_keys = TABLE_PRIMARY_KEYS.get(edv_table_name.upper(), [])
   
   # First insert the records from the Athena result set into the postgres farm_records_reporting db
    athena_insert = f"""{read_script(bucket_name, athena_insert_query)}"""
    print(f"athena_insert: {athena_insert}")
    
    # Execute Athena Insert query
    athena_client = boto3.client('athena')
    response = athena_client.start_query_execution(
        QueryString=athena_insert,
        QueryExecutionContext={'Database': database_name},
        ResultConfiguration={'OutputLocation': s3_athena_output_location}
    )
    query_execution_id = response['QueryExecutionId']
        
    print(f"query_execution_id:{query_execution_id}")
        
    # Wait for query to complete
    while True:
        query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status = query_status['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(300)
        
    if status == 'SUCCEEDED':
        print(f"242 Athena query succeeded. Results in: {s3_athena_output_location}")
        
        # Read Athena results CSV output by default
        csv_file_path = f"{s3_athena_output_location}"
        csv_file_list = s3_S3FileSystem.glob(csv_file_path)
        new_csv_file_list = []
        print(f"248 csv_file_list: {csv_file_list}")
            
        for file_path in csv_file_list:
            if file_path.endswith(".csv"):
                file_path_final = f"s3://{file_path}"
                new_csv_file_list.append(file_path_final)
                print(f"csv file path: {file_path_final}")
    
        print(f"new_csv_file_list:{new_csv_file_list}")
        
        # Get the datatypes for the table of interest to allow the data from the 
        # csv to be properly formatted into the pandas df
        dtypes_pd = read_json(bucket_name, pandas_datatype, edv_table_name.lower())
        print('line 263')
        pandas_df = pd.read_csv(new_csv_file_list[0], dtype=dtypes_pd)
        print('line 265')
        
        print(f"printing df info...")
        print(pandas_df.info())
        
        print(f"\nnumber of rows to insert.. 266")
        num_rows = len(pandas_df) 
        print(num_rows)
        
        if num_rows > 0:
            # Get the datatypes for the table of interest to allow the data from the 
            # csv to be properly formatted before inserted into the pg table
            dtypes = read_json(bucket_name, JSON_FILE_NAME, edv_table_name.lower())
            
            try:
                print("Loading data into Postgres... # line 276")
                # Connection to the postgres EDV database (farm_records_reporting)
                conn = pg8000.connect(
                    host=edv_db_host,
                    port=edv_db_port,
                    user=edv_db_user,
                    password=edv_db_password,
                    database=edv_db_name
                )
                
                start = 0
                end = 1000000
                print(f'Attempting to load between {start} and {end}')
                while start < num_rows:
                    wr.postgresql.to_sql(
                        df=pandas_df.iloc[start:end],
                        table=edv_table_name.lower(),
                        schema="farm_records_reporting",
                        mode="append",
                        con=conn,
                        index=False,
                        use_column_names=True,
                        dtype=dtypes
                        )
                    print(f'loaded between {start} and {end}')
                    start = end
                    end = end + 1000000
                print("Data loaded successfully.")
                print(f"\n{num_rows} inserted into the farm_records_reporting.{edv_table_name.lower()}.")
                conn.commit()
                print(f"commit complete")
            # except (Exception, psycopg2.DatabaseError) as error:
            #     raise("Error: %s" % error)
            #     conn.rollback()
            except Exception as e:
                print(f"An error occurred: {e}")
                raise(e)
                conn.rollback()
            finally:
                conn.close()
        else:
            print(f"There is no new data to insert into farm_records_reporting.{edv_table_name.lower()}")
    else:
        raise(f"Athena query failed with status: {status}")
        
######################################################################
# Run the script on athena and load the updated data to postgres (Update)
######################################################################
def run_athena_update_query (bucket_name, edv_table_name):
    
    database_name = athena_database
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(bucket_name)
    
    s3_athena_output_location = f"s3://{bucket_name}/{athena_output_location}/{date_portion}/{job_id}/{edv_table_name.upper()}/UPDATE/"
    print(f"s3_athena_output_location:{s3_athena_output_location}")
    
    athena_update_query = f"{sql_scripts_prefix}/{edv_table_name.upper()}/{edv_table_name.upper()}_UPDATE_ATHENA.sql"
    # Get primary keys for the table
    # primary_keys = TABLE_PRIMARY_KEYS.get(edv_table_name.upper(), [])
    
    # Second update the records from the Athena result set to the postgres farm_records_reporting db
    athena_update = f"""{read_script(bucket_name, athena_update_query)}"""
    print(f"athena_update: {athena_update}")
    
    # Execute Athena Insert query
    athena_client = boto3.client('athena')
    response = athena_client.start_query_execution(
        QueryString=athena_update,
        QueryExecutionContext={'Database': database_name},
        ResultConfiguration={'OutputLocation': s3_athena_output_location}
    )
    query_execution_id = response['QueryExecutionId']
        
    print(f"query_execution_id:{query_execution_id}")
        
    # Wait for query to complete
    while True:
        query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status = query_status['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(300)
        
    if status == 'SUCCEEDED':
        print(f"362 Athena query succeeded. Results in: {s3_athena_output_location}")
        
        # Read Athena results CSV output by default
        csv_file_path = f"{s3_athena_output_location}"
        csv_file_list = s3_S3FileSystem.glob(csv_file_path)
        new_csv_file_list = []
        print(f"368 csv_file_list: {csv_file_list}")
            
        for file_path in csv_file_list:
            if file_path.endswith(".csv"):
                file_path_final = f"s3://{file_path}"
                new_csv_file_list.append(file_path_final)
                print(f"csv file path: {file_path_final}")
        
        print(f"new_csv_file_list:{new_csv_file_list}")
        
        # Get the datatypes for the table of interest to allow the data from the 
        # csv to be properly formatted into the pandas df
        dtypes_pd = read_json(bucket_name, pandas_datatype, edv_table_name.lower())
        
        pandas_df = pd.read_csv(new_csv_file_list[0], dtype=dtypes_pd)
        
        print(f"\nnumber of rows to update.")
        num_rows = len(pandas_df) 
        print(num_rows)
        
        if num_rows > 0:
            # Get the datatypes for the table of interest to allow the data from the 
            # csv to be properly formatted before inserted into the pg table
            dtypes = read_json(bucket_name, JSON_FILE_NAME, edv_table_name.lower())
            
            try:
                print("Loading data into Postgres... # 385")
                # Connection to the postgres EDV database (farm_records_reporting)
                
                conn = pg8000.connect(
                    host=edv_db_host,
                    port=edv_db_port,
                    user=edv_db_user,
                    password=edv_db_password,
                    database=edv_db_name
                )
                start = 0
                end = 1000000
                num_rows = len(pandas_df) 
                while start < num_rows:
                    print(num_rows)
                    print(f'Attempting to load between {start} and {end}')
                    wr.postgresql.to_sql(
                        df=pandas_df,
                        table=edv_table_name.lower(),
                        schema="farm_records_reporting",
                        con=conn,
                        index=False,
                        use_column_names=True,
                        dtype=dtypes,
                        mode="upsert",
                        upsert_conflict_columns= upsert_pk[edv_table_name.upper()]  # Columns to identify conflicts (primary keys)
                        )
                    print("Data updated successfully. between {start} and {end}")
                    start = end
                    end = end + 1000000
                print(f"\n{num_rows} updated in the farm_records_reporting.{edv_table_name.lower()}.")
                conn.commit()
                print(f"commit complete")
            # except (Exception, psycopg2.DatabaseError) as error:
            #     raise("Error: %s" % error)
            #     conn.rollback()
            except Exception as e:
                print(f"An error occurred: {e}")
                raise(e)
                conn.rollback()
            finally:
                conn.close()
        else:
            print(f"There is no new data to update into farm_records_reporting.{edv_table_name.lower()}")
    else:
        raise(f"Athena query failed with status: {status}")
        
######################################################################
# Run the script on athena and load the new data to postgres, this insert will not
# look to compare the values of the new data against data existing in the table, no left
# join back to the FARM_RECORDS_REPORTING table(Insert)
######################################################################

def run_athena_insert_source_query (bucket_name, edv_table_name):
    
    database_name = athena_database
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(bucket_name)
    
    s3_athena_output_location = f"s3://{bucket_name}/{athena_output_location}/{date_portion}/{job_id}/{edv_table_name.upper()}/INSERT/"
    print(f"s3_athena_output_location:{s3_athena_output_location}")
    
    athena_insert_query = f"{sql_scripts_prefix}/{edv_table_name.upper()}/{edv_table_name.upper()}_INSERT_NO_COMPARE_ATHENA.sql"
    # Get primary keys for the table
    # primary_keys = TABLE_PRIMARY_KEYS.get(edv_table_name.upper(), [])
   
   # First insert the records from the Athena result set into the postgres farm_records_reporting db
    athena_insert = f"""{read_script(bucket_name, athena_insert_query)}"""
    print(f"athena_insert: {athena_insert}")
    
    # Execute Athena Insert query
    athena_client = boto3.client('athena')
    response = athena_client.start_query_execution(
        QueryString=athena_insert,
        QueryExecutionContext={'Database': database_name},
        ResultConfiguration={'OutputLocation': s3_athena_output_location}
    )
    query_execution_id = response['QueryExecutionId']
        
    print(f"query_execution_id:{query_execution_id}")
        
    # Wait for query to complete
    while True:
        query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status = query_status['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(300)
        
    if status == 'SUCCEEDED':
        print(f"484 Athena query succeeded. Results in: {s3_athena_output_location}")
        
        # Read Athena results CSV output by default
        csv_file_path = f"{s3_athena_output_location}"
        csv_file_list = s3_S3FileSystem.glob(csv_file_path)
        new_csv_file_list = []
        print(f"490 csv_file_list: {csv_file_list}")
            
        for file_path in csv_file_list:
            if file_path.endswith(".csv"):
                file_path_final = f"s3://{file_path}"
                new_csv_file_list.append(file_path_final)
                print(f"csv file path: {file_path_final}")
    
        print(f"new_csv_file_list:{new_csv_file_list}")
        
        # Get the datatypes for the table of interest to allow the data from the 
        # csv to be properly formatted into the pandas df
        dtypes_pd = read_json(bucket_name, pandas_datatype, edv_table_name.lower())
        
        pandas_df = pd.read_csv(new_csv_file_list[0], dtype=dtypes_pd)
        
        print(f"printing df info...")
        print(pandas_df.info())
        
        print(f"\nnumber of rows to insert.. 491")
        num_rows = len(pandas_df) 
        print(num_rows)
        
        if num_rows > 0:
            # Get the datatypes for the table of interest to allow the data from the 
            # csv to be properly formatted before inserted into the pg table
            dtypes = read_json(bucket_name, JSON_FILE_NAME, edv_table_name.lower())
            
            try:
                print("Loading data into Postgres... 501")
                # Connection to the postgres EDV database (farm_records_reporting)
                # .iloc[start:end]
                
                conn = pg8000.connect(
                    host=edv_db_host,
                    port=edv_db_port,
                    user=edv_db_user,
                    password=edv_db_password,
                    database=edv_db_name
                )
                start = 0
                end = 1000000
                print(f'514:  Attempting to load between {start} and {end}')
                while start < num_rows:
                    wr.postgresql.to_sql(
                        df=pandas_df.iloc[start:end],
                        table=edv_table_name.lower(),
                        schema="farm_records_reporting",
                        mode="append",
                        con=conn,
                        index=False,
                        use_column_names=True,
                        dtype=dtypes
                        )
                    print(f'526:  loaded between {start} and {end}')
                    start = end
                    end = end + 1000000
                print("Data loaded successfully.")
                print(f"\n{num_rows} inserted into the farm_records_reporting.{edv_table_name.lower()}.")
                conn.commit()
                print(f"commit complete")
            # except (Exception, psycopg2.DatabaseError) as error:
            #     raise("Error: %s" % error)
            #     conn.rollback()
            except Exception as e:
                print(f"An error occurred: {e}")
                raise(e)
                conn.rollback()
            finally:
                conn.close()
        else:
            print(f"There is no new data to insert into farm_records_reporting.{edv_table_name.lower()}")
    else:
        raise(f"Athena query failed with status: {status}")
######################################################################
# MAIN - RUN THE INSERT AND THEN RUN THE UPDATE FOR THE TABLE PASSED FROM PARAMS
######################################################################
if run_type == 'INSERT':
    run_athena_insert_query(SOURCE_BUCKET, edv_table_name)
    
elif run_type == 'UPDATE':
    run_athena_update_query(SOURCE_BUCKET, edv_table_name)
    
elif run_type == 'BOTH':
    run_athena_insert_query(SOURCE_BUCKET, edv_table_name)
    
    run_athena_update_query(SOURCE_BUCKET, edv_table_name)
    
elif run_type == 'NO_COMPARE':
    run_athena_insert_source_query(SOURCE_BUCKET, edv_table_name)