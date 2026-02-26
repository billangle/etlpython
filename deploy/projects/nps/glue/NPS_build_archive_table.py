# Author : Craig Barnes
# ALASKA NORTHSTAR Contractor for USDA
# Date : 2024-11-06
# MDART-3161
# Source:  flat file in s3_bucket
# Target:  parquet files in s3 cleansed zone
# this is a daily incremental for the mainframe files that have 
# monthly full rebuild 
#
# 20260201 POPSUP-7222 added modification to
#          write parquet to final per 
#          per use of the   "load_type": "full_day_load", work with data which does not have the 
#              need for load full day then several daily deltas.  
#          to handle more than just nps, added arg --area.  
#          eliminated the write to Redshift step (we don't write stage tables to Redshift
# dgsprous - 20260225 - added logic to take yesterday iff environ == prod and area == nrrs and present_hour < 20


import gzip
import sys
import json
import boto3
import logging
from io import StringIO
import csv
import datetime
import os
from pyspark.sql.functions import col, lit, coalesce, when, concat, current_timestamp, date_format, row_number
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, SQLContext, functions as F
from pyspark.sql import SQLContext
from pyspark.sql import Window
from py4j.java_gateway import java_import
from pyspark.sql.types import DecimalType, DoubleType, IntegerType, StringType, StructType, StructField, TimestampType, LongType, DateType
import pytz


# @params: [JOB_NAME, source_path, destination_path, dbtable, schema_name]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'dbtable', 'schema_name', 'environ', 'utils', 'force_full_rebuild', 'area'])

# Initialize logging and get job arguments
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
sqlContext = SQLContext(spark)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
environ = args['environ']
ENVIRON = environ.upper()
area = args["area"]

# Define some standard reusable terminology variables
environ = args['environ']
ENVIRON = environ.upper()
logger.info(f"args: {args}")
tmstmp = datetime.now().strftime("%Y%m%d%H%M")
datestmp = datetime.utcnow().strftime("%Y%m%d")

# manual override for dev/test
#datestmp = '20250613'

tablename = args['dbtable']
schemaname = args['schema_name']
s3_uri = args['utils']

# dgsprous 20250822 - convert to AgCloud. DEV/CERT/PROD
# Environmental-specific settings
if environ == 'dev':
    rs_catalog_connection = "FSA-{}-redshift-conn".format(ENVIRON)
    arnrole = "arn:aws:iam::241533156429:role/FSA_Redshift_Role"
    secret_name = "FSA-{}-secrets".format(ENVIRON)
    jdbc_url = "jdbc:redshift://disc-fsa-dev-redshift.ckjzj4bsjear.us-east-1.redshift.amazonaws.com:8200/redshift_db"
    jdbc_url_jl = "jdbc:redshift://disc-fsa-dev-redshift.ckjzj4bsjear.us-east-1.redshift.amazonaws.com:8200/redshift_db"
elif environ == 'cert':
    rs_catalog_connection = "FSA-{}-redshift-conn".format(ENVIRON)
    arnrole = "arn:aws:iam::241533156429:role/FSA_Redshift_Role"
    secret_name = "FSA-{}-Secrets".format(ENVIRON)
    jdbc_url = "jdbc:redshift://disc-fsa-cert-redshift.ckjzj4bsjear.us-east-1.redshift.amazonaws.com:5439/redshift_db"
    jdbc_url_jl = "jdbc:redshift://disc-fsa-cert-redshift.ckjzj4bsjear.us-east-1.redshift.amazonaws.com:5439/redshift_db_cert"
elif environ == 'prod':
    rs_catalog_connection = "FSA-{}-redshift-conn".format(ENVIRON)
    arnrole = "arn:aws:iam::253490756794:role/FSA_Redshift_Role"
    secret_name = "FSA-{}-secrets".format(ENVIRON)
    jdbc_url = "jdbc:redshift://disc-fsa-prod-redshift.co7jv5kzm7ac.us-east-1.redshift.amazonaws.com:5439/redshift_db".format(environ)
    jdbc_url_jl = "jdbc:redshift://disc-fsa-prod-redshift.co7jv5kzm7ac.us-east-1.redshift.amazonaws.com:5439/redshift_db".format(environ)
   
s3bucket_srcode_util = 'c108-{}-fpacfsa-landing-zone'.format(environ)
key_name = 'dmart/fwadm_utils/utils.py'
#End block for environmental specific settings

#Block for properties necessary to read from Redshift database
logger.info(f"Secret problem debug:  {secret_name} {environ}")
secrets_manager_client = boto3.client('secretsmanager')
get_secret_value_response = secrets_manager_client.get_secret_value(SecretId=secret_name)
logger.info(f"FSA-{ENVIRON}-secrets: {get_secret_value_response}")
secret = json.loads(get_secret_value_response['SecretString'])
username = secret['user_db_redshift']
password = secret['pass_db_redshift']
properties = {
    "user": username,
    "password": password,
    "aws_iam_role": arnrole,
    "driver": "com.amazon.redshift.jdbc.Driver"
}
#End block for properties to read from Redshift database

#Conn options used for writing to Redshift database using Glue context
conn_options = {
    "dbtable": "{}.{}".format(schemaname,tablename),
    "database": "redshift_db",
    #"preactions":"truncate table {}.{};".format(schemaname,tablename),
    "aws_iam_role": arnrole
}

# Block needed to be able to import utils from S3. Utils holds a process table read/write code
s3 = boto3.client('s3')
download_path = '/tmp/utils.py'
s3.download_file(s3bucket_srcode_util, key_name, download_path)
#Add directory to system path
sys.path.insert(0, '/tmp')
import utils
# End block for reading utils
# This block sets up the connection to Postgres to the process tables
glue_connection_name = "FSA-{}-PG-DART114".format(ENVIRON)
java_import(sc._gateway.jvm,"java.sql.DriverManager")
dart_process_control_db_name = 'metadata_edw'
source_jdbc_conf = glueContext.extract_jdbc_conf(glue_connection_name)
pcdb_conn = sc._gateway.jvm.DriverManager.getConnection(
    source_jdbc_conf.get("url") + "/" + dart_process_control_db_name,
    source_jdbc_conf.get("user"),
    source_jdbc_conf.get("password")
)
pcdb_conn.setAutoCommit(False)
print(pcdb_conn.getMetaData())
print("Process control DB Connected...")
# End block for Postgres connection

starttime = datetime.now()
starttime_m = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
timestamp_format = "yyyy-MM-dd-HH.mm.ss.SSSSSS"

# Define source path and destination paths
source_path = f"s3://c108-{environ}-fpacfsa-landing-zone/dmart/raw/fwadm"
source_path_json = f"{source_path}/SCHEMA_JSON/{area.upper()}/{tablename}.json"
print(f"source_path_json: {source_path_json}")

json_table = utils.pull_json(source_path_json)
print(f"source_path_json: {source_path_json}")

load_type = json_table["load_type"]
print(f"load_type: {load_type}")
filename_a = json_table["source_table_name_A"]
print(f"filename_a: {filename_a}")
if load_type == 'full_day_load':
    filename_b = filename_del = 'NotUsed'
else:
    filename_b = json_table["source_table_name_B"]
    print(f"filename_b: {filename_b}")
    filename_del = json_table["delete_table_name"]
    print(f"filename_del: {filename_del}")
filename_target = json_table["target_table_name"]
print(f"filename_target: {filename_target}")
primary_key = json_table["primary_key"]
print(f"primary_key: {primary_key}")

filename_arch = json_table.get("archive_table_name")

if filename_arch == None:
    create_empty_arch = True
    print("no archive file")
else:
    print(f"filename_arch: {filename_arch}")
    source_path_archive = f"{source_path}/nps_arch/{filename_arch}"
    create_empty_arch = False

destinationpath = f"s3://c108-{environ}-fpacfsa-cleansed-zone/dmart/fwadm/{area}/{filename_target}"
print('toad')
class RedshiftJDBC:
    def __init__(self):
        self.url = jdbc_url
        self.user = username
        self.password = password
      
# Main logic
if __name__ == "__main__":
    # Connect to Redshift and prepare for operations
    redshift_jdbc = RedshiftJDBC()
    
def folder_exists(filepath):
    bucket_name, key_name = filepath.replace("s3://", "").split("/",1)
    print(f"bucket_name: {bucket_name}")
    print(f"key_name: {key_name}")
    if not key_name.endswith('/'):
        key_name = key_name+'/' 
    resp = s3.list_objects(Bucket=bucket_name, Prefix=key_name, Delimiter='/',MaxKeys=1)
    return 'Contents' in resp
    
def file_exists(filepath):
    bucket_name, key_name = filepath.replace("s3://", "").split("/",1)
    print(f"bucket_name: {bucket_name}")
    print(f"key_name: {key_name}")
    try:
        s3.head_object(Bucket=bucket_name, Key=key_name)
        print("file found")
        return True
    except Exception as e:
        if e.response['Error']['Code'] == '404' or e.response['Error']['Code'] == 'NoSuchKey':
            return False
        else:
            logger.error(f"Error: {e} : {filepath}")

print('lizz')            
# run query against cmn_dim_dm.dt_dim to find the last load_dt
# then get all the days to load since then.  
# prod FULL load dt is the the second saturday of every month and is loaded every day
# the other environments full load date is every friday and it only runs weekdays
if(environ == 'prod'):
    strsql = """(
WITH load_dt AS (
SELECT top 1 dateadd(DAY, 1, cal_dt) cal_dt
FROM cmn_dim_dm.dt_dim dd 
WHERE cal_dt <= current_date 
AND week_day_nbr = 7
AND cal_mo_day_nm_nbr = 2
ORDER BY cal_dt DESC 
)
SELECT  to_char(dd.cal_dt, 'YYYYmmdd') folderdate
FROM load_dt ld
JOIN cmn_dim_dm.dt_dim dd 
ON ld.cal_dt <= dd.cal_dt 
AND dd.cal_dt <= current_date 
ORDER BY dd.cal_dt 
)"""

else:
    strsql = f""" (
WITH load_dt AS (
SELECT top 1 cal_dt
FROM cmn_dim_dm.dt_dim dd 
WHERE cal_dt <= '{datestmp}' 
AND week_day_nbr = 6
ORDER BY cal_dt DESC 
)
SELECT DISTINCT to_char(dd.cal_dt, 'YYYYmmdd') folderdate
FROM load_dt ld
JOIN cmn_dim_dm.dt_dim dd 
ON ld.cal_dt <= dd.cal_dt 
AND dd.cal_dt <= '{datestmp}' 
AND week_day_nbr NOT IN (1,7)
ORDER BY dd.cal_dt 
)
 """

if load_type == 'full_day_load':
    # per POPSUP-7222.  we are just working with a full load everyday situation
    strsql = f"(select '{datestmp}' folderdate)"

print(f"strsql: {strsql}")

df_loaddate = sqlContext.read.jdbc(url=jdbc_url_jl, table=strsql, properties=properties)
df_loaddate.show(5)  

date_count = df_loaddate.count()
print(f"date_count: {date_count}")
isloadarchive = (date_count == 1)
print(f"isloadarchive: {isloadarchive}")

i = 0

if (date_count > 1 and load_type == 'incremental'):
    #not a load date and incremental load this should be a standard load
    #we need to make sure we have a file to build on from the last run
    datestmp_ystdy = df_loaddate.collect()[-2]["folderdate"]
    print(f"datestmp_ystdy: {datestmp_ystdy}")
    source_path_yesterday = f"{destinationpath}/{datestmp_ystdy}/"   
    if folder_exists(source_path_yesterday): 
        #This should be the normal run except for load date
        #setting i so the loop only runs once for today.
        isloadarchive = False
        i = date_count - 1
    else:
        #there is no file for yesterday: 
        if (environ != "prod" or args['force_full_rebuild']):
            logger.info("we can't find yesterday's output file, so we are just going to rebuild from last load date")
            isloadarchive = True
            i = 0
        elif (environ == 'prod'):    
            #there is not a file from yesterday, but we do not want to default to starting from load date and looping to today. 
            logger.error(f"There is not an output file from yesterday's load. Manual intervention is needed.")
elif (date_count == 1):
    #doing a full load because today full load day
    logger.info("full load day")
    isloadarchive = True
    i = 0
elif (load_type == "full"):
    logger.info("doing full rebuilt because flag was set in json file to do full reload.")
    #json file is set to full load, will go back to last full load date and loop thru days and build daily incremental files
    isloadarchive = True
    i = 0
else: #nothing should ever get to this point
    logger.error("Something went wrong and we cannot continue")

source_schema = utils.get_schema_from_source_schema(json_table["source_table_schema"])
delete_schema = utils.get_schema_from_source_schema(json_table["delete_table_schema"])

#print(f"source_schema: {source_schema}")
datestmp = df_loaddate.collect()[i]["folderdate"]
present_hour = datetime.now().astimezone(pytz.timezone('US/Central')).strftime("%H")
print(present_hour)
if (int(present_hour)<20, area.lower() == 'nrrs',environ.lower()=='prod') == (True,True,True):
    print('line 316')
    datestmp = datetime.utcnow().astimezone(pytz.timezone('US/Central')) + timedelta(days=-1)
    datestmp = datestmp.strftime("%Y%m%d")
    tmstmp = datetime.now().astimezone(pytz.timezone('US/Central')) + timedelta(days=-1)
    datestmp = tmstmp.strftime("%Y%m%d%H%M")

while i < date_count:
    print(f"inside loop loading date: {datestmp}")
    source_path_01a = f"{source_path}/{datestmp}/{filename_a}"
    source_path_01b = f"{source_path}/{datestmp}/{filename_b}"
    source_path_delete = f"{source_path}/{datestmp}/{filename_del}"
    destination_path = f"{destinationpath}/{datestmp}"

    if isloadarchive:
        # Read file from S3 path into Spark DataFrame (Tab Delimeted gzip file - No Header)
        
        if create_empty_arch:
            logger.info('creating empty dataframe ')
            df_pybl = spark.createDataFrame([], schema=source_schema)
        elif (file_exists(source_path_archive)):
            logger.info(f"Reading source data from S3 path: {source_path_archive}")
            df_pybl = spark.read.csv(source_path_archive, schema=source_schema, sep="\t", ignoreLeadingWhiteSpace=True, timestampFormat=timestamp_format)

    
    else:
        print("load yesterdays file")
        print(f"datestmp_ystdy: {datestmp_ystdy}")
        source_path_yesterday = f"{destinationpath}/{datestmp_ystdy}/"
        print(f"source_path_yesterday: {source_path_yesterday}")
        df_pybl = spark.read.parquet(source_path_yesterday)
    
    #df_pybl.printSchema()
    #df_pybl.show(5)
    print(f'df_pybl count: {df_pybl.count()}')    
    
    
    # for non-prod environments we don't get NPS files on mondays
    # but we still need the parquet files in cleansed zone for other pipelines
    # this is also the case for holidays during the week
    # so we will just ignore if the file does not exist   
    # for production we will raise an error, because we cannot skip a day

    if file_exists(source_path_01a):
        print(f'reading file A: {source_path_01a}')
        df_01a = spark.read.csv(source_path_01a, schema=source_schema, sep="\t", ignoreLeadingWhiteSpace=True, timestampFormat=timestamp_format)
        #df_01a.printSchema()
        #df_01a.show(15)
        print(f'df_01a count: {df_01a.count()}')  
    
        if not isloadarchive:
            #remove any record from yesterdays output that is in todays files.  
            df_pybl = df_pybl.join(df_01a, df_pybl[primary_key] == df_01a[primary_key], "leftanti")
            print(f'df_pybl count after leftanti join with A: {df_pybl.count()}')    
    
        df_pybl = df_pybl.union(df_01a)
        print(f'df_pybl union df_01a count: {df_pybl.count()}')
    elif not isloadarchive:
        logger.info(f"Load file does not exist.  {source_path_01a}.  Ignoring and continuing with the load.")
    else:
        logger.error(f"Load file does not exist.  {source_path_01a}")
    

    if (file_exists(source_path_01b)):
        print(f'reading file B: {source_path_01b}')
        df_01b = spark.read.csv(source_path_01b, schema=source_schema, sep="\t", ignoreLeadingWhiteSpace=True, timestampFormat=timestamp_format)
        #df_01b.printSchema()
        #df_01b.show(15)
        print(f'df_01b count: {df_01b.count()}')  
        
        if not isloadarchive:
            #remove any record from yesterdays output that is in todays files.  
            df_pybl = df_pybl.join(df_01b, df_pybl[primary_key] == df_01b[primary_key], "leftanti")
            print(f'df_pybl count after leftanti join with B: {df_pybl.count()}')    
        
        df_pybl = df_pybl.union(df_01b)
        print(f'df_pybl union df_01b count: {df_pybl.count()}')
    elif not isloadarchive:
        logger.info(f"Load file does not exist.  {source_path_01b}.  Ignoring and continuing with the load.")
    else:
        logger.error(f"Load file does not exist.  {source_path_01b}")
   
    
    if (file_exists(source_path_delete)):
        print(f"reading delete file:' {source_path_delete}")
        df_delete = spark.read.csv(source_path_delete, schema=delete_schema, sep="\t", ignoreLeadingWhiteSpace=True, timestampFormat=timestamp_format)
        df_pybl = df_pybl.join(df_delete, df_pybl[primary_key] == df_delete[primary_key], "leftanti")
        print(f'df_pybl after delete file count: {df_pybl.count()}')
    elif not isloadarchive:
        logger.info(f"Load file does not exist.  {source_path_delete}.  Ignoring and continuing with the load.")
    else:
        logger.error(f"Load file does not exist.  {source_path_delete}")

  
    try:
        logger.info(f"Saving data to the cleansed Zone in Parquet format: {destination_path}")
        df_pybl.write.mode("overwrite").parquet(f"{destination_path}")
        # original instructions were to write to this path.  Leave if they change their minds.  add line to write to Final
        # final_path = f'c108-{ENVIRON}-fpacfsa-final-zone/{BIZAREA}/{tablename}'
        #         s3://c108-dev-fpacfsa-final-zone/car/
        # fpath = f's3://c108-{environ.lower()}-fpacfsa-final-zone/{area}/{tablename}'
        #logger.info(f"Data successfully saved to the Final Zone in Parquet format at: \n{destination_path} and\n {fpath}")
        #df_pybl.write.mode("overwrite").parquet(fpath)
        # NB:  NPS-mainframe, NPS and NRRS are on diff schedules.  Don't overwrite with no data present.  
        #      there should always be data but frequently one of the three won't have data transfered today.  
        if df_pybl.count() > 0:
            fpath = f's3://c108-{environ.lower()}-fpacfsa-final-zone/dmart/fwadm/{area}/{tablename}'
            logger.info(f"Data successfully saved to the SECOND Final Zone in Parquet format at: \n{destination_path} and\n {fpath}")
            df_pybl.write.mode("overwrite").parquet(fpath)
        else:
            logger.info(f"No new data today")
    
    except Exception as e:
        logger.error(f"Error while saving data to the Final Zone in Parquet format: {e}")
    
    i = i + 1
    print(f"completed loop {i} of {date_count}")
    datestmp_ystdy = datestmp
    print(f"next loop datestmp: {datestmp}")
    isloadarchive = False #set to false so next loop works


endtime_m = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
job_stat_nm = f"FWADM {area} {tablename} successfully loaded"
utils.data_ppln_job_insert(job_stat_nm, 'Complete', starttime_m, endtime_m, pcdb_conn, ENVIRON, area.upper())

logger.info("Glue Job finished.")
job.commit()


endtime_m = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
job_stat_nm = f"FWADM {area} {tablename} successfully loaded"
utils.data_ppln_job_insert(job_stat_nm, 'Complete', starttime_m, endtime_m, pcdb_conn, ENVIRON, area.upper())

logger.info("Glue Job finished.")
job.commit()


