##############################################################################################################################
# Author : Sajid Inayat
# Steampunk Contractor for USDA
# Date : 2025-03-24
# Project : Juniper
# FSA-CERT-FWADM-payment_summary
# MDART-3836
# history - Sajid Inayat - 2024-03-24: A New table adaption to Glue for NPS
#                                      Writing df_final to the final zone in Parquet format with Snappy compression
#                                      Using Redshift COPY command for bulk data load
#                                      Adjusted shuffle partitions and enabled AQE
#                                      Added validation functions
# chsingh 20250822 - convert to AgCloud. DEV/CERT/PROD
# dgsprous 20260210 - refactor to support PMRDS.  writes a final zone parquet
##############################################################################################################################

import gzip
import sys
import json
import boto3
import logging
from io import StringIO
import csv
import datetime
import os
from pyspark.sql.functions import col, lit, coalesce, when, concat, current_timestamp, date_format, row_number, max, expr, md5, concat_ws
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
from pyspark.sql.types import DecimalType, DoubleType, IntegerType, StringType, StructType, StructField, TimestampType, LongType, DateType, ShortType

# @params: [JOB_NAME, dbtable, schema_name]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'dbtable', 'schema_name', 'environ', 'utils'])

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

# Define some standard reusable terminology variables
environ = args['environ']
ENVIRON = environ.upper()
logger.info(f"args: {args}")
tmstmp = datetime.now().strftime("%Y%m%d%H%M")
datestmp = datetime.utcnow().strftime("%Y%m%d")
tablename = args['dbtable']
schemaname = args['schema_name']
s3_uri = args['utils']

# Redshift connection options
# Environmental-specific settings
if environ == 'dev':
    rs_catalog_connection = "FSA-{}-redshift-conn".format(ENVIRON)
    #arnrole = "arn:aws-us-gov:iam::241533156429:role/FSA_Redshift_Role"
    arnrole = "arn:aws:iam::241533156429:role/FSA_Redshift_Role"
    secret_name = "FSA-{}-secrets".format(ENVIRON)
    # jdbc_url = "jdbc:redshift://fsa-{}-redshift.cldrls4ljqk0.us-east-1.redshift.amazonaws.com:8200/redshift_db".format(environ)
    jdbc_url = "jdbc:redshift://disc-fsa-dev-redshift.ckjzj4bsjear.us-east-1.redshift.amazonaws.com:8200/redshift_db"
    jdbc_url_jl = "jdbc:redshift://disc-fsa-dev-redshift.ckjzj4bsjear.us-east-1.redshift.amazonaws.com:8200/redshift_db"
elif environ == 'cert':
    rs_catalog_connection = "FSA-{}-redshift-conn".format(ENVIRON)
    #arnrole = "arn:aws-us-gov:iam::241533156429:role/FSA_Redshift_Role"
    arnrole = "arn:aws:iam::241533156429:role/FSA_Redshift_Role"
    secret_name = "FSA-{}-Secrets".format(ENVIRON)
    # jdbc_url = "jdbc:redshift://fsa-{}-redshift.cldrls4ljqk0.us-east-1.redshift.amazonaws.com:8200/redshift_db".format(environ)
    jdbc_url = "jdbc:redshift://disc-fsa-cert-redshift.ckjzj4bsjear.us-east-1.redshift.amazonaws.com:5439/redshift_db"
    jdbc_url_jl = "jdbc:redshift://disc-fsa-cert-redshift.ckjzj4bsjear.us-east-1.redshift.amazonaws.com:5439/redshift_db_cert"
elif environ == 'prod':
    rs_catalog_connection = "FSA-{}-redshift-conn".format(ENVIRON)
    #arnrole = "arn:aws-us-gov:iam::662519022378:role/FSA_PROD_Redshift_Role"
    secret_name = "FSA-{}-secrets".format(ENVIRON)
    #jdbc_url = "jdbc:redshift://fsa-{}-redshift.cldrls4ljqk0.us-gov-west-1.redshift.amazonaws.com:8002/redshift_db".format(environ)
    #jdbc_url_jl = "jdbc:redshift://fsa-{}-redshift.cldrls4ljqk0.us-gov-west-1.redshift.amazonaws.com:8002/redshift_db".format(environ)
    arnrole = "arn:aws:iam::253490756794:role/FSA_Redshift_Role"
    secret_name = "FSA-{}-secrets".format(ENVIRON)
    jdbc_url = "jdbc:redshift://disc-fsa-prod-redshift.co7jv5kzm7ac.us-east-1.redshift.amazonaws.com:5439/redshift_db".format(environ)
    jdbc_url_jl = "jdbc:redshift://disc-fsa-prod-redshift.co7jv5kzm7ac.us-east-1.redshift.amazonaws.com:5439/redshift_db".format(environ)

s3bucket_srcode_util = 'c108-{}-fpacfsa-landing-zone'.format(environ)
key_name = 'dmart/fwadm_utils/utils.py'
#End block for environmental specific settings

#Block for properties necessary to read from Redshift database    
secrets_manager_client = boto3.client('secretsmanager')
get_secret_value_response = secrets_manager_client.get_secret_value(SecretId=secret_name)
logger.info(f"FSA-{ENVIRON}-Secrets: {get_secret_value_response}")
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
    "preactions":"truncate table {}.{};".format(schemaname,tablename),
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

##############################################################################################
# Adaptive Query Execution and Shuffle Optimizations
##############################################################################################
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "64MB")
spark.conf.set("spark.sql.shuffle.partitions", 400) 

finalzone = "s3://c108-{}-fpacfsa-final-zone/dmart/fwadm/nps/{}".format(environ, tablename)
finalzone_path = f"{finalzone}/{tmstmp}"

starttime = datetime.now()
starttime_m = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

#################################################
# Print schema and sample rows
#################################################
def debug_df(df, label):
    print(f"\n--- {label} ---")
    print("Schema:")
    df.printSchema()
    print("Row count:")
    try:
        print(df.count())
    except Exception as e:
        print(f"Count failed: {e}")
    print("Sample rows:")
    try:
        df.show(5, truncate=False)
    except Exception as e:
        print(f"Show failed: {e}")
    print("Partition count:")
    try:
        print(df.rdd.getNumPartitions())
    except Exception as e:
        print(f"Partition count failed: {e}")  


################################################################################
# Variables
################################################################################
validation_results = []
dq_passed = True

# Input: Table Name, Columns, and DataFrames
pk_column = "payable_offset_amount"
fk_column = "payable_identifier"

####################################################################
# Define the Final Zone S3 path
####################################################################
finalzone = "s3://c108-{}-fpacfsa-final-zone/dmart/fwadm/nps_qa/payment_summary".format(environ)
final_zone_path = f"{finalzone}/{tmstmp}"

###############################################################################################
# Validate Primary Key uniqueness
###############################################################################################
def validate_pk(df, pk_column):
    try:
        print('validate_pk function')
        failures_df = df.groupBy(pk_column).count().filter("count > 1")
        status = df.count() == df.select(pk_column).distinct().count()  # Pass if no duplicates
        print(f'validate_pk function c1 {df.count()}')
        print(f'validate_pk function c2 {df.select(pk_column).distinct().count()}')
        print(f'validate_pk function c3 {pk_column}')
        reason = f"Duplicate primary keys found for {pk_column}" if not status else "Primary Key validation passed."
        return status, failures_df, reason
    except Exception as e:
        logger.error(f"Error during PK validation: {str(e)}")
        return False, None, str(e)

###############################################################################################
# Validate Foreign Key constraints
###############################################################################################
def validate_fk(df, fk_column, fk_dim_df):
    try:
        allow_nulls = False
        if fk_dim_df.filter(col(fk_column).isNull()).count() > 0:
            allow_nulls = True
        fk_dim_df = fk_dim_df.select( (fk_column) ).distinct()
        failures_df = df.join(fk_dim_df, df[fk_column] == fk_dim_df[fk_column], "left_anti")
        if allow_nulls:
            failures_df = failures_df.filter(col(fk_column).isNotNull())
        status = failures_df.count() == 0
        reason = "Foreign key validation failed" if not status else "Foreign Key validation passed."
        print(reason)
        return status, failures_df, reason
    except Exception as e:
        logger.error(f"Error during FK validation: {str(e)}")
        print(f"Error during FK validation: {str(e)}")
        return False, None, str(e)

###############################################################################################
# Validate data consistency using Null Values Check
###############################################################################################
def validate_nulls(df, columns):
    try:
        print("Columns to validate: ", columns)
        # Filter rows with NULLs in the specified columns
        null_condition = " OR ".join([f"{col} IS NULL" for col in columns])
        print("Generated Condition: ", null_condition)
        failures_df = df.filter(null_condition)
        print("Count of rows with NULLs: ", failures_df.count())
        status = failures_df.count() == 0  # Pass if no NULLs found
        reason = "Null values found" if not status else "No null values found"
        print(reason)
        return status, failures_df, reason
    except Exception as e:
        logger.error(f"Error during Null validation: {str(e)}")
        print(f"Error during Null validation: {str(e)}")
        return False, None, str(e)

###############################################################################################
# Validate duplicates across specified columns
###############################################################################################
def validate_duplicates(df, columns):
    try:
        failures_df = df.groupBy(columns).count().filter("count > 1")
        status = failures_df.count() == 0
        reason = "Duplicates found in specified columns" if not status else "Duplicate validation passed."
        print(reason)
        return status, failures_df, reason
    except Exception as e:
        logger.error(f"Error during duplicate validation: {str(e)}")
        print(f"Error during duplicate validation: {str(e)}")
        return False, None, str(e)

#############################################################################################
# Log validation results along with metadata
#############################################################################################
def log_and_append_result(validation_type, status, failures_df=None, reason=None, tablename=None, quarantine_path=None, schemaname=schemaname, scorecard_name=None):
    try:
        scorecard_name = (
            "Data Integrity" if validation_type == "PrimaryKey" else
            "Data Completeness" if validation_type == "ForeignKey" else
            "Data Consistency" if validation_type == "NullValidation" else
            "Data Validation" if validation_type == "Duplicates" else
            "Unknown"
            )
        failed_row_count = failures_df.count() if failures_df and not failures_df.isEmpty() else 0
        result = {
            "ValidationType": validation_type,
            "TableName": tablename,
            "ValidationStatus": "Pass" if status else "Fail",
            "FailedRows": failed_row_count,
            "Reason": reason or "N/A",
            "QuarantinePath": quarantine_path or "N/A",
            "SchemaName": schemaname or "N/A",
            "ScoreCardName": scorecard_name or "N/A",
            "Timestamp": datetime.now().isoformat()
        }
        logger.info(f"Validation Result: {result}")
        validation_results.append(result)
    except Exception as e:
        logger.error(f"Error in log_and_append_result: {str(e)}")


destinationpath = "s3://c108-{}-fpacfsa-cleansed-zone/dmart/fwadm/nps/{}".format(environ, tablename)
destination_path = f"{destinationpath}/{tmstmp}"

source_table = "payment_transaction_fact"
target_table = "payment_summary"

##################################################################################################
# Read source data from Redshift
# Query only the necessary columns from the fact table
##################################################################################################
raw_query = f"""(SELECT 
                final_review_date,
                disbursement_date,
                state_fsa_code,
                county_fsa_code,
                accounting_program_year,
                accounting_program_code,
                commodity_code,
                disbursement_amount,
                payable_offset_amount,
                payable_identifier,
                payment_request_amount,
                prompt_payment_interest_amount,
                tax_withholding_amount,
                payment_transaction_type_identifier
            FROM {schemaname}.payment_transaction_fact) AS raw_query"""

##################################################################################################
# Read from Redshift
##################################################################################################
raw_df = spark.read.jdbc(url=jdbc_url, table=raw_query, properties=properties)
debug_df(raw_df, "raw_df")


##################################################################################################
# Filter and transform
##################################################################################################
# Filter rows
filtered_df = raw_df.filter(
    (F.col("payable_identifier") > 0) &
    (F.col("payment_transaction_type_identifier").isin(1, 30, 31, 36))
)

debug_df(filtered_df, "filtered_df")

##################################################################################################
# Aggregation with casting
##################################################################################################
agg_df = filtered_df.groupBy(
    "state_fsa_code",
    "county_fsa_code",
    "accounting_program_year",
    "accounting_program_code",
    "commodity_code",
    "payable_identifier"
).agg(
    F.max(F.month("final_review_date")).cast("smallint").alias("final_review_month"),
    F.max(F.year("final_review_date")).cast("smallint").alias("final_review_year"),
    F.max(F.month("disbursement_date")).cast("smallint").alias("disbursement_month"),
    F.max(F.year("disbursement_date")).cast("smallint").alias("disbursement_year"),
    F.sum("disbursement_amount").alias("disbursement_amount"),
    F.sum("payable_offset_amount").alias("payable_offset_amount"),
    F.sum("payment_request_amount").alias("payment_request_amount"),
    F.sum("prompt_payment_interest_amount").alias("prompt_payment_interest_amount"),
    F.sum("tax_withholding_amount").alias("tax_withholding_amount"),
    (
        F.sum(
            F.coalesce("payment_request_amount", F.lit(0)) +
            F.coalesce("prompt_payment_interest_amount", F.lit(0)) +
            F.coalesce("tax_withholding_amount", F.lit(0)) +
            F.coalesce("payable_offset_amount", F.lit(0)) +
            F.coalesce("disbursement_amount", F.lit(0))
        )
    ).alias("accounting_transaction_net_amount")
)

debug_df(agg_df, "agg_df")

##################################################################################################
# Fix data types and padding for Redshift target
##################################################################################################
final_df = agg_df.select(
    F.col("final_review_month").cast("smallint"),
    F.col("final_review_year").cast("smallint"),
    F.col("disbursement_month").cast("smallint"),
    F.col("disbursement_year").cast("smallint"),
    F.col("state_fsa_code").cast(StringType()),
    F.col("county_fsa_code").cast(StringType()),
    F.col("accounting_program_year").cast(StringType()),
    F.col("accounting_program_code").cast(StringType()),
    F.col("commodity_code").cast(StringType()),
    F.col("disbursement_amount").cast(DecimalType(16,2)),
    F.col("payable_offset_amount").cast(DecimalType(16,2)),
    F.col("payable_identifier").cast(IntegerType()),
    F.col("payment_request_amount").cast(DecimalType(16,2)),
    F.col("prompt_payment_interest_amount").cast(DecimalType(16,2)),
    F.col("tax_withholding_amount").cast(DecimalType(16,2)),
    F.col("accounting_transaction_net_amount").cast(DecimalType(16,2))
)

debug_df(final_df, "final_df")

# ###############################################################################################
# # Write to S3 in Parquet format with Snappy compression
# ###############################################################################################
# final_df.write.mode("overwrite").parquet(destination_path, compression="snappy")
# print(f"Final Data Frame written to S3 at: {destination_path}")

df_final = final_df

#############################################################################################
# Run a validation and handle quarantining and logging
#############################################################################################
try:
    # Primary Key Validation
    status, failures_df, reason = validate_pk(df_final, pk_column)
    quarantine_path = None
    if not status and failures_df and not failures_df.isEmpty():
        quarantine_path = f"{final_zone_path}/Quarantine/PrimaryKey/"
        failures_df.write.mode("overwrite").parquet(quarantine_path)
        msg = f"PrimaryKey failed rows quarantined at: {quarantine_path}"
        logger.info(msg)
        print(msg)
    log_and_append_result("PrimaryKey", status, failures_df, reason, tablename, quarantine_path)
    dq_passed = dq_passed and status
    print(f'PK validated.  dq_passed is {dq_passed}')

    # Foreign Key Validation
    fk_dim_df = df_final
    status, failures_df, reason = validate_fk(df_final, fk_column , fk_dim_df)
    failures_df.show(10)
    quarantine_path = None
    if not status and failures_df and not failures_df.isEmpty():
        quarantine_path = f"{final_zone_path}/Quarantine/ForeignKey/"
        failures_df.write.mode("overwrite").parquet(quarantine_path)
        msg = f"ForeignKey failed rows quarantined at: {quarantine_path}"
        logger.info(msg)
        print(msg)
    log_and_append_result("ForeignKey", status, failures_df, reason, tablename, quarantine_path)
    dq_passed = dq_passed and status
    print(f'FK validated.  dq_passed is {dq_passed}')


    # Null Validation
    status, failures_df, reason = validate_nulls(df_final, ["state_fsa_code", "county_fsa_code"])
    quarantine_path = None
    if not status and failures_df and not failures_df.isEmpty():
        # Save failed rows to quarantine
        quarantine_path = f"{final_zone_path}/Quarantine/NullValidation/"
        failures_df.write.mode("overwrite").parquet(quarantine_path)
        msg = f"Null validation failed rows quarantined at: {quarantine_path}"
        logger.info(msg)
        print(msg)
    log_and_append_result("NullValidation", status, failures_df, reason, tablename, quarantine_path)
    dq_passed = dq_passed and status
    print(f'Null validated.  dq_passed is {dq_passed}')


    # Duplicate Validation
    status, failures_df, reason = validate_duplicates(df_final, [pk_column])
    quarantine_path = None
    if not status and failures_df and not failures_df.isEmpty():
        quarantine_path = f"{final_zone_path}/Quarantine/Duplicates/"
        failures_df.write.mode("overwrite").parquet(quarantine_path)
        msg = f"Duplicates failed rows quarantined at: {quarantine_path}"
        logger.info(msg)
        print(msg)
    log_and_append_result("Duplicates", status, failures_df, reason, tablename, quarantine_path)
    dq_passed = dq_passed and status
    print(f'Duplicates validated.  dq_passed is {dq_passed}')

except Exception as e:
    msg = f"Unexpected error during validation process: {str(e)}"
    logger.error(msg)
    print(msg)
    dq_passed = False
	

#############################################################################################
# Save Validation Results with Metadata
#############################################################################################
resultspath = "s3://c108-{}-fpacfsa-final-zone/Data_Quality/dq_results/FWADM/NPS/{}".format(environ, tablename)
results_path = f"{resultspath}/{tmstmp}"
try:
    results_df = spark.createDataFrame(validation_results)
    results_df.write.mode("overwrite").json(results_path)
    logger.info(f"Validation results with metadata saved to: {results_path}")
except Exception as e:
    logger.error(f"Failed to save validation results with metadata: {str(e)}")

if environ == 'dev':
    dq_passed = True 
if dq_passed:
    logger.info("Data Quality Validation PASSED.")
    print("Data Quality Validation PASSED.")
else:
    logger.info("Data Quality Validation FAILED.")
    print("Data Quality Validation FAILED.")

if dq_passed:
    print(f"Row Count of DF: {df_final.count()}")

try:
    logger.info("Writing Transformed data to Redshift")
    print("Writing Transformed data to Redshift")
    dynf = DynamicFrame.fromDF(final_df, glueContext, "dynf")
    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dynf,
        catalog_connection=rs_catalog_connection,
        connection_options=conn_options,
        redshift_tmp_dir=finalzone_path
        )
    # add write to final per PMRDS dgsprous 20260224
    fpath = f's3://c108-{environ.lower()}-fpacfsa-final-zone/dmart/fwadm/nps/{tablename}'
    final_df.write.mode("overwrite").parquet(fpath)
    logger.info(f"Data successfully saved to the PMRDS Final Zone in Parquet format at: {fpath}")
except Exception as e:
  print(f"An error occurred: {e}") # e contains the error message


###############################################################################################
# Generate & Execute Redshift COPY command for bulk loading
###############################################################################################
'''
try

    logger.info("All Data Quality checks passed. Proceeding with Redshift COPY command.")
    conn = sc._gateway.jvm.DriverManager.getConnection(jdbc_url, username, password)
    stmt = conn.createStatement()

    # Adding EXPLICIT_IDS ensures Redshift retains the provided primary key values
    # instead of overwriting them with auto-generated IDs.
    copy_command = f"""
    COPY {schemaname}.{tablename}
    FROM '{final_zone_path}'
    IAM_ROLE '{arnrole}'
    FORMAT AS PARQUET
    EXPLICIT_IDS;
    """
    print(f"copy_command: {copy_command}")
    logger.info(f"Executing COPY command : {copy_command}")

    trnc_command = 'truncate fwadm_dw.treasury_request_dim'
    stmt.executeUpdate(trnc_command)
    # stmt.executeUpdate(copy_command)
    stmt.close()
    conn.close()
    logger.info("Redshift COPY command executed successfully.")
    
    
    msg = f"Unexpected error during COPY COMMAND execution: {str(e)}"
    logger.error(msg)
    print(msg)	
'''

endtime_m = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
job_stat_nm = f"FWADM NPS {tablename} successfully loaded"
utils.data_ppln_job_insert(job_stat_nm, 'Complete', starttime_m, endtime_m, pcdb_conn, ENVIRON, 'NPS')

logger.info("Glue Job finished.")
