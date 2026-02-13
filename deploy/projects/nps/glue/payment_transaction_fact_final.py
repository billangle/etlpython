####################################################################################################################################
# Author : Talha Mohammed
# ALASKA NORTHSTAR Contractor for USDA
# Date : 2025-06-30
# Project : Juniper_jdf
# FSA-DEV-FWADM-payment_transaction_fact
# MDART-4140
## This Script takes PTF1, PTF2, PTF3 and combines them together and prints to RDS
### Also need to add Validation in this Step.
# dgsprous 20250822 - convert to AgCloud. DEV/CERT/PROD
# dgsprous 20260210 - refactor to support PMRDS.  writes a final zone parquet
####################################################################################################################################

import gzip
import sys
import json
import boto3
import logging
from io import StringIO
import csv
import datetime
import os
from pyspark.sql.functions import count, col, lit, coalesce, when, concat, current_timestamp, date_format, length, row_number,substring, sum as _sum, trim, max as sparkMax, min as sparkMin, monotonically_increasing_id, collect_list, size, broadcast, concat_ws
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
from pyspark.sql.types import DecimalType, ShortType, DoubleType, IntegerType, StringType, StructType, StructField, TimestampType, LongType, ArrayType
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import spark_partition_id
from pyspark.sql.functions import udf, split
import re

# Initialize Glue context, Spark context, and get arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'dbtable', 'schema_name', 'environ', 'utils'])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
sqlContext = SQLContext(spark)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
logger = glueContext.get_logger()
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED") # Allow datetimes before 1900-00-00

# Define table name, schema name, and environment variables
environ = args['environ']
ENVIRON = environ.upper()
s3_uri = args['utils']
tablename = args['dbtable']
schemaname = args['schema_name']
logger.info(f"args: {args}")
tmstmp = datetime.now().strftime("%Y%m%d%H%M")
datestmp = datetime.now().strftime("%Y%m%d")

# Block for environmental specific settings


# Environmental-specific settings
if environ == 'dev':
    rs_catalog_connection = "FSA-{}-redshift-conn".format(ENVIRON)
    arnrole = "arn:aws:iam::241533156429:role/FSA_Redshift_Role"
    secret_name = "FSA-{}-secrets".format(ENVIRON)
    jdbc_url = "jdbc:redshift://disc-fsa-dev-redshift.ckjzj4bsjear.us-east-1.redshift.amazonaws.com:8200/redshift_db"
    jdbc_url_jl = "jdbc:redshift://disc-fsa-dev-redshift.ckjzj4bsjear.us-east-1.redshift.amazonaws.com:8200/redshift_db"
    CFI_date = 20241212
elif environ == 'cert':
    rs_catalog_connection = "FSA-{}-redshift-conn".format(ENVIRON)
    arnrole = "arn:aws:iam::241533156429:role/FSA_Redshift_Role"
    secret_name = "FSA-{}-Secrets".format(ENVIRON)
    jdbc_url = "jdbc:redshift://10.219.30.52:5439/redshift_db"
    jdbc_url_jl = "jdbc:redshift://disc-fsa-cert-redshift.ckjzj4bsjear.us-east-1.redshift.amazonaws.com:5439/redshift_db_cert"
    CFI_date = 20241213
elif environ == 'prod':
    rs_catalog_connection = "FSA-{}-redshift-conn".format(ENVIRON)
    arnrole = "arn:aws:iam::253490756794:role/FSA_Redshift_Role"
    secret_name = "FSA-{}-secrets".format(ENVIRON)
    jdbc_url = "jdbc:redshift://disc-fsa-prod-redshift.co7jv5kzm7ac.us-east-1.redshift.amazonaws.com:5439/redshift_db".format(environ)
    jdbc_url_jl = "jdbc:redshift://disc-fsa-prod-redshift.co7jv5kzm7ac.us-east-1.redshift.amazonaws.com:5439/redshift_db".format(environ)
    CFI_date = 20250103

s3bucket_srcode_util = 'c108-{}-fpacfsa-landing-zone'.format(environ)
key_name = 'dmart/fwadm_utils/utils.py'
#End block for environmental specific settings

# Block to fetch Redshift credentials from Secrets Manager
secrets_manager_client = boto3.client('secretsmanager')
get_secret_value_response = secrets_manager_client.get_secret_value(SecretId=secret_name)
logger.info(f"FSA-{ENVIRON}-Secrets: {get_secret_value_response}")
secret = json.loads(get_secret_value_response['SecretString'])
username = secret['user_db_redshift']
if ENVIRON=='CERT': username=username.upper()
password = secret['pass_db_redshift']
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
    #"dbtable": "{}.{}".format(schemaname,tablename),
    "dbtable": "{}.payment_transaction_fact".format(schemaname),
    "database": "redshift_db",
    "preactions":"truncate table {}.{};".format(schemaname,tablename),
    "aws_iam_role": arnrole,
    "extracopyoptions": "EMPTYASNULL"
}


# Block to load utils from S3
s3 = boto3.client("s3")
download_path = "/tmp/utils.py"
s3.download_file(s3bucket_srcode_util, key_name, download_path)
#Add directory to system path
sys.path.insert(0, "/tmp")
import utils as utils
# End block for reading utils

# This block sets up the connection to Postgres to the process tables
glue_connection_name = "FSA-{}-PG-DART114".format(ENVIRON)
java_import(sc._gateway.jvm,"java.sql.DriverManager")
dart_process_control_db_name = "metadata_edw"
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

################################################################################
# Varibales
################################################################################
validation_results = []
dq_passed = True

# Input: Table Name, Columns, and DataFrames
pk_column = "schedule_identifier"
fk_column = "treasury_control_number"

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

        
#Code Start

#datestmp = '20250916'
#Source Paths
part1_path = 's3://c108-{}-fpacfsa-final-zone/dmart/fwadm/nps/payment_transaction_fact_part1/{}'.format(environ, datestmp)
part2_path = 's3://c108-{}-fpacfsa-final-zone/dmart/fwadm/nps/payment_transaction_fact_part2/{}'.format(environ, datestmp)
part3_path = 's3://c108-{}-fpacfsa-final-zone/dmart/fwadm/nps/payment_transaction_fact_part3/{}'.format(environ, datestmp)

# destinationpath = "s3://c108-{}-fpacfsa-cleansed-zone/dmart/fwadm/nps/{}".format(environ, tablename)
# destination_path = f"{destinationpath}"

dfs = []
# Read in PTF Part 1
try:
    print("Reading in PTF Part 1 from ", part1_path)
    part1_df = spark.read.parquet(part1_path)
    # debug_df(part1_df, "part 1")
    dfs.append(part1_df)
except Exception as e:
    print(f"Failed to Load Part1: {e}")
#PTF2
try:
    print("Reading in PTF Part 2 from ", part2_path)
    part2_df = spark.read.parquet(part2_path)
    # debug_df(part2_df, "part 2")
    dfs.append(part2_df)
except Exception as e:
    print(f"Failed to Load Part2: {e}")

#PTF3
try:
    print("Reading in PTF Part 3 from ", part3_path)
    part3_df = spark.read.parquet(part3_path)
    # debug_df(part3_df, "part 3")
    dfs.append(part3_df)
except Exception as e:
    print(f"Failed to Load Part3: {e}")
    
#unions
if dfs:
    combined_df = dfs[0]
    for df in dfs[1:]:
        combined_df = combined_df.unionByName(df, allowMissingColumns=True)
    
    print("Unions were succesfull!.")
    # print("Row Count of Combined DF: ", combined_df.count())

else:
    print("No DFs were succseffully loaded.")
    
# debug_df(combined_df, "Combined DF After Unions")

#Code to Trimcolumn lenghts for strings to match DDL
column_limits = {
    "accounting_payee_type_code": 1,
    "accounting_transaction_code": 3,
    "payable_status_code": 2,
    "state_fsa_code": 2,
    "county_fsa_code": 3,
    "accounting_program_year": 4,
    "accounting_program_code": 4,
    "accounting_reference_1_code": 2,
    "accounting_reference_1_number": 12,
    "accounting_reference_2_code": 2,
    "accounting_reference_2_number": 12,
    "accounting_reference_3_code": 2,
    "accounting_reference_3_number": 12,
    "accounting_reference_4_code": 2,
    "accounting_reference_4_number": 12,
    "accounting_reference_5_code": 2,
    "accounting_reference_5_number": 12,
    "commodity_code": 4,
    "business_party_identifier": 18,
    "tax_identification": 9,
    "tax_identification_type_code": 1,
    "program_alpha_code": 14,
    "disbursement_status_code": 2,
    "last_disbursement_status_code": 2,
    "disbursement_type_code": 1,
    "payable_completed_indicator": 1,
    "external_disbursement_identification": 8,
    "external_disbursement_type_code": 1,
    "offset_override_reason_code": 2,
    "claim_identifier": 18,
    "legacy_receivable_number": 18,
    "offset_status_code": 2,
    "obligation_full_partial_paid_indicator": 1,
    "obligation_status_code": 1,
    "core_accounting_period": 6,
    "unit_of_measure_code": 3,
    "program_category_code": 3,
    "core_program_type_code": 3,
    "system_code": 2,
    "core_organization_code": 5,
    "reversal_indicator": 1,
    "data_source_acronym": 5,
    "duns_plus4_nbr": 4,
    "duns_nbr": 9,
    "sam_unique_entity_identifier": 12,
}

for col_name, max_len in column_limits.items():
    if col_name in combined_df.columns:
        combined_df = combined_df.withColumn(
            col_name,
            substring(trim(col(col_name)), 1, max_len)
        )
        

final_df = combined_df.select(
    col("payment_transaction_type_identifier").cast(IntegerType()),
    col("accounting_customer_identifier").cast(IntegerType()),
    col("accounting_program_identifier").cast(IntegerType()),
    col("sampling_identifier").cast(IntegerType()),
    col("national_joint_payment_identifier").cast(IntegerType()),
    col("accounting_payee_type_code").cast(StringType()),
    col("accounting_service_request_date").cast(TimestampType()),
    col("accounting_transaction_date").cast(TimestampType()),
    col("accounting_transaction_code").cast(StringType()),
    col("budget_fiscal_year").cast(ShortType()),
    col("prompt_payment_interest_date").cast(TimestampType()),
    col("payable_status_code").cast(StringType()),
    col("state_fsa_code").cast(StringType()),
    col("county_fsa_code").cast(StringType()),
    col("accounting_program_year").cast(StringType()),
    col("accounting_program_code").cast(StringType()),
    col("accounting_reference_1_code").cast(StringType()),
    col("accounting_reference_1_number").cast(StringType()),
    col("accounting_reference_2_code").cast(StringType()),
    col("accounting_reference_2_number").cast(StringType()),
    col("accounting_reference_3_code").cast(StringType()),
    col("accounting_reference_3_number").cast(StringType()),
    col("accounting_reference_4_code").cast(StringType()),
    col("accounting_reference_4_number").cast(StringType()),
    col("accounting_reference_5_code").cast(StringType()),
    col("accounting_reference_5_number").cast(StringType()),
    col("commodity_code").cast(StringType()),
    col("business_party_identifier").cast(StringType()),
    col("tax_identification").cast(StringType()),
    col("tax_identification_type_code").cast(StringType()),
    col("program_alpha_code").cast(StringType()),
    col("disbursement_amount").cast(DecimalType(16,2)),
    col("disbursement_identifier").cast(IntegerType()),
    col("disbursement_date").cast(TimestampType()),
    col("disbursement_status_code").cast(StringType()),
    col("last_disbursement_status_code").cast(StringType()),
    col("disbursement_type_code").cast(StringType()),
    col("payable_completed_indicator").cast(StringType()),
    col("external_disbursement_identification").cast(StringType()),
    col("external_disbursement_type_code").cast(StringType()),
    col("payable_offset_amount").cast(DecimalType(16,2)),
    col("receivable_identifier").cast(IntegerType()),
    col("payable_identifier").cast(IntegerType()),
    col("receivable_amount").cast(DecimalType(16,2)),
    col("offset_override_reason_code").cast(StringType()),
    col("claim_identifier").cast(StringType()),
    col("final_review_date").cast(TimestampType()),
    col("payment_request_amount").cast(DecimalType(16,2)),
    col("prompt_payment_interest_amount").cast(DecimalType(16,2)),
    col("tax_withholding_amount").cast(DecimalType(16,2)),
    col("legacy_receivable_number").cast(StringType()),
    col("disbursement_confirmation_identifier").cast(IntegerType()),
    col("disbursement_status_date").cast(TimestampType()),
    col("offset_status_code").cast(StringType()),
    col("national_assignment_identifier").cast(IntegerType()),
    col("assignee_accounting_customer_identifier").cast(IntegerType()),
    col("assignment_amount").cast(DecimalType(16,2)),
    col("assignment_paid_amount").cast(DecimalType(16,2)),
    col("accounting_charge_amount").cast(DecimalType(16,2)),
    col("obligation_identifier").cast(IntegerType()),
    col("obligation_full_partial_paid_indicator").cast(StringType()),
    col("obligation_amount").cast(DecimalType(16,2)),
    col("obligation_approval_date").cast(TimestampType()),
    col("obligation_balance_amount").cast(DecimalType(16,2)),
    col("obligation_status_code").cast(StringType()),
    col("core_accounting_period").cast(StringType()),
    col("accounting_transaction_quantity").cast(DecimalType(16,2)),
    col("unit_of_measure_code").cast(StringType()),
    col("program_category_code").cast(StringType()),
    col("core_program_type_code").cast(StringType()),
    col("adjusted_obligation_identifier").cast(IntegerType()),
    col("system_code").cast(StringType()),
    col("core_organization_code").cast(StringType()),
    col("obligation_liquidation_request_identifier").cast(IntegerType()),
    col("creation_date").cast(TimestampType()),
    col("creation_datetime").cast(TimestampType()),
    col("reversal_indicator").cast(StringType()),
    col("obligation_transaction_amount").cast(DecimalType(16,2)),
    col("payment_received_date").cast(TimestampType()),
    col("payment_issue_date").cast(TimestampType()),
    col("accounting_transmission_date").cast(TimestampType()),
    col("bfy_obligation_transaction_amount").cast(DecimalType(16,2)),
    col("bfy_obligation_amount").cast(DecimalType(16,2)),
    col("bfy_obligation_balance_amount").cast(DecimalType(16,2)),
    col("fund_allotment_identifier").cast(IntegerType()),
    col("obligation_adjustment_budget_fiscal_year_identifier").cast(IntegerType()),
    col("obligation_budget_fiscal_year_identifier").cast(IntegerType()),
    col("obligation_liquidation_request_budget_fiscal_year_identifier").cast(IntegerType()),
    col("accounting_transaction_net_amount").cast(DecimalType(16,2)),
    col("total_payable_offset_amount").cast(DecimalType(16,2)),
    col("data_source_acronym").cast(StringType()),
    col("interest_penalty_amount").cast(DecimalType(16,2)),
    col("additional_interest_penalty_amount").cast(DecimalType(16,2)),
    col("duns_plus4_nbr").cast(StringType()),
    col("duns_nbr").cast(StringType()),
    col("sam_unique_entity_identifier").cast(StringType())
)

debug_df(final_df, "FINAL DF")

'''
#debug
nulls = final_df.filter(col("payment_transaction_type_identifier").isNull()).count()
print(f"Nulls in PK COlumn: {nulls}")

final_df = final_df.filter(col("payment_transaction_type_identifier").isNotNull())
print("Final DF Row Count after delete Nulls :", final_df.count())
'''
#########################################################################################################
# Add validation functions here
#########################################################################################################

# Define Final Zone
finalzone = "s3://c108-{}-fpacfsa-final-zone/dmart/fwadm/nps_qa/{}".format(environ, tablename)
finalzone_path = f"{finalzone}/{tmstmp}"

###############################################################################################
# Write to S3 in Parquet format with Snappy compression
###############################################################################################

# Write to Final Zone
#final_df.write.mode("overwrite").parquet(destination_path, compression="snappy")
#print(f"Final Data Frame written to S3 at: {destination_path}")

#############################################################################################
# Run a validation and handle quarantining and logging
#############################################################################################
try:
    # Primary Key Validation
    status, failures_df, reason = validate_pk(final_df, "payment_transaction_type_identifier")
    quarantine_path = None
    if not status and failures_df and not failures_df.isEmpty():
        quarantine_path = f"{finalzone_path}/Quarantine/PrimaryKey/"
        failures_df.write.mode("overwrite").parquet(quarantine_path)
        msg = f"PrimaryKey failed rows quarantined at: {quarantine_path}"
        logger.info(msg)
        print(msg)
    log_and_append_result("PrimaryKey", status, failures_df, reason, tablename, quarantine_path)
    dq_passed = dq_passed and status
    print(f'PK validated.  dq_passed is {dq_passed}')

    # Foreign Key Validation
    fk_dim_df = final_df
    status, failures_df, reason = validate_fk(final_df, "accounting_service_request_date", fk_dim_df)
    failures_df.show(10)
    quarantine_path = None
    if not status and failures_df and not failures_df.isEmpty():
        quarantine_path = f"{finalzone_path}/Quarantine/ForeignKey/"
        failures_df.write.mode("overwrite").parquet(quarantine_path)
        msg = f"ForeignKey failed rows quarantined at: {quarantine_path}"
        logger.info(msg)
        print(msg)
    log_and_append_result("ForeignKey", status, failures_df, reason, tablename, quarantine_path)
    dq_passed = dq_passed and status
    print(f'FK validated.  dq_passed is {dq_passed}')


    # Null Validation
    status, failures_df, reason = validate_nulls(final_df, ["business_party_identifier", "payable_identifier"])
    quarantine_path = None
    if not status and failures_df and not failures_df.isEmpty():
        # Save failed rows to quarantine
        quarantine_path = f"{finalzone_path}/Quarantine/NullValidation/"
        failures_df.write.mode("overwrite").parquet(quarantine_path)
        msg = f"Null validation failed rows quarantined at: {quarantine_path}"
        logger.info(msg)
        print(msg)
    log_and_append_result("NullValidation", status, failures_df, reason, tablename, quarantine_path)
    dq_passed = dq_passed and status
    print(f'Null validated.  dq_passed is {dq_passed}')


    # Duplicate Validation
    status, failures_df, reason = validate_duplicates(final_df, ["commodity_code"])
    quarantine_path = None
    if not status and failures_df and not failures_df.isEmpty():
        quarantine_path = f"{finalzone_path}/Quarantine/Duplicates/"
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

print(conn_options)
fpath = f's3://c108-{environ.lower()}-fpacfsa-final-zone/dmart/fwadm/nps/{tablename}'
final_df.write.mode("overwrite").parquet(fpath)

### Print to RDS (try DynamicFrame if not use COPY CMD)
logger.info("Writing Transformed data to Redshift")
print("Writing Transformed data to Redshift")
dynf = DynamicFrame.fromDF(final_df, glueContext, "dynf")
glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dynf,
        catalog_connection=rs_catalog_connection,
        connection_options=conn_options,
        redshift_tmp_dir=finalzone_path
        )
fpath = f's3://c108-{environ.lower()}-fpacfsa-final-zone/dmart/fwadm/nps/{tablename}'
final_df.write.mode("overwrite").parquet(fpath)
logger.info(f"Data successfully saved to the PMRDS Final Zone in Parquet format at: {fpath}")



'''
try:
    logger.info("Proceeding with Redshift COPY command.")
    conn = sc._gateway.jvm.DriverManager.getConnection(jdbc_url, username, password)
    stmt = conn.createStatement()
    #################################################################################
    # Generate Redshift COPY command for bulk loading
    #################################################################################
    # COPY {schemaname}.{tablename}
    copy_command = f"""
    COPY {schemaname}.{tablename}
    FROM '{finalzone_path}'
    IAM_ROLE '{arnrole}'
    FORMAT AS PARQUET;
    """
    print(f"copy_command: {copy_command}")
    logger.info(f"Executing COPY command : {copy_command}")
    trnc_command = 'truncate fwadm_dw.payment_transaction_fact'
    stmt.executeUpdate(trnc_command)
    stmt.executeUpdate(copy_command)
    stmt.close()
    conn.close()
    logger.info("Redshift COPY command executed successfully.")
    
except Exception as e:
    msg = f"Unexpected error during COPY COMMAND execution: {str(e)}"
    logger.error(msg)
    print(msg)
'''

endtime_m = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
job_stat_nm = f"FWADM NPS {tablename} successfully loaded"
utils.data_ppln_job_insert(job_stat_nm, 'Complete', starttime_m, endtime_m, pcdb_conn, ENVIRON, 'NPS')

job.commit()
