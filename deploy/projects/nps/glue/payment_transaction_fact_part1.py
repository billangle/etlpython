####################################################################################################################################
# Author : Ahmad Hanif
# ALASKA NORTHSTAR Contractor for USDAT
# Date : 2025-04-17
# Project : Juniper_jdf
# FSA-DEV-FWADM-treasury_request_fact
# MDART-4140
# History - 05292025 
#           Sajid Inayat 
#            - Added pattern-based slide/realignment logic using a custom UDF. 
#            - Realigns each row after reading from the cleansed zone by looking for the first valid decimal (column 3) and a 
#              valid timestamp (column 26). 
#            - Used Python regex to detect decimals and timestamps.
#            - Applies the slide UDF to realign arrays and correct data shifts
#            - Expands the realigned arrays back to columns. Assigns the columns to target_schema.fieldNames(), filling missing 
#              values as empty strings and trimming extras.
#            - Casts all columns to their intended data types per target_schema.
#            - Used S3 Final Zone as intended for prestine data data.
# 07/01/25 Talha: Rewrote large parts of the script and Debugged
# 20260210 DGS added material per PMRDS support.  Redirect parquets
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
from pyspark.sql.types import DecimalType, DoubleType, IntegerType, StringType, StructType, StructField, TimestampType, LongType, ArrayType
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
datestmp = datetime.utcnow().strftime("%Y%m%d")

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
    jdbc_url = "jdbc:redshift://disc-fsa-cert-redshift.ckjzj4bsjear.us-east-1.redshift.amazonaws.com:5439/redshift_db"
    jdbc_url_jl = "jdbc:redshift://disc-fsa-cert-redshift.ckjzj4bsjear.us-east-1.redshift.amazonaws.com:5439/redshift_db_cert"
    CFI_date = 20241213
elif environ == 'prod':
    rs_catalog_connection = "FSA-{}-redshift-conn".format(ENVIRON)
    secret_name = "FSA-{}-secrets".format(ENVIRON)
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
    "dbtable": "{}.{}".format(schemaname,tablename),
    "database": "redshift_db",
#    "preactions":"truncate table {}.{};".format(schemaname,tablename),
    "aws_iam_role": arnrole
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

#Code Start
# datestmp = '20250710' # Tesing Purposes

# Import the source files designate the destination path
#datestmp = '20250916'
qct07_path = "s3://c108-{}-fpacfsa-landing-zone/dmart/raw/fwadm/{}/MQCXTFIL.QCNPSS07.FULL".format(environ, datestmp)
qct75_path = "s3://c108-{}-fpacfsa-landing-zone/dmart/raw/fwadm/{}/MQCXTFIL.QCFSAS75.FULL".format(environ, datestmp)
qct01_path = "s3://c108-{}-fpacfsa-cleansed-zone/dmart/fwadm/nps/qct01pybl/{}/".format(environ, datestmp)
payable_payment_reply_path = "s3://c108-{}-fpacfsa-landing-zone/dmart/raw/fwadm/{}/sqlNPS_Payable_Payment_Reply.txt".format(environ, datestmp)
destinationpath = "s3://c108-{}-fpacfsa-cleansed-zone/dmart/fwadm/nps_qa/{}".format(environ, tablename)
destination_path = f"{destinationpath}/{tmstmp}/"

#########################################################################################################################################
# Function Name: clean_string_columns
# Purpose:
# Cleanses string-type columns in a DataFrame by removing all non-printable ASCII characters.

    # Removes all non-printable ASCII characters from string columns in the provided DataFrame.
    # :param df: Input Spark DataFrame
    # :return: Cleaned Spark DataFrame with sanitized string columns

# Expected Output:
# A DataFrame with string columns containing only printable characters, preventing issues during downstream type casting or data loading.
#########################################################################################################################################
def clean_string_columns(df):
    for f in df.schema.fields:
        if isinstance(f.dataType, StringType):
            df = df.withColumn(f.name, regexp_replace(col(f.name), '[^\x20-\x7E]', ''))
    return df


#########################################################################################################################################
# Function Name: validate_schema
# Purpose:
# Validates that each column in the DataFrame can be cast to its corresponding target data type as defined in the schema. Helps to find 
# problematic or dirty data early.

    # Validates if DataFrame columns can be cast to their respective data types as per target_schema.
    # Prints failed casts and example bad rows.
    # :param df: Input DataFrame
    # :param target_schema: StructType schema definition for validation

# Expected Output:
# Prints out columns that have casting issues, and displays a sample of problematic rows.
#########################################################################################################################################
def validate_schema(df, target_schema):
    for field in target_schema.fields:
        if field.name in df.columns:
            failed_count = df.filter(~col(field.name).cast(field.dataType).isNotNull() & col(field.name).isNotNull()).count()
            print(f"[VALIDATE] Column: {field.name} | Type: {field.dataType} | Failed Casts: {failed_count}")
            if failed_count > 0:
                bad_rows = df.filter(~col(field.name).cast(field.dataType).isNotNull() & col(field.name).isNotNull())
                bad_rows.show(5, truncate=False)

target_schema = StructType([
    StructField("payment_transaction_type_identifier", IntegerType(), True),
    StructField("accounting_customer_identifier", IntegerType(), True),
    StructField("accounting_program_identifier", IntegerType(), True),
    StructField("sampling_identifier", IntegerType(), True),
    StructField("national_joint_payment_identifier", IntegerType(), True),
    StructField("accounting_payee_type_code", StringType(), True),
    StructField("accounting_service_request_date", TimestampType(), True),
    StructField("accounting_transaction_date", TimestampType(), True),
    StructField("accounting_transaction_code", StringType(), True),
    StructField("budget_fiscal_year", IntegerType(), True),
    StructField("prompt_payment_interest_date", TimestampType(), True),
    StructField("payable_status_code", StringType(), True),
    StructField("state_fsa_code", StringType(), True),
    StructField("county_fsa_code", StringType(), True),
    StructField("accounting_program_year", StringType(), True),
    StructField("accounting_program_code", StringType(), True),
    StructField("accounting_reference_1_code", StringType(), True),
    StructField("accounting_reference_1_number", StringType(), True),
    StructField("accounting_reference_2_code", StringType(), True),
    StructField("accounting_reference_2_number", StringType(), True),
    StructField("accounting_reference_3_code", StringType(), True),
    StructField("accounting_reference_3_number", StringType(), True),
    StructField("accounting_reference_4_code", StringType(), True),
    StructField("accounting_reference_4_number", StringType(), True),
    StructField("accounting_reference_5_code", StringType(), True),
    StructField("accounting_reference_5_number", StringType(), True),
    StructField("commodity_code", StringType(), True),
    StructField("business_party_identifier", StringType(), True),
    StructField("tax_identification", StringType(), True),
    StructField("tax_identification_type_code", StringType(), True),
    StructField("program_alpha_code", StringType(), True),
    StructField("disbursement_amount", DecimalType(16,2), True),
    StructField("disbursement_identifier", IntegerType(), True),
    StructField("disbursement_date", TimestampType(), True),
    StructField("disbursement_status_code", StringType(), True),
    StructField("last_disbursement_status_code", StringType(), True),
    StructField("disbursement_type_code", StringType(), True),
    StructField("payable_completed_indicator", StringType(), True),
    StructField("external_disbursement_identification", StringType(), True),
    StructField("external_disbursement_type_code", StringType(), True),
    StructField("payable_offset_amount", DecimalType(16,2), True),
    StructField("receivable_identifier", IntegerType(), True),
    StructField("payable_identifier", IntegerType(), True),
    StructField("receivable_amount", DecimalType(16,2), True),
    StructField("offset_override_reason_code", StringType(), True),
    StructField("claim_identifier", StringType(), True),
    StructField("final_review_date", TimestampType(), True),
    StructField("payment_request_amount", DecimalType(16,2), True),
    StructField("prompt_payment_interest_amount", DecimalType(16,2), True),
    StructField("tax_withholding_amount", DecimalType(16,2), True),
    StructField("legacy_receivable_number", StringType(), True),
    StructField("disbursement_confirmation_identifier", IntegerType(), True),
    StructField("disbursement_status_date", TimestampType(), True),
    StructField("offset_status_code", StringType(), True),
    StructField("national_assignment_identifier", IntegerType(), True),
    StructField("assignee_accounting_customer_identifier", IntegerType(), True),
    StructField("assignment_amount", DecimalType(16,2), True),
    StructField("assignment_paid_amount", DecimalType(16,2), True),
    StructField("accounting_charge_amount", DecimalType(16,2), True),
    StructField("obligation_identifier", IntegerType(), True),
    StructField("obligation_full_partial_paid_indicator", StringType(), True),
    StructField("obligation_amount", DecimalType(16,2), True),
    StructField("obligation_approval_date", TimestampType(), True),
    StructField("obligation_balance_amount", DecimalType(16,2), True),
    StructField("obligation_status_code", StringType(), True),
    StructField("core_accounting_period", StringType(), True),
    StructField("accounting_transaction_quantity", DecimalType(16,2), True),
    StructField("unit_of_measure_code", StringType(), True),
    StructField("program_category_code", StringType(), True),
    StructField("core_program_type_code", StringType(), True),
    StructField("adjusted_obligation_identifier", IntegerType(), True),
    StructField("system_code", StringType(), True),
    StructField("core_organization_code", StringType(), True),
    StructField("obligation_liquidation_request_identifier", IntegerType(), True),
    StructField("creation_datetime", TimestampType(), True),
    StructField("creation_date", TimestampType(), True),
    StructField("reversal_indicator", StringType(), True),
    StructField("obligation_transaction_amount", DecimalType(16,2), True),
    StructField("payment_received_date", TimestampType(), True),
    StructField("payment_issue_date", TimestampType(), True),
    StructField("accounting_transmission_date", TimestampType(), True),
    StructField("bfy_obligation_transaction_amount", DecimalType(16,2), True),
    StructField("bfy_obligation_amount", DecimalType(16,2), True),
    StructField("bfy_obligation_balance_amount", DecimalType(16,2), True),
    StructField("fund_allotment_identifier", IntegerType(), True),
    StructField("obligation_budget_fiscal_year_identifier", IntegerType(), True),
    StructField("obligation_adjustment_budget_fiscal_year_identifier", IntegerType(), True),
    StructField("obligation_liquidation_request_budget_fiscal_year_identifier", IntegerType(), True),
    StructField("accounting_transaction_net_amount", DecimalType(16,2), True),
    StructField("total_payable_offset_amount", DecimalType(16,2), True),
    StructField("data_source_acronym", StringType(), True),
    StructField("interest_penalty_amount", DecimalType(16,2), True),
    StructField("additional_interest_penalty_amount", DecimalType(16,2), True),
    StructField("duns_plus4_nbr", StringType(), True),
    StructField("duns_nbr", StringType(), True),
    StructField("sam_unique_entity_identifier", StringType(), True)
])

qct75_schema = StructType([
    StructField("acct_ref_id",StringType(), False),
    StructField("acct_ref_cd",StringType(), False),
    StructField("acct_ref_desc",StringType(), False),
    StructField("last_chg_dt",StringType(), False),
    StructField("last_chg_user_nm",StringType(), False)
    ])
    
qct75_df = spark.read.csv(qct75_path, header=False, schema=qct75_schema, sep="\t")
qct75_df = qct75_df.withColumn("acct_ref_id", qct75_df["acct_ref_id"].cast(IntegerType()))
qct75_df = clean_string_columns(qct75_df)
debug_df(qct75_df, "qct75_df - After Initial Read")

qct07_schema = StructType([
    StructField("pybl_ref_id",StringType(), False),
    StructField("pybl_id",StringType(), False),
    StructField("acct_ref_id",StringType(), False),
    StructField("acct_ref_nbr",StringType(), False),
    StructField("last_chg_dt",StringType(), False),
    StructField("last_chg_user_nm",StringType(), False),
    StructField("st_fsa_cd",StringType(), False),
    StructField("cnty_fsa_cd",StringType(), False),
    StructField("data_invalid_indicator",StringType(), False)
    ])
    
qct07_df = spark.read.csv(qct07_path, header=False, schema=qct07_schema, sep="\t")
qct07_df = qct07_df \
    .withColumn("pybl_ref_id", qct07_df["pybl_ref_id"].cast(IntegerType())) \
    .withColumn("pybl_id", qct07_df["pybl_id"].cast(IntegerType())) \
    .withColumn("acct_ref_id", qct07_df["acct_ref_id"].cast(IntegerType())) \
    # .withColumn("data_invalid_indicator", lit("N"))
qct07_df = clean_string_columns(qct07_df)
debug_df(qct07_df, "qct07_df - After Initial Read")

qct01_schema = StructType([
    StructField("pybl_id",IntegerType(), False),
    StructField("acct_cust_id",IntegerType(), False),
    StructField("acct_pgm_id",IntegerType(), False),
    StructField("sys_cd",StringType(), False),
    StructField("smpl_id",IntegerType(), False),
    StructField("acct_svc_rqst_dt",StringType(), False),
    StructField("acct_txn_cd",StringType(), False),
    StructField("acct_txn_dt",TimestampType(), False),
    StructField("bia_rqst_ind",StringType(), False),
    StructField("bdgt_fscl_yr",IntegerType(), False),
    StructField("pymt_cmdy_qty",DecimalType(16,2), False),
    StructField("cmn_cust_nm",StringType(), False),
    StructField("grp_pybl_nbr",IntegerType(), False),
    StructField("nres_aln_ind",StringType(), False),
    StructField("prmpt_pymt_int_dt",TimestampType(), False),
    StructField("pybl_stat_cd",StringType(), False),
    StructField("pymt_rqst_amt",DecimalType(16,2), False),
    StructField("stmt_gnrt_dt",TimestampType(), False),
    StructField("txn_rqst_id",StringType(), False),
    StructField("adtl_sel_item_ind",StringType(), False),
    StructField("ck_out_dt",StringType(), False),
    StructField("ck_out_user_nm",StringType(), False),
    StructField("dspt_lcl_ind",StringType(), False),
    StructField("dspt_ntl_ind",StringType(), False),
    StructField("fnl_rvw_dt",StringType(), False),
    StructField("fnl_rvw_lvl_cd",StringType(), False),
    StructField("fnl_rvw_user_nm",StringType(), False),
    StructField("init_rvw_dt",StringType(), False),
    StructField("init_rvw_user_nm",StringType(), False),
    StructField("rndm_sel_item_ind",StringType(), False),
    StructField("smpl_bat_item_ind",StringType(), False),
    StructField("smpl_insp_fail_ind",StringType(), False),
    StructField("last_chg_dt",StringType(), False),
    StructField("last_chg_user_nm",StringType(), False),
    StructField("st_fsa_cd",StringType(), False),
    StructField("cnty_fsa_cd",StringType(), False),
    StructField("acct_pgm_yr",StringType(), False),
    StructField("prim_acct_ref_cd",StringType(), False),
    StructField("prim_acct_ref_nbr",StringType(), False),
    StructField("scnd_acct_ref_cd",StringType(), False),
    StructField("scnd_acct_ref_nbr",StringType(), False),
    StructField("adtl_acct_ref_ind",StringType(), False),
    StructField("acct_pgm_cd",StringType(), False),
    StructField("cmdy_cd",StringType(), False),
    StructField("bus_pty_id",StringType(), False),
    StructField("tax_id",StringType(), False),
    StructField("tax_id_type_cd",StringType(), False),
    StructField("late_pymt_day_ct",IntegerType(), False),
    StructField("prmpt_pymt_int_amt",DecimalType(16,2), False),
    StructField("prmpt_pymt_int_rt",DecimalType(7,4), False),
    StructField("prmpt_pymt_rsn_cd",StringType(), False),
    StructField("tax_whld_amt",DecimalType(16,2), False),
    StructField("tax_whld_pct",DecimalType(7,4), False),
    StructField("tax_whld_type_cd",StringType(), False),
    StructField("pymt_proc_ind",StringType(), False),
    StructField("cnfrm_proc_ind",StringType(), False),
    StructField("pgm_alp_cd",StringType(), False),
    StructField("pymt_ofst_alow_ind",StringType(), False),
    StructField("obl_id",IntegerType(), False),
    StructField("obl_pymt_ind",StringType(), False),
    StructField("disb_ck_frc_ind",StringType(), False),
    StructField("alt_pye_pmsn_ind",StringType(), False),
    StructField("pymt_disc_term_des",StringType(), False),
    StructField("data_src_acro",StringType(), False),
    StructField("pymt_isu_dt",TimestampType(), False),
    StructField("pymt_rcv_dt",StringType(), False),
    StructField("int_due_prn_amt",DecimalType(16,2), False),
    StructField("vol_whld_pct",DecimalType(5,3), False),
    StructField("invol_whld_pct",DecimalType(5,3), False),
    StructField("form_1099_ind",StringType(), False),
    StructField("dir_atrb_grp_nbr",StringType(), False),
    StructField("dth_pymt_rsn_cd",StringType(), False),
    StructField("txn_rqst_pkg_id",IntegerType(), False),
    StructField("gr_loan_shr_amt",DecimalType(16,2), False),
    StructField("cncl_init_user_id",StringType(), False),
    StructField("cncl_init_dt",StringType(), False),
    StructField("cncl_fnl_user_id",StringType(), False),
    StructField("cncl_fnl_dt",StringType(), False),
    StructField("cncl_rsn_desc",StringType(), False),
    StructField("data_invalid_indicator",StringType(), False)
    ])
    
# qct01_df = spark.read.csv(qct01_path, header=False, schema=qct01_schema, sep="\t")
qct01_df = spark.read.parquet(qct01_path, ignoreLeadingWhiteSpace=True)
qct01_df = clean_string_columns(qct01_df)
debug_df(qct01_df, "qct01_df - After Initial Read")

payable_payment_reply_schema = StructType([
    StructField("pybl_id",IntegerType(), False),
    StructField("acct_cust_id",IntegerType(), False),
    StructField("acct_pgm_id",IntegerType(), False),
    StructField("smpl_id",IntegerType(), False),
    StructField("acct_svc_rqst_dt",TimestampType(), False),
    StructField("acct_txn_cd",StringType(), False),
    StructField("acct_txn_dt",TimestampType(), False),
    StructField("bia_rqst_ind",StringType(), False),
    StructField("bdgt_fscl_yr",IntegerType(), False),
    StructField("pymt_cmdy_qty",DecimalType(16,2), False),
    StructField("cmn_cust_nm",StringType(), False),
    StructField("grp_pybl_nbr",IntegerType(), False),
    StructField("nres_aln_ind",StringType(), False),
    StructField("prmpt_pymt_int_dt",TimestampType(), False),
    StructField("pymt_rqst_amt",DecimalType(16,2), False),
    StructField("stmt_gnrt_dt",TimestampType(), False),
    StructField("adtl_sel_item_ind",StringType(), False),
    StructField("ck_out_dt",TimestampType(), False),
    StructField("ck_out_user_nm",StringType(), False),
    StructField("dspt_lcl_ind",StringType(), False),
    StructField("dspt_ntl_ind",StringType(), False),
    StructField("fnl_rvw_dt",TimestampType(), False),
    StructField("fnl_rvw_lvl_cd",StringType(), False),
    StructField("fnl_rvw_user_nm",StringType(), False),
    StructField("init_rvw_dt",TimestampType(), False),
    StructField("init_rvw_user_nm",StringType(), False),
    StructField("rndm_sel_item_ind",StringType(), False),
    StructField("smpl_bat_item_ind",StringType(), False),
    StructField("smpl_insp_fail_ind",StringType(), False),
    StructField("prim_acct_ref_cd",StringType(), False),
    StructField("prim_acct_ref_nbr",StringType(), False),
    StructField("scnd_acct_ref_cd",StringType(), False),
    StructField("scnd_acct_ref_nbr",StringType(), False),
    StructField("adtl_acct_ref_ind",StringType(), False),
    StructField("acct_pgm_cd",StringType(), False),
    StructField("cmdy_cd",StringType(), False),
    StructField("bus_pty_id",StringType(), False),
    StructField("tax_id",StringType(), False),
    StructField("tax_id_type_cd",StringType(), False),
    StructField("late_pymt_day_ct",IntegerType(), False),
    StructField("prmpt_pymt_int_amt",DecimalType(16,2), False),
    StructField("prmpt_pymt_int_rt",DecimalType(7,4), False),
    StructField("prmpt_pymt_rsn_cd",StringType(), False),
    StructField("tax_whld_amt",DecimalType(16,2), False),
    StructField("tax_whld_pct",DecimalType(7,4), False),
    StructField("tax_whld_type_cd",StringType(), False),
    StructField("pgm_alp_cd",StringType(), False),
    StructField("pymt_ofst_alow_ind",StringType(), False),
    StructField("obl_id",IntegerType(), False),
    StructField("obl_pymt_ind",StringType(), False),
    StructField("disb_ck_frc_ind",StringType(), False),
    StructField("alt_pye_pmsn_ind",StringType(), False),
    StructField("pymt_disc_term_des",StringType(), False),
    StructField("data_src_acro",StringType(), False),
    StructField("pymt_isu_dt",TimestampType(), False),
    StructField("pybl_data_stat_cd",StringType(), False),
    StructField("pybl_cre_dt",TimestampType(), False),
    StructField("pybl_cre_user_nm",StringType(), False),
    StructField("last_chg_dt",TimestampType(), False),
    StructField("last_chg_user_nm",StringType(), False),
    StructField("cnfrm_proc_ind",StringType(), False),
    StructField("txn_rqst_id",StringType(), False),
    StructField("pybl_stat_cd",StringType(), False),
    StructField("sys_cd",StringType(), False),
    StructField("acct_pgm_yr",StringType(), False),
    StructField("pymt_proc_ind",StringType(), False),
    StructField("pymt_data_stat_cd",StringType(), False),
    StructField("pymt_cre_dt",TimestampType(), False),
    StructField("pymt_cre_user_nm",StringType(), False),
    StructField("last_upd_dt",TimestampType(), False),
    StructField("last_upd_user_nm",StringType(), False),
    StructField("data_migr_stat_cd",StringType(), False),
    StructField("st_fsa_cd",StringType(), False),
    StructField("cnty_fsa_cd",StringType(), False),
    StructField("migr_stat_chg_dt",TimestampType(), False),
    StructField("pymt_rcv_dt",TimestampType(), False),
    StructField("int_due_prn_amt",DecimalType(16,2), False),
    StructField("vol_whld_pct",DecimalType(5,3), False),
    StructField("invol_whld_pct",DecimalType(5,3), False),
    StructField("form_1099_ind",StringType(), False),
    StructField("dir_atrb_grp_nbr",StringType(), False),
    StructField("dth_pymt_rsn_cd",StringType(), False),
    StructField("txn_rqst_pkg_id",IntegerType(), False),
    StructField("gr_loan_shr_amt",DecimalType(16,2), False),
    StructField("cncl_init_user_id",StringType(), False),
    StructField("cncl_init_dt",TimestampType(), False),
    StructField("cncl_fnl_user_id",StringType(), False),
    StructField("cncl_fnl_dt",TimestampType(), False),
    StructField("cncl_rsn_desc",StringType(), False)
    ])
    
payable_payment_reply_df = spark.read.csv(payable_payment_reply_path, header=False, schema=payable_payment_reply_schema, sep="\t")
payable_payment_reply_df = clean_string_columns(payable_payment_reply_df)
debug_df(payable_payment_reply_df, "payable_payment_reply_df - After Initial Read")

window = Window.partitionBy("p1.pybl_id").orderBy("acct_ref_nbr")

t1 = qct07_df.alias("p1").join(
    qct75_df.alias("p2"),
    col("p1.acct_ref_id") == col("p2.acct_ref_id"),
    "inner"
).withColumn(
    "id", row_number().over(window)
).select(
    "p1.pybl_ref_id",
    "acct_ref_nbr",
    "acct_ref_cd",
    "pybl_id",
    lit(1).alias("order_num"),
    "id"
)
# debug_df(t1, "t1 - After Join and Row Number")


t2 = t1.groupby("pybl_id").agg(count("*").alias("cnt"))
# debug_df(t2, "t2 - After GroupBy/Count")


t3 = t1.join(t2.filter("cnt > 2"), "pybl_id") \
    .groupby("pybl_id") \
    .agg(sparkMax("id").alias("max_id"))
# debug_df(t3, "t3 - After Join/GroupBy/Max")

t4 = t1.groupBy("pybl_id").agg(sparkMin("id").alias("min_id"))
debug_df(t4, "t4 - After GroupBy/Min")

# t5
t5 = t1.join(t2.filter("cnt > 1"), "pybl_id") \
    .join(t3.withColumnRenamed("max_id", "id"), ["id", "pybl_id"], "left_anti") \
    .join(t4.withColumnRenamed("min_id", "id"), ["id", "pybl_id"], "left_anti") \
    .orderBy("pybl_id", "id") \
    .select(
    col("id"),
    col("pybl_id")
    )
# debug_df(t5, "t5 - After Join Left Anti")

t6 = (
    t2.alias("t2")
    .join(t4.alias("t4"), col("t2.pybl_id") == col("t4.pybl_id"), "inner")
    .join(t1.alias("min_val"),
          (col("t2.pybl_id") == col("min_val.pybl_id")) &
          (col("min_val.id") == col("t4.min_id")),
          "inner")
    .join(t3.alias("t3"), col("t2.pybl_id") == col("t3.pybl_id"), "left")
    .join(t1.alias("max_val"),
          (col("t3.pybl_id") == col("max_val.pybl_id")) &
          (col("max_val.id") == col("t3.max_id")),
          "left")
    .join(t5.alias("t5"), col("t2.pybl_id") == col("t5.pybl_id"), "left")
    .join(t1.alias("avg_val"),
          (col("t5.pybl_id") == col("avg_val.pybl_id")) &
          (col("avg_val.id") == col("t5.id")),
          "left")
    .select(
        col("t2.pybl_id"),
        col("t2.cnt"),
        col("t3.max_id"),
        col("t4.min_id"),
        col("min_val.acct_ref_nbr").alias("accounting_reference_3_number"),
        col("min_val.acct_ref_cd").alias("accounting_reference_3_code"),
        col("max_val.acct_ref_nbr").alias("accounting_reference_5_number"),
        col("max_val.acct_ref_cd").alias("accounting_reference_5_code"),
        col("avg_val.acct_ref_nbr").alias("accounting_reference_4_number"),
        col("avg_val.acct_ref_cd").alias("accounting_reference_4_code")
    )
)
# debug_df(t6, "t6 - After Multiple Joins (Min/Max/Avg)")

### Payable Table 01 - PART 1 (DB2)
payable_table_01 = (
    qct01_df.alias("q01")
    .join(t6.alias("t6"), col("t6.pybl_id") == col("q01.pybl_id"), how="left")
    .join(payable_payment_reply_df.alias("payable"), col("q01.pybl_id") == col("payable.pybl_id"), how="left_anti")
    .select(
    concat_ws("", col("q01.st_fsa_cd"), col("q01.cnty_fsa_cd")).alias("core_organization_code"),
    lit("01").alias("payment_transaction_type_identifier"),
    col("q01.acct_cust_id").alias("accounting_customer_identifier"),
    col("q01.acct_pgm_cd").alias("accounting_program_code"),
    col("q01.acct_pgm_id").alias("accounting_program_identifier"),
    col("q01.acct_pgm_yr").alias("accounting_program_year"),
    col("q01.prim_acct_ref_cd").alias("accounting_reference_1_code"),
    col("q01.prim_acct_ref_nbr").alias("accounting_reference_1_number"),
    col("q01.scnd_acct_ref_cd").alias("accounting_reference_2_code"),
    col("q01.scnd_acct_ref_nbr").alias("accounting_reference_2_number"),
    col("t6.accounting_reference_3_code"),
    col("t6.accounting_reference_3_number"),
    col("t6.accounting_reference_4_code"),
    col("t6.accounting_reference_4_number"),
    col("t6.accounting_reference_5_code"),
    col("t6.accounting_reference_5_number"),
    col("q01.acct_svc_rqst_dt").alias("accounting_service_request_date"),
    col("q01.acct_txn_cd").alias("accounting_transaction_code"),
    col("q01.acct_txn_dt").alias("accounting_transaction_date"),
    col("q01.pymt_cmdy_qty").alias("accounting_transaction_quantity"),
    col("q01.bdgt_fscl_yr").alias("budget_fiscal_year"),
    col("q01.cmdy_cd").alias("commodity_code"),
    col("q01.bus_pty_id").alias("business_party_identifier"),
    col("q01.cnty_fsa_cd").alias("county_fsa_code"),
    col("q01.fnl_rvw_dt").alias("final_review_date"),
    col("q01.obl_pymt_ind").alias("obligation_full_partial_paid_indicator"),
    col("q01.obl_id").alias("obligation_identifier"),
    col("q01.pybl_id").alias("payable_identifier"),
    col("q01.pybl_stat_cd").alias("payable_status_code"),
    coalesce(
    	when(col("q01.pybl_stat_cd") == "CU", col("q01.pymt_rqst_amt") * -1) \
    	.otherwise(col("q01.pymt_rqst_amt")),
    	lit(0)
    ).alias("payment_request_amount"),
    col("q01.pgm_alp_cd").alias("program_alpha_code"),
    when(col("acct_txn_cd").isin("197", "198"), lit(0))
        .when(col("pybl_stat_cd") == "CU", coalesce(col("prmpt_pymt_int_amt"), lit(0)) * -1)
        .otherwise(coalesce(col("prmpt_pymt_int_amt"), lit(0))).alias("prompt_payment_interest_amount"),
    col("q01.prmpt_pymt_int_dt").alias("prompt_payment_interest_date"),
    col("q01.smpl_id").alias("sampling_identifier"),
    col("q01.st_fsa_cd").alias("state_fsa_code"),
    col("q01.sys_cd").alias("system_code"),
    col("q01.tax_id").alias("tax_identification"),
    col("q01.tax_id_type_cd").alias("tax_identification_type_code"),
    coalesce(col("q01.tax_whld_amt") * -1, lit(0)).alias("tax_withholding_amount"),
    lit(0.00).cast(DecimalType(16,2)).alias("disbursement_amount"),
    lit(0).alias("payable_offset_amount"),
    lit(0).alias("receivable_amount"),
    lit(0).alias("assignment_paid_amount"),
    lit(0).alias("assignment_amount"),
    lit(0).alias("obligation_balance_amount"),
    lit(0).alias("obligation_amount"),
    lit(0).alias("obligation_transaction_amount"),
    lit(0).alias("accounting_charge_amount"),
    col("q01.pymt_rcv_dt").alias("payment_received_date"),
    col("q01.pymt_isu_dt").alias("payment_issue_date"),
    lit(0).alias("fund_allotment_identifier"),
    lit(0).alias("bfy_obligation_transaction_amount"),
    lit(0).alias("bfy_obligation_amount"),
    lit(0).alias("bfy_obligation_balance_amount"),
    col("q01.data_src_acro").alias("data_source_acronym"),
    when(col("q01.acct_txn_cd") != "198", lit(0.00))
        .when(col("q01.pybl_stat_cd") == "CU", coalesce(col("q01.prmpt_pymt_int_amt"), lit(0)) * -1)
        .otherwise(coalesce(col("q01.prmpt_pymt_int_amt"), lit(0))).alias("interest_penalty_amount"),
    when(col("q01.acct_txn_cd") != "197", lit(0.00))
         .when(col("q01.pybl_stat_cd") == "CU", coalesce(col("q01.prmpt_pymt_int_amt"), lit(0)) * -1)
         .otherwise(coalesce(col("q01.prmpt_pymt_int_amt"), lit(0))).alias("additional_interest_penalty_amount")
))
# debug_df(payable_table_01, "First 01 Payable Table")

### Payable Table 01 - PART 2 - CU LOAD (DB2)
payable_table_02 = (
    qct01_df.alias("q01").filter(col("pybl_stat_cd") == "CU")
    .join(payable_payment_reply_df.alias("payable"), col("q01.pybl_id") == col("payable.pybl_id"), how="left_anti")
    .select(
    concat_ws("", col("q01.st_fsa_cd"), col("q01.cnty_fsa_cd")).alias("core_organization_code"),
    lit("01").alias("payment_transaction_type_identifier"),
    col("q01.acct_cust_id").alias("accounting_customer_identifier"),
    col("q01.acct_pgm_cd").alias("accounting_program_code"),
    col("q01.acct_pgm_id").alias("accounting_program_identifier"),
    col("q01.acct_pgm_yr").alias("accounting_program_year"),
    col("q01.prim_acct_ref_cd").alias("accounting_reference_1_code"),
    col("q01.prim_acct_ref_nbr").alias("accounting_reference_1_number"),
    col("q01.scnd_acct_ref_cd").alias("accounting_reference_2_code"),
    col("q01.scnd_acct_ref_nbr").alias("accounting_reference_2_number"),
    col("q01.acct_svc_rqst_dt").alias("accounting_service_request_date"),
    lit("").alias("accounting_transaction_code"),
    col("q01.acct_txn_dt").alias("accounting_transaction_date"),
    col("q01.pymt_cmdy_qty").alias("accounting_transaction_quantity"),
    col("q01.bdgt_fscl_yr").alias("budget_fiscal_year"),
    col("q01.cmdy_cd").alias("commodity_code"),
    col("q01.bus_pty_id").alias("business_party_identifier"),
    col("q01.cnty_fsa_cd").alias("county_fsa_code"),
    col("q01.fnl_rvw_dt").alias("final_review_date"),
    col("q01.obl_pymt_ind").alias("obligation_full_partial_paid_indicator"),
    col("q01.obl_id").alias("obligation_identifier"),
    col("q01.pybl_id").alias("payable_identifier"),
    lit("C3").alias("payable_status_code"),
    col("q01.pymt_rqst_amt").alias("payment_request_amount"),
    col("q01.pgm_alp_cd").alias("program_alpha_code"),
    when(col("acct_txn_cd").isin("197", "198"), lit(0))
        .when(col("pybl_stat_cd") == "CU", coalesce(col("prmpt_pymt_int_amt"), lit(0)) * -1)
        .otherwise(coalesce(col("prmpt_pymt_int_amt"), lit(0))).alias("prompt_payment_interest_amount"),
    col("q01.prmpt_pymt_int_dt").alias("prompt_payment_interest_date"),
    col("q01.smpl_id").alias("sampling_identifier"),
    col("q01.st_fsa_cd").alias("state_fsa_code"),
    col("q01.sys_cd").alias("system_code"),
    col("q01.tax_id").alias("tax_identification"),
    col("q01.tax_id_type_cd").alias("tax_identification_type_code"),
    coalesce(col("q01.tax_whld_amt") * -1, lit(0)).alias("tax_withholding_amount"),
    lit(0.00).cast(DecimalType(16,2)).alias("disbursement_amount"),
    lit(0).alias("payable_offset_amount"),
    lit(0).alias("receivable_amount"),
    lit(0).alias("assignment_paid_amount"),
    lit(0).alias("assignment_amount"),
    lit(0).alias("obligation_transaction_amount"),
    lit(0).alias("obligation_balance_amount"),
    col("q01.pymt_rcv_dt").alias("payment_received_date"),
    col("q01.pymt_isu_dt").alias("payment_issue_date"),
    lit(0).alias("fund_allotment_identifier"),
    lit(0).alias("bfy_obligation_transaction_amount"),
    lit(0).alias("bfy_obligation_amount"),
    lit(0).alias("bfy_obligation_balance_amount"),
    col("q01.data_src_acro").alias("data_source_acronym"),
    when(col("q01.acct_txn_cd") != "198", lit(0.00))
        .when(col("q01.pybl_stat_cd") == "CU", coalesce(col("q01.prmpt_pymt_int_amt"), lit(0)) * -1)
        .otherwise(coalesce(col("q01.prmpt_pymt_int_amt"), lit(0))).alias("interest_penalty_amount"),
    when(col("q01.acct_txn_cd") != "197", lit(0.00))
         .when(col("q01.pybl_stat_cd") == "CU", coalesce(col("q01.prmpt_pymt_int_amt"), lit(0)) * -1)
         .otherwise(coalesce(col("q01.prmpt_pymt_int_amt"), lit(0))).alias("additional_interest_penalty_amount")
))
# debug_df(payable_table_02, "Second 01 Payable Table")

### Payable Table 01 - PART 3 (SQL)
payable_table_03 = (
    payable_payment_reply_df.alias("q01")
    .join(t6.alias("t6"), col("t6.pybl_id") == col("q01.pybl_id"), how="left")
    .select(
    concat_ws("", col("q01.st_fsa_cd"), col("q01.cnty_fsa_cd")).alias("core_organization_code"),
    lit("01").alias("payment_transaction_type_identifier"),
    col("q01.acct_cust_id").alias("accounting_customer_identifier"),
    col("q01.acct_pgm_cd").alias("accounting_program_code"),
    col("q01.acct_pgm_id").alias("accounting_program_identifier"),
    col("q01.acct_pgm_yr").alias("accounting_program_year"),
    col("q01.prim_acct_ref_cd").alias("accounting_reference_1_code"),
    col("q01.prim_acct_ref_nbr").alias("accounting_reference_1_number"),
    col("q01.scnd_acct_ref_cd").alias("accounting_reference_2_code"),
    col("q01.scnd_acct_ref_nbr").alias("accounting_reference_2_number"),
    col("t6.accounting_reference_3_code"),
    col("t6.accounting_reference_3_number"),
    col("t6.accounting_reference_4_code"),
    col("t6.accounting_reference_4_number"),
    col("t6.accounting_reference_5_code"),
    col("t6.accounting_reference_5_number"),
    col("q01.acct_svc_rqst_dt").alias("accounting_service_request_date"),
    col("q01.acct_txn_cd").alias("accounting_transaction_code"),
    col("q01.acct_txn_dt").alias("accounting_transaction_date"),
    col("q01.pymt_cmdy_qty").alias("accounting_transaction_quantity"),
    col("q01.bdgt_fscl_yr").alias("budget_fiscal_year"),
    col("q01.cmdy_cd").alias("commodity_code"),
    col("q01.bus_pty_id").alias("business_party_identifier"),
    col("q01.cnty_fsa_cd").alias("county_fsa_code"),
    col("q01.fnl_rvw_dt").alias("final_review_date"),
    col("q01.obl_pymt_ind").alias("obligation_full_partial_paid_indicator"),
    col("q01.obl_id").alias("obligation_identifier"),
    col("q01.pybl_id").alias("payable_identifier"),
    col("q01.pybl_stat_cd").alias("payable_status_code"),
    coalesce(
    	when(col("q01.pybl_stat_cd") == "CU", col("q01.pymt_rqst_amt") * -1) \
    	.otherwise(col("q01.pymt_rqst_amt")),
    	lit(0)
    ).alias("payment_request_amount"),
    col("q01.pgm_alp_cd").alias("program_alpha_code"),
    when(col("acct_txn_cd").isin("197", "198"), lit(0))
        .when(col("pybl_stat_cd") == "CU", coalesce(col("prmpt_pymt_int_amt"), lit(0)) * -1)
        .otherwise(coalesce(col("prmpt_pymt_int_amt"), lit(0))).alias("prompt_payment_interest_amount"),
    col("q01.prmpt_pymt_int_dt").alias("prompt_payment_interest_date"),
    col("q01.smpl_id").alias("sampling_identifier"),
    col("q01.st_fsa_cd").alias("state_fsa_code"),
    col("q01.sys_cd").alias("system_code"),
    col("q01.tax_id").alias("tax_identification"),
    col("q01.tax_id_type_cd").alias("tax_identification_type_code"),
    coalesce(col("q01.tax_whld_amt") * -1, lit(0)).alias("tax_withholding_amount"),
    lit(0.00).cast(DecimalType(16,2)).alias("disbursement_amount"),
    lit(0).alias("payable_offset_amount"),
    lit(0).alias("receivable_amount"),
    lit(0).alias("assignment_paid_amount"),
    lit(0).alias("assignment_amount"),
    lit(0).alias("obligation_balance_amount"),
    lit(0).alias("obligation_amount"),
    lit(0).alias("obligation_transaction_amount"),
    lit(0).alias("accounting_charge_amount"),
    col("q01.pymt_rcv_dt").alias("payment_received_date"),
    col("q01.pymt_isu_dt").alias("payment_issue_date"),
    lit(0).alias("fund_allotment_identifier"),
    lit(0).alias("bfy_obligation_transaction_amount"),
    lit(0).alias("bfy_obligation_amount"),
    lit(0).alias("bfy_obligation_balance_amount"),
    col("q01.data_src_acro").alias("data_source_acronym"),
    when(col("q01.acct_txn_cd") != "198", lit(0.00))
        .when(col("q01.pybl_stat_cd") == "CU", coalesce(col("q01.prmpt_pymt_int_amt"), lit(0)) * -1)
        .otherwise(coalesce(col("q01.prmpt_pymt_int_amt"), lit(0))).alias("interest_penalty_amount"),
    when(col("q01.acct_txn_cd") != "197", lit(0.00))
         .when(col("q01.pybl_stat_cd") == "CU", coalesce(col("q01.prmpt_pymt_int_amt"), lit(0)) * -1)
         .otherwise(coalesce(col("q01.prmpt_pymt_int_amt"), lit(0))).alias("additional_interest_penalty_amount")
))
# debug_df(payable_table_03, "Third 01 Payable Table")


### Payable Table 01 - PART 2 - CU LOAD (SQL)
payable_table_04 = (
    payable_payment_reply_df.alias("q01").filter(col("pybl_stat_cd") == "CU")    .select(
    concat_ws("", col("q01.st_fsa_cd"), col("q01.cnty_fsa_cd")).alias("core_organization_code"),
    lit("01").alias("payment_transaction_type_identifier"),
    col("q01.acct_cust_id").alias("accounting_customer_identifier"),
    col("q01.acct_pgm_cd").alias("accounting_program_code"),
    col("q01.acct_pgm_id").alias("accounting_program_identifier"),
    col("q01.acct_pgm_yr").alias("accounting_program_year"),
    col("q01.prim_acct_ref_cd").alias("accounting_reference_1_code"),
    col("q01.prim_acct_ref_nbr").alias("accounting_reference_1_number"),
    col("q01.scnd_acct_ref_cd").alias("accounting_reference_2_code"),
    col("q01.scnd_acct_ref_nbr").alias("accounting_reference_2_number"),
    col("q01.acct_svc_rqst_dt").alias("accounting_service_request_date"),
    lit("").alias("accounting_transaction_code"),
    col("q01.acct_txn_dt").alias("accounting_transaction_date"),
    col("q01.pymt_cmdy_qty").alias("accounting_transaction_quantity"),
    col("q01.bdgt_fscl_yr").alias("budget_fiscal_year"),
    col("q01.cmdy_cd").alias("commodity_code"),
    col("q01.bus_pty_id").alias("business_party_identifier"),
    col("q01.cnty_fsa_cd").alias("county_fsa_code"),
    col("q01.fnl_rvw_dt").alias("final_review_date"),
    col("q01.obl_pymt_ind").alias("obligation_full_partial_paid_indicator"),
    col("q01.obl_id").alias("obligation_identifier"),
    col("q01.pybl_id").alias("payable_identifier"),
    lit("C3").alias("payable_status_code"),
    col("q01.pymt_rqst_amt").alias("payment_request_amount"),
    col("q01.pgm_alp_cd").alias("program_alpha_code"),
    when(col("acct_txn_cd").isin("197", "198"), lit(0))
        .when(col("pybl_stat_cd") == "CU", coalesce(col("prmpt_pymt_int_amt"), lit(0)) * -1)
        .otherwise(coalesce(col("prmpt_pymt_int_amt"), lit(0))).alias("prompt_payment_interest_amount"),
    col("q01.prmpt_pymt_int_dt").alias("prompt_payment_interest_date"),
    col("q01.smpl_id").alias("sampling_identifier"),
    col("q01.st_fsa_cd").alias("state_fsa_code"),
    col("q01.sys_cd").alias("system_code"),
    col("q01.tax_id").alias("tax_identification"),
    col("q01.tax_id_type_cd").alias("tax_identification_type_code"),
    coalesce(col("q01.tax_whld_amt") * -1, lit(0)).alias("tax_withholding_amount"),
    lit(0.00).cast(DecimalType(16,2)).alias("disbursement_amount"),
    lit(0).alias("payable_offset_amount"),
    lit(0).alias("receivable_amount"),
    lit(0).alias("assignment_paid_amount"),
    lit(0).alias("assignment_amount"),
    lit(0).alias("obligation_transaction_amount"),
    lit(0).alias("obligation_balance_amount"),
    col("q01.pymt_rcv_dt").alias("payment_received_date"),
    col("q01.pymt_isu_dt").alias("payment_issue_date"),
    lit(0).alias("fund_allotment_identifier"),
    lit(0).alias("bfy_obligation_transaction_amount"),
    lit(0).alias("bfy_obligation_amount"),
    lit(0).alias("bfy_obligation_balance_amount"),
    col("q01.data_src_acro").alias("data_source_acronym"),
    when(col("q01.acct_txn_cd") != "198", lit(0.00))
        .when(col("q01.pybl_stat_cd") == "CU", coalesce(col("q01.prmpt_pymt_int_amt"), lit(0)) * -1)
        .otherwise(coalesce(col("q01.prmpt_pymt_int_amt"), lit(0))).alias("interest_penalty_amount"),
    when(col("q01.acct_txn_cd") != "197", lit(0.00))
         .when(col("q01.pybl_stat_cd") == "CU", coalesce(col("q01.prmpt_pymt_int_amt"), lit(0)) * -1)
         .otherwise(coalesce(col("q01.prmpt_pymt_int_amt"), lit(0))).alias("additional_interest_penalty_amount")
))
# debug_df(payable_table_04, "Fourth 01 Payable Table")

#Union all 4 Tables 
unioned_df = (
    payable_table_01
    .unionByName(payable_table_02, allowMissingColumns=True)
    .unionByName(payable_table_03, allowMissingColumns=True)
    .unionByName(payable_table_04, allowMissingColumns=True)
)
# debug_df(unioned_df, "Unioned DF")

# --- Final select ---
df_final_select = unioned_df.select(
    col("payment_transaction_type_identifier").cast(IntegerType()),
    col("accounting_customer_identifier").cast(IntegerType()),
    col("accounting_program_identifier").cast(IntegerType()),
    col("sampling_identifier").cast(IntegerType()),
    lit(None).alias("national_joint_payment_identifier").cast(IntegerType()),
    lit(None).alias("accounting_payee_type_code").cast(StringType()),
    col("accounting_service_request_date").cast(TimestampType()),
    col("accounting_transaction_date").cast(TimestampType()),
    col("accounting_transaction_code").cast(StringType()),
    col("budget_fiscal_year").cast(IntegerType()),
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
    lit(None).alias("disbursement_identifier").cast(IntegerType()),
    lit(None).alias("disbursement_date").cast(TimestampType()),
    lit(None).alias("disbursement_status_date").cast(TimestampType()),
    lit(None).alias("disbursement_status_code").cast(StringType()),
    lit(None).alias("last_disbursement_status_code").cast(StringType()),
    lit(None).alias("disbursement_type_code").cast(StringType()),
    lit(None).alias("disbursement_confirmation_identifier").cast(IntegerType()),
    lit(None).alias("external_disbursement_identification").cast(StringType()),
    lit(None).alias("external_disbursement_type_code").cast(StringType()),
    lit(None).alias("payable_completed_indicator").cast(StringType()),
    col("payable_offset_amount").cast(DecimalType(16,2)),
    lit(None).alias("receivable_identifier").cast(IntegerType()),
    col("payable_identifier").cast(IntegerType()),
    col("receivable_amount").cast(DecimalType(16,2)),
    lit(None).alias("offset_override_reason_code").cast(StringType()),
    lit(None).alias("claim_identifier").cast(StringType()),
    col("final_review_date").cast(TimestampType()),
    col("payment_request_amount").cast(DecimalType(16,2)),
    col("prompt_payment_interest_amount").cast(DecimalType(16,2)),
    col("tax_withholding_amount").cast(DecimalType(16,2)),
    lit(None).alias("legacy_receivable_number").cast(StringType()),
    lit(None).alias("offset_status_code").cast(StringType()),
    lit(None).alias("national_assignment_identifier").cast(IntegerType()),
    lit(None).alias("assignee_accounting_customer_identifier").cast(IntegerType()),
    col("assignment_amount").cast(DecimalType(16,2)),
    col("assignment_paid_amount").cast(DecimalType(16,2)),
    col("accounting_charge_amount").cast(DecimalType(16,2)),
    col("obligation_identifier").cast(IntegerType()),
    col("obligation_full_partial_paid_indicator").cast(StringType()),
    col("obligation_amount").cast(DecimalType(16,2)),
    lit(None).alias("obligation_approval_date").cast(TimestampType()),
    col("obligation_balance_amount").cast(DecimalType(16,2)),
    lit(None).alias("obligation_status_code").cast(StringType()),
    lit(None).alias("core_accounting_period").cast(StringType()),
    col("accounting_transaction_quantity").cast(DecimalType(16,2)),
    lit(None).alias("unit_of_measure_code").cast(StringType()),
    lit(None).alias("program_category_code").cast(StringType()),
    lit(None).alias("core_program_type_code").cast(StringType()),
    lit(None).alias("adjusted_obligation_identifier").cast(IntegerType()),
    col("system_code").cast(StringType()),
    col("core_organization_code").cast(StringType()),
    lit(None).alias("obligation_liquidation_request_identifier").cast(IntegerType()),
    lit(None).alias("creation_datetime").cast(TimestampType()),
    lit(None).alias("creation_date").cast(TimestampType()),
    lit(None).alias("reversal_indicator").cast(StringType()),
    col("obligation_transaction_amount").cast(DecimalType(16,2)),
    col("payment_received_date").cast(TimestampType()),
    col("payment_issue_date").cast(TimestampType()),
    lit(None).alias("accounting_transmission_date").cast(TimestampType()),
    col("bfy_obligation_transaction_amount").cast(DecimalType(16,2)),
    col("bfy_obligation_amount").cast(DecimalType(16,2)),
    col("bfy_obligation_balance_amount").cast(DecimalType(16,2)),
    col("fund_allotment_identifier").cast(IntegerType()),
    lit(None).alias("obligation_budget_fiscal_year_identifier").cast(IntegerType()),
    lit(None).alias("obligation_adjustment_budget_fiscal_year_identifier").cast(IntegerType()),
    lit(None).alias("obligation_liquidation_request_budget_fiscal_year_identifier").cast(IntegerType()),
    lit(None).alias("accounting_transaction_net_amount").cast(DecimalType(16,2)),
    lit(None).alias("total_payable_offset_amount").cast(DecimalType(16,2)),
    col("data_source_acronym").cast(StringType()),
    col("interest_penalty_amount").cast(DecimalType(16,2)),
    col("additional_interest_penalty_amount").cast(DecimalType(16,2)),
    lit(None).alias("duns_plus4_nbr").cast(StringType()),
    lit(None).alias("duns_nbr").cast(StringType()),
    lit(None).alias("sam_unique_entity_identifier").cast(StringType())
)

debug_df(df_final_select, "df_final_select - ready to write to S3 cleansed")

'''
#####################################################################################
# Write to S3
#####################################################################################
df_strings = df_final_select.select([F.col(c.name).cast("string").alias(c.name) for c in df_final_select.schema])
df_strings.write.mode("overwrite").parquet(destination_path, compression="snappy")
print(f"Row count after all columns converted to string: {df_strings.count()}")


#####################################################################################
# Read the string-typed DataFrame from Cleansed Zone (Parquet)
#####################################################################################
df_strings = spark.read.parquet(destination_path)

#####################################################################################
# Function Name: robust_slide (slide_udf)
# Purpose:
# Corrects “shifted” data rows by searching for a valid decimal value in the 3rd column 
# and a timestamp in the 26th column (as per business rules). Shifts data left/right 
# by up to 3 columns, or pads with empty strings if needed, to align columns for correct 
# mapping and type casting.

# Expected Output:
# A realigned row (list of strings) where expected columns match expected types, or 
# `None` if no valid alignment found.
#####################################################################################
EXPECTED_DECIMAL_COL = 2  # zero-based index for decimal (3rd col)
EXPECTED_TIMESTAMP_COL = 25  # zero-based index for timestamp (26th col)
DESIRED_COLS = len(target_schema.fieldNames())  # or hardcode as needed
MAX_SHIFT = 3  

def robust_slide(arr):
    decimal_pattern = re.compile(r"^-?\d+(\.\d+)?$")
    ts_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d{1,6})?$")
    # Try shifting from -MAX_SHIFT (pad left) up to +MAX_SHIFT (skip cols at start)
    for shift in range(-MAX_SHIFT, MAX_SHIFT+1):
        if shift < 0:
            shifted = [""] * abs(shift) + arr
        else:
            shifted = arr[shift:]
        # Ensure length
        shifted = shifted[:DESIRED_COLS] + [""] * (DESIRED_COLS - len(shifted))
        # Test expected types at expected columns
        dec_val = shifted[EXPECTED_DECIMAL_COL] if len(shifted) > EXPECTED_DECIMAL_COL else ""
        ts_val = shifted[EXPECTED_TIMESTAMP_COL] if len(shifted) > EXPECTED_TIMESTAMP_COL else ""
        if decimal_pattern.match(str(dec_val or "")) and ts_pattern.match(str(ts_val or "")):
            return shifted
    return None  # No valid alignment found

slide_udf = udf(robust_slide, ArrayType(StringType()))



#####################################################################################
# Re-split and apply slide correction
# Purpose:
# Splits tab-delimited string columns, applies the slide correction UDF, and expands 
# the corrected array back into named columns based on the target schema.

# Expected Output:
# A DataFrame where all rows are correctly realigned and ready for strict type casting.
#####################################################################################
if "value" not in df_strings.columns:
    # Join all string columns as tab separated, to mimic the original line
    df_strings = df_strings.withColumn(
        "value",
        F.concat_ws("\t", *df_strings.columns)
    )

df_slid = df_strings.withColumn(
    "split_row", split(col("value"), "\t")
).withColumn(
    "aligned_row", slide_udf(col("split_row"))
).filter(
    col("aligned_row").isNotNull()
)

#####################################################################################
# Unpack aligned_row to columns
#####################################################################################
for idx, col_name in enumerate(target_schema.fieldNames()):
    df_slid = df_slid.withColumn(col_name, col("aligned_row")[idx])

df_slid = df_slid.select(target_schema.fieldNames())

#####################################################################################
# Purpose:
# Map and cast each column, fill missing as nulls

# Expected Output:
# A DataFrame with each field strictly typed, null-safe and in the correct column order.
#####################################################################################
mapped_cols = []
for field in target_schema.fields:
    mapped_col = (
        when(trim(col(field.name)).isin("", "null", None), None)
        .otherwise(col(field.name).cast(field.dataType))
        .alias(field.name)
    )
    mapped_cols.append(mapped_col)

df_casted = df_final_select.select(mapped_cols)

# Debug and Validate
debug_df(df_casted, "df_casted - after slide and schema align")
# validate_schema(df_casted, target_schema)
'''

final_df = df_final_select
# final_df.show()
print("final Count: ", final_df.count())

#########################################################################################################
# Write to S3 final zone in parquet format
#########################################################################################################
finalzone = "s3://c108-{}-fpacfsa-final-zone/dmart/fwadm/nps/{}_part1".format(environ, tablename)
finalzone_path = f"{finalzone}/{datestmp}"
print(f"finalzone_path in S3: {finalzone_path}")

final_df.write.mode("overwrite").parquet(finalzone_path, compression="snappy")
print(f"Final Data Frame written to S3 at: {finalzone_path}")

endtime_m = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

job_stat_nm = f"FWADM NPS {tablename} successfully loaded"
utils.data_ppln_job_insert(job_stat_nm, 'Complete', starttime_m, endtime_m, pcdb_conn, ENVIRON, 'NPS')

job.commit()
