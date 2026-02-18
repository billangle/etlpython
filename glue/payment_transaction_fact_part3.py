####################################################################################################################################
# Author : Ahmad Hanif
# ALASKA NORTHSTAR Contractor for USDAT
# Date : 2025-04-17
# Project : Juniper
# FSA-DEV-FWADM-treasury_request_fact
# MDART-4140
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
import pytz
import os
from pyspark.sql.functions import count, col, lit, coalesce, when, concat, current_timestamp, date_format, length, row_number,substring, sum as _sum, trim, max as sparkMax, min as sparkMin, concat_ws, to_timestamp, expr, ltrim, trim
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
from pyspark.sql.types import DecimalType, DoubleType, IntegerType, StringType, StructType, StructField, TimestampType, LongType
from pyspark.sql.functions import regexp_replace

# Initialize Glue context, Spark context, and get arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'dbtable', 'schema_name', 'environ', 'utils'])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
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
properties = {
    "user": username,
    "password": password,
[O    "preactions":"truncate table {}.{};".format(schemaname,tablename),
    "aws_iam_role": arnrole,
    "driver": "com.amazon.redshift.jdbc.Driver"
}

[I#Conn options used for writing to Redshift database using Glue context
conn_options = {
    "dbtable": "{}.{}".format(schemaname,tablename),
    "database": "redshift_db",
#    "preactions":"truncate table {}.{};".format(schemaname,tablename),
    "aws_iam_role": arnrole
}

[O
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

#Code Start
#datestmp = '20250916' # Testing Purposes

# Import the source files designate the destination path
qct01_path = "s3://c108-{}-fpacfsa-cleansed-zone/dmart/fwadm/nps/qct01pybl/{}/".format(environ, datestmp)
disbursement_path = "s3://c108-{}-fpacfsa-landing-zone/dmart/raw/fwadm/{}/sqlNPS_Disbursement.txt".format(environ, datestmp)
payable_payment_reply_path = "s3://c108-{}-fpacfsa-landing-zone/dmart/raw/fwadm/{}/sqlNPS_Payable_Payment_Reply.txt".format(environ, datestmp)
qct03_path = "s3://c108-{}-fpacfsa-cleansed-zone/dmart/fwadm/nps/qct03disb/{}/".format(environ, datestmp)
qct36_path = "s3://c108-{}-fpacfsa-cleansed-zone/dmart/fwadm/nps/qct36cnfrm/{}/".format(environ, datestmp)

# Special logic to handle QCT77 file as it comes in later in the day and we need to use the day befores file ONLY IN PROD
present_hour = datetime.now().astimezone(pytz.timezone('US/Central')).strftime("%H")
print(present_hour)
# PROD is ran in evenings.  All others after midnight. - Also, recovery the next day will not need files moved around.  
if (int(present_hour)<20, environ=='prod') == (True, True): 
    # Adjusting dates for recovery in prod next day or where NRRS nightly hit the midnight witching hour
    tempdatestmp = datetime.utcnow().astimezone(pytz.timezone('US/Central')) + timedelta(days=-1)
    tempdatestmp = tempdatestmp.strftime("%Y%m%d")
    print("using yesterdays file for QCT77")
    qct77_path = "s3://c108-{}-fpacfsa-landing-zone/dmart/raw/fwadm/{}/QCT77ASGN.txt".format(environ, tempdatestmp)
else:
    print("using todays file for QCT77")
    qct77_path = "s3://c108-{}-fpacfsa-landing-zone/dmart/raw/fwadm/{}/QCT77ASGN.txt".format(environ, datestmp)

destinationpath = "s3://c108-{}-fpacfsa-cleansed-zone/dmart/fwadm/nps_qa/{}".format(environ, tablename)
destination_path = f"{destinationpath}/{tmstmp}/"
    
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
    
qct01_df = spark.read.parquet(qct01_path, ignoreLeadingWhiteSpace=True)
qct01_df = qct01_df.withColumn("pybl_id", ltrim(col("pybl_id")))
print("qct01_df", qct01_df.count())

disbursement_schema = StructType([
    StructField("disb_id",IntegerType(), False),
    StructField("pybl_id",IntegerType(), False),
    StructField("fncl_acct_id",IntegerType(), False),
    StructField("acct_pye_type_cd",StringType(), False),
    StructField("ntl_asgn_id",IntegerType(), False),
    StructField("bus_type_cd",StringType(), False),
    StructField("disb_amt",DecimalType(16,2), False),
    StructField("disb_dt",TimestampType(), False),
    StructField("disb_stat_cd",StringType(), False),
    StructField("disb_type_cd",StringType(), False),
    StructField("email_adr",StringType(), False),
    StructField("fmt_pye_nm",StringType(), False),
    StructField("ntl_jnt_pymt_id",IntegerType(), False),
    StructField("tax_id",StringType(), False),
    StructField("tax_id_type_cd",StringType(), False),
    StructField("adr_info_line",StringType(), False),
    StructField("carr_rt_cd",StringType(), False),
    StructField("city_nm",StringType(), False),
    StructField("dlvr_adr_line",StringType(), False),
    StructField("dlvr_pnt_bar_cd",StringType(), False),
    StructField("fgn_adr_ind",StringType(), False),
    StructField("fgn_adr_line",StringType(), False),
    StructField("st_abr",StringType(), False),
    StructField("post_cd",StringType(), False),
    StructField("st_fsa_cd",StringType(), False),
    StructField("cnty_fsa_cd",StringType(), False),
    StructField("bank_rt_nbr",StringType(), False),
    StructField("fncl_cust_acct_cd",StringType(), False),
    StructField("fncl_cust_acct_nbr",StringType(), False),
    StructField("acct_type_cd",StringType(), False),
    StructField("alt_pye_pmsn_ind",StringType(), False),
    StructField("prmpt_pymt_int_amt",DecimalType(16,2), False),
    StructField("acct_ref_cd",StringType(), False),
    StructField("acct_ref_nbr",StringType(), False),
    StructField("duns_nbr",StringType(), False),
    StructField("data_stat_cd",StringType(), False),
    StructField("cre_dt",TimestampType(), False),
    StructField("cre_user_nm",StringType(), False),
    StructField("last_chg_dt",TimestampType(), False),
    StructField("last_chg_user_nm",StringType(), False),
    StructField("acct_cust_id",IntegerType(), False),
    StructField("duns_plus4_nbr",StringType(), False),
    StructField("ctry_nm",StringType(), False),
    StructField("ctry_div_nm",StringType(), False),
    StructField("ctry_iso_alp_2_cd",StringType(), False),
    StructField("sam_uniq_enty_id",StringType(), False)
    ])
    
disbursement_df = spark.read.csv(disbursement_path, header=False, schema=disbursement_schema, sep="\t")
print("disbursement_df :", disbursement_df.count())

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
print("payable_payment_reply_df", payable_payment_reply_df.count())

qct03_schema = StructType([
    StructField("disb_id",IntegerType(), False),
    StructField("pybl_id",IntegerType(), False),
    StructField("fncl_acct_id",IntegerType(), False),
    StructField("acct_pye_type_cd",StringType(), False),
    StructField("ntl_asgn_id",IntegerType(), False),
    StructField("bus_type_cd",StringType(), False),
    StructField("disb_amt",DecimalType(16,2), False),
    StructField("disb_dt",TimestampType(), False),
    StructField("disb_stat_cd",StringType(), False),
    StructField("disb_type_cd",StringType(), False),
    StructField("email_adr",StringType(), False),
    StructField("fmt_pye_nm",StringType(), False),
    StructField("ntl_jnt_pymt_id",IntegerType(), False),
    StructField("tax_id",StringType(), False),
    StructField("tax_id_type_cd",StringType(), False),
    StructField("last_chg_dt",StringType(), False),
    StructField("last_chg_user_nm",StringType(), False),
    StructField("adr_info_line",StringType(), False),
    StructField("carr_rt_cd",StringType(), False),
    StructField("city_nm",StringType(), False),
    StructField("dlvr_adr_line",StringType(), False),
    StructField("dlvr_pnt_bar_cd",StringType(), False),
    StructField("fgn_adr_ind",StringType(), False),
    StructField("fgn_adr_line",StringType(), False),
    StructField("st_abr",StringType(), False),
    StructField("post_cd",StringType(), False),
    StructField("st_fsa_cd",StringType(), False),
    StructField("cnty_fsa_cd",StringType(), False),
    StructField("bank_rt_nbr",StringType(), False),
    StructField("fncl_cust_acct_cd",StringType(), False),
    StructField("fncl_cust_acct_nbr",StringType(), False),
    StructField("acct_type_cd",StringType(), False),
    StructField("alt_pye_pmsn_ind",StringType(), False),
    StructField("prmpt_pymt_int_amt",DecimalType(16,2), False),
    StructField("acct_ref_cd",StringType(), False),
    StructField("acct_ref_nbr",StringType(), False),
    StructField("duns_nbr",StringType(), False),
    StructField("acct_cust_id",IntegerType(), False),
    StructField("duns_plus4_nbr",StringType(), False),
    StructField("ctry_nm",StringType(), False),
    StructField("ctry_div_nm",StringType(), False),
    StructField("ctry_iso_alp_2_cd",StringType(), False),
    StructField("sam_uniq_enty_id",StringType(), False),
    StructField("data_invalid_indicator",StringType(), False)
    ])
    
qct03_df = spark.read.parquet(qct03_path, ignoreLeadingWhiteSpace=True)
qct03_df = qct03_df.withColumn("disb_id", ltrim(col("disb_id")))
# print("qct03_df", qct03_df.count())
debug_df(qct03_df, "QCT03 DF Debugging")

qct77_schema = StructType([
    StructField("ntl_asgn_id",IntegerType(), False),
    StructField("data_stat_cd",StringType(), False),
    StructField("last_chg_dt",StringType(), False),
    StructField("cre_dt",StringType(), False),
    StructField("last_chg_user_nm",StringType(), False),
    StructField("cre_user_nm",StringType(), False),
    StructField("acct_pgm_id",IntegerType(), False),
    StructField("acct_cust_id",IntegerType(), False),
    StructField("asgn_acct_cust_id",IntegerType(), False),
    StructField("st_fsa_cd",StringType(), False),
    StructField("cnty_fsa_cd",StringType(), False),
    StructField("asgn_amt",DecimalType(16,2), False),
    StructField("asgn_paid_amt",DecimalType(16,2), False),
    StructField("asgn_agr_sgn_ind",StringType(), False),
    StructField("agr_stat_nm",StringType(), False),
    StructField("asgn_jnt_pye_box",StringType(), False),
    StructField("acct_pgm_yr",StringType(), False),
    StructField("acct_ref_nbr",StringType(), False),
    StructField("acct_ref_cd",StringType(), False),
    StructField("doc_sgn_dt",StringType(), False),
    StructField("pend_chg_id",IntegerType(), False),
    StructField("orgn_st_fsa_cd",StringType(), False),
    StructField("orgn_cnty_fsa_cd",StringType(), False),
    StructField("duns_nbr",StringType(), False),
    StructField("asgn_duns_nbr",StringType(), False),
    StructField("asgn_st_fsa_cd",StringType(), False),
    StructField("asgn_cnty_fsa_cd",StringType(), False),
    StructField("asgn_duns_plus4_nbr",StringType(), False),
    StructField("pybl_id",IntegerType(), False),
    StructField("asgn_sam_uniq_enty_id",StringType(), False),
    StructField("sam_uniq_enty_id",StringType(), False),
    StructField("data_invalid_indicator",StringType(), False)
    ])
    
qct77_df = spark.read.csv(qct77_path, header=False, schema=qct77_schema, sep="\t")
print("qct77_df", qct77_df.count())

# qct36_schema = StructType([
#     StructField("disb_cnfrm_id",IntegerType(), False),
#     StructField("disb_id",IntegerType(), False),
#     StructField("cnfrm_proc_ind",StringType(), False),
#     StructField("disb_stat_cd",StringType(), False),
#     StructField("disb_stat_dt",TimestampType(), False),
#     StructField("extl_disb_id",StringType(), False),
#     StructField("extl_disb_type_cd",StringType(), False),
#     StructField("last_chg_dt",StringType(), False),
#     StructField("last_chg_user_nm",StringType(), False),
#     StructField("st_fsa_cd",StringType(), False),
#     StructField("cnty_fsa_cd",StringType(), False),
#     StructField("bank_rt_nbr",StringType(), False),
#     StructField("fncl_cust_acct_nbr",StringType(), False),
#     StructField("acct_type_cd",StringType(), False),
#     StructField("fncl_cust_acct_cd",StringType(), False),
#     StructField("expr_ck_dt",TimestampType(), False),
#     StructField("wrt_off_dt",TimestampType(), False),
#     StructField("data_invalid_indicator",StringType(), False)
#     ])
    
qct36_df = spark.read.parquet(qct36_path, ignoreLeadingWhiteSpace=True)
qct36_df = qct36_df.withColumn("disb_cnfrm_id", ltrim(col("disb_cnfrm_id")))
print("qct36_df", qct36_df.count())

# q01 setup
q01_df_part1 = qct01_df.join(
    payable_payment_reply_df.select("pybl_id"), on="pybl_id", how="left_anti"
).selectExpr(
    "pybl_id", "acct_cust_id", "acct_pgm_cd", "acct_pgm_id", "acct_pgm_yr",
    "prim_acct_ref_cd", "prim_acct_ref_nbr", "scnd_acct_ref_cd", "scnd_acct_ref_nbr",
    "acct_svc_rqst_dt", "acct_txn_dt", "bdgt_fscl_yr", "cmdy_cd", "bus_pty_id",
    "fnl_rvw_dt", "obl_pymt_ind", "obl_id", "pybl_stat_cd", "pgm_alp_cd",
    "prmpt_pymt_int_dt", "smpl_id", "sys_cd", "pymt_rcv_dt", "pymt_isu_dt",
    "data_src_acro", "acct_txn_cd"
)
# print("q01_df_part1")

q01_df_part2 = payable_payment_reply_df.selectExpr(
    "pybl_id", "acct_cust_id", "acct_pgm_cd", "acct_pgm_id", "acct_pgm_yr",
    "prim_acct_ref_cd", "prim_acct_ref_nbr", "scnd_acct_ref_cd", "scnd_acct_ref_nbr",
    "acct_svc_rqst_dt", "acct_txn_dt", "bdgt_fscl_yr", "cmdy_cd", "bus_pty_id",
    "fnl_rvw_dt", "obl_pymt_ind", "obl_id", "pybl_stat_cd", "pgm_alp_cd",
    "prmpt_pymt_int_dt", "smpl_id", "sys_cd", "pymt_rcv_dt", "pymt_isu_dt",
    "data_src_acro", "acct_txn_cd"
)
# print("q01_df_part2")

q01_df = q01_df_part1.unionByName(q01_df_part2)
# print("q01_df")

# q03 setup
q03_df = qct03_df.join(
    disbursement_df.select("disb_id"), on="disb_id", how="left_anti"
)
# print("q03_df")

# 36 Disbursement Confirmation Table
df_36_part1 = (
    q01_df.alias("q01")
    .join(q03_df.alias("q03"), col("q03.pybl_id") == col("q01.pybl_id"), "inner")
    .join(qct36_df.alias("q36"), col("q36.disb_id") == col("q03.disb_id"), "left")
    .join(qct77_df.alias("q77"), col("q77.ntl_asgn_id") == col("q03.ntl_asgn_id"), "left")
    .select(
        concat(col("q03.st_fsa_cd"), col("q03.cnty_fsa_cd")).alias("core_organization_code"),
        lit("36").alias("payment_transaction_type_identifier"),
        col("q01.acct_cust_id").alias("accounting_customer_identifier"),
        col("q01.acct_pgm_cd").alias("accounting_program_code"),
        col("q01.acct_pgm_id").alias("accounting_program_identifier"),
        col("q01.acct_pgm_yr").alias("accounting_program_year"),
        col("q01.prim_acct_ref_cd").alias("accounting_reference_1_code"),
        substring(col("q01.prim_acct_ref_nbr"),1,10).alias("accounting_reference_1_number"),
        col("q01.scnd_acct_ref_cd").alias("accounting_reference_2_code"),
        col("q01.scnd_acct_ref_nbr").alias("accounting_reference_2_number"),
        col("q01.acct_svc_rqst_dt").alias("accounting_service_request_date"),
        lit("").alias("accounting_transaction_code"),
        col("q01.acct_txn_dt").alias("accounting_transaction_date"),
        lit(0).alias("accounting_transaction_quantity"),
        col("q01.bdgt_fscl_yr").alias("budget_fiscal_year"),
        col("q01.cmdy_cd").alias("commodity_code"),
        col("q01.bus_pty_id").alias("business_party_identifier"),
        col("q01.fnl_rvw_dt").alias("final_review_date"),
        col("q01.obl_pymt_ind").alias("obligation_full_partial_paid_indicator"),
        col("q01.obl_id").alias("obligation_identifier"),
        col("q01.pybl_stat_cd").alias("payable_status_code"),
        lit(0).alias("payment_request_amount"),
        col("q01.pgm_alp_cd").alias("program_alpha_code"),
        when(col("q01.acct_txn_cd").isin("197", "198"), lit(0)).otherwise(
            when(col("q36.disb_stat_cd") == "CS", expr("coalesce(q03.prmpt_pymt_int_amt, 0)"))
            .otherwise(expr("coalesce(q03.prmpt_pymt_int_amt, 0) * -1"))
        ).alias("prompt_payment_interest_amount"),
        col("q01.prmpt_pymt_int_dt").alias("prompt_payment_interest_date"),
        col("q01.smpl_id").alias("sampling_identifier"),
        col("q01.sys_cd").alias("system_code"),
        col("q03.tax_id").alias("tax_identification"),
        col("q03.tax_id_type_cd").alias("tax_identification_type_code"),
        when(col("q03.acct_pye_type_cd").isin("F", "I", "V"), col("q03.disb_amt") * -1).otherwise(lit(0)).alias("tax_withholding_amount"),
        col("q03.acct_pye_type_cd").alias("accounting_payee_type_code"),
        col("q03.cnty_fsa_cd").alias("county_fsa_code"),
        when(col("q03.acct_pye_type_cd").isin("R", "F", "I", "V"), lit(0))
            .when(col("q36.disb_stat_cd") == "CS", col("q03.disb_amt"))
            .when(col("q36.disb_stat_cd").isin("CE", "WX"), expr("abs(q03.disb_amt)"))
            .otherwise(col("q03.disb_amt") * -1).alias("disbursement_amount"),
        col("q36.disb_cnfrm_id").alias("disbursement_confirmation_identifier"),
        col("q03.disb_dt").alias("disbursement_date"),
        col("q03.disb_id").alias("disbursement_identifier"),
        col("q36.disb_stat_dt").alias("disbursement_status_date"),
        when(col("q36.disb_stat_cd").isNotNull(), col("q36.disb_stat_cd"))
            .otherwise(col("q03.disb_stat_cd")).alias("disbursement_status_code"),
        col("q03.disb_type_cd").alias("disbursement_type_code"),
        col("q03.pybl_id").alias("payable_identifier"),
        col("q03.st_fsa_cd").alias("state_fsa_code"),
        col("q77.ntl_asgn_id").alias("national_assignment_identifier"),
        col("q03.ntl_jnt_pymt_id").alias("national_joint_payment_identifier"),
        col("q77.asgn_acct_cust_id").alias("assignee_accounting_customer_identifier"),
        col("q77.asgn_amt").alias("assignment_amount"),
        col("q77.asgn_paid_amt").alias("assignment_paid_amount"),
        col("q03.disb_stat_cd").alias("last_disbursement_status_code"),
        col("q36.cnfrm_proc_ind").alias("payable_completed_indicator"),
        col("q36.extl_disb_id").alias("external_disbursement_identification"),
        col("q36.extl_disb_type_cd").alias("external_disbursement_type_code"),
        when(col("q03.acct_pye_type_cd") == "R", col("q03.disb_amt") * -1).otherwise(lit(0)).alias("payable_offset_amount"),
        lit(0).alias("receivable_amount"),
        lit(0).alias("obligation_transaction_amount"),
        lit(0).alias("obligation_balance_amount"),
        lit(0).alias("obligation_amount"),
        col("q01.pymt_rcv_dt").alias("payment_received_date"),
        col("q01.pymt_isu_dt").alias("payment_issue_date"),
        lit(0).alias("fund_allotment_identifier"),
        lit(0).alias("bfy_obligation_transaction_amount"),
        lit(0).alias("bfy_obligation_amount"),
        lit(0).alias("bfy_obligation_balance_amount"),
        col("q01.data_src_acro").alias("data_source_acronym"),
        when(col("q01.acct_txn_cd") != "198", lit(0.00))
            .when(col("q36.disb_stat_cd") == "CS", expr("coalesce(q03.prmpt_pymt_int_amt, 0)"))
            .otherwise(expr("coalesce(q03.prmpt_pymt_int_amt, 0) * -1")).alias("interest_penalty_amount"),
        when(col("q01.acct_txn_cd") != "197", lit(0.00))
            .when(col("q36.disb_stat_cd") == "CS", expr("coalesce(q03.prmpt_pymt_int_amt, 0)"))
            .otherwise(expr("coalesce(q03.prmpt_pymt_int_amt, 0) * -1")).alias("additional_interest_penalty_amount"),
        col("q03.duns_nbr"),
        col("q03.duns_plus4_nbr"),
        col("q03.sam_uniq_enty_id").alias("sam_unique_entity_identifier")
    )
)
# debug_df(df_36_part1, "part 1")

# 36 Disbursement Confirmation Table PART 2 (Uses disbursement as q03)
df_36_part2 = (
    disbursement_df.alias("q03")
    .join(q01_df.alias("q01"), col("q03.pybl_id") == col("q01.pybl_id"), "inner")
    .join(qct36_df.alias("q36"), col("q36.disb_id") == col("q03.disb_id"), "left")
    .join(qct77_df.alias("q77"), col("q77.ntl_asgn_id") == col("q03.ntl_asgn_id"), "left")
    .select(
        concat(col("q03.st_fsa_cd"), col("q03.cnty_fsa_cd")).alias("core_organization_code"),
        lit("36").alias("payment_transaction_type_identifier"),
        col("q01.acct_cust_id").alias("accounting_customer_identifier"),
        col("q01.acct_pgm_cd").alias("accounting_program_code"),
        col("q01.acct_pgm_id").alias("accounting_program_identifier"),
        col("q01.acct_pgm_yr").alias("accounting_program_year"),
        col("q01.prim_acct_ref_cd").alias("accounting_reference_1_code"),
        substring(col("q01.prim_acct_ref_nbr"),1,10).alias("accounting_reference_1_number"),
        col("q01.scnd_acct_ref_cd").alias("accounting_reference_2_code"),
        col("q01.scnd_acct_ref_nbr").alias("accounting_reference_2_number"),
        col("q01.acct_svc_rqst_dt").alias("accounting_service_request_date"),
        lit("").alias("accounting_transaction_code"),
        col("q01.acct_txn_dt").alias("accounting_transaction_date"),
        lit(0).alias("accounting_transaction_quantity"),
        col("q01.bdgt_fscl_yr").alias("budget_fiscal_year"),
        col("q01.cmdy_cd").alias("commodity_code"),
        col("q01.bus_pty_id").alias("business_party_identifier"),
        col("q01.fnl_rvw_dt").alias("final_review_date"),
        col("q01.obl_pymt_ind").alias("obligation_full_partial_paid_indicator"),
        col("q01.obl_id").alias("obligation_identifier"),
        col("q01.pybl_stat_cd").alias("payable_status_code"),
        lit(0).alias("payment_request_amount"),
        col("q01.pgm_alp_cd").alias("program_alpha_code"),
        when(col("q01.acct_txn_cd").isin("197", "198"), lit(0)).otherwise(
            when(col("q36.disb_stat_cd") == "CS", expr("coalesce(q03.prmpt_pymt_int_amt, 0)"))
            .otherwise(expr("coalesce(q03.prmpt_pymt_int_amt, 0) * -1"))
        ).alias("prompt_payment_interest_amount"),
        col("q01.prmpt_pymt_int_dt").alias("prompt_payment_interest_date"),
        col("q01.smpl_id").alias("sampling_identifier"),
        col("q01.sys_cd").alias("system_code"),
        col("q03.tax_id").alias("tax_identification"),
        col("q03.tax_id_type_cd").alias("tax_identification_type_code"),
        when(col("q03.acct_pye_type_cd").isin("F", "I", "V"), col("q03.disb_amt") * -1).otherwise(lit(0)).alias("tax_withholding_amount"),
        col("q03.acct_pye_type_cd").alias("accounting_payee_type_code"),
        col("q03.cnty_fsa_cd").alias("county_fsa_code"),
        
        when(col("q03.acct_pye_type_cd").isin("R", "F", "I", "V"), lit(0))
            .when(col("q36.disb_stat_cd") == "CS", col("q03.disb_amt"))
            .otherwise(col("q03.disb_amt") * -1).alias("disbursement_amount"),
        col("q36.disb_cnfrm_id").alias("disbursement_confirmation_identifier"),
        col("q03.disb_dt").alias("disbursement_date"),
        col("q03.disb_id").alias("disbursement_identifier"),
        col("q36.disb_stat_dt").alias("disbursement_status_date"),
        when(col("q36.disb_stat_cd").isNotNull(), col("q36.disb_stat_cd"))
            .otherwise(col("q03.disb_stat_cd")).alias("disbursement_status_code"),
        col("q03.disb_type_cd").alias("disbursement_type_code"),
        col("q03.pybl_id").alias("payable_identifier"),
        col("q03.st_fsa_cd").alias("state_fsa_code"),
        col("q77.ntl_asgn_id").alias("national_assignment_identifier"),
        col("q03.ntl_jnt_pymt_id").alias("national_joint_payment_identifier"),
        col("q77.asgn_acct_cust_id").alias("assignee_accounting_customer_identifier"),
        col("q77.asgn_amt").alias("assignment_amount"),
        col("q77.asgn_paid_amt").alias("assignment_paid_amount"),
        col("q03.disb_stat_cd").alias("last_disbursement_status_code"),
        col("q36.cnfrm_proc_ind").alias("payable_completed_indicator"),
        col("q36.extl_disb_id").alias("external_disbursement_identification"),
        col("q36.extl_disb_type_cd").alias("external_disbursement_type_code"),
        when(col("q03.acct_pye_type_cd") == "R", col("q03.disb_amt") * -1).otherwise(lit(0)).alias("payable_offset_amount"),
        lit(0).alias("receivable_amount"),
        lit(0).alias("obligation_transaction_amount"),
        lit(0).alias("obligation_balance_amount"),
        lit(0).alias("obligation_amount"),
        col("q01.pymt_rcv_dt").alias("payment_received_date"),
        col("q01.pymt_isu_dt").alias("payment_issue_date"),
        lit(0).alias("fund_allotment_identifier"),
        lit(0).alias("bfy_obligation_transaction_amount"),
        lit(0).alias("bfy_obligation_amount"),
        lit(0).alias("bfy_obligation_balance_amount"),
        col("q01.data_src_acro").alias("data_source_acronym"),
        when(col("q01.acct_txn_cd") != "198", lit(0.00))
            .when(col("q36.disb_stat_cd") == "CS", expr("coalesce(q03.prmpt_pymt_int_amt, 0)"))
            .otherwise(expr("coalesce(q03.prmpt_pymt_int_amt, 0) * -1")).alias("interest_penalty_amount"),
        when(col("q01.acct_txn_cd") != "197", lit(0.00))
            .when(col("q36.disb_stat_cd") == "CS", expr("coalesce(q03.prmpt_pymt_int_amt, 0)"))
            .otherwise(expr("coalesce(q03.prmpt_pymt_int_amt, 0) * -1")).alias("additional_interest_penalty_amount"),
        col("q03.duns_nbr"),
        col("q03.duns_plus4_nbr"),
        col("q03.sam_uniq_enty_id").alias("sam_unique_entity_identifier")
    )
)
# debug_df(df_36_part2, "part 2")


#Union the 2 36 disbursement tables
final_df = df_36_part1.unionByName(df_36_part2)

final_df = final_df.select(
    col("payment_transaction_type_identifier").cast(IntegerType()),
    col("accounting_customer_identifier").cast(IntegerType()),
    col("accounting_program_identifier").cast(IntegerType()),
    col("sampling_identifier").cast(IntegerType()),
    col("national_joint_payment_identifier").cast(IntegerType()),
    col("accounting_payee_type_code").cast(StringType()),
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
    lit("").cast(StringType()).alias("accounting_reference_3_code"),
    lit("").cast(StringType()).alias("accounting_reference_3_number"),
    lit("").cast(StringType()).alias("accounting_reference_4_code"),
    lit("").cast(StringType()).alias("accounting_reference_4_number"),
    lit("").cast(StringType()).alias("accounting_reference_5_code"),
    lit("").cast(StringType()).alias("accounting_reference_5_number"),
    col("commodity_code").cast(StringType()),
    col("business_party_identifier").cast(StringType()),
    col("tax_identification").cast(StringType()),
    col("tax_identification_type_code").cast(StringType()),
    col("program_alpha_code").cast(StringType()),
    col("disbursement_amount").cast(DecimalType(16, 2)),
    col("disbursement_identifier").cast(IntegerType()),
    col("disbursement_date").cast(TimestampType()),
    col("disbursement_status_date").cast(TimestampType()),
    col("disbursement_status_code").cast(StringType()),
    col("last_disbursement_status_code").cast(StringType()),
    col("disbursement_type_code").cast(StringType()),
    col("disbursement_confirmation_identifier").cast(IntegerType()),
    col("external_disbursement_identification").cast(StringType()),
    col("external_disbursement_type_code").cast(StringType()),
    col("payable_completed_indicator").cast(StringType()),
    col("payable_offset_amount").cast(DecimalType(16, 2)),
    #lit(0).cast(IntegerType()).alias("receivable_identifier"),
    lit(None).cast(IntegerType()).alias("receivable_identifier"),
    col("payable_identifier").cast(IntegerType()),
    col("receivable_amount").cast(DecimalType(16, 2)),
    lit("").cast(StringType()).alias("offset_override_reason_code"),
    lit("").cast(StringType()).alias("claim_identifier"),
    col("final_review_date").cast(TimestampType()),
    col("payment_request_amount").cast(DecimalType(16, 2)),
    col("prompt_payment_interest_amount").cast(DecimalType(16, 2)),
    col("tax_withholding_amount").cast(DecimalType(16, 2)),
    lit("").cast(StringType()).alias("legacy_receivable_number"),
    lit("").cast(StringType()).alias("offset_status_code"),
    col("national_assignment_identifier").cast(IntegerType()),
    col("assignee_accounting_customer_identifier").cast(IntegerType()),
    col("assignment_amount").cast(DecimalType(16, 2)),
    col("assignment_paid_amount").cast(DecimalType(16, 2)),
    lit(0.00).cast(DecimalType(16, 2)).alias("accounting_charge_amount"),
    col("obligation_identifier").cast(IntegerType()),
    col("obligation_full_partial_paid_indicator").cast(StringType()),
    col("obligation_amount").cast(DecimalType(16, 2)),
    lit(None).cast(TimestampType()).alias("obligation_approval_date"),
    col("obligation_balance_amount").cast(DecimalType(16, 2)),
    lit("").cast(StringType()).alias("obligation_status_code"),
    lit("").cast(StringType()).alias("core_accounting_period"),
    col("accounting_transaction_quantity").cast(DecimalType(16, 2)),
    lit("").cast(StringType()).alias("unit_of_measure_code"),
    lit("").cast(StringType()).alias("program_category_code"),
    lit("").cast(StringType()).alias("core_program_type_code"),
    lit(0).cast(IntegerType()).alias("adjusted_obligation_identifier"),
    col("system_code").cast(StringType()),
    col("core_organization_code").cast(StringType()),
    #lit(0).cast(IntegerType()).alias("obligation_liquidation_request_identifier"),
    lit(None).cast(IntegerType()).alias("obligation_liquidation_request_identifier"),
    lit(None).cast(TimestampType()).alias("creation_datetime"),
    lit(None).cast(TimestampType()).alias('creation_date'),
    lit("").cast(StringType()).alias("reversal_indicator"),
    col("obligation_transaction_amount").cast(DecimalType(16, 2)),
    col("payment_received_date").cast(TimestampType()),
    col("payment_issue_date").cast(TimestampType()),
    lit(None).cast(TimestampType()).alias("accounting_transmission_date"),
    col("bfy_obligation_transaction_amount").cast(DecimalType(16, 2)),
    col("bfy_obligation_amount").cast(DecimalType(16, 2)),
    col("bfy_obligation_balance_amount").cast(DecimalType(16, 2)),
    col("fund_allotment_identifier").cast(IntegerType()),
    #lit(0).cast(IntegerType()).alias("obligation_budget_fiscal_year_identifier"),
    lit(None).cast(DecimalType(16,2)).alias("obligation_budget_fiscal_year_identifier"),
    #lit(0).cast(IntegerType()).alias("obligation_adjustment_budget_fiscal_year_identifier"),
    lit(None).cast(DecimalType(16,2)).alias("obligation_adjustment_budget_fiscal_year_identifier"),
    #lit(0).cast(IntegerType()).alias("obligation_liquidation_request_budget_fiscal_year_identifier"),
    lit(None).cast(IntegerType()).alias("obligation_liquidation_request_budget_fiscal_year_identifier"),
    #lit(0.00).alias("accounting_transaction_net_amount").cast(DecimalType(16, 2)),
    lit(None).cast(DecimalType(16,2)).alias("accounting_transaction_net_amount"),
    #lit(0.00).alias("total_payable_offset_amount").cast(DecimalType(16, 2)),
    lit(None).cast(DecimalType(16,2)).alias("total_payable_offset_amount"),
    col("data_source_acronym").cast(StringType()),
    col("interest_penalty_amount").cast(DecimalType(16, 2)),
    col("additional_interest_penalty_amount").cast(DecimalType(16, 2)),
    col("duns_plus4_nbr").cast(StringType()),
    col("duns_nbr").cast(StringType()),
    col("sam_unique_entity_identifier").cast(StringType())
    )
debug_df(final_df, "Final DF")
#########################################################################################################
# Write to final zone in parquet format
#########################################################################################################
finalzone = "s3://c108-{}-fpacfsa-final-zone/dmart/fwadm/nps/{}_part3".format(environ, tablename)
finalzone_path = f"{finalzone}/{datestmp}"
print(f"finalzone_path in S3: {finalzone_path}")

# Write the curated DataFrame to the final zone in Parquet format
final_df.write.mode("overwrite").parquet(finalzone_path, compression="snappy")
print(f"Final Data Frame written to S3 at: {finalzone_path}")


'''
try:
    logger.info("Writing Transformed data to Redshift")
    print("Writing Transformed data to Redshift")
    dynf = DynamicFrame.fromDF(final_df, glueContext, "dynf")
    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dynf,
        catalog_connection=rs_catalog_connection,
        connection_options=conn_options,
        redshift_tmp_dir=destination_path
        )
except Exception as e:
  print(f"An error occurred: {e}") # e contains the error message
'''

endtime = datetime.now()
print("Write complete to {}.  Start: {}.  End: {}".format(destination_path, starttime, endtime))
logger.info("Write complete to {}.  Start: {}.  End: {}".format(destination_path, starttime, endtime))
job.commit()

