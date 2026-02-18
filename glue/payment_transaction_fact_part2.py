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
import os
from pyspark.sql.functions import count, col, lit, coalesce, when, concat, current_timestamp, date_format, length, row_number,substring, sum as _sum, trim, max as sparkMax, min as sparkMin, concat_ws, to_timestamp, expr
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
from pyspark.sql.types import DecimalType, ShortType, DoubleType, IntegerType, StringType, StructType, StructField, TimestampType, LongType
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
    "preactions":"truncate table {}.{};".format(schemaname,tablename),
    "aws_iam_role": arnrole,
    "driver": "com.amazon.redshift.jdbc.Driver"
}

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

#Code Start
# datestmp = '20250711' # Tesing Purposes

# Import the source files designate the destination path
#datestmp = '20250916'
qct75_path = "s3://c108-{}-fpacfsa-landing-zone/dmart/raw/fwadm/{}/MQCXTFIL.QCFSAS75.FULL".format(environ, datestmp) #Done
qct42_path = "s3://c108-{}-fpacfsa-landing-zone/dmart/raw/fwadm/{}/MQCXTFIL.QCNPSS42.FULL".format(environ, datestmp) #Done
obligation_path = "s3://c108-{}-fpacfsa-landing-zone/dmart/raw/fwadm/{}/sqlNPS_Obligation1.txt".format(environ, datestmp) #Done
obligation_adjustment_path = "s3://c108-{}-fpacfsa-landing-zone/dmart/raw/fwadm/{}/sqlNPS_Obligation_Adjustment.txt".format(environ, datestmp) #Done
obligation_adjustment_byf_path = "s3://c108-{}-fpacfsa-landing-zone/dmart/raw/fwadm/{}/sqlNPS_Obligation_Adjustment_BFY.txt".format(environ, datestmp) #Done
obligation_byf_path = "s3://c108-{}-fpacfsa-landing-zone/dmart/raw/fwadm/{}/sqlNPS_Obligation_BFY.txt".format(environ, datestmp) #Done
obligation_liquidation_request_path = "s3://c108-{}-fpacfsa-landing-zone/dmart/raw/fwadm/{}/sqlNPS_Obligation_Liquidation_Request1.txt".format(environ, datestmp)
obligation_liquidation_request_byf_path = "s3://c108-{}-fpacfsa-landing-zone/dmart/raw/fwadm/{}/sqlNPS_Obligation_Liquidation_Request_BFY.txt".format(environ, datestmp)
qct05_path = "s3://c108-{}-fpacfsa-landing-zone/dmart/raw/fwadm/CFI/qct05rcv/20250528/qct05rcv_1.parquet".format(environ)

destinationpath = "s3://c108-{}-fpacfsa-cleansed-zone/dmart/fwadm/nps_qa/{}".format(environ, tablename)
destination_path = f"{destinationpath}/{tmstmp}/"

qct75_schema = StructType([
    StructField("acct_ref_id",IntegerType(), False),
    StructField("data_stat_cd",StringType(), False),
    StructField("last_chg_dt",StringType(), False),
    StructField("cre_dt",StringType(), False),
    StructField("last_chg_user_nm",StringType(), False),
    StructField("cre_user_nm",StringType(), False),
    StructField("acct_ref_cd",StringType(), False),
    StructField("acct_ref_desc",StringType(), False)
    ])
    
qct75_df = spark.read.csv(qct75_path, header=False, schema=qct75_schema, sep="\t")
print(f"qct75_df: {qct75_df.count()}")

qct42_schema = StructType([
    StructField("rcv_id",StringType(), False),
    StructField("acct_ref_id",StringType(), False),
    StructField("acct_ref_nbr",StringType(), False),
    StructField("prim_ref_ind",StringType(), False),
    StructField("last_chg_dt",StringType(), False),
    StructField("last_chg_user_nm",StringType(), False)
    ])
    
qct42_df = spark.read.csv(qct42_path, header=False, schema=qct42_schema, sep="\t")
qct42_df = qct42_df.select(
    col("rcv_id").cast(IntegerType()),
    col("acct_ref_id").cast(IntegerType()),
    col("acct_ref_nbr"),
    col("prim_ref_ind"),
    col("last_chg_dt"),
    col("last_chg_user_nm")
    )   
print(f"qct42_df: {qct42_df.count()}")

obligation_schema = StructType([
    StructField("obl_id",IntegerType(), False),
    StructField("st_fsa_cd",StringType(), False),
    StructField("cnty_fsa_cd",StringType(), False),
    StructField("acct_pgm_cd",StringType(), False),
    StructField("acct_ref_1_cd",StringType(), False),
    StructField("acct_ref_1_nbr",StringType(), False),
    StructField("bdgt_fscl_yr",IntegerType(), False),
    StructField("obl_apvl_dt",TimestampType(), False),
    StructField("orgn_obl_amt",DecimalType(16,2), False),
    StructField("obl_amt",DecimalType(16,2), False),
    StructField("acct_txn_cd",StringType(), False),
    StructField("cmdy_cd",StringType(), False),
    StructField("acct_pgm_yr",StringType(), False),
    StructField("sys_cd",StringType(), False),
    StructField("bus_pty_id",StringType(), False),
    StructField("acct_ref_2_cd",StringType(), False),
    StructField("acct_ref_2_nbr",StringType(), False),
    StructField("acct_ref_3_cd",StringType(), False),
    StructField("acct_ref_3_nbr",StringType(), False),
    StructField("acct_ref_4_cd",StringType(), False),
    StructField("acct_ref_4_nbr",StringType(), False),
    StructField("acct_ref_5_cd",StringType(), False),
    StructField("acct_ref_5_nbr",StringType(), False),
    StructField("cre_user_nm",StringType(), False),
    StructField("last_chg_user_nm",StringType(), False),
    StructField("last_chg_dt",TimestampType(), False),
    StructField("txn_rqst_id",StringType(), False),
    StructField("cre_dt",TimestampType(), False),
    StructField("core_acct_prd",StringType(), False),
    StructField("cmnt_txt",StringType(), False),
    StructField("chrt_pgm_yr",StringType(), False),
    StructField("obl_bal_amt",DecimalType(16,2), False),
    StructField("obl_stat_cd",StringType(), False),
    StructField("acct_trnsm_dt",TimestampType(), False),
    StructField("data_src_acro",StringType(), False),
    StructField("bfy_obl_amt",DecimalType(), False),
    StructField("fund_alot_id",IntegerType(), False),
    StructField("bfy_obl_bal_amt",DecimalType(16,2), False),
    StructField("dobl_rqr_ind",StringType(), False)
    ])
obligation_df = spark.read.csv(obligation_path, header=False, schema=obligation_schema, sep="\t")
print(f"obligation_df: {obligation_df.count()}")

obligation_adjustment_schema = StructType([
    StructField("adj_obl_id",IntegerType(), False),
    StructField("obl_id",IntegerType(), False),
    StructField("obl_evnt_type_cd",StringType(), False),
    StructField("adj_obl_amt",DecimalType(16,2), False),
    StructField("acct_txn_cd",StringType(), False),
    StructField("obl_pymt_ind",StringType(), False),
    StructField("txn_rqst_id",StringType(), False),
    StructField("sys_cd",StringType(), False),
    StructField("acct_pgm_yr",StringType(), False),
    StructField("adj_obl_apvl_dt",TimestampType(), False),
    StructField("pybl_id",IntegerType(), False),
    StructField("acct_trnsm_dt",TimestampType(), False),
    StructField("core_acct_prd",StringType(), False),
    StructField("cre_dt",TimestampType(), False),
    StructField("cre_user_nm",StringType(), False),
    StructField("last_chg_user_nm",StringType(), False),
    StructField("last_chg_dt",TimestampType(), False),
    StructField("rvrs_ind",StringType(), False),
    StructField("bdgt_fscl_yr",IntegerType(), False),
    StructField("bfy_adj_obl_amt",DecimalType(16,2), False),
    StructField("fund_alot_id",IntegerType(), False)
    ])
    
obligation_adjustment_df = spark.read.csv(obligation_adjustment_path, header=False, schema=obligation_adjustment_schema, sep="\t")
print(f"obligation_adjustment_df: {obligation_adjustment_df.count()}")

obligation_adjustment_byf_schema = StructType([
    StructField("obl_adj_bfy_id",IntegerType(), False),
    StructField("fund_alot_id",IntegerType(), False),
    StructField("adj_obl_id",IntegerType(), False),
    StructField("bdgt_fscl_yr",IntegerType(), False),
    StructField("bfy_adj_obl_amt",DecimalType(16,2), False),
    StructField("acct_txn_cd",StringType(), False),
    StructField("acct_trnsm_dt",TimestampType(), False),
    StructField("cre_dt",TimestampType(), False),
    StructField("cre_user_nm",StringType(), False),
    StructField("last_chg_dt",TimestampType(), False),
    StructField("last_chg_user_nm",StringType(), False)
    ])
    
obligation_adjustment_byf_df = spark.read.csv(obligation_adjustment_byf_path, header=False, schema=obligation_adjustment_byf_schema, sep="\t")
print(f"obligation_adjustment_byf_df: {obligation_adjustment_byf_df.count()}")

obligation_byf_schema = StructType([
    StructField("obl_bfy_id",IntegerType(), False),
    StructField("fund_alot_id",IntegerType(), False),
    StructField("obl_id",IntegerType(), False),
    StructField("bdgt_fscl_yr",IntegerType(), False),
    StructField("bfy_obl_amt",DecimalType(16,2), False),
    StructField("bfy_obl_bal_amt",DecimalType(16,2), False),
    StructField("cre_dt",TimestampType(), False),
    StructField("cre_user_nm",StringType(), False),
    StructField("last_chg_dt",TimestampType(), False),
    StructField("last_chg_user_nm",StringType(), False)
    ])
    
obligation_byf_df = spark.read.csv(obligation_byf_path, header=False, schema=obligation_byf_schema, sep="\t")
print(f"obligation_byf_df: {obligation_byf_df.count()}")

obligation_liquidation_request_schema = StructType([
    StructField("obl_lqd_rqst_id",IntegerType(), False),
    StructField("obl_id",IntegerType(), False),
    StructField("pybl_id",IntegerType(), False),
    StructField("txn_rqst_id",StringType(), False),
    StructField("sys_cd",StringType(), False),
    StructField("acct_pgm_yr",StringType(), False),
    StructField("acct_svc_rqst_dt",TimestampType(), False),
    StructField("txn_amt",DecimalType(16,2), False),
    StructField("obl_pymt_ind",StringType(), False),
    StructField("lqd_stat_cd",StringType(), False),
    StructField("cre_dt",TimestampType(), False),
    StructField("cre_user_nm",StringType(), False),
    StructField("last_chg_dt",TimestampType(), False),
    StructField("last_chg_user_nm",StringType(), False),
    StructField("rvrs_ind",StringType(), False),
    StructField("bdgt_fscl_yr",IntegerType(), False),
    StructField("bfy_txn_amt",DecimalType(16,2), False),
    StructField("fund_ctl_evnt_type",StringType(), False)
    ])
    
obligation_liquidation_request_df = spark.read.csv(obligation_liquidation_request_path, header=False, schema=obligation_liquidation_request_schema, sep="\t")
print(f"obligation_liquidation_request_df: {obligation_liquidation_request_df.count()}")

obligation_liquidation_request_byf_schema = StructType([
    StructField("obl_lqd_bfy_id",IntegerType(), False),
    StructField("obl_lqd_rqst_id",IntegerType(), False),
    StructField("bdgt_fscl_yr",IntegerType(), False),
    StructField("bfy_txn_amt",DecimalType(16,2), False),
    StructField("cre_dt",TimestampType(), False),
    StructField("cre_user_nm",StringType(), False),
    StructField("last_chg_dt",TimestampType(), False),
    StructField("last_chg_user_nm",StringType(), False),
    StructField("fund_ctl_evnt_type",StringType(), False)
    ])
    
obligation_liquidation_request_byf_df = spark.read.csv(obligation_liquidation_request_byf_path, header=False, schema=obligation_liquidation_request_byf_schema, sep="\t")
print(f"obligation_liquidation_request_byf_df: {obligation_liquidation_request_byf_df.count()}")

qct05_schema = StructType([
    StructField("rcv_id",IntegerType(), False),
    StructField("sys_cd",StringType(), False),
    StructField("acct_pgm_id",IntegerType(), False),
    StructField("int_acct_pgm_id",IntegerType(), False),
    StructField("st_fsa_cd",StringType(), False),
    StructField("cnty_fsa_cd",StringType(), False),
    StructField("lgcy_rcv_nbr",StringType(), False),
    StructField("rcv_stat_cd",StringType(), False),
    StructField("rcv_grp_nbr",IntegerType(), False),
    StructField("rcv_amt",DecimalType(16,2), False),
    StructField("rcv_due_dt",TimestampType(), False),
    StructField("init_ntfy_dt",TimestampType(), False),
    StructField("debt_rsn_cd",StringType(), False),
    StructField("debt_dcvr_cd",StringType(), False),
    StructField("debt_aud_nbr",StringType(), False),
    StructField("txn_rqst_id",StringType(), False),
    StructField("acct_txn_dt",TimestampType(), False),
    StructField("acct_svc_rqst_dt",StringType(), False),
    StructField("int_rt",DecimalType(7,4), False),
    StructField("int_strt_dt",TimestampType(), False),
    StructField("bia_rqst_ind",StringType(), False),
    StructField("last_chg_dt",StringType(), False),
    StructField("last_chg_user_nm",StringType(), False),
    StructField("acct_pgm_yr",StringType(), False),
    StructField("pybl_id",IntegerType(), False)
    ])
    
qct05_df = spark.read.parquet(qct05_path, header=False, inferschema=True, sep="\t")
print(f"qct05_df: {qct05_df.count()}")
# qct05_df.printSchema()
# qct05_df.show()

# r1
r1_raw = (
    qct42_df \
    .join(qct75_df, qct42_df["acct_ref_id"] == qct75_df["acct_ref_id"], "inner") \
    .select(
        qct42_df["acct_ref_nbr"],
        qct75_df["acct_ref_cd"],
        qct42_df["rcv_id"],
    )
)

window = Window.orderBy("rcv_id", "acct_ref_nbr", "acct_ref_cd")
r1 = (
    r1_raw.withColumn("id", row_number().over(window))
          .withColumn("order_num", lit(1))
          .select("id", "acct_ref_nbr", "acct_ref_cd", "rcv_id", "order_num")
)
# print(f"r1: {r1.count()}")
#r2
r2 = r1.groupBy("rcv_id").agg(count("*").alias("cnt"))
# print(f"r2: {r2.count()}")
#r3
r3 = r1.groupBy("rcv_id").agg(sparkMin("id").alias("min_id"))
# print(f"r3: {r3.count()}")

# r4
r1_not_r3 = r1.join(r3.select("min_id"), r1["id"] == r3["min_id"], how="left_anti")
r4 = r1_not_r3.groupBy("rcv_id").agg(sparkMin("id").alias("min_id"))
# print(f"r4 {r4.count()}")
# r5
r1_not_r3_r4 = r1_not_r3.join(r4.select("min_id"), r1_not_r3["id"] == r4["min_id"], how="left_anti")
r5 = r1_not_r3_r4.groupBy("rcv_id").agg(sparkMin("id").alias("min_id"))
# print(f"r5: {r5.count()}")
# r6
r1_not_r3_r4_r5 = r1_not_r3_r4.join(r5.select("min_id"), r1_not_r3_r4["id"] == r5["min_id"], how="left_anti")
r6 = r1_not_r3_r4_r5.groupBy("rcv_id").agg(sparkMin("id").alias("min_id"))
# print(f"r6: {r6.count()}")
# r7
r1_not_r3_r4_r5_r6 = r1_not_r3_r4_r5.join(r6.select("min_id"), r1_not_r3_r4_r5["id"] == r6["min_id"], how="left_anti")
r7 = r1_not_r3_r4_r5_r6.groupBy("rcv_id").agg(sparkMin("id").alias("min_id"))
# print(f"r7: {r7.count()}")
# r8
r8 = (
    r2.alias("r2") \
    .join(r3, "rcv_id") \
    .join(r1.selectExpr("id as id1", "acct_ref_nbr as acct_ref_nbr1", "acct_ref_cd as acct_ref_cd1", "rcv_id"), r3["min_id"] == col("id1")) \
    .join(r4.alias("r4"), "rcv_id", "left") \
    .join(r1.selectExpr("id as id2", "acct_ref_nbr as acct_ref_nbr2", "acct_ref_cd as acct_ref_cd2", "rcv_id"), col("r4.min_id") == col("id2"), "left") \
    .join(r5.alias("r5"), "rcv_id", "left") \
    .join(r1.selectExpr("id as id3", "acct_ref_nbr as acct_ref_nbr3", "acct_ref_cd as acct_ref_cd3", "rcv_id"), col("r5.min_id") == col("id3"), "left") \
    .join(r6.alias("r6"), "rcv_id", "left") \
    .join(r1.selectExpr("id as id4", "acct_ref_nbr as acct_ref_nbr4", "acct_ref_cd as acct_ref_cd4", "rcv_id"), col("r6.min_id") == col("id4"), "left") \
    .join(r7.alias("r7"), "rcv_id", "left") \
    .join(r1.selectExpr("id as id5", "acct_ref_nbr as acct_ref_nbr5", "acct_ref_cd as acct_ref_cd5", "rcv_id"), col("r7.min_id") == col("id5"), "left") \
    .select(
        col("r2.rcv_id"), 
        col("r2.cnt"),
        col("acct_ref_nbr1").alias("accounting_reference_1_number"),
        col("acct_ref_cd1").alias("accounting_reference_1_code"),
        col("acct_ref_nbr2").alias("accounting_reference_2_number"),
        col("acct_ref_cd2").alias("accounting_reference_2_code"),
        col("acct_ref_nbr3").alias("accounting_reference_3_number"),
        col("acct_ref_cd3").alias("accounting_reference_3_code"),
        col("acct_ref_nbr4").alias("accounting_reference_4_number"),
        col("acct_ref_cd4").alias("accounting_reference_4_code"),
        col("acct_ref_nbr5").alias("accounting_reference_5_number"),
        col("acct_ref_cd5").alias("accounting_reference_5_code")
    )
)
# print(f"r8: {r8.count()}")

df_type_97 = (
    obligation_adjustment_df.alias("obl_adj")
    .join(
        obligation_df.alias("obl"),
        col("obl.obl_id") == col("obl_adj.obl_id"),
        how="left"
    )
    .select(
        lit(97).cast(IntegerType()).alias("payment_transaction_type_identifier"),
        lit(None).alias("accounting_customer_identifier"),
        lit(None).alias("accounting_program_identifier"),
        lit(None).alias("sampling_identifier"),
        lit(None).alias("national_joint_payment_identifier"),
        lit(None).alias("accounting_payee_type_code"),
        lit(None).cast(TimestampType()).alias("accounting_service_request_date"),
        lit(None).cast(TimestampType()).alias("accounting_transaction_date"),
        col("obl_adj.acct_txn_cd").alias("accounting_transaction_code"),
        col("obl_adj.bdgt_fscl_yr").alias("budget_fiscal_year"),
        lit(None).cast(TimestampType()).alias("prompt_payment_interest_date"),
        lit('').alias("payable_status_code"),
        col("obl.st_fsa_cd").alias("state_fsa_code"),
        col("obl.cnty_fsa_cd").alias("county_fsa_code"),
        col("obl_adj.acct_pgm_yr").alias("accounting_program_year"),
        col("obl.acct_pgm_cd").alias("accounting_program_code"),
        col("obl.acct_ref_1_cd").alias("accounting_reference_1_code"),
        col("obl.acct_ref_1_nbr").alias("accounting_reference_1_number"),
        col("obl.acct_ref_2_cd").alias("accounting_reference_2_code"),
        col("obl.acct_ref_2_nbr").alias("accounting_reference_2_number"),
        lit('').alias("accounting_reference_3_code"),
        lit('').alias("accounting_reference_3_number"),
        lit('').alias("accounting_reference_4_code"),
        lit('').alias("accounting_reference_4_number"),
        lit('').alias("accounting_reference_5_code"),
        lit('').alias("accounting_reference_5_number"),
        col("obl.cmdy_cd").alias("commodity_code"),
        col("obl.bus_pty_id").alias("business_party_identifier"),
        lit('').alias("tax_identification"),
        lit('').alias("tax_identification_type_code"),
        lit('').alias("program_alpha_code"),
        lit(0).alias("disbursement_amount"),
        #lit(0).alias("disbursement_identifier"),
        lit(None).cast(IntegerType()).alias("disbursement_identifier"),
        lit(None).cast(TimestampType()).alias("disbursement_date"),
        lit(None).cast(TimestampType()).alias("disbursement_status_date"),
        lit('').alias("disbursement_status_code"),
        lit('').alias("last_disbursement_status_code"),
        lit('').alias("disbursement_type_code"),
        #lit(0).alias("disbursement_confirmation_identifier"),
        lit(None).cast(IntegerType()).alias("disbursement_confirmation_identifier"),
        lit('').alias("external_disbursement_identification"),
        lit('').alias("external_disbursement_type_code"),
        lit('').alias("payable_completed_indicator"),
        lit(0).alias("payable_offset_amount"),
        #lit(0).alias("receivable_identifier"),
        lit(None).cast(IntegerType()).alias("receivable_identifier"),
        col("obl_adj.pybl_id").alias("payable_identifier"),
        lit(0).alias("receivable_amount"),
        lit('').alias("offset_override_reason_code"),
        lit('').alias("claim_identifier"),
        lit(None).cast(TimestampType()).alias("final_review_date"),
        lit(0).alias("payment_request_amount"),
        lit(0).alias("prompt_payment_interest_amount"),
        lit(0).alias("tax_withholding_amount"),
        lit('').alias("legacy_receivable_number"),
        lit('').alias("offset_status_code"),
        #lit(0).alias("national_assignment_identifier"),
        lit(None).cast(IntegerType()).alias("national_assignment_identifier"),
        #lit(0).alias("assignee_accounting_customer_identifier"),
        lit(None).cast(IntegerType()).alias("assignee_accounting_customer_identifier"),
        lit(0).alias("assignment_amount"),
        lit(0).alias("assignment_paid_amount"),
        lit(0.00).alias("accounting_charge_amount"),
        col("obl_adj.obl_id").alias("obligation_identifier"),
        lit('').alias("obligation_full_partial_paid_indicator"),
        lit(0).alias("obligation_amount"),
        col("obl.obl_apvl_dt").alias("obligation_approval_date"),
        lit(0).alias("obligation_balance_amount"),
        substring(col("obl.obl_stat_cd"),1,1).alias("obligation_status_code"),
        lit('').alias("core_accounting_period"),
        lit(0).alias("accounting_transaction_quantity"),
        lit('').alias("unit_of_measure_code"),
        lit('').alias("program_category_code"),
        lit('').alias("core_program_type_code"),
        col("obl_adj.adj_obl_id").alias("adjusted_obligation_identifier"),
        col("obl_adj.sys_cd").alias("system_code"),
        concat(col("obl.st_fsa_cd"), col("obl.cnty_fsa_cd")).alias("core_organization_code"),
        #lit(0).alias("obligation_liquidation_request_identifier"),
        lit(None).cast(IntegerType()).alias("obligation_liquidation_request_identifier"),
        col("obl_adj.cre_dt").alias("creation_datetime"),
        date_format(col("obl_adj.cre_dt"), "yyyy-MM-dd").alias("creation_date"),
        col("obl_adj.rvrs_ind").alias("reversal_indicator"),
        when(
            ((col("obl_adj.acct_txn_cd").isin(320, 340)) & (col("obl_adj.rvrs_ind") == 'Y')) |
            ((col("obl_adj.acct_txn_cd").isin(350, 370)) & (col("obl_adj.rvrs_ind") == 'N')) |
            ((col("obl_adj.acct_txn_cd") == 325) & (col("obl_adj.rvrs_ind") == 'Y')),
            col("obl_adj.adj_obl_amt") * -1
        ).otherwise(col("obl_adj.adj_obl_amt")).alias("obligation_transaction_amount"),
        lit(None).cast(TimestampType()).alias("payment_received_date"),
        lit(None).cast(TimestampType()).alias("payment_issue_date"),
        col("obl_adj.acct_trnsm_dt").alias("accounting_transmission_date"),
        when(
            ((col("obl_adj.acct_txn_cd").isin(320, 340)) & (col("obl_adj.rvrs_ind") == 'Y')) |
            ((col("obl_adj.acct_txn_cd").isin(350, 370)) & (col("obl_adj.rvrs_ind") == 'N')) |
            ((col("obl_adj.acct_txn_cd") == 325) & (col("obl_adj.rvrs_ind") == 'Y')),
            col("obl_adj.bfy_adj_obl_amt") * -1
        ).otherwise(col("obl_adj.bfy_adj_obl_amt")).alias("bfy_obligation_transaction_amount"),
        lit(0).alias("bfy_obligation_amount"),
        lit(0).alias("bfy_obligation_balance_amount"),
        col("obl_adj.fund_alot_id").alias("fund_allotment_identifier"),
        #lit(0).alias("obligation_budget_fiscal_year_identifier"),
        lit(None).cast(IntegerType()).alias("obligation_budget_fiscal_year_identifier"),
        #lit(0).alias("obligation_adjustment_budget_fiscal_year_identifier"),
        lit(None).cast(IntegerType()).alias("obligation_adjustment_budget_fiscal_year_identifier"),
        #lit(0).alias("obligation_liquidation_request_budget_fiscal_year_identifier"),
        lit(None).cast(IntegerType()).alias("obligation_liquidation_request_budget_fiscal_year_identifier"),
        #lit(0.00).alias("accounting_transaction_net_amount"),
        lit(None).cast(DecimalType(16,2)).alias("accounting_transaction_net_amount"),
        #lit(0.00).alias("total_payable_offset_amount"),
        lit(None).cast(DecimalType(16,2)).alias("total_payable_offset_amount"),
        col("obl.data_src_acro").alias("data_source_acronym"),
        lit(0.00).alias("interest_penalty_amount"),
        lit(0.00).alias("additional_interest_penalty_amount"),
        #lit('').alias("duns_plus4_nbr"),
        lit(None).cast(StringType()).alias("duns_plus4_nbr"),
        #lit('').alias("duns_nbr"),
        lit(None).cast(StringType()).alias("duns_nbr"),
        lit('').alias("sam_unique_entity_identifier")
    )
)
# debug_df(df_type_97, "df_type_97")

# Type 98: Obligation Liquidation
df_type_98 = (
    obligation_liquidation_request_df.alias("obl_liq")
    .join(
        obligation_df.alias("obl"),
        col("obl.obl_id") == col("obl_liq.obl_id"),
        how="left"
    )
.select(
        lit(98).cast(IntegerType()).alias("payment_transaction_type_identifier"),
        lit(None).alias("accounting_customer_identifier"),
        lit(None).alias("accounting_program_identifier"),
        lit(None).alias("sampling_identifier"),
        lit(None).alias("national_joint_payment_identifier"),
        lit(None).alias("accounting_payee_type_code"),
        col("obl_liq.acct_svc_rqst_dt").alias("accounting_service_request_date"),
        lit(None).cast(TimestampType()).alias("accounting_transaction_date"),
        lit(360).alias("accounting_transaction_code"),
        col("obl_liq.bdgt_fscl_yr").alias("budget_fiscal_year"),
        lit(None).cast(TimestampType()).alias("prompt_payment_interest_date"),
        lit('').alias("payable_status_code"),
        col("obl.st_fsa_cd").alias("state_fsa_code"),
        col("obl.cnty_fsa_cd").alias("county_fsa_code"),
        col("obl_liq.acct_pgm_yr").alias("accounting_program_year"),
        col("obl.acct_pgm_cd").alias("accounting_program_code"),
        col("obl.acct_ref_1_cd").alias("accounting_reference_1_code"),
        col("obl.acct_ref_1_nbr").alias("accounting_reference_1_number"),
        col("obl.acct_ref_2_cd").alias("accounting_reference_2_code"),
        col("obl.acct_ref_2_nbr").alias("accounting_reference_2_number"),
        lit('').alias("accounting_reference_3_code"),
        lit('').alias("accounting_reference_3_number"),
        lit('').alias("accounting_reference_4_code"),
        lit('').alias("accounting_reference_4_number"),
        lit('').alias("accounting_reference_5_code"),
        lit('').alias("accounting_reference_5_number"),
        col("obl.cmdy_cd").alias("commodity_code"),
        col("obl.bus_pty_id").alias("business_party_identifier"),
        lit('').alias("tax_identification"),
        lit('').alias("tax_identification_type_code"),
        lit('').alias("program_alpha_code"),
        lit(0).alias("disbursement_amount"),
        #lit(0).alias("disbursement_identifier"),
        lit(None).cast(IntegerType()).alias("disbursement_identifier"),
        lit(None).cast(TimestampType()).alias("disbursement_date"),
        lit(None).cast(TimestampType()).alias("disbursement_status_date"),
        lit('').alias("disbursement_status_code"),
        lit('').alias("last_disbursement_status_code"),
        lit('').alias("disbursement_type_code"),
        #lit(0).alias("disbursement_confirmation_identifier"),
        lit(None).cast(IntegerType()).alias("disbursement_confirmation_identifier"),
        lit('').alias("external_disbursement_identification"),
        lit('').alias("external_disbursement_type_code"),
        lit('').alias("payable_completed_indicator"),
        lit(0).alias("payable_offset_amount"),
        #lit(0).alias("receivable_identifier"),
        lit(None).cast(IntegerType()).alias("receivable_identifier"),
        col("obl_liq.pybl_id").alias("payable_identifier"),
        lit(0).alias("receivable_amount"),
        lit('').alias("offset_override_reason_code"),
        lit('').alias("claim_identifier"),
        lit(None).cast(TimestampType()).alias("final_review_date"),
        lit(0).alias("payment_request_amount"),
        lit(0).alias("prompt_payment_interest_amount"),
        lit(0).alias("tax_withholding_amount"),
        lit('').alias("legacy_receivable_number"),
        lit('').alias("offset_status_code"),
        #lit(0).alias("national_assignment_identifier"),
        lit(None).cast(IntegerType()).alias("national_assignment_identifier"),
        #lit(0).alias("assignee_accounting_customer_identifier"),
        lit(None).cast(IntegerType()).alias("assignee_accounting_customer_identifier"),
        lit(0).alias("assignment_amount"),
        lit(0).alias("assignment_paid_amount"),
        lit(0.00).alias("accounting_charge_amount"),
        col("obl_liq.obl_id").alias("obligation_identifier"),
        col("obl_liq.obl_pymt_ind").alias("obligation_full_partial_paid_indicator"),
        lit(0).alias("obligation_amount"),
        col("obl.obl_apvl_dt").alias("obligation_approval_date"),
        lit(0).alias("obligation_balance_amount"),
        substring(col("obl.obl_stat_cd"),1,1).alias("obligation_status_code"),
        date_format(expr("add_months(obl_liq.cre_dt, 3)"), "yyyyMM").alias("core_accounting_period"),
        lit(0).alias("accounting_transaction_quantity"),
        lit('').alias("unit_of_measure_code"),
        lit('').alias("program_category_code"),
        lit('').alias("core_program_type_code"),
        lit(0).alias("adjusted_obligation_identifier"),
        col("obl_liq.sys_cd").alias("system_code"),
        concat(col("obl.st_fsa_cd"), col("obl.cnty_fsa_cd")).alias("core_organization_code"),
        col("obl_liq.obl_lqd_rqst_id").alias("obligation_liquidation_request_identifier"),
        col("obl_liq.cre_dt").alias("creation_datetime"),
        date_format(col("obl_liq.cre_dt"), "yyyy-MM-dd").alias("creation_date"),
        col("obl_liq.rvrs_ind").alias("reversal_indicator"),
        when(col("obl_liq.rvrs_ind") == "N", col("obl_liq.txn_amt") * -1).otherwise(col("obl_liq.txn_amt")).alias("obligation_transaction_amount"),
        lit(None).cast(TimestampType()).alias("payment_received_date"),
        lit(None).cast(TimestampType()).alias("payment_issue_date"),
        lit(None).cast(TimestampType()).alias("accounting_transmission_date"),
        when(col("obl_liq.rvrs_ind") == "N", col("obl_liq.bfy_txn_amt") * -1).otherwise(col("obl_liq.bfy_txn_amt")).alias("bfy_obligation_transaction_amount"),
        lit(0).alias("bfy_obligation_amount"),
        lit(0).alias("bfy_obligation_balance_amount"),
        lit(0).alias("fund_allotment_identifier"),
        #lit(0).alias("obligation_budget_fiscal_year_identifier"),
        lit(None).cast(IntegerType()).alias("obligation_budget_fiscal_year_identifier"),
        #lit(0).alias("obligation_adjustment_budget_fiscal_year_identifier"),
        lit(None).cast(IntegerType()).alias("obligation_adjustment_budget_fiscal_year_identifier"),
        lit(0).alias("obligation_liquidation_request_budget_fiscal_year_identifier"),
        #lit(0.00).alias("accounting_transaction_net_amount"),
        lit(None).cast(DecimalType(16,2)).alias("accounting_transaction_net_amount"),
        #lit(0.00).alias("total_payable_offset_amount"),
        lit(None).cast(DecimalType(16,2)).alias("total_payable_offset_amount"),
        col("obl.data_src_acro").alias("data_source_acronym"),
        lit(0.00).alias("interest_penalty_amount"),
        lit(0.00).alias("additional_interest_penalty_amount"),
        #lit('').alias("duns_plus4_nbr"),
        lit(None).cast(StringType()).alias("duns_plus4_nbr"),
        #lit('').alias("duns_nbr"),
        lit(None).cast(StringType()).alias("duns_nbr"),
        lit('').alias("sam_unique_entity_identifier")
    )
)
# debug_df(df_type_98, "df_type_98")

# Type 96: Obligation
df_type_96 = (
    obligation_df
    .select(
        lit(96).cast(IntegerType()).alias("payment_transaction_type_identifier"),
        lit(None).alias("accounting_customer_identifier"),
        lit(None).alias("accounting_program_identifier"),
        lit(None).alias("sampling_identifier"),
        lit(None).alias("national_joint_payment_identifier"),
        lit(None).alias("accounting_payee_type_code"),
        lit(None).cast(TimestampType()).alias("accounting_service_request_date"),
        lit(None).cast(TimestampType()).alias("accounting_transaction_date"),
        col("acct_txn_cd").alias("accounting_transaction_code"),
        col("bdgt_fscl_yr").alias("budget_fiscal_year"),
        lit(None).cast(TimestampType()).alias("prompt_payment_interest_date"),
        lit('').alias("payable_status_code"),
        col("st_fsa_cd").alias("state_fsa_code"),
        col("cnty_fsa_cd").alias("county_fsa_code"),
        col("acct_pgm_yr").alias("accounting_program_year"),
        col("acct_pgm_cd").alias("accounting_program_code"),
        col("acct_ref_1_cd").alias("accounting_reference_1_code"),
        col("acct_ref_1_nbr").alias("accounting_reference_1_number"),
        col("acct_ref_2_cd").alias("accounting_reference_2_code"),
        col("acct_ref_2_nbr").alias("accounting_reference_2_number"),
        lit('').alias("accounting_reference_3_code"),
        lit('').alias("accounting_reference_3_number"),
        lit('').alias("accounting_reference_4_code"),
        lit('').alias("accounting_reference_4_number"),
        lit('').alias("accounting_reference_5_code"),
        lit('').alias("accounting_reference_5_number"),
        col("cmdy_cd").alias("commodity_code"),
        col("bus_pty_id").alias("business_party_identifier"),
        lit('').alias("tax_identification"),
        lit('').alias("tax_identification_type_code"),
        lit('').alias("program_alpha_code"),
        lit(0).alias("disbursement_amount"),
        #lit(0).alias("disbursement_identifier"),
        lit(None).cast(IntegerType()).alias("disbursement_identifier"),
        lit(None).cast(TimestampType()).alias("disbursement_date"),
        lit(None).cast(TimestampType()).alias("disbursement_status_date"),
        lit('').alias("disbursement_status_code"),
        lit('').alias("last_disbursement_status_code"),
        lit('').alias("disbursement_type_code"),
        #lit(0).alias("disbursement_confirmation_identifier"),
        lit(None).cast(IntegerType()).alias("disbursement_confirmation_identifier"),
        lit('').alias("external_disbursement_identification"),
        lit('').alias("external_disbursement_type_code"),
        lit('').alias("payable_completed_indicator"),
        lit(0).alias("payable_offset_amount"),
        #lit(0).alias("receivable_identifier"),
        lit(None).cast(IntegerType()).alias("receivable_identifier"),
        lit(0).alias("payable_identifier"),
        lit(0).alias("receivable_amount"),
        lit('').alias("offset_override_reason_code"),
        lit('').alias("claim_identifier"),
        lit(None).cast(TimestampType()).alias("final_review_date"),
        lit(0).alias("payment_request_amount"),
        lit(0).alias("prompt_payment_interest_amount"),
        lit(0).alias("tax_withholding_amount"),
        lit('').alias("legacy_receivable_number"),
        lit('').alias("offset_status_code"),
        #lit(0).alias("national_assignment_identifier"),
        lit(None).cast(IntegerType()).alias("national_assignment_identifier"),
        #lit(0).alias("assignee_accounting_customer_identifier"),
        lit(None).cast(IntegerType()).alias("assignee_accounting_customer_identifier"),
        lit(0).alias("assignment_amount"),
        lit(0).alias("assignment_paid_amount"),
        lit(0.00).alias("accounting_charge_amount"),
        col("obl_id").alias("obligation_identifier"),
        lit('').alias("obligation_full_partial_paid_indicator"),
        col("obl_amt").alias("obligation_amount"),
        col("obl_apvl_dt").alias("obligation_approval_date"),
        col("obl_bal_amt").alias("obligation_balance_amount"),
        substring(col("obl_stat_cd"),1,1).alias("obligation_status_code"),
        col("core_acct_prd").alias("core_accounting_period"),
        lit(0).alias("accounting_transaction_quantity"),
        lit('').alias("unit_of_measure_code"),
        lit('').alias("program_category_code"),
        lit('').alias("core_program_type_code"),
        lit(0).alias("adjusted_obligation_identifier"),
        col("sys_cd").alias("system_code"),
        concat(col("st_fsa_cd"), col("cnty_fsa_cd")).alias("core_organization_code"),
        #lit(0).alias("obligation_liquidation_request_identifier"),
        lit(None).cast(IntegerType()).alias("obligation_liquidation_request_identifier"),
        col("cre_dt").alias("creation_datetime"),
        date_format(col("cre_dt"), "yyyy-MM-dd").alias("creation_date"),
        lit('').alias("reversal_indicator"),
        col("orgn_obl_amt").alias("obligation_transaction_amount"),
        lit(None).cast(TimestampType()).alias("payment_received_date"),
        lit(None).cast(TimestampType()).alias("payment_issue_date"),
        col("acct_trnsm_dt").alias("accounting_transmission_date"),
        col("orgn_obl_amt").alias("bfy_obligation_transaction_amount"),
        col("bfy_obl_amt").alias("bfy_obligation_amount"),
        col("bfy_obl_bal_amt").alias("bfy_obligation_balance_amount"),
        col("fund_alot_id").alias("fund_allotment_identifier"),
        #lit(0).alias("obligation_budget_fiscal_year_identifier"),
        lit(None).cast(IntegerType()).alias("obligation_budget_fiscal_year_identifier"),
        #lit(0).alias("obligation_adjustment_budget_fiscal_year_identifier"),
        lit(None).cast(IntegerType()).alias("obligation_adjustment_budget_fiscal_year_identifier"),
        #lit(0).alias("obligation_liquidation_request_budget_fiscal_year_identifier"),
        lit(None).cast(IntegerType()).alias("obligation_liquidation_request_budget_fiscal_year_identifier"),
        #lit(0.00).alias("accounting_transaction_net_amount"),
        lit(None).cast(DecimalType(16,2)).alias("accounting_transaction_net_amount"),
        #lit(0.00).alias("total_payable_offset_amount"),
        lit(None).cast(DecimalType(16,2)).alias("total_payable_offset_amount"),
        col("data_src_acro").alias("data_source_acronym"),
        lit(0.00).alias("interest_penalty_amount"),
        lit(0.00).alias("additional_interest_penalty_amount"),
        #lit('').alias("duns_plus4_nbr"),
        lit(None).cast(StringType()).alias("duns_plus4_nbr"),
        #lit('').alias("duns_nbr"),
        lit(None).cast(StringType()).alias("duns_nbr"),
        lit('').alias("sam_unique_entity_identifier")
    )
)
# debug_df(df_type_96, "df_type_96")


# Type 197
df_type_197 = (
    obligation_adjustment_byf_df.alias("b")
    .join(obligation_adjustment_df.alias("a"), col("a.adj_obl_id") == col("b.adj_obl_id"))
    .join(obligation_df.alias("o"), col("o.obl_id") == col("a.obl_id"), how="left")
    .select(
        lit(197).cast(IntegerType()).alias("payment_transaction_type_identifier"),
        lit(None).alias("accounting_customer_identifier"),
        lit(None).alias("accounting_program_identifier"),
        lit(None).alias("sampling_identifier"),
        lit(None).alias("national_joint_payment_identifier"),
        lit(None).alias("accounting_payee_type_code"),
        lit(None).cast(TimestampType()).alias("accounting_service_request_date"),
        lit(None).cast(TimestampType()).alias("accounting_transaction_date"),
        col("b.acct_txn_cd").alias("accounting_transaction_code"),
        col("b.bdgt_fscl_yr").alias("budget_fiscal_year"),
        lit(None).cast(TimestampType()).alias("prompt_payment_interest_date"),
        lit('').alias("payable_status_code"),
        col("o.st_fsa_cd").alias("state_fsa_code"),
        col("o.cnty_fsa_cd").alias("county_fsa_code"),
        col("a.acct_pgm_yr").alias("accounting_program_year"),
        col("o.acct_pgm_cd").alias("accounting_program_code"),
        col("o.acct_ref_1_cd").alias("accounting_reference_1_code"),
        col("o.acct_ref_1_nbr").alias("accounting_reference_1_number"),
        col("o.acct_ref_2_cd").alias("accounting_reference_2_code"),
        col("o.acct_ref_2_nbr").alias("accounting_reference_2_number"),
        lit('').alias("accounting_reference_3_code"),
        lit('').alias("accounting_reference_3_number"),
        lit('').alias("accounting_reference_4_code"),
        lit('').alias("accounting_reference_4_number"),
        lit('').alias("accounting_reference_5_code"),
        lit('').alias("accounting_reference_5_number"),
        col("o.cmdy_cd").alias("commodity_code"),
        col("o.bus_pty_id").alias("business_party_identifier"),
        lit('').alias("tax_identification"),
        lit('').alias("tax_identification_type_code"),
        lit('').alias("program_alpha_code"),
        lit(0).alias("disbursement_amount"),
        #lit(0).alias("disbursement_identifier"),
        lit(None).cast(IntegerType()).alias("disbursement_identifier"),
        lit(None).cast(TimestampType()).alias("disbursement_date"),
        lit(None).cast(TimestampType()).alias("disbursement_status_date"),
        lit('').alias("disbursement_status_code"),
        lit('').alias("last_disbursement_status_code"),
        lit('').alias("disbursement_type_code"),
        #lit(0).alias("disbursement_confirmation_identifier"),
        lit(None).cast(IntegerType()).alias("disbursement_confirmation_identifier"),
        lit('').alias("external_disbursement_identification"),
        lit('').alias("external_disbursement_type_code"),
        lit('').alias("payable_completed_indicator"),
        lit(0).alias("payable_offset_amount"),
        #lit(0).alias("receivable_identifier"),
        lit(None).cast(IntegerType()).alias("receivable_identifier"),
        col("a.pybl_id").alias("payable_identifier"),
        lit(0).alias("receivable_amount"),
        lit('').alias("offset_override_reason_code"),
        lit('').alias("claim_identifier"),
        lit(None).cast(TimestampType()).alias("final_review_date"),
        lit(0).alias("payment_request_amount"),
        lit(0).alias("prompt_payment_interest_amount"),
        lit(0).alias("tax_withholding_amount"),
        lit('').alias("legacy_receivable_number"),
        lit('').alias("offset_status_code"),
        #lit(0).alias("national_assignment_identifier"),
        lit(None).cast(IntegerType()).alias("national_assignment_identifier"),
        #lit(0).alias("assignee_accounting_customer_identifier"),
        lit(None).cast(IntegerType()).alias("assignee_accounting_customer_identifier"),
        lit(0).alias("assignment_amount"),
        lit(0).alias("assignment_paid_amount"),
        lit(0.00).alias("accounting_charge_amount"),
        col("a.obl_id").alias("obligation_identifier"),
        lit('').alias("obligation_full_partial_paid_indicator"),
        lit(0).alias("obligation_amount"),
        col("o.obl_apvl_dt").alias("obligation_approval_date"),
        lit(0).alias("obligation_balance_amount"),
        substring(col("o.obl_stat_cd"),1,1).alias("obligation_status_code"),
        lit('').alias("core_accounting_period"),
        lit(0).alias("accounting_transaction_quantity"),
        lit('').alias("unit_of_measure_code"),
        lit('').alias("program_category_code"),
        lit('').alias("core_program_type_code"),
        col("b.adj_obl_id").alias("adjusted_obligation_identifier"),
        col("a.sys_cd").alias("system_code"),
        concat(col("o.st_fsa_cd"), col("o.cnty_fsa_cd")).alias("core_organization_code"),
        #lit(0).alias("obligation_liquidation_request_identifier"),
        lit(None).cast(IntegerType()).alias("obligation_liquidation_request_identifier"),
        col("b.cre_dt").alias("creation_datetime"),
        date_format(col("b.cre_dt"), "yyyy-MM-dd").alias("creation_date"),
        col("a.rvrs_ind").alias("reversal_indicator"),
        when(
            ((col("b.acct_txn_cd").isin(320, 340)) & (col("a.rvrs_ind") == "Y")) |
            ((col("b.acct_txn_cd").isin(350, 370)) & (col("a.rvrs_ind") == "N")) |
            ((col("b.acct_txn_cd") == 325) & (col("a.rvrs_ind") == "Y")),
            col("b.bfy_adj_obl_amt") * -1
        ).otherwise(col("b.bfy_adj_obl_amt")).alias("obligation_transaction_amount"),
        lit(None).cast(TimestampType()).alias("payment_received_date"),
        lit(None).cast(TimestampType()).alias("payment_issue_date"),
        col("b.acct_trnsm_dt").alias("accounting_transmission_date"),
        when(
            ((col("b.acct_txn_cd").isin(320, 340)) & (col("a.rvrs_ind") == "Y")) |
            ((col("b.acct_txn_cd").isin(350, 370)) & (col("a.rvrs_ind") == "N")) |
            ((col("b.acct_txn_cd") == 325) & (col("a.rvrs_ind") == "Y")),
            col("b.bfy_adj_obl_amt") * -1
        ).otherwise(col("b.bfy_adj_obl_amt")).alias("bfy_obligation_transaction_amount"),
        lit(0).alias("bfy_obligation_amount"),
        lit(0).alias("bfy_obligation_balance_amount"),
        col("b.fund_alot_id").alias("fund_allotment_identifier"),
        #lit(0).alias("obligation_budget_fiscal_year_identifier"),
        lit(None).cast(IntegerType()).alias("obligation_budget_fiscal_year_identifier"),
        col("b.obl_adj_bfy_id").alias("obligation_adjustment_budget_fiscal_year_identifier"),
        lit(0).alias("obligation_liquidation_request_budget_fiscal_year_identifier"),
        #lit(0.00).alias("accounting_transaction_net_amount"),
        lit(None).cast(DecimalType(16,2)).alias("accounting_transaction_net_amount"),
        #lit(0.00).alias("total_payable_offset_amount"),
        lit(None).cast(DecimalType(16,2)).alias("total_payable_offset_amount"),
        col("o.data_src_acro").alias("data_source_acronym"),
        lit(0.00).alias("interest_penalty_amount"),
        lit(0.00).alias("additional_interest_penalty_amount"),
        #lit('').alias("duns_plus4_nbr"),
        lit(None).cast(StringType()).alias("duns_plus4_nbr"),
        #lit('').alias("duns_nbr"),
        lit(None).cast(StringType()).alias("duns_nbr"),
        lit('').alias("sam_unique_entity_identifier")
    )
)
# debug_df(df_type_197, "DF 197")

# Type 198
df_type_198 = (
    obligation_liquidation_request_byf_df.alias("b")
    .join(obligation_liquidation_request_df.alias("a"), col("a.obl_lqd_rqst_id") == col("b.obl_lqd_rqst_id"))
    .join(obligation_df.alias("o"), col("o.obl_id") == col("a.obl_id"))
     .select(
        lit(198).cast(IntegerType()).alias("payment_transaction_type_identifier"),
        lit(None).alias("accounting_customer_identifier"),
        lit(None).alias("accounting_program_identifier"),
        lit(None).alias("sampling_identifier"),
        lit(None).alias("national_joint_payment_identifier"),
        lit(None).alias("accounting_payee_type_code"),
        col("a.acct_svc_rqst_dt").alias("accounting_service_request_date"),
        lit(None).cast(TimestampType()).alias("accounting_transaction_date"),
        lit(360).alias("accounting_transaction_code"),
        col("b.bdgt_fscl_yr").alias("budget_fiscal_year"),
        lit(None).cast(TimestampType()).alias("prompt_payment_interest_date"),
        lit('').alias("payable_status_code"),
        col("o.st_fsa_cd").alias("state_fsa_code"),
        col("o.cnty_fsa_cd").alias("county_fsa_code"),
        col("a.acct_pgm_yr").alias("accounting_program_year"),
        col("o.acct_pgm_cd").alias("accounting_program_code"),
        col("o.acct_ref_1_cd").alias("accounting_reference_1_code"),
        col("o.acct_ref_1_nbr").alias("accounting_reference_1_number"),
        col("o.acct_ref_2_cd").alias("accounting_reference_2_code"),
        col("o.acct_ref_2_nbr").alias("accounting_reference_2_number"),
        lit('').alias("accounting_reference_3_code"),
        lit('').alias("accounting_reference_3_number"),
        lit('').alias("accounting_reference_4_code"),
        lit('').alias("accounting_reference_4_number"),
        lit('').alias("accounting_reference_5_code"),
        lit('').alias("accounting_reference_5_number"),
        col("o.cmdy_cd").alias("commodity_code"),
        col("o.bus_pty_id").alias("business_party_identifier"),
        lit('').alias("tax_identification"),
        lit('').alias("tax_identification_type_code"),
        lit('').alias("program_alpha_code"),
        lit(0).alias("disbursement_amount"),
        #lit(0).alias("disbursement_identifier"),
        lit(None).cast(IntegerType()).alias("disbursement_identifier"),
        lit(None).cast(TimestampType()).alias("disbursement_date"),
        lit(None).cast(TimestampType()).alias("disbursement_status_date"),
        lit('').alias("disbursement_status_code"),
        lit('').alias("last_disbursement_status_code"),
        lit('').alias("disbursement_type_code"),
        #lit(0).alias("disbursement_confirmation_identifier"),
        lit(None).cast(IntegerType()).alias("disbursement_confirmation_identifier"),
        lit('').alias("external_disbursement_identification"),
        lit('').alias("external_disbursement_type_code"),
        lit('').alias("payable_completed_indicator"),
        lit(0).alias("payable_offset_amount"),
        #lit(0).alias("receivable_identifier"),
        lit(None).cast(IntegerType()).alias("receivable_identifier"),
        col("a.pybl_id").alias("payable_identifier"),
        lit(0).alias("receivable_amount"),
        lit('').alias("offset_override_reason_code"),
        lit('').alias("claim_identifier"),
        lit(None).cast(TimestampType()).alias("final_review_date"),
        lit(0).alias("payment_request_amount"),
        lit(0).alias("prompt_payment_interest_amount"),
        lit(0).alias("tax_withholding_amount"),
        lit('').alias("legacy_receivable_number"),
        lit('').alias("offset_status_code"),
        #lit(0).alias("national_assignment_identifier"),
        lit(None).cast(IntegerType()).alias("national_assignment_identifier"),
        #lit(0).alias("assignee_accounting_customer_identifier"),
        lit(None).cast(IntegerType()).alias("assignee_accounting_customer_identifier"),
        lit(0).alias("assignment_amount"),
        lit(0).alias("assignment_paid_amount"),
        lit(0.00).alias("accounting_charge_amount"),
        col("a.obl_id").alias("obligation_identifier"),
        col("a.obl_pymt_ind").alias("obligation_full_partial_paid_indicator"),
        lit(0).alias("obligation_amount"),
        col("o.obl_apvl_dt").alias("obligation_approval_date"),
        lit(0).alias("obligation_balance_amount"),
        substring(col("o.obl_stat_cd"),1,1).alias("obligation_status_code"),
        date_format(expr("add_months(b.cre_dt, 3)"), "yyyyMM").alias("core_accounting_period"),
        lit(0).alias("accounting_transaction_quantity"),
        lit('').alias("unit_of_measure_code"),
        lit('').alias("program_category_code"),
        lit('').alias("core_program_type_code"),
        lit(0).alias("adjusted_obligation_identifier"),
        col("a.sys_cd").alias("system_code"),
        concat(col("o.st_fsa_cd"), col("o.cnty_fsa_cd")).alias("core_organization_code"),
        col("a.obl_lqd_rqst_id").alias("obligation_liquidation_request_identifier"),
        col("b.cre_dt").alias("creation_datetime"),
        date_format(col("b.cre_dt"), "yyyy-MM-dd").alias("creation_date"),
        col("a.rvrs_ind").alias("reversal_indicator"),
        lit(0).alias("obligation_transaction_amount"),
        lit(None).cast(TimestampType()).alias("payment_received_date"),
        lit(None).cast(TimestampType()).alias("payment_issue_date"),
        lit(None).cast(TimestampType()).alias("accounting_transmission_date"),
        when(col("a.rvrs_ind") == "N", col("b.bfy_txn_amt") * -1)
            .otherwise(col("b.bfy_txn_amt")).alias("bfy_obligation_transaction_amount"),
        lit(0).alias("bfy_obligation_amount"),
        lit(0).alias("bfy_obligation_balance_amount"),
        lit(0).alias("fund_allotment_identifier"),
        #lit(0).alias("obligation_budget_fiscal_year_identifier"),
        lit(None).cast(IntegerType()).alias("obligation_budget_fiscal_year_identifier"),
        #lit(0).alias("obligation_adjustment_budget_fiscal_year_identifier"),
        lit(None).cast(IntegerType()).alias("obligation_adjustment_budget_fiscal_year_identifier"),
        col("b.obl_lqd_bfy_id").alias("obligation_liquidation_request_budget_fiscal_year_identifier"),
        #lit(0.00).alias("accounting_transaction_net_amount"),
        lit(None).cast(DecimalType(16,2)).alias("accounting_transaction_net_amount"),
        #lit(0.00).alias("total_payable_offset_amount"),
        lit(None).cast(DecimalType(16,2)).alias("total_payable_offset_amount"),
        col("o.data_src_acro").alias("data_source_acronym"),
        lit(0.00).alias("interest_penalty_amount"),
        lit(0.00).alias("additional_interest_penalty_amount"),
        #lit('').alias("duns_plus4_nbr"),
        lit(None).cast(StringType()).alias("duns_plus4_nbr"),
        #lit('').alias("duns_nbr"),
        lit(None).cast(StringType()).alias("duns_nbr"),
        lit('').alias("sam_unique_entity_identifier")
    )
)
# debug_df(df_type_198, "df_type_198")

# 196
df_type_196 = (
    obligation_byf_df.alias("b")
    .join(obligation_df.alias("o"), col("o.obl_id") == col("b.obl_id"), how="inner")
.select(
        lit(196).cast(IntegerType()).alias("payment_transaction_type_identifier"),
        lit(None).alias("accounting_customer_identifier"),
        lit(None).alias("accounting_program_identifier"),
        lit(None).alias("sampling_identifier"),
        lit(None).alias("national_joint_payment_identifier"),
        lit(None).alias("accounting_payee_type_code"),
        lit(None).cast(TimestampType()).alias("accounting_service_request_date"),
        lit(None).cast(TimestampType()).alias("accounting_transaction_date"),
        lit('').alias("accounting_transaction_code"),
        col("b.bdgt_fscl_yr").alias("budget_fiscal_year"),
        lit(None).cast(TimestampType()).alias("prompt_payment_interest_date"),
        lit('').alias("payable_status_code"),
        col("o.st_fsa_cd").alias("state_fsa_code"),
        col("o.cnty_fsa_cd").alias("county_fsa_code"),
        col("o.acct_pgm_yr").alias("accounting_program_year"),
        col("o.acct_pgm_cd").alias("accounting_program_code"),
        col("o.acct_ref_1_cd").alias("accounting_reference_1_code"),
        col("o.acct_ref_1_nbr").alias("accounting_reference_1_number"),
        col("o.acct_ref_2_cd").alias("accounting_reference_2_code"),
        col("o.acct_ref_2_nbr").alias("accounting_reference_2_number"),
        lit('').alias("accounting_reference_3_code"),
        lit('').alias("accounting_reference_3_number"),
        lit('').alias("accounting_reference_4_code"),
        lit('').alias("accounting_reference_4_number"),
        lit('').alias("accounting_reference_5_code"),
        lit('').alias("accounting_reference_5_number"),
        col("o.cmdy_cd").alias("commodity_code"),
        col("o.bus_pty_id").alias("business_party_identifier"),
        lit('').alias("tax_identification"),
        lit('').alias("tax_identification_type_code"),
        lit('').alias("program_alpha_code"),
        lit(0).alias("disbursement_amount"),
        #lit(0).alias("disbursement_identifier"),
        lit(None).cast(IntegerType()).alias("disbursement_identifier"),
        lit(None).cast(TimestampType()).alias("disbursement_date"),
        lit(None).cast(TimestampType()).alias("disbursement_status_date"),
        lit('').alias("disbursement_status_code"),
        lit('').alias("last_disbursement_status_code"),
        lit('').alias("disbursement_type_code"),
        #lit(0).alias("disbursement_confirmation_identifier"),
        lit(None).cast(IntegerType()).alias("disbursement_confirmation_identifier"),
        lit('').alias("external_disbursement_identification"),
        lit('').alias("external_disbursement_type_code"),
        lit('').alias("payable_completed_indicator"),
        lit(0).alias("payable_offset_amount"),
        #lit(0).alias("receivable_identifier"),
        lit(None).cast(IntegerType()).alias("receivable_identifier"),
        lit(0).alias("payable_identifier"),
        lit(0).alias("receivable_amount"),
        lit('').alias("offset_override_reason_code"),
        lit('').alias("claim_identifier"),
        lit(None).cast(TimestampType()).alias("final_review_date"),
        lit(0).alias("payment_request_amount"),
        lit(0).alias("prompt_payment_interest_amount"),
        lit(0).alias("tax_withholding_amount"),
        lit('').alias("legacy_receivable_number"),
        lit('').alias("offset_status_code"),
        #lit(0).alias("national_assignment_identifier"),
        lit(None).cast(IntegerType()).alias("national_assignment_identifier"),
        #lit(0).alias("assignee_accounting_customer_identifier"),
        lit(None).cast(IntegerType()).alias("assignee_accounting_customer_identifier"),
        lit(0).alias("assignment_amount"),
        lit(0).alias("assignment_paid_amount"),
        lit(0.00).alias("accounting_charge_amount"),
        col("o.obl_id").alias("obligation_identifier"),
        lit('').alias("obligation_full_partial_paid_indicator"),
        lit(0).alias("obligation_amount"),
        col("o.obl_apvl_dt").alias("obligation_approval_date"),
        lit(0).alias("obligation_balance_amount"),
        substring(col("o.obl_stat_cd"),1,1).alias("obligation_status_code"),
        lit('').alias("core_accounting_period"),
        lit(0).alias("accounting_transaction_quantity"),
        lit('').alias("unit_of_measure_code"),
        lit('').alias("program_category_code"),
        lit('').alias("core_program_type_code"),
        lit(0).alias("adjusted_obligation_identifier"),
        col("o.sys_cd").alias("system_code"),
        concat(col("o.st_fsa_cd"), col("o.cnty_fsa_cd")).alias("core_organization_code"),
        #lit(0).alias("obligation_liquidation_request_identifier"),
        lit(None).cast(IntegerType()).alias("obligation_liquidation_request_identifier"),
        col("b.cre_dt").alias("creation_datetime"),
        date_format(col("b.cre_dt"), "yyyy-MM-dd").alias("creation_date"),
        lit('').alias("reversal_indicator"),
        lit(0).alias("obligation_transaction_amount"),
        lit(None).cast(TimestampType()).alias("payment_received_date"),
        lit(None).cast(TimestampType()).alias("payment_issue_date"),
        lit('').alias("accounting_transmission_date"),
        lit(0).alias("bfy_obligation_transaction_amount"),
        col("b.BFY_OBL_AMT").alias("bfy_obligation_amount"),
        col("b.BFY_OBL_BAL_AMT").alias("bfy_obligation_balance_amount"),
        col("b.fund_alot_id").alias("fund_allotment_identifier"),
        col("b.obl_bfy_id").alias("obligation_budget_fiscal_year_identifier"),
        #lit(0).alias("obligation_adjustment_budget_fiscal_year_identifier"),
        lit(None).cast(IntegerType()).alias("obligation_adjustment_budget_fiscal_year_identifier"),
        lit(0).alias("obligation_liquidation_request_budget_fiscal_year_identifier"),
        #lit(0.00).alias("accounting_transaction_net_amount"),
        lit(None).cast(DecimalType(16,2)).alias("accounting_transaction_net_amount"),
        #lit(0.00).alias("total_payable_offset_amount"),
        lit(None).cast(DecimalType(16,2)).alias("total_payable_offset_amount"),
        col("o.data_src_acro").alias("data_source_acronym"),
        lit(0.00).alias("interest_penalty_amount"),
        lit(0.00).alias("additional_interest_penalty_amount"),
        #lit('').alias("duns_plus4_nbr"),
        lit(None).cast(StringType()).alias("duns_plus4_nbr"),
        #lit('').alias("duns_nbr"),
        lit(None).cast(StringType()).alias("duns_nbr"),
        lit('').alias("sam_unique_entity_identifier")
    )
)
# debug_df(df_type_196, "df_type_196")

# 05
df_type_05 = (
    qct05_df.alias("q05")
    .join(r8.alias("r8"), col("r8.rcv_id") == col("q05.rcv_id"), how="left")
.select(
        lit(5).cast(IntegerType()).alias("payment_transaction_type_identifier"),
        lit(None).alias("accounting_customer_identifier"),
        col("q05.acct_pgm_id").alias("accounting_program_identifier"),
        lit(None).alias("sampling_identifier"),
        lit(0).alias("national_joint_payment_identifier"),
        lit('').alias("accounting_payee_type_code"),
        col("q05.acct_svc_rqst_dt").alias("accounting_service_request_date"),
        col("q05.acct_txn_dt").alias("accounting_transaction_date"),
        lit(None).alias("accounting_transaction_code"),
        lit(None).alias("budget_fiscal_year"),
        lit(None).cast(TimestampType()).alias("prompt_payment_interest_date"),
        lit('').alias("payable_status_code"),
        col("q05.st_fsa_cd").alias("state_fsa_code"),
        col("q05.cnty_fsa_cd").alias("county_fsa_code"),
        col("q05.acct_pgm_yr").alias("accounting_program_year"),
        lit(None).alias("accounting_program_code"),
        col("r8.accounting_reference_1_code"),
        col("r8.accounting_reference_1_number"),
        col("r8.accounting_reference_2_code"),
        col("r8.accounting_reference_2_number"),
        col("r8.accounting_reference_3_code"),
        col("r8.accounting_reference_3_number"),
        col("r8.accounting_reference_4_code"),
        col("r8.accounting_reference_4_number"),
        col("r8.accounting_reference_5_code"),
        col("r8.accounting_reference_5_number"),
        lit('').alias("commodity_code"),
        lit('').alias("business_party_identifier"),
        lit('').alias("tax_identification"),
        lit('').alias("tax_identification_type_code"),
        lit('').alias("program_alpha_code"),
        lit(0).alias("disbursement_amount"),
        #lit(0).alias("disbursement_identifier"),
        lit(None).cast(IntegerType()).alias("disbursement_identifier"),
        lit(None).cast(TimestampType()).alias("disbursement_date"),
        lit(None).cast(TimestampType()).alias("disbursement_status_date"),
        lit('').alias("disbursement_status_code"),
        lit('').alias("last_disbursement_status_code"),
        lit('').alias("disbursement_type_code"),
        #lit(0).alias("disbursement_confirmation_identifier"),
        lit(None).cast(IntegerType()).alias("disbursement_confirmation_identifier"),
        lit('').alias("external_disbursement_identification"),
        lit('').alias("external_disbursement_type_code"),
        lit('').alias("payable_completed_indicator"),
        lit(0).alias("payable_offset_amount"),
        col("q05.rcv_id").alias("receivable_identifier"),
        lit(0).alias("payable_identifier"),
        col("q05.rcv_amt").alias("receivable_amount"),
        lit('').alias("offset_override_reason_code"),
        lit('').alias("claim_identifier"),
        lit(None).cast(TimestampType()).alias("final_review_date"),
        lit(0).alias("payment_request_amount"),
        lit(0).alias("prompt_payment_interest_amount"),
        lit(0).alias("tax_withholding_amount"),
        col("q05.lgcy_rcv_nbr").alias("legacy_receivable_number"),
        lit('').alias("offset_status_code"),
        #lit(0).alias("national_assignment_identifier"),
        lit(None).cast(IntegerType()).alias("national_assignment_identifier"),
        #lit(0).alias("assignee_accounting_customer_identifier"),
        lit(None).cast(IntegerType()).alias("assignee_accounting_customer_identifier"),
        lit(0).alias("assignment_amount"),
        lit(0).alias("assignment_paid_amount"),
        lit(0.00).alias("accounting_charge_amount"),
        lit(0).alias("obligation_identifier"),
        lit('').alias("obligation_full_partial_paid_indicator"),
        lit(0).alias("obligation_amount"),
        lit(None).cast(TimestampType()).alias("obligation_approval_date"),
        lit(0).alias("obligation_balance_amount"),
        lit('').alias("obligation_status_code"),
        lit('').alias("core_accounting_period"),
        lit(0).alias("accounting_transaction_quantity"),
        lit('').alias("unit_of_measure_code"),
        lit('').alias("program_category_code"),
        lit('').alias("core_program_type_code"),
        lit(0).alias("adjusted_obligation_identifier"),
        col("q05.sys_cd").alias("system_code"),
        concat(col("q05.st_fsa_cd"), col("q05.cnty_fsa_cd")).alias("core_organization_code"),
        #lit(0).alias("obligation_liquidation_request_identifier"),
        lit(None).cast(IntegerType()).alias("obligation_liquidation_request_identifier"),
        lit(None).cast(TimestampType()).alias("creation_datetime"),
        lit(None).cast(TimestampType()).alias("creation_date"),
        lit('').alias("reversal_indicator"),
        lit(0).alias("obligation_transaction_amount"),
        lit(None).cast(TimestampType()).alias("payment_received_date"),
        lit(None).cast(TimestampType()).alias("payment_issue_date"),
        lit(None).cast(TimestampType()).alias("accounting_transmission_date"),
        lit(0).alias("bfy_obligation_transaction_amount"),
        lit(0).alias("bfy_obligation_amount"),
        lit(0).alias("bfy_obligation_balance_amount"),
        lit(0).alias("fund_allotment_identifier"),
        #lit(0).alias("obligation_budget_fiscal_year_identifier"),
        lit(None).cast(IntegerType()).alias("obligation_budget_fiscal_year_identifier"),
        #lit(0).alias("obligation_adjustment_budget_fiscal_year_identifier"),
        lit(None).cast(IntegerType()).alias("obligation_adjustment_budget_fiscal_year_identifier"),
        lit(0).alias("obligation_liquidation_request_budget_fiscal_year_identifier"),
        #lit(0.00).alias("accounting_transaction_net_amount"),
        lit(None).cast(DecimalType(16,2)).alias("accounting_transaction_net_amount"),
        #lit(0.00).alias("total_payable_offset_amount"),
        lit(None).cast(DecimalType(16,2)).alias("total_payable_offset_amount"),
        lit('').alias("data_source_acronym"),
        lit(0.00).alias("interest_penalty_amount"),
        lit(0.00).alias("additional_interest_penalty_amount"),
        #lit('').alias("duns_plus4_nbr"),
        lit(None).cast(StringType()).alias("duns_plus4_nbr"),
        #lit('').alias("duns_nbr"),
        lit(None).cast(StringType()).alias("duns_nbr"),
        lit('').alias("sam_unique_entity_identifier")
    )
)
# debug_df(df_type_05, "QCT05")

# Combine all into one final DataFrame (Only use the Type 5 for Cert and Prod)
final_fact_df = df_type_97.unionByName(df_type_96, True)\
                          .unionByName(df_type_98, True)\
                          .unionByName(df_type_197, True)\
                          .unionByName(df_type_198, True)\
                          .unionByName(df_type_196, True)

if environ in ('cert', 'prod'):
    final_fact_df = final_fact_df.unionByName(df_type_05, True)
# debug_df(final_fact_df, "final_fact_df")

final_fact_df = final_fact_df.select(
    col("payment_transaction_type_identifier").cast(IntegerType()),
    lit(None).alias("accounting_customer_identifier").cast(IntegerType()),
    col("accounting_program_identifier").cast(IntegerType()),
    lit(None).alias("sampling_identifier").cast(IntegerType()),
    lit(None).alias("national_joint_payment_identifier").cast(IntegerType()),
    lit(None).alias("accounting_payee_type_code").cast(StringType()),
    col("accounting_service_request_date").cast(TimestampType()),
    col("accounting_transaction_date").cast(TimestampType()),
    col("accounting_transaction_code").cast(StringType()),
    col("budget_fiscal_year").cast(ShortType()),
    col("prompt_payment_interest_date").cast(TimestampType()),
    lit(None).alias("payable_status_code").cast(StringType()),
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
    col("disbursement_status_date").cast(TimestampType()),
    col("disbursement_status_code").cast(StringType()),
    col("last_disbursement_status_code").cast(StringType()),
    col("disbursement_type_code").cast(StringType()),
    col("disbursement_confirmation_identifier").cast(IntegerType()),
    col("external_disbursement_identification").cast(StringType()),
    col("external_disbursement_type_code").cast(StringType()),
    col("payable_completed_indicator").cast(StringType()),
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
    col("creation_datetime").cast(TimestampType()),
    col("creation_date").cast(TimestampType()),
    col("reversal_indicator").cast(StringType()),
    col("obligation_transaction_amount").cast(DecimalType(16,2)),
    col("payment_received_date").cast(TimestampType()),
    col("payment_issue_date").cast(TimestampType()),
    col("accounting_transmission_date").cast(TimestampType()),
    col("bfy_obligation_transaction_amount").cast(DecimalType(16,2)),
    col("bfy_obligation_amount").cast(DecimalType(16,2)),
    col("bfy_obligation_balance_amount").cast(DecimalType(16,2)),
    col("fund_allotment_identifier").cast(IntegerType()),
    col("obligation_budget_fiscal_year_identifier").cast(IntegerType()),
    col("obligation_adjustment_budget_fiscal_year_identifier").cast(IntegerType()),
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

final_df = final_fact_df
debug_df(final_df, "FINAL DF")
#########################################################################################################
# Write to final zone in parquet format
#########################################################################################################
finalzone = "s3://c108-{}-fpacfsa-final-zone/dmart/fwadm/nps/{}_part2".format(environ, tablename)
finalzone_path = f"{finalzone}/{datestmp}"
print(f"finalzone_path in S3: {finalzone_path}")

# Write the curated DataFrame to the final zone in Parquet format
final_df.write.mode("overwrite").parquet(finalzone_path, compression="snappy")
print(f"Final Data Frame written to S3 at: {finalzone_path}")


'''
try:
    logger.info("Writing Transformed data to Redshift")
    print("Writing Transformed data to Redshift")
    dynf = DynamicFrame.fromDF(final_fact_df, glueContext, "dynf")
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
