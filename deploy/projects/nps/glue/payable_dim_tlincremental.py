i# Author : Cynthia Singh
# Steampunk contractor
# 2024-07-21 / 2025-01-31
# source:  S3 file for incremental loads
# destination:  Redshift fwadm_dw.payable_dim
# 7/7/25 Talha Rewrote some parts of the code & did some debugging
# dgsprous 20260210 - refactor to support PMRDS.  writes a final zone parquet


from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SQLContext, functions as F, Row
from pyspark.sql.types import DecimalType, ShortType, DoubleType, IntegerType, StringType, StructType, StructField, DateType, TimestampType, FloatType
from pyspark.sql.functions import col, count, upper, lit, coalesce, when, regexp_extract, concat, concat_ws, substring, ltrim, trim, date_trunc, broadcast, sum as spark_sum, abs as abs_col, max, min, max as sql_max, min as sql_min, monotonically_increasing_id, current_date, current_timestamp, to_timestamp, date_format, row_number, expr, add_months, trunc, year, month, date_add, dayofweek
from datetime import datetime, timedelta
from py4j.java_gateway import java_import
import sys
import boto3
import logging
import json
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'schema_name', 'dbtable', 'environ', 'utils'])

# Initialize Spark
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
sqlContext = SQLContext(spark)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
logger = glueContext.get_logger()
logger.info(f"args: {args}")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED") # Allow datetimes before 1900-00-00

# Define some standard reusable terminology variables
environ = args['environ']
ENVIRON = environ.upper()
tmstmp = datetime.now().strftime("%Y%m%d%H%M")
datestmp = datetime.utcnow().strftime("%Y%m%d")
tablename = args['dbtable']
schemaname = args['schema_name']
dbtable = args['dbtable']
s3_uri = args['utils']

# Redshift connection options
if environ == 'dev':
    rs_catalog_connection = "FSA-{}-redshift-conn".format(ENVIRON)
    arnrole = "arn:aws-us-gov:iam::662519022378:role/FSA_Redshift_Role"
    secret_name = "FSA-{}-secrets".format(ENVIRON)
    jdbc_url = "jdbc:redshift://fsa-{}-redshift.cldrls4ljqk0.us-gov-west-1.redshift.amazonaws.com:8002/redshift_db".format(environ)
    jdbc_url_jl = "jdbc:redshift://fsa-{}-redshift.cldrls4ljqk0.us-gov-west-1.redshift.amazonaws.com:8002/redshift_db".format(environ)
    
elif environ == 'cert':
    rs_catalog_connection = "FSA-{}-redshift-conn".format(ENVIRON)
    arnrole = "arn:aws-us-gov:iam::662519022378:role/FSA_Redshift_Role"
    secret_name = "FSA-{}-Secrets".format(ENVIRON)
    jdbc_url = "jdbc:redshift://fsa-{}-redshift.cldrls4ljqk0.us-gov-west-1.redshift.amazonaws.com:9002/redshift_db".format(environ)
    jdbc_url_jl = "jdbc:redshift://fsa-{}-redshift.cldrls4ljqk0.us-gov-west-1.redshift.amazonaws.com:9002/redshift_db_cert".format(environ)

    
elif environ == 'prod':
    rs_catalog_connection = "FSA-{}-redshift-conn".format(ENVIRON)
    arnrole = "arn:aws-us-gov:iam::662519022378:role/FSA_PROD_Redshift_Role"
    secret_name = "FSA-{}-secrets".format(ENVIRON)
    jdbc_url = "jdbc:redshift://fsa-{}-redshift.cldrls4ljqk0.us-gov-west-1.redshift.amazonaws.com:8002/redshift_db".format(environ)
    jdbc_url_jl = "jdbc:redshift://fsa-{}-redshift.cldrls4ljqk0.us-gov-west-1.redshift.amazonaws.com:8002/redshift_db".format(environ)

############################################################################################################################
# Block for environmental specific settings
############################################################################################################################
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
from utils import validate_pk, validate_fk, validate_nulls, validate_duplicates, log_and_append_result
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
# End block for Postgres 

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "64MB")
spark.conf.set("spark.sql.shuffle.partitions", 200) 

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
        
# datestmp = '20251217' # Used only for debugging purposes
# Import source data files
payable_payment_reply = "s3://c108-{}-fpacfsa-landing-zone/dmart/raw/fwadm/{}/sqlNPS_Payable_Payment_Reply.txt".format(environ, datestmp)
qct01pybl = "s3://c108-{}-fpacfsa-cleansed-zone/dmart/fwadm/nps/qct01pybl/{}".format(environ, datestmp)
qct07pyblr = "s3://c108-{}-fpacfsa-landing-zone/dmart/raw/fwadm/{}/MQCXTFIL.QCNPSS07.FULL".format(environ, datestmp)
qct75acctr = "s3://c108-{}-fpacfsa-landing-zone/dmart/raw/fwadm/{}/MQCXTFIL.QCFSAS75.FULL".format(environ, datestmp)

#Designate destination path
destinationpath = "s3://c108-{}-fpacfsa-cleansed-zone/dmart/fwadm/nps/payable_dim".format(environ)
destination_path = f's3://c108-{environ.lower()}-fpacfsa-final-zone/dmart/fwadm/nps/{tablename}'


####################################################################
# Define the Final Zone S3 path
####################################################################
finalzone = "s3://c108-{}-fpacfsa-final-zone/dmart/fwadm/nps/payable_dim".format(environ)
final_zone_path = f"{finalzone}/{tmstmp}"

# Create timestamp variable to keep the value consistent across the job run
now_tmstmp = datetime.utcnow() - timedelta(minutes=2)
now_tmstmp = now_tmstmp.strftime('%Y%m%d %H%M')
print('NOW:  {}'.format(now_tmstmp))


starttime = datetime.now()
starttime_m = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

class RedshiftJDBC:
    def __init__(self):
        self.url = jdbc_url
        self.user = username
        self.password = password
    def execute_delete(self, pks):
        query = f"DELETE FROM {schemaname}.{tablename} WHERE to_char( last_change_date, 'YYYYMMDD HH24MI') < '{now_tmstmp}' and payable_identifier IN ({','.join([str(pk) for pk in pks])})"
        try:
            #conn = sc._gateway.jvm.DriverManager.getConnection(self.url, self.user, self.password)
            stmt = conn.createStatement()
            stmt.executeUpdate(query)
            stmt.close()
            conn.close()
            logger.info("Delete operation executed successfully.")
            print(query)
        except Exception as e:
            logger.info("Delete query was: {}".format(query))
            logger.error(f"Failed to execute delete operation: {e}")

# Setting up source tables and data types
payable_payment_reply_schema = StructType([
    StructField("pybl_id", IntegerType()),
	StructField("acct_cust_id", IntegerType()),
	StructField("acct_pgm_id", IntegerType()),
	StructField("smpl_id", IntegerType()),
	StructField("acct_svc_rqst_dt", TimestampType()),
	StructField("acct_txn_cd", StringType()),
	StructField("acct_txn_dt", TimestampType()),
	StructField("bia_rqst_ind", StringType()),
	StructField("bdgt_fscl_yr", IntegerType()),
	StructField("pymt_cmdy_qty", DecimalType(16,2)),
	StructField("cmn_cust_nm", StringType()),
	StructField("grp_pybl_nbr", IntegerType()),
	StructField("nres_aln_ind", StringType()),
	StructField("prmpt_pymt_int_dt", TimestampType()),
	StructField("pymt_rqst_amt", DecimalType(16,2)),
	StructField("stmt_gnrt_dt", TimestampType()),
	StructField("adtl_sel_item_ind", StringType()),
	StructField("ck_out_dt", TimestampType()),
    StructField("ck_out_user_nm", StringType()),
    StructField("dspt_lcl_ind", StringType()),
	StructField("dspt_ntl_ind", StringType()),
    StructField("fnl_rvw_dt", TimestampType()),
	StructField("fnl_rvw_lvl_cd", StringType()),
	StructField("fnl_rvw_user_nm", StringType()),
	StructField("init_rvw_dt", TimestampType()),
	StructField("init_rvw_user_nm", StringType()),
	StructField("rndm_sel_item_ind", StringType()),
	StructField("smpl_bat_item_ind", StringType()),
	StructField("smpl_insp_fail_ind", StringType()),
	StructField("prim_acct_ref_cd", StringType()),
    StructField("prim_acct_ref_nbr", StringType()),
	StructField("scnd_acct_ref_cd", StringType()),
	StructField("scnd_acct_ref_nbr", StringType()),
	StructField("adtl_acct_ref_ind", StringType()),
	StructField("acct_pgm_cd", StringType()),
	StructField("cmdy_cd", StringType()),
	StructField("bus_pty_id", StringType()),
	StructField("tax_id", StringType()),
	StructField("tax_id_type_cd", StringType()),
	StructField("late_pymt_day_ct", IntegerType()),
	StructField("prmpt_pymt_int_amt", DecimalType(16,2)),
	StructField("prmpt_pymt_int_rt", DecimalType(7,4)),
    StructField("prmpt_pymt_rsn_cd", StringType()),
	StructField("tax_whld_amt", DecimalType(16,2)),
	StructField("tax_whld_pct", DecimalType(7,4)),
    StructField("tax_whld_type_cd", StringType()),
	StructField("pgm_alp_cd", StringType()),
    StructField("pymt_ofst_alow_ind", StringType()),
	StructField("obl_id", IntegerType()),
	StructField("obl_pymt_ind", StringType()),
	StructField("disb_ck_frc_ind", StringType()),
	StructField("alt_pye_pmsn_ind", StringType()),
	StructField("pymt_disc_term_des", StringType()),
	StructField("data_src_acro", StringType()),
	StructField("pymt_isu_dt", TimestampType()),
	StructField("pybl_data_stat_cd", StringType()),
	StructField("pybl_cre_dt", TimestampType()),
    StructField("pybl_cre_user_nm", StringType()),
	StructField("last_chg_dt", TimestampType()),
	StructField("last_chg_user_nm", StringType()),
	StructField("cnfrm_proc_ind", StringType()),
	StructField("txn_rqst_id", StringType()),
	StructField("pybl_stat_cd", StringType()),
	StructField("sys_cd", StringType()),
	StructField("acct_pgm_yr", StringType()),
	StructField("pymt_proc_ind", StringType()),
	StructField("pymt_data_stat_cd", StringType()),
	StructField("pymt_cre_dt", TimestampType()),
	StructField("pymt_cre_user_nm", StringType()),
	StructField("last_upd_dt", TimestampType()),
	StructField("last_upd_user_nm", StringType()),
	StructField("data_migr_stat_cd", StringType()),
	StructField("st_fsa_cd", StringType()),
	StructField("cnty_fsa_cd", StringType()),
	StructField("migr_stat_chg_dt", TimestampType()),
	StructField("pymt_rcv_dt", TimestampType()),
	StructField("int_due_prn_amt", DecimalType(16,2)),
	StructField("vol_whld_pct", DecimalType(5,3)),
	StructField("invol_whld_pct", DecimalType(5,3)),
	StructField("form_1099_ind", StringType()),
	StructField("dir_atrb_grp_nbr", StringType()),
	StructField("dth_pymt_rsn_cd", StringType()),
	StructField("txn_rqst_pkg_id", IntegerType()),
    StructField("gr_loan_shr_amt", DecimalType(16,2)),
	StructField("cncl_init_user_id", StringType()),
	StructField("cncl_init_dt", TimestampType()),
	StructField("cncl_fnl_user_id", StringType()),
	StructField("cncl_fnl_dt", TimestampType()),
	StructField("cncl_rsn_desc", StringType())
])

payable_payment_reply_df = spark.read.csv(payable_payment_reply, header=False, schema=payable_payment_reply_schema, sep="\t", ignoreLeadingWhiteSpace=True)
ppr = payable_payment_reply_df

# debug_df(payable_payment_reply_df, "payable_payment_reply_df")

qct01pybl_schema = StructType([
    StructField("pybl_id", IntegerType()),
	StructField("acct_cust_id", IntegerType()),
	StructField("acct_pgm_id", IntegerType()),
	StructField("sys_cd", StringType()),
	StructField("smpl_id", IntegerType()),
	StructField("acct_svc_rqst_dt", StringType()),
	StructField("acct_txn_cd", StringType()),
	StructField("acct_txn_dt", TimestampType()),
	StructField("bia_rqst_ind", StringType()),
	StructField("bdgt_fscl_yr", IntegerType()),
	StructField("pymt_cmdy_qty", DecimalType(16,2)),
	StructField("cmn_cust_nm", StringType()),
	StructField("grp_pybl_nbr", IntegerType()),
	StructField("nres_aln_ind", StringType()),
	StructField("prmpt_pymt_int_dt", TimestampType()),
	StructField("pybl_stat_cd", StringType()),
	StructField("pymt_rqst_amt", DecimalType(16,2)),
	StructField("stmt_gnrt_dt", TimestampType()),
    StructField("txn_rqst_id", StringType()),
    StructField("adtl_sel_item_ind", StringType()),
	StructField("ck_out_dt", StringType()),
    StructField("ck_out_user_nm", StringType()),
	StructField("dspt_lcl_ind", StringType()),
	StructField("dspt_ntl_ind", StringType()),
	StructField("fnl_rvw_dt", StringType()),
	StructField("fnl_rvw_lvl_cd", StringType()),
	StructField("fnl_rvw_user_nm", StringType()),
	StructField("init_rvw_dt", StringType()),
	StructField("init_rvw_user_nm", StringType()),
	StructField("rndm_sel_item_ind", StringType()),
    StructField("smpl_bat_item_ind", StringType()),
	StructField("smpl_insp_fail_ind", StringType()),
	StructField("last_chg_dt", StringType()),
	StructField("last_chg_user_nm", StringType()),
	StructField("st_fsa_cd", StringType()),
	StructField("cnty_fsa_cd", StringType()),
	StructField("acct_pgm_yr", StringType()),
	StructField("prim_acct_ref_cd", StringType()),
	StructField("prim_acct_ref_nbr", StringType()),
	StructField("scnd_acct_ref_cd", StringType()),
	StructField("scnd_acct_ref_nbr", StringType()),
	StructField("acct_pgm_cd", StringType()),
    StructField("cmdy_cd", StringType()),
	StructField("bus_pty_id", StringType()),
	StructField("tax_id", StringType()),
    StructField("tax_id_type_cd", StringType()),
    StructField("late_pymt_day_ct", IntegerType()),
    StructField("prmpt_pymt_int_amt", DecimalType(16,2)),
	StructField("prmpt_pymt_int_rt", DecimalType(7,4)),
	StructField("prmpt_pymt_rsn_cd", StringType()),
    StructField("tax_whld_amt", DecimalType(16,2)),
	StructField("tax_whld_pct", DecimalType(7,4)),
    StructField("tax_whld_type_cd", StringType()),
	StructField("pymt_proc_ind", StringType()),
	StructField("cnfrm_proc_ind", StringType()),
	StructField("pgm_alp_cd", StringType()),
	StructField("pymt_ofst_alow_ind", StringType()),
	StructField("obl_id", IntegerType()),
	StructField("obl_pymt_ind", StringType()),
	StructField("disb_ck_frc_ind", StringType()),
	StructField("alt_pye_pmsn_ind", StringType()),
	StructField("pymt_disc_term_des", StringType()),
    StructField("data_src_acro", StringType()),
	StructField("pymt_isu_dt", DateType()),
	StructField("pymt_rcv_dt", StringType()),
	StructField("int_due_prn_amt", DecimalType(16,2)),
	StructField("vol_whld_pct", DecimalType(5,3)),
	StructField("invol_whld_pct", DecimalType(5,3)),
	StructField("form_1099_ind", StringType()),
	StructField("dir_atrb_grp_nbr", StringType()),
	StructField("dth_pymt_rsn_cd", StringType()),
	StructField("txn_rqst_pkg_id", IntegerType()),
	StructField("gr_loan_shr_amt", DecimalType(16,2)),
	StructField("cncl_init_user_id", StringType()),
	StructField("cncl_init_dt", StringType()),
	StructField("cncl_fnl_user_id", StringType()),
	StructField("cncl_fnl_dt", StringType()),
	StructField("cncl_rsn_desc", StringType())
])

source_fmt = "yyyy-MM-dd-HH.mm.ss.SSSSSS"
target_fmt = "yyyy-MM-dd-HH:mm:ss"

# Read source data file
qct01pybl_df = spark.read.parquet(qct01pybl, ignoreLeadingWhiteSpace=True, timestampFormat=source_fmt)
qct01pybl_df = qct01pybl_df.withColumn("pymt_isu_dt", col("pymt_isu_dt").cast(TimestampType()))

# debug_df(qct01pybl_df, "qct01pybl_df")

# Trim whitespace from each column to remove leading/trailing spaces where necessary
qct01pybl_df = qct01pybl_df.withColumn("pybl_id", ltrim(col("pybl_id")))
q = qct01pybl_df

# Ensure last_chg_dt is a timestamp b/c it is a string at this point
# Format with matching src_last_chg_dt in dataframe assc_s
cols_to_format = [
    "last_chg_dt",
    "init_rvw_dt",
    "fnl_rvw_dt",
    "acct_svc_rqst_dt",
    "pymt_rcv_dt"
]

for c in cols_to_format:
    qct01pybl_df = qct01pybl_df.withColumn(
        c,
        date_trunc(
            "second",
            to_timestamp(col(c), source_fmt),  # converts the string to a timestamp
        )
)

print("Line 428")    
qct01pybl_df.select(
    "last_chg_dt",
    "init_rvw_dt",
    "fnl_rvw_dt",
    "pymt_rcv_dt",
    "acct_svc_rqst_dt"
).show(5,truncate=False)

debug_df(qct01pybl_df, "qct01pybl_df")

qct07pyblr_schema = StructType([
    StructField("pybl_ref_id", IntegerType()),
	StructField("pybl_id", IntegerType()),
	StructField("acct_ref_id", IntegerType()),
	StructField("acct_ref_nbr", StringType()),
	StructField("last_chg_dt", StringType()),
	StructField("last_chg_user_nm", StringType()),
	StructField("st_fsa_cd", StringType()),
	StructField("cnty_fsa_cd", StringType()),
	StructField("data_invalid_indicator", StringType())
])

qct07pyblr_df = spark.read.csv(qct07pyblr, header=False, schema=qct07pyblr_schema, sep="\t", ignoreLeadingWhiteSpace=True)

qct07pyblr_df = (
    qct07pyblr_df
    .withColumn(
        "last_chg_dt",
        date_trunc(
            "second",
            to_timestamp(
                trim(col("last_chg_dt")), # clean away spaces
                "yyyy-MM-dd-HH.mm.ss.SSSSSS")
        )
    )
)

# debug_df(qct07pyblr_df, "qct07pyblr_df")

qct75acctr_schema = StructType([
    StructField("acct_ref_id", IntegerType()),
	StructField("acct_ref_cd", StringType()),
	StructField("acct_ref_desc", StringType()),
	StructField("last_chg_dt", StringType()),
	StructField("last_chg_user_nm", StringType())
])

qct75acctr_df = spark.read.csv(qct75acctr, header=False, schema=qct75acctr_schema, sep="\t", ignoreLeadingWhiteSpace=True)

qct75acctr_df = (
    qct75acctr_df
    .withColumn(
        "last_chg_dt",
        date_trunc(
            "second",
            to_timestamp(
                trim(col("last_chg_dt")), # clean away spaces
                "yyyy-MM-dd-HH.mm.ss.SSSSSS")
        )
    )
)

# Trim whitespace from each column to remove leading/trailing spaces where necessary
qct75acctr_df = qct75acctr_df.withColumn("acct_ref_id", ltrim(col("acct_ref_id")))
# debug_df(qct75acctr_df, "qct75acctr_df")

# I was told to use assc_s in place of business_party_role and also use for t_eauth_id
assc_s_query = f"""(SELECT
                        src_cre_dt,
                        src_last_chg_dt,
                        auth_sys_user_id,
                        last_nm,
                        fst_nm,
                        mid_init
                    FROM edv.assc_s) AS assc_s_query"""

df_assc_s = spark.read.jdbc(url=jdbc_url_jl, table=assc_s_query, properties=properties)

# debug_df(df_assc_s, "df_assc_s")

df_assc_s = (
    df_assc_s
    .withColumn(
        "src_last_chg_dt",
        date_trunc(
            "second",
            to_timestamp(col("src_last_chg_dt"), "yyyy-MM-dd-HH:mm:ss.SSS")
        )
    )   
)

# debug_df(df_assc_s, "df_assc_s")


# Creating temporary DataFrames
# T1
df_t1 = qct07pyblr_df.alias("p1").join(
    qct75acctr_df.alias("acctr"),
    col("p1.acct_ref_id") == col("acctr.acct_ref_id"),
    "inner"
).select(
    col("p1.pybl_ref_id").alias("id"),
    "acct_ref_nbr",
    "acct_ref_cd",
    "pybl_id",
    lit(1).alias("order_num")
)
# debug_df(df_t1, "df_t1")

#T2
df_t1 = df_t1.repartition(col("pybl_id") )
df_t2 = df_t1.groupBy(col("pybl_id")).agg(count("*").alias("cnt"))
# debug_df(df_t2, "df_t2")
df_t2 = df_t2.repartition(col("pybl_id") )

#T3
df_t3 = df_t1.alias("t1") \
    .join(df_t2.filter("cnt > 2"), "pybl_id") \
    .groupBy("pybl_id") \
    .agg(F.max("pybl_id").alias("max_id")
).select("max_id", "pybl_id")
# debug_df(df_t3, "df_t3")

#T4
df_t4 = df_t1.groupBy("pybl_id").agg(F.min("id").alias("min_id")).select("min_id", "pybl_id")
# debug_df(df_t4, "df_t4")


#T5
df_t5 = (
    df_t1.join(df_t2.filter("cnt > 1"), on="pybl_id") \
    .join(df_t3.select(col("max_id").alias("id")), on="id", how="left_anti") \
    .join(df_t4.select(col("min_id").alias("id")), on="id", how="left_anti") \
    .select("id", "pybl_id")
)
# debug_df(df_t5, "df_t5")

#T6
min_val = df_t1.select(col("id").alias("min_id"), col("acct_ref_nbr").alias("accounting_reference_3_number"), col("acct_ref_cd").alias("accounting_reference_3_code"))
max_val = df_t1.select(col("id").alias("max_id"), col("acct_ref_nbr").alias("accounting_reference_5_number"), col("acct_ref_cd").alias("accounting_reference_5_code"))
avg_val = df_t1.select(col("id").alias("id_avg"), col("acct_ref_nbr").alias("accounting_reference_4_number"), col("acct_ref_cd").alias("accounting_reference_4_code"))

df_t6 = (
    df_t2.join(df_t4, "pybl_id") \
    .join(min_val, "min_id") \
    .join(df_t3, "pybl_id", "left") \
    .join(max_val, "max_id", "left") \
    .join(df_t5, "pybl_id", "left") \
    .join(avg_val, df_t5.id == avg_val.id_avg, "left") \
    .select(
        "pybl_id", "cnt", "max_id", "min_id", 
        "accounting_reference_3_number", "accounting_reference_3_code",
        "accounting_reference_5_number", "accounting_reference_5_code",
        "accounting_reference_4_number", "accounting_reference_4_code"
    )
)
# debug_df(df_t6, "df_t6")

#t_eauth_id 
t_eauth_id = df_assc_s.groupBy("auth_sys_user_id")\
                    .agg(max("src_last_chg_dt").alias("max_last_chg_dt")) \
                    .withColumnRenamed("auth_sys_user_id", "eauth_id")

# debug_df(t_eauth_id, "t_eauth_id")

#t_eauth_master
joined_df = df_assc_s.join(t_eauth_id,
                                (df_assc_s.auth_sys_user_id == t_eauth_id.eauth_id) & 
                                (df_assc_s.src_last_chg_dt == t_eauth_id.max_last_chg_dt),
                                how="inner")
debug_df(joined_df, "joined_df")

j_all = (
    joined_df.alias("j")
        .join(
            q.alias("q"),
            col("j.src_last_chg_dt") == col("q.last_chg_dt"),
            how="left"
        )
        .select(
            F.col("src_last_chg_dt"),
            F.col("last_chg_dt"))
)

# debug_df(j_all, "j_all")

j = j_all


t_eauth_master = joined_df.select(   
    col("auth_sys_user_id").alias("eauth_id"),
    when(col("mid_init").isNull(),
         concat_ws(" ", upper(col("fst_nm")), upper(col("last_nm")))
        ).otherwise(
            concat_ws(" ", upper(col("fst_nm")), upper(col("mid_init")), upper(col("last_nm")))
        ).alias("eauth_name")
)
# debug_df(t_eauth_master, "t_eauth_master")

#t_init_user
### Part 1 for qct
t_init_user_part1 = qct01pybl_df.alias("a") \
    .join(payable_payment_reply_df.select("pybl_id").distinct().alias("ppr"), "pybl_id", "left_anti") \
    .join(t_eauth_master.alias("b"), col("a.init_rvw_user_nm") == col("b.eauth_id"), "left_outer") \
    .select(col("a.pybl_id"), coalesce(col("b.eauth_name"), col("a.init_rvw_user_nm")).alias("init_eauth_name")) \
    .withColumn("table_ind", lit("qct"))
    
# debug_df(t_init_user_part1, "t_init_user_part1")
    
### Part 2 for ppr    
t_init_user_part2 = payable_payment_reply_df.alias("a") \
    .join(t_eauth_master.alias("b"), col("a.init_rvw_user_nm") == col("b.eauth_id"), "left_outer") \
    .select(col("a.pybl_id"), coalesce(col("b.eauth_name"), col("a.init_rvw_user_nm")).alias("init_eauth_name")) \
    .withColumn("table_ind", lit("ppr"))

# debug_df(t_init_user_part2, "t_init_user_part2")

### Union t_init_user
t_init_user = t_init_user_part1.union(t_init_user_part2).select("pybl_id","init_eauth_name","table_ind").distinct()
debug_df(t_init_user, "t_init_user")

#t_fnl_user
### part 1 for qct
t_fnl_user_part1 = qct01pybl_df.alias("a") \
    .join(payable_payment_reply_df.select("pybl_id").alias("ppr"), "pybl_id", "left_anti") \
    .join(t_eauth_master.alias("b"), col("a.fnl_rvw_user_nm") == col("b.eauth_id"), "left_outer")\
    .select(col("a.pybl_id"), coalesce(col("b.eauth_name"), col("a.fnl_rvw_user_nm")).alias("fnl_eauth_name"))
   
### part 2 for ppr    
t_fnl_user_part2 = payable_payment_reply_df.alias("a") \
    .join(t_eauth_master.alias("b"), col("a.fnl_rvw_user_nm") == col("b.eauth_id"), "left_outer")\
    .select(col("a.pybl_id"), coalesce(col("b.eauth_name"), col("a.fnl_rvw_user_nm")).alias("fnl_eauth_name"))
    
### union t_fnl_user
t_fnl_user = t_fnl_user_part1.union(t_fnl_user_part2).distinct()
# debug_df(t_fnl_user, "t_fnl_user")

# t_pybl_eauth
t_pybl_eauth = t_init_user.alias("x") \
    .join(t_fnl_user.alias("y"), "pybl_id") \
    .select(
        "x.pybl_id",
        "x.init_eauth_name",
        "y.fnl_eauth_name",
        "x.table_ind"
    )
# debug_df(t_pybl_eauth, "t_pybl_eauth")

# First Insert SELECT STMNT
final_df_part1 = (
    q.alias("q")
    .join(t_pybl_eauth.filter(col("table_ind") == "qct").alias("e"), "pybl_id")
    .join(df_t6.alias("t6"), "pybl_id", "left_outer")
    .select(
        col("q.pybl_id").alias("payable_identifier"),
        col("q.bia_rqst_ind").alias("bia_request_indicator"),
        col("q.nres_aln_ind").alias("nonresident_alien_indicator"),
        col("q.adtl_sel_item_ind").alias("associated_random_selection_indicator"),
        col("q.ck_out_dt").alias("check_out_date"),
        col("q.ck_out_user_nm").alias("check_out_user_name"),
        col("q.grp_pybl_nbr").alias("group_payable_number"),
        col("q.dspt_lcl_ind").alias("payable_dispute_local_indicator"),
        col("q.dspt_ntl_ind").alias("payable_dispute_national_indicator"),
        col("q.fnl_rvw_lvl_cd").alias("final_review_level_code"),
        col("e.fnl_eauth_name").alias("final_review_user_name"),
        col("q.init_rvw_dt").alias("initial_review_date"),
        col("e.init_eauth_name").alias("initial_review_user_name"),
        col("q.rndm_sel_item_ind").alias("random_selected_item_indicator"),
        col("q.smpl_bat_item_ind").alias("sampling_batch_item_indicator"),
        col("q.smpl_insp_fail_ind").alias("sampling_inspection_failed_indicator"),
        col("q.late_pymt_day_ct").alias("late_payment_day_count"),
        col("q.prmpt_pymt_int_rt").alias("prompt_payment_interest_rate"),
        col("q.prmpt_pymt_rsn_cd").alias("prompt_payment_reason_code"),
        col("q.tax_whld_pct").alias("tax_withholding_percent"),
        col("q.tax_whld_type_cd").alias("tax_withholding_type_code"),
        col("q.pymt_proc_ind").alias("payable_approved_indicator"),
        col("q.cnfrm_proc_ind").alias("payable_completed_indicator"),
        col("q.cmn_cust_nm").alias("common_customer_name"),
        col("q.stmt_gnrt_dt").alias("statement_generated_date"),
        col("q.txn_rqst_id").alias("transaction_request_identification"),
        col("q.pymt_ofst_alow_ind").alias("payment_offset_allowed_indicator"),
        col("q.DISB_CK_FRC_IND").alias("disbursement_check_forced_indicator"),
        col("q.ALT_PYE_PMSN_IND").alias("alternate_payee_permission_indicator"),
        col("q.pymt_disc_term_des").alias("payment_discount_terms_description"),
        col("q.last_chg_dt").alias("last_change_date"),
        # if we want final_df_part1 to fall back to j when q is null:
        # F.coalesce(col("q.last_chg_dt"), col("j.src_last_chg_dt")).alias("src_last_chg_dt"),
        # col("src_last_chg_dt").cast(TimestampType()).alias("source_last_change_date"),
        col("q.last_chg_user_nm").alias("source_last_change_user_name"),
        col("q.vol_whld_pct").alias("voluntary_withholding_percentage"),
        col("q.invol_whld_pct").alias("involuntary_withholding_percentage"),
        col("q.int_due_prn_amt").alias("interest_due_principal_amount"),
        col("q.form_1099_ind").alias("form_1099_indicator"),
        lit(" ").alias("data_migration_status_code"),
        lit("N").alias("data_invalid_indicator"),
        col("q.dir_atrb_grp_nbr").alias("direct_attribution_group_number"),
        col("q.acct_txn_cd").alias("accounting_transaction_code"),
        col("q.tax_id").alias("tax_identification"),
        substring("q.tax_id", 6, 4).alias("tax_identification_last_4"),
        col("q.tax_id_type_cd").alias("tax_identification_type_code"),
        col("q.acct_cust_id").alias("accounting_customer_identifier"),
        col("q.acct_pgm_cd").alias("accounting_program_code"),
        col("q.acct_pgm_yr").alias("accounting_program_year"),
        col("q.prim_acct_ref_nbr").alias("accounting_reference_1_number"),
        col("q.prim_acct_ref_cd").alias("accounting_reference_1_code"),
        col("q.scnd_acct_ref_nbr").alias("accounting_reference_2_number"),
        col("q.scnd_acct_ref_cd").alias("accounting_reference_2_code"),
        col("t6.accounting_reference_3_number").alias("accounting_reference_3_number"),
        col("t6.accounting_reference_3_code").alias("accounting_reference_3_code"),
        col("t6.accounting_reference_4_number").alias("accounting_reference_4_number"),
        col("t6.accounting_reference_4_code").alias("accounting_reference_4_code"),
        col("t6.accounting_reference_5_number").alias("accounting_reference_5_number"),
        col("t6.accounting_reference_5_code").alias("accounting_reference_5_code"),
        col("q.pybl_stat_cd").alias("payable_status_code"),
        concat(col("q.st_fsa_cd"),col("q.cnty_fsa_cd")).alias("core_organization_code"),
        col("q.st_fsa_cd").alias("state_fsa_code"),
        col("q.cnty_fsa_cd").alias("county_fsa_code"),
        coalesce(col("q.pymt_isu_dt").cast("timestamp"), F.to_timestamp(lit("1900-01-01 00:00:00"))).alias("payment_issue_date"),
        col("q.pymt_rqst_amt").alias("payment_request_amount"),
        when(col("q.acct_txn_cd").isin(197,198), lit(0)) \
            .otherwise(col("prmpt_pymt_int_amt")).alias("prompt_payment_interest_amount"),
        when(col("q.acct_txn_cd") == 198, col("prmpt_pymt_int_amt")) \
            .otherwise(lit(None)).alias("interest_penalty_amount"),
        when(col("q.acct_txn_cd") == 197, col("prmpt_pymt_int_amt")) \
            .otherwise(lit(None)).alias("additional_interest_penalty_amount"),
        col("q.pgm_alp_cd").alias("program_alpha_code"),
        col("q.fnl_rvw_dt").alias("final_review_date"),
        col("q.dth_pymt_rsn_cd").alias("death_payment_reason_code"),
        col("q.txn_rqst_pkg_id").alias("transaction_request_package_identifier"),
        col("q.gr_loan_shr_amt").alias("gross_loan_share_amount"),
        col("q.obl_id").alias("obligation_identifier"),
        col("q.cncl_init_user_id").alias("cancellation_initiation_user_identification"),
        col("q.cncl_init_dt").alias("cancellation_initiation_date"),
        col("q.cncl_fnl_user_id").alias("cancellation_finalized_user_identification"),
        col("q.cncl_fnl_dt").alias("cancellation_finalized_date"),
        col("q.cncl_rsn_desc").alias("cancellation_reason_description"),
        col("q.acct_pgm_id").alias("accounting_program_identifier"),
        col("q.acct_svc_rqst_dt").alias("accounting_service_request_date"),
        col("q.bdgt_fscl_yr").alias("budget_fiscal_year"),
        col("q.prmpt_pymt_int_dt").alias("prompt_payment_interest_date"),
        col("q.cmdy_cd").alias("commodity_code"),
        col("q.bus_pty_id").alias("business_party_identifier"),
        col("q.tax_whld_amt").alias("tax_withholding_amount"),
        col("q.pymt_cmdy_qty").alias("accounting_transaction_quantity"),
        col("q.sys_cd").alias("system_code"),
        col("q.pymt_rcv_dt").alias("payment_received_date"),
        col("q.data_src_acro").alias("data_source_acronym")
    ).dropDuplicates(["payable_identifier"])
)

# debug_df(final_df_part1, "final_df_part1")

# Add the final src_last_chg_dt field with the desired logic involving nulls
final_df_part1 = (
    final_df_part1.withColumn(
        "src_last_chg_dt",
        F.date_trunc(
            "second",  # truncates to whole seconds, no .SSS
            F.to_timestamp("last_change_date", "yyyy-MM-dd-HH.mm.ss.SSSSSS")
        )
    )
    .drop("last_change_date")
)

debug_df(final_df_part1, "final_df_part1")

print("This is the count of NULLS vs empty strings vs literal 'NULL")
# Count NULLS vs empty strings vs literal "NULL"
final_df_part1.agg(
    F.count(F.when(F.col("src_last_chg_dt").isNull(), 1)).alias("null_count"),
    F.count(F.when(F.col("src_last_chg_dt") == "", 1)).alias("empty_string_count"),
    F.count(F.when(F.col("src_last_chg_dt") == "NULL", 1)).alias("literal_NULL_count")
).show()

final_df_part1 = final_df_part1.withColumnRenamed(
    "src_last_chg_dt", "source_last_change_date"
)

debug_df(final_df_part1, "Select stmnt #1 (final_df_part1)")

# Second Insert SELECT STMNT from SQL Server Records
final_df_part2 = (
    payable_payment_reply_df.alias("ppr")
    .join(t_pybl_eauth.filter(col("table_ind") == "ppr").alias("e"), "pybl_id")
    .join(df_t6.alias("t6"), "pybl_id", "left_outer")
    .select(
        col("pybl_id").alias("payable_identifier"),
        col("bia_rqst_ind").alias("bia_request_indicator"),
        col("nres_aln_ind").alias("nonresident_alien_indicator"),
        col("adtl_sel_item_ind").alias("associated_random_selection_indicator"),
        col("ck_out_dt").alias("check_out_date"),
        col("ck_out_user_nm").alias("check_out_user_name"),
        col("grp_pybl_nbr").alias("group_payable_number"),
        col("dspt_lcl_ind").alias("payable_dispute_local_indicator"),
        col("dspt_ntl_ind").alias("payable_dispute_national_indicator"),
        col("fnl_rvw_lvl_cd").alias("final_review_level_code"),
        col("e.fnl_eauth_name").alias("final_review_user_name"),
        col("init_rvw_dt").alias("initial_review_date"),
        col("e.init_eauth_name").alias("initial_review_user_name"),
        col("rndm_sel_item_ind").alias("random_selected_item_indicator"),
        col("smpl_bat_item_ind").alias("sampling_batch_item_indicator"),
        col("smpl_insp_fail_ind").alias("sampling_inspection_failed_indicator"),
        col("late_pymt_day_ct").alias("late_payment_day_count"),
        col("prmpt_pymt_int_rt").alias("prompt_payment_interest_rate"),
        col("prmpt_pymt_rsn_cd").alias("prompt_payment_reason_code"),
        col("tax_whld_pct").alias("tax_withholding_percent"),
        col("tax_whld_type_cd").alias("tax_withholding_type_code"),
        col("pymt_proc_ind").alias("payable_approved_indicator"),
        col("cnfrm_proc_ind").alias("payable_completed_indicator"),
        col("cmn_cust_nm").alias("common_customer_name"),
        col("stmt_gnrt_dt").alias("statement_generated_date"),
        col("txn_rqst_id").alias("transaction_request_identification"),
        col("pymt_ofst_alow_ind").alias("payment_offset_allowed_indicator"),
        col("DISB_CK_FRC_IND").alias("disbursement_check_forced_indicator"),
        col("ALT_PYE_PMSN_IND").alias("alternate_payee_permission_indicator"),
        col("pymt_disc_term_des").alias("payment_discount_terms_description"),
        col("ppr.last_chg_dt").alias("last_change_date"),
        # col("ppr.last_chg_dt").alias("last_change_date"),
        # if we want final_df_part1 to fall back to j when q is null:
        # F.coalesce(col("ppr.last_chg_dt"), col("j.src_last_chg_dt")).alias("src_last_chg_dt"),
        col("last_chg_user_nm").alias("source_last_change_user_name"),
        col("vol_whld_pct").alias("voluntary_withholding_percentage"),
        col("invol_whld_pct").alias("involuntary_withholding_percentage"),
        col("int_due_prn_amt").alias("interest_due_principal_amount"),
        col("form_1099_ind").alias("form_1099_indicator"),
        col("data_migr_stat_cd").alias("data_migration_status_code"),
        lit("N").alias("data_invalid_indicator"),
        col("dir_atrb_grp_nbr").alias("direct_attribution_group_number"),
        col("acct_txn_cd").alias("accounting_transaction_code"),
        col("tax_id").alias("tax_identification"),
        substring("tax_id", 6, 4).alias("tax_identification_last_4"),
        col("tax_id_type_cd").alias("tax_identification_type_code"),
        col("acct_cust_id").alias("accounting_customer_identifier"),
        col("acct_pgm_cd").alias("accounting_program_code"),
        col("acct_pgm_yr").alias("accounting_program_year"),
        col("prim_acct_ref_nbr").alias("accounting_reference_1_number"),
        col("prim_acct_ref_cd").alias("accounting_reference_1_code"),
        col("scnd_acct_ref_nbr").alias("accounting_reference_2_number"),
        col("scnd_acct_ref_cd").alias("accounting_reference_2_code"),
        col("t6.accounting_reference_3_number").alias("accounting_reference_3_number"),
        col("t6.accounting_reference_3_code").alias("accounting_reference_3_code"),
        col("t6.accounting_reference_4_number").alias("accounting_reference_4_number"),
        col("t6.accounting_reference_4_code").alias("accounting_reference_4_code"),
        col("t6.accounting_reference_5_number").alias("accounting_reference_5_number"),
        col("t6.accounting_reference_5_code").alias("accounting_reference_5_code"),
        col("pybl_stat_cd").alias("payable_status_code"),
        concat(col("st_fsa_cd"),col("cnty_fsa_cd")).alias("core_organization_code"),
        col("st_fsa_cd").alias("state_fsa_code"),
        col("cnty_fsa_cd").alias("county_fsa_code"),
        coalesce(col("pymt_isu_dt").cast("timestamp"), F.to_timestamp(lit("1900-01-01 00:00:00"))).alias("payment_issue_date"),
        col("pymt_rqst_amt").alias("payment_request_amount"),
        when(col("acct_txn_cd").isin(197,198), lit(0)) \
            .otherwise(col("prmpt_pymt_int_amt")).alias("prompt_payment_interest_amount"),
        when(col("acct_txn_cd") == 198, col("prmpt_pymt_int_amt")) \
            .otherwise(lit(None)).alias("interest_penalty_amount"),
        when(col("acct_txn_cd") == 197, col("prmpt_pymt_int_amt")) \
            .otherwise(lit(None)).alias("additional_interest_penalty_amount"),
        col("pgm_alp_cd").alias("program_alpha_code"),
        col("fnl_rvw_dt").alias("final_review_date"),
        col("dth_pymt_rsn_cd").alias("death_payment_reason_code"),
        col("txn_rqst_pkg_id").alias("transaction_request_package_identifier"),
        col("gr_loan_shr_amt").alias("gross_loan_share_amount"),
        col("obl_id").alias("obligation_identifier"),
        col("cncl_init_user_id").alias("cancellation_initiation_user_identification"),
        col("cncl_init_dt").alias("cancellation_initiation_date"),
        col("cncl_fnl_user_id").alias("cancellation_finalized_user_identification"),
        col("cncl_fnl_dt").alias("cancellation_finalized_date"),
        col("cncl_rsn_desc").alias("cancellation_reason_description"),
        col("acct_pgm_id").alias("accounting_program_identifier"),
        col("acct_svc_rqst_dt").alias("accounting_service_request_date"),
        col("bdgt_fscl_yr").alias("budget_fiscal_year"),
        col("prmpt_pymt_int_dt").alias("prompt_payment_interest_date"),
        col("cmdy_cd").alias("commodity_code"),
        col("bus_pty_id").alias("business_party_identifier"),
        col("tax_whld_amt").alias("tax_withholding_amount"),
        col("pymt_cmdy_qty").alias("accounting_transaction_quantity"),
        col("sys_cd").alias("system_code"),
        col("pymt_rcv_dt").alias("payment_received_date"),
        col("data_src_acro").alias("data_source_acronym")
    ).dropDuplicates(["payable_identifier"])
)

# debug_df(final_df_part2, "final_df_part2")

# Add the final src_last_chg_dt field with the desired logic involving nulls
final_df_part2 = (
    final_df_part2
    .withColumn("src_last_chg_dt", col("last_change_date"))
    .drop("last_change_date")
)

debug_df(final_df_part2, "final_df_part2")

print("This is the count of NULLS vs empty strings vs literal 'NULL")
# Count NULLS vs empty strings vs literal "NULL"
final_df_part2.agg(
    F.count(F.when(F.col("src_last_chg_dt").isNull(), 1)).alias("null_count"),
    F.count(F.when(F.col("src_last_chg_dt") == "", 1)).alias("empty_string_count"),
    F.count(F.when(F.col("src_last_chg_dt") == "NULL", 1)).alias("literal_NULL_count")
).show()

final_df_part2 = final_df_part2.withColumnRenamed(
    "src_last_chg_dt", "source_last_change_date"
)

# Count the number of NULLS in the "payment_received_date" column
null_count = final_df_part2.filter(col("payment_received_date").isNull()).count()

if null_count > 0:
    print(f"There are {null_count} NULL values in 'payment_received_date' column.")
    # To view the rows with NULLS:
    final_df_part2.filter(col("payment_received_date").isNull()).show()
else:
    print("No NULL values found in the 'payment_received_date' column.")
    
# Let's check for empty strings or whitespace as well
# Redshift will interpret these as NULLs if the column is NOT NULL):
print("Checking null situation in payment_received_date")
empty_or_null_count = final_df_part2.filter(
    (col("payment_received_date").isNull()) |
    (col("payment_received_date") == "") |
    (col("payment_received_date") == "NULL")
).count()
if empty_or_null_count > 0:
    print(f"There are {empty_or_null_count} rows with NULL, empty, or whitespace values in 'payment_received_date'.")
    final_df_part2.filter(
        (col("payment_received_date").isNull()) |
        (col("payment_received_date") == "") |
        (col("payment_received_date") == "NULL")
    ).show()
else:
    print("No NULL, empty, or whitespace values found in 'payment_received_date'.")

print("This is the count of NULLS vs empty strings vs literal 'NULL")

# Count NULLS vs empty strings vs literal "NULL"
final_df_part2.agg(
    F.count(F.when(F.col("payment_received_date").isNull(), 1)).alias("null_count"),
    F.count(F.when(F.col("payment_received_date") == "", 1)).alias("empty_string_count"),
    F.count(F.when(F.col("payment_received_date") == "NULL", 1)).alias("literal_NULL_count")
).show()

   
debug_df(final_df_part2, "Select stmnt #2 (final_df_part2)")


#combine PARt 1 and Part 2 
final_df_combined = final_df_part1.union(final_df_part2)
debug_df(final_df_combined, "final_df_combined")

# Ensure last_chg_dt is a timestamp b/c it is a string at this point
cols_to_format = [
    "source_last_change_date",
    "initial_review_date",
    "final_review_date",
    "accounting_service_request_date",
    "payment_received_date"
]

for c in cols_to_format:
    final_df_combined = final_df_combined.withColumn(
        c,
        date_trunc(
            "second",
            to_timestamp(col(c), source_fmt),  # converts the string to a timestamp
        )
)

print("Line 997")    
final_df_combined.select(
    "source_last_change_date",
    "initial_review_date",
    "final_review_date",
    "accounting_service_request_date",
    "payment_received_date"
).show(5,truncate=False)

debug_df(final_df_combined, "final_df_combined")

# Trim all String Columns to match DDL in RDS
column_limits = {
    "bia_request_indicator": 1,
    "nonresident_alien_indicator": 1,
    "associated_random_selection_indicator": 1,
    "payable_dispute_local_indicator": 1,
    "payable_dispute_national_indicator": 1,
    "random_selected_item_indicator": 1,
    "sampling_batch_item_indicator": 1,
    "sampling_inspection_failed_indicator": 1,
    "payable_approved_indicator": 1,
    "payable_completed_indicator": 1,
    "payment_offset_allowed_indicator": 1,
    "disbursement_check_forced_indicator": 1,
    "alternate_payee_permission_indicator": 1,
    "form_1099_indicator": 1,
    "data_migration_status_code": 1,
    "data_invalid_indicator": 1,
    "tax_identification_type_code": 1,
    "final_review_level_code": 2,
    "prompt_payment_reason_code": 2,
    "tax_withholding_type_code": 2,
    "accounting_reference_1_code": 2,
    "accounting_reference_2_code": 2,
    "accounting_reference_3_code": 2,
    "accounting_reference_4_code": 2,
    "accounting_reference_5_code": 2,
    "payable_status_code": 2,
    "state_fsa_code": 2,
    "death_payment_reason_code": 2,
    "system_code": 2,
    "accounting_transaction_code": 3,
    "county_fsa_code": 3,
    "tax_identification_last_4": 4,
    "accounting_program_code": 4,
    "accounting_program_year": 4,
    "commodity_code": 4,
    "core_organization_code": 5,
    "data_source_acronym": 5,
    "tax_identification": 9,
    "accounting_reference_1_number": 12,
    "accounting_reference_2_number": 12,
    "accounting_reference_3_number": 12,
    "accounting_reference_4_number": 12,
    "accounting_reference_5_number": 12,
    "program_alpha_code": 14,
    "transaction_request_identification": 15,
    "business_party_identifier": 18,
    "interest_due_principal_amount": 20,
    "payment_request_amount": 20,
    "prompt_payment_interest_amount": 20,
    "interest_penalty_amount": 20,
    "additional_interest_penalty_amount": 20,
    "gross_loan_share_amount": 20,
    "direct_attribution_group_number": 30,
    "check_out_user_name": 50,
    "final_review_user_name": 50,
    "initial_review_user_name": 50,
    "common_customer_name": 50,
    "payment_discount_terms_description": 50,
    "source_last_change_user_name": 50,
    "cancellation_initiation_user_identification": 50,
    "cancellation_finalized_user_identification": 50,
    "cancellation_reason_description": 255
}

def trim_df(df, column_limits):
    transformed_cols = [
        trim(col(c).cast(StringType())).substr(1, column_limits[c]).alias(c)
        if c in column_limits else col(c)
        for c in df.columns
    ]
    return df.select(*transformed_cols)

final_df_trimmed = trim_df(final_df_combined, column_limits)
debug_df(final_df_trimmed, "final_df_trimmed: Final DF After Trimming ")

#Handle columns with Bad Values Default Values: 
fill_na_df = final_df_trimmed.fillna({
    "form_1099_indicator": " ",
    "data_invalid_indicator": "N",
    "accounting_program_year": " "
})
fill_na_df.filter(col("accounting_reference_1_number").rlike(r"^[^a-zA-Z0-9]+$")).show(truncate=False)
defaults_handled_df = fill_na_df.withColumn(
    "accounting_reference_1_number",
    when(col("payable_identifier").isin('4495020','21477027','21477028'), "FIXED")
    .otherwise(col("accounting_reference_1_number"))
)

# FINAL SELECT STATEMENT w Casts that Match Target Schema
final_df_select = defaults_handled_df.select(
    col("payable_identifier").cast(IntegerType()),
    col("bia_request_indicator").cast(StringType()),
    col("nonresident_alien_indicator").cast(StringType()),
    col("associated_random_selection_indicator").cast(StringType()),
    col("check_out_date").cast(TimestampType()),
    col("check_out_user_name").cast(StringType()),
    col("group_payable_number").cast(IntegerType()),
    col("payable_dispute_local_indicator").cast(StringType()),
    col("payable_dispute_national_indicator").cast(StringType()),
    col("final_review_level_code").cast(StringType()),
    col("final_review_user_name").cast(StringType()),
    col("initial_review_date").cast(TimestampType()),
    col("initial_review_user_name").cast(StringType()),
    col("random_selected_item_indicator").cast(StringType()),
    col("sampling_batch_item_indicator").cast(StringType()),
    col("sampling_inspection_failed_indicator").cast(StringType()),
    col("late_payment_day_count").cast(ShortType()),
    col("prompt_payment_interest_rate").cast(DecimalType(7,4)),
    col("prompt_payment_reason_code").cast(StringType()),
    col("tax_withholding_percent").cast(DecimalType(7,4)),
    col("tax_withholding_type_code").cast(StringType()),
    col("payable_approved_indicator").cast(StringType()),
    col("payable_completed_indicator").cast(StringType()),
    col("common_customer_name").cast(StringType()),
    col("statement_generated_date").cast(TimestampType()),
    col("transaction_request_identification").cast(StringType()),
    col("payment_offset_allowed_indicator").cast(StringType()),
    col("disbursement_check_forced_indicator").cast(StringType()),
    col("alternate_payee_permission_indicator").cast(StringType()),
    col("payment_discount_terms_description").cast(StringType()),
    col("source_last_change_date").cast(TimestampType()),
    col("source_last_change_user_name").cast(StringType()),
    col("voluntary_withholding_percentage").cast(DecimalType(5,3)),
    col("involuntary_withholding_percentage").cast(DecimalType(5,3)),
    col("interest_due_principal_amount").cast(StringType()),
    col("form_1099_indicator").cast(StringType()),
    col("data_migration_status_code").cast(StringType()),
    col("data_invalid_indicator").cast(StringType()),
    col("direct_attribution_group_number").cast(StringType()),
    col("accounting_transaction_code").cast(StringType()),
    col("tax_identification").cast(StringType()),
    col("tax_identification_last_4").cast(StringType()),
    col("tax_identification_type_code").cast(StringType()),
    col("accounting_customer_identifier").cast(IntegerType()),
    col("accounting_program_code").cast(StringType()),
    col("accounting_program_year").cast(StringType()),
    col("accounting_reference_1_number").cast(StringType()),
    col("accounting_reference_1_code").cast(StringType()),
    col("accounting_reference_2_number").cast(StringType()),
    col("accounting_reference_2_code").cast(StringType()),
    col("accounting_reference_3_number").cast(StringType()),
    col("accounting_reference_3_code").cast(StringType()),
    col("accounting_reference_4_number").cast(StringType()),
    col("accounting_reference_4_code").cast(StringType()),
    col("accounting_reference_5_number").cast(StringType()),
    col("accounting_reference_5_code").cast(StringType()),
    col("payable_status_code").cast(StringType()),
    col("core_organization_code").cast(StringType()),
    col("state_fsa_code").cast(StringType()),
    col("county_fsa_code").cast(StringType()),
    col("payment_issue_date").cast(TimestampType()),
    col("payment_request_amount").cast(StringType()),
    col("prompt_payment_interest_amount").cast(StringType()),
    col("interest_penalty_amount").cast(StringType()),
    col("additional_interest_penalty_amount").cast(StringType()),
    col("program_alpha_code").cast(StringType()),
    col("final_review_date").cast(TimestampType()),
    col("death_payment_reason_code").cast(StringType()),
    col("transaction_request_package_identifier").cast(IntegerType()),
    col("gross_loan_share_amount").cast(StringType()),
    col("obligation_identifier").cast(IntegerType()),
    col("cancellation_initiation_user_identification").cast(StringType()),
    col("cancellation_initiation_date").cast(TimestampType()),
    col("cancellation_finalized_user_identification").cast(StringType()),
    col("cancellation_finalized_date").cast(TimestampType()),
    col("cancellation_reason_description").cast(StringType()),
    col("accounting_program_identifier").cast(IntegerType()),
    col("accounting_service_request_date").cast(TimestampType()),
    col("budget_fiscal_year").cast(ShortType()),
    col("prompt_payment_interest_date").cast(TimestampType()),
    col("commodity_code").cast(StringType()),
    col("business_party_identifier").cast(StringType()),
    col("tax_withholding_amount").cast(DecimalType(16,2)),
    col("accounting_transaction_quantity").cast(DecimalType(16,2)),
    col("system_code").cast(StringType()),
    col("payment_received_date").cast(TimestampType()),
    col("data_source_acronym").cast(StringType()),
)
debug_df(final_df_select, "FINAL DF Select")

# Add Extra Blank row w PK of 0
extra_row_schema = final_df_select.schema
extra_row_values = [(
    0,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,
    None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None," "," ",
    " ",None," "," "," "," ",0," "," "," "," ",None,None,None,None,None,None,None,
    None," "," "," "," ",datetime(1900,1,1,0,0),None,None,None,None," ",None,None,
    None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None
)]
extra_row_df = spark.createDataFrame(extra_row_values, schema=extra_row_schema)

final_df_wExtraRow = final_df_select.unionByName(extra_row_df)

final_df = final_df_wExtraRow
debug_df(final_df, "FINAL DF BEFORE WRITE TO RDS")

############### Debugging #############################

# Count the number of NULLS in the "source_last_change_date" column
null_count = final_df.filter(col("source_last_change_date").isNull()).count()

if null_count > 0:
    print(f"There are {null_count} NULL values in 'source_last_change_date' column.")
    # To view the rows with NULLS:
    final_df.filter(col("source_last_change_date").isNull()).show()
else:
    print("No NULL values found in the 'source_last_change_date' column.")
    
# Let's check for empty strings or whitespace as well
# Redshift will interpret these as NULLs if the column is NOT NULL):
empty_or_null_count = final_df.filter(
    (col("source_last_change_date").isNull()) |
    (col("source_last_change_date") == "") |
    (col("source_last_change_date") == "NULL")
).count()
if empty_or_null_count > 0:
    print(f"There are {empty_or_null_count} rows with NULL, empty, or whitespace values in 'source_last_change_date'.")
    final_df.filter(
        (col("source_last_change_date").isNull()) |
        (col("source_last_change_date") == "") |
        (col("source_last_change_date") == "NULL")
    ).show()
else:
    print("No NULL, empty, or whitespace values found in 'source_last_change_date'.")



target_columns = [
    "payable_identifier","form_1099_indicator","data_migration_status_code","data_invalid_indicator",
    "accounting_transaction_code","tax_identification","tax_identification_last_4",
    "tax_identification_type_code","accounting_customer_identifier","accounting_program_code",
    "accounting_program_year","accounting_reference_1_number","accounting_reference_1_code",
    "payable_status_code","core_organization_code","state_fsa_code",
    "county_fsa_code","payment_issue_date","program_alpha_code"
]
null_counts = [F.sum(col(c).isNull().cast("int")).alias(f"{c}_nulls") for c in target_columns]
final_df.select(null_counts).show()

string_columns = [f.name for f in final_df.schema.fields if isinstance(f.dataType, StringType)]
length_exprs = [F.max(F.length(col(c))).alias(f"{c}_max_len") for c in string_columns]
final_df.select(length_exprs).show()
###################################################


###############################################################################################
# Write to S3 in Parquet format with Snappy compression
###############################################################################################
final_df.write.mode("overwrite").parquet(destination_path, compression="snappy")
print(f"Final Data Frame written to S3 at: {destination_path}")

#############################################################################################
# Run a validation and handle quarantining and logging
#############################################################################################
# Variables

validation_results = []
dq_passed = True

# Input: Table Name, Columns, and DataFrames
pk_column = "payable_identifier"
fk_column = "bia_request_indicator"
df_final = final_df

try:
    # Primary Key Validation
    status, failures_df, reason = validate_pk(df_final, pk_column, logger)
    quarantine_path = None
    if not status and failures_df and not failures_df.isEmpty():
        quarantine_path = f"{final_zone_path}/Quarantine/PrimaryKey/"
        failures_df.write.mode("overwrite").parquet(quarantine_path)
        msg = f"PrimaryKey failed rows quarantined at: {quarantine_path}"
        logger.info(msg)
        print(msg)
    log_and_append_result("PrimaryKey", status, failures_df, reason, tablename, quarantine_path, schemaname, logger, validation_results)
    dq_passed = dq_passed and status
    print(f'PK validated.  dq_passed is {dq_passed}')

    # Foreign Key Validation
    fk_dim_df = df_final
    status, failures_df, reason = validate_fk(df_final, fk_column, fk_dim_df, logger)
    failures_df.show(5)
    quarantine_path = None
    if not status and failures_df and not failures_df.isEmpty():
        quarantine_path = f"{final_zone_path}/Quarantine/ForeignKey/"
        failures_df.write.mode("overwrite").parquet(quarantine_path)
        msg = f"ForeignKey failed rows quarantined at: {quarantine_path}"
        logger.info(msg)
        print(msg)
    log_and_append_result("ForeignKey", status, failures_df, reason, tablename, quarantine_path, schemaname, logger, validation_results)
    dq_passed = dq_passed and status
    print(f'FK validated.  dq_passed is {dq_passed}')


    # Null Validation
    status, failures_df, reason = validate_nulls(df_final, ["data_migration_status_code", "tax_identification_type_code"], logger)
    quarantine_path = None
    if not status and failures_df and not failures_df.isEmpty():
        # Save failed rows to quarantine
        quarantine_path = f"{final_zone_path}/Quarantine/NullValidation/"
        failures_df.write.mode("overwrite").parquet(quarantine_path)
        msg = f"Null validation failed rows quarantined at: {quarantine_path}"
        logger.info(msg)
        print(msg)
    log_and_append_result("NullValidation", status, failures_df, reason, tablename, quarantine_path, schemaname, logger, validation_results)
    dq_passed = dq_passed and status
    print(f'Null validated.  dq_passed is {dq_passed}')


    # Duplicate Validation
    status, failures_df, reason = validate_duplicates(df_final, ["payable_identifier"], logger)
    quarantine_path = None
    if not status and failures_df and not failures_df.isEmpty():
        quarantine_path = f"{final_zone_path}/Quarantine/Duplicates/"
        failures_df.write.mode("overwrite").parquet(quarantine_path)
        msg = f"Duplicates failed rows quarantined at: {quarantine_path}"
        logger.info(msg)
        print(msg)
    log_and_append_result("Duplicates", status, failures_df, reason, tablename, quarantine_path, schemaname, logger, validation_results)
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
results_path = f"{resultspath}/{tmstmp}/"
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

'''
### Try Writing to RDS using Dynamic Frame since COPY CMD being funky
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
##############################################################################################
# Generate & Execute Redshift COPY command for bulk loading
############################################################################################### 
try:
    logger.info("All Data Quality checks passed. Proceeding with Redshift COPY command.")
    conn = sc._gateway.jvm.DriverManager.getConnection(jdbc_url, username, password)
    stmt = conn.createStatement()
    # if CFI_records_present: 
    #     print('Deleting yesterdays incrementals')
    #     stmt.executeUpdate(f"delete from fwadm_dw.{tablename} where {pk_column} > 0")
    #################################################################################
    # Generate Redshift COPY command for bulk loading
    # Adding EXPLICIT_IDS ensures Redshift retains the provided primary key values
    # instead of overwriting them with auto-generated IDs.
    #################################################################################
    copy_command = f"""
    COPY {schemaname}.{tablename}
    FROM '{destination_path}'
    IAM_ROLE '{arnrole}'
    FORMAT AS PARQUET
    EXPLICIT_IDS;
    """
    print(f"copy_command: {copy_command}")
    logger.info(f"Executing COPY command : {copy_command}")
    trnc_command = 'truncate fwadm_dw.payable_dim'
    stmt.executeUpdate(trnc_command)
    stmt.executeUpdate(copy_command)
    stmt.close()
    conn.close()
    logger.info("Redshift COPY command executed successfully.")
    
except Exception as e:
    msg = f"Unexpected error during COPY COMMAND execution: {str(e)}"
    logger.error(msg)
    print(msg)


endtime_m = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
job_stat_nm = f"FWADM NPS {tablename} successfully loaded"
utils.data_ppln_job_insert(job_stat_nm, 'Complete', starttime_m, endtime_m, pcdb_conn, ENVIRON, 'NPS')

logger.info("Glue Job finished.")
job.commit()
