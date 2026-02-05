###################################################################################
# Developer Name: Sajid Inayat
# Date: 01/28/25
# Script Name: FSA-PMRDS-ODS-TO-STG
# History:
#   01/28/25 - Sajid -- Initial version for PMRDS pipeline:
#                       - Reads ODS source tables from Final Zone
#                       - Aggregates by fiscal dimensions
#                       - Writes PMRDS_STG tables to Cleansed Zone
#                       - Built-in DQ validation at end
###################################################################################

import sys
import json
import boto3
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import *


###################################################################################
# Parse Job Arguments
###################################################################################
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'env',
    'ppln_nm',
    'step_nm',
    'operator_nm',
    'system_date'
])


if '--system_date' in sys.argv:
    args_with_date = getResolvedOptions(sys.argv, ['system_date'])
    SYSTEM_DATE = args_with_date['system_date']
else:
    SYSTEM_DATE = datetime.now().strftime('%Y%m%d')

JOB_NAME = args['JOB_NAME']
ENV = args['env'].lower()
PPLN_NM = args['ppln_nm']
STEP_NM = args['step_nm']
OPERATOR_NM = args['operator_nm']
SYSTEM_DATE = args['system_date']  # Format: YYYYMMDD

print("=" * 60)
print(f"PMRDS Stage 1: ODS to STG (with DQ)")
print("=" * 60)
print(f"JOB_NAME: {JOB_NAME}")
print(f"ENV: {ENV}")
print(f"PPLN_NM: {PPLN_NM}")
print(f"STEP_NM: {STEP_NM}")
print(f"OPERATOR_NM: {OPERATOR_NM}")
print(f"SYSTEM_DATE: {SYSTEM_DATE}")
print("=" * 60)

###################################################################################
# Initialize Glue Context
###################################################################################
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, args)

###################################################################################
# Performance Optimization
###################################################################################
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

###################################################################################
# Job initiation
###################################################################################
job.init(JOB_NAME, args)

###################################################################################
# Job Status Tracker - Track job execution status - writes to S3 for Step Function
###################################################################################

class JobStatus:
    def __init__(self, job_name, stage, env, system_date):
        self.job_name = job_name
        self.stage = stage  # "STAGE1" or "STAGE2"
        self.env = env
        self.system_date = system_date
        self.start_time = datetime.now()
        self.end_time = None
        self.status = "SUCCESS"  # SUCCESS, WARNING, FAILED
        
        self.sources_processed = []
        self.sources_missing = []
        self.tables_written = []
        self.tables_skipped = []
        self.warnings = []
        self.errors = []
        self.metrics = {
            "total_input_rows": 0,
            "total_output_rows": 0
        }
    
    def add_source_found(self, source_name, row_count):
        self.sources_processed.append({"name": source_name, "rows": row_count})
        self.metrics["total_input_rows"] += row_count
    
    def add_source_missing(self, source_name, path):
        self.sources_missing.append({"name": source_name, "path": path})
        self.warnings.append(f"Source missing: {source_name}")
        if self.status == "SUCCESS":
            self.status = "WARNING"
    
    def add_table_written(self, table_name, row_count, path):
        self.tables_written.append({"name": table_name, "rows": row_count, "path": path})
        self.metrics["total_output_rows"] += row_count
    
    def add_table_skipped(self, table_name, reason):
        self.tables_skipped.append({"name": table_name, "reason": reason})
        self.warnings.append(f"Table skipped: {table_name} - {reason}")
    
    def add_warning(self, message):
        self.warnings.append(message)
        if self.status == "SUCCESS":
            self.status = "WARNING"
    
    def add_error(self, message):
        self.errors.append(message)
        self.status = "FAILED"
    
    def finalize(self):
        self.end_time = datetime.now()
        self.duration_seconds = (self.end_time - self.start_time).total_seconds()
    
    ##############################################################################
    # Return status as dictionary for JSON
    ##############################################################################
    def to_dict(self):
        self.finalize()
        return {
            "job_name": self.job_name,
            "stage": self.stage,
            "env": self.env,
            "system_date": self.system_date,
            "status": self.status,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "duration_seconds": self.duration_seconds,
            "sources_processed": self.sources_processed,
            "sources_missing": self.sources_missing,
            "tables_written": self.tables_written,
            "tables_skipped": self.tables_skipped,
            "metrics": self.metrics,
            "warnings": self.warnings,
            "errors": self.errors
        }

    ##############################################################################
    # Write status JSON to S3
    ##############################################################################    
    def write_to_s3(self, bucket, prefix="PMRDS_STATUS"):
        s3 = boto3.client('s3')
        key = f"{prefix}/{self.system_date}/{self.stage}_status.json"
        
        try:
            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=json.dumps(self.to_dict(), indent=2),
                ContentType='application/json'
            )
            print(f"Status written to s3://{bucket}/{key}")
            return f"s3://{bucket}/{key}"
        except Exception as e:
            print(f"Failed to write status: {str(e)}")
            return None

###################################################################################
# Initialize status tracker
###################################################################################
status = JobStatus(JOB_NAME, "STAGE1", ENV, SYSTEM_DATE)

print("=" * 80)
print(f"JOB: {JOB_NAME} | ENV: {ENV} | DATE: {SYSTEM_DATE}")
print("=" * 80)

########################################################################################################
# Environment Configuration
########################################################################################################

BUCKETS = {
    "dev": {"final": "c108-dev-fpacfsa-final-zone", "cleansed": "c108-dev-fpacfsa-cleansed-zone"},
    "cert": {"final": "c108-cert-fpacfsa-final-zone", "cleansed": "c108-cert-fpacfsa-cleansed-zone"},
    "prod": {"final": "c108-prod-fpacfsa-final-zone", "cleansed": "c108-prod-fpacfsa-cleansed-zone"}
}

FINAL_BUCKET = BUCKETS[ENV]["final"]
CLEANSED_BUCKET = BUCKETS[ENV]["cleansed"]

########################################################################################################
# Source Paths
########################################################################################################

SOURCE_PATHS = {
    # CPS
    "CPS_PYMT_EVNT": f"s3://{FINAL_BUCKET}/cps/payment_event/",
    "CPS_DIR_ATRB_CNFRM": f"s3://{FINAL_BUCKET}/cps/direct_attribution_confirmation/",
    "CPS_ACCT_PGM": f"s3://{FINAL_BUCKET}/cps/accounting_program/",
    
    # SBSD
    "SBSD_PYMT_ATRB": f"s3://{FINAL_BUCKET}/sbsd/payment_attribution/",
    "SBSD_PRPS_PYMT": f"s3://{FINAL_BUCKET}/sbsd/proposed_payment/",
    
    # ARCPLC
    "ARC_PYMT_EVNT": f"s3://{FINAL_BUCKET}/arcplc/payment_event/",
    "ARC_DIR_ATRB_CNFRM": f"s3://{FINAL_BUCKET}/arcplc/direct_attribution_confirmation/",
    "ARC_ANL_CTR": f"s3://{FINAL_BUCKET}/arcplc/annual_contract/",
    "ARC_ANL_REV_PRTC_PGM": f"s3://{FINAL_BUCKET}/arcplc/annual_revenue_protection_program/",
    "ARC_CTR_PGM": f"s3://{FINAL_BUCKET}/arcplc/contract_program/",
    "ARC_PGM_YR": f"s3://{FINAL_BUCKET}/arcplc/program_year/",
    
    # NPS
    "NPS_PYBL": f"s3://{FINAL_BUCKET}/dmart/fwadm/nps/payable_dim/",
    "NPS_CORE_XREF": f"s3://{FINAL_BUCKET}/dmart/fwadm/nps/core_cross_reference/",
    "NPS_ACCT_PGM": f"s3://{FINAL_BUCKET}/dmart/fwadm/nps/accounting_program/",
    "NPS_PYMT_TXN_FACT": f"s3://{FINAL_BUCKET}/dmart/fwadm/nps/payment_transaction_fact/",
    "NPS_PYMT_SUMM": f"s3://{FINAL_BUCKET}/dmart/fwadm/nps/payment_summary/",
    
    # NRRS
    "NRRS_GL_TXN_QUE": f"s3://{FINAL_BUCKET}/dmart/fwadm/nrrs/gl_transaction_queue/",
    "NRRS_GL_TXN_QUE_ARCV": f"s3://{FINAL_BUCKET}/dmart/fwadm/nrrs/gl_transaction_queue_archive/",
    "NRRS_RECEIVABLE": f"s3://{FINAL_BUCKET}/dmart/fwadm/nrrs/receivable/",
    "NRRS_RCV_BAL_DET": f"s3://{FINAL_BUCKET}/dmart/fwadm/nrrs/receivable_balance_detail/",
}

def get_stg_output_path(table_name: str) -> str:
    return f"s3://{CLEANSED_BUCKET}/PMRDS_STG/{table_name}/dt={SYSTEM_DATE}"

########################################################################################################
# Read source with status tracking
########################################################################################################
def read_source_safe(source_name: str):
    path = SOURCE_PATHS.get(source_name)
    if not path:
        status.add_warning(f"Unknown source: {source_name}")
        return None
    
    try:
        df = spark.read.parquet(path)
        count = df.count()
        if count == 0:
            status.add_source_missing(source_name, f"{path} (empty)")
            print(f"{source_name}: Empty (0 rows)")
            return None
        
        status.add_source_found(source_name, count)
        print(f"{source_name}: {count:,} rows")
        return df
    except Exception as e:
        status.add_source_missing(source_name, path)
        print(f"{source_name}: Not found at {path}")
        return None

########################################################################################################
# Fiscal Year Calculation: Oct=Month 1 of next year
########################################################################################################
def add_fiscal_year(df, date_col):
    return df.withColumn(
        "FSCL_YR",
        F.when(F.month(F.col(date_col)) >= 10, F.year(F.col(date_col)) + 1)
         .otherwise(F.year(F.col(date_col)))
    ).withColumn(
        "FSCL_MO",
        F.when(F.month(F.col(date_col)) >= 10, F.month(F.col(date_col)) - 9)
         .otherwise(F.month(F.col(date_col)) + 3)
    )

########################################################################################################
# Aggregation Functions
########################################################################################################

########################################################################################################
# CPS payment_event aggregation
########################################################################################################
def aggregate_cps():
    print("\n>>> Processing CPS...")
    
    df = read_source_safe("CPS_PYMT_EVNT")
    if df is None:
        return None
    
    if "data_status_code" in df.columns:
        df = df.filter(F.col("data_status_code") == "A")
    
    date_col = next((c for c in ["payment_event_date", "pymt_evnt_dt", "PYMT_EVNT_DT"] if c in df.columns), None)
    if date_col:
        df = add_fiscal_year(df, date_col)
    else:
        df = df.withColumn("FSCL_YR", F.lit(None).cast("int")).withColumn("FSCL_MO", F.lit(None).cast("int"))
    
    amt_col = next((c for c in ["payment_amount", "pymt_amt", "PYMT_AMT"] if c in df.columns), None)
    if not amt_col:
        status.add_warning("CPS: Could not find payment amount column")
        return None
    
    pgm_col = next((c for c in ["accounting_program_identifier", "acct_pgm_id", "ACCT_PGM_ID"] if c in df.columns), None)
    
    group_cols = ["FSCL_YR", "FSCL_MO"]
    if pgm_col:
        group_cols.append(pgm_col)
    
    df_agg = df.groupBy(*group_cols).agg(
        F.sum(F.col(amt_col).cast("decimal(18,2)")).alias("PYMT_AMT"),
        F.count("*").alias("TXN_CT")
    ).withColumn("SRC_SYS", F.lit("CPS")).withColumn("LOAD_DT", F.current_timestamp())
    
    if pgm_col and pgm_col != "ACCT_PGM_ID":
        df_agg = df_agg.withColumnRenamed(pgm_col, "ACCT_PGM_ID")
    
    return df_agg

########################################################################################################
# SBSD payment_attribution aggregation
########################################################################################################
def aggregate_sbsd():
    print("\n>>> Processing SBSD...")
    
    df = read_source_safe("SBSD_PYMT_ATRB")
    if df is None:
        return None
    
    if "data_status_code" in df.columns:
        df = df.filter(F.col("data_status_code") == "A")
    
    cols = df.columns
    atrb_amt = next((c for c in ["payment_attribution_amount", "pymt_atrb_amt", "PYMT_ATRB_AMT"] if c in cols), None)
    net_amt = next((c for c in ["net_payment_amount", "net_pymt_amt", "NET_PYMT_AMT"] if c in cols), None)
    
    if not atrb_amt and not net_amt:
        status.add_warning("SBSD: Could not find payment amount columns")
        return None
    
    pgm_col = next((c for c in ["accounting_program_code", "acct_pgm_cd", "ACCT_PGM_CD"] if c in cols), None)
    yr_col = next((c for c in ["subsidiary_period_start_year", "sbsd_prd_strt_yr", "SBSD_PRD_STRT_YR"] if c in cols), None)
    st_col = next((c for c in ["state_fsa_code", "st_fsa_cd", "ST_FSA_CD"] if c in cols), None)
    cnty_col = next((c for c in ["county_fsa_code", "cnty_fsa_cd", "CNTY_FSA_CD"] if c in cols), None)
    
    group_cols = [c for c in [pgm_col, yr_col, st_col, cnty_col] if c]
    if not group_cols:
        df = df.withColumn("ACCT_PGM_CD", F.lit("ALL"))
        group_cols = ["ACCT_PGM_CD"]
    
    agg_exprs = [F.count("*").alias("TXN_CT")]
    if atrb_amt:
        agg_exprs.append(F.sum(F.col(atrb_amt).cast("decimal(18,2)")).alias("PYMT_ATRB_AMT"))
    if net_amt:
        agg_exprs.append(F.sum(F.col(net_amt).cast("decimal(18,2)")).alias("NET_PYMT_AMT"))
    
    cust_col = next((c for c in ["subsidiary_customer_identifier", "sbsd_cust_id"] if c in cols), None)
    if cust_col:
        agg_exprs.append(F.countDistinct(cust_col).alias("CUST_CT"))
    
    df_agg = df.groupBy(*group_cols).agg(*agg_exprs)
    df_agg = df_agg.withColumn("SRC_SYS", F.lit("SBSD")).withColumn("LOAD_DT", F.current_timestamp())
    
    rename_map = {pgm_col: "ACCT_PGM_CD", yr_col: "PGM_YR", st_col: "ST_FSA_CD", cnty_col: "CNTY_FSA_CD"}
    for old, new in rename_map.items():
        if old and old in df_agg.columns and old != new:
            df_agg = df_agg.withColumnRenamed(old, new)
    
    return df_agg

########################################################################################################
# ARCPLC payment_event aggregation
########################################################################################################
def aggregate_arcplc():
    print("\n>>> Processing ARCPLC...")
    
    df = read_source_safe("ARC_PYMT_EVNT")
    if df is None:
        return None
    
    if "data_status_code" in df.columns:
        df = df.filter(F.col("data_status_code") == "A")
    
    date_col = next((c for c in ["payment_event_date", "pymt_evnt_dt", "PYMT_EVNT_DT"] if c in df.columns), None)
    if date_col:
        df = add_fiscal_year(df, date_col)
    else:
        df = df.withColumn("FSCL_YR", F.lit(None).cast("int")).withColumn("FSCL_MO", F.lit(None).cast("int"))
    
    cols = df.columns
    amt_col = next((c for c in ["payment_amount", "pymt_amt", "PYMT_AMT"] if c in cols), None)
    pgm_col = next((c for c in ["accounting_program_identifier", "acct_pgm_id", "ACCT_PGM_ID"] if c in cols), None)
    
    if not amt_col:
        status.add_warning("ARCPLC: Could not find payment amount column")
        return None
    
    group_cols = ["FSCL_YR", "FSCL_MO"]
    if pgm_col:
        group_cols.append(pgm_col)
    
    df_agg = df.groupBy(*group_cols).agg(
        F.sum(F.col(amt_col).cast("decimal(18,2)")).alias("PYMT_AMT"),
        F.count("*").alias("TXN_CT")
    ).withColumn("SRC_SYS", F.lit("ARCPLC")).withColumn("LOAD_DT", F.current_timestamp())
    
    if pgm_col and pgm_col != "ACCT_PGM_ID":
        df_agg = df_agg.withColumnRenamed(pgm_col, "ACCT_PGM_ID")
    
    return df_agg

########################################################################################################
# NPS payable aggregation
########################################################################################################
# def aggregate_nps():
#     print("\n>>> Processing NPS...")
    
#     df = read_source_safe("NPS_PYBL")
#     if df is None:
#         return None
    
#     cols = df.columns
#     amt_col = next((c for c in ["PYMT_RQST_AMT", "pymt_rqst_amt", "payment_request_amount"] if c in cols), None)
#     int_col = next((c for c in ["PRMPT_PYMT_INT_AMT", "prmpt_pymt_int_amt"] if c in cols), None)
    
#     if not amt_col:
#         status.add_warning("NPS: Could not find payment amount column")
#         return None
    
#     # fscl_yr_col = next((c for c in ["BDGT_FSCL_YR", "bdgt_fscl_yr"] if c in cols), None)
#     # st_col = next((c for c in ["ST_FSA_CD", "st_fsa_cd"] if c in cols), None)
#     # cnty_col = next((c for c in ["CNTY_FSA_CD", "cnty_fsa_cd"] if c in cols), None)
#     # pgm_col = next((c for c in ["ACCT_PGM_CD", "acct_pgm_cd"] if c in cols), None)
#     # pgm_yr_col = next((c for c in ["ACCT_PGM_YR", "acct_pgm_yr"] if c in cols), None)
    
#     fscl_yr_col = next((c for c in ["BDGT_FSCL_YR", "bdgt_fscl_yr", "budget_fiscal_year"] if c in cols), None)
#     st_col = next((c for c in ["ST_FSA_CD", "st_fsa_cd", "state_fsa_code"] if c in cols), None)
#     cnty_col = next((c for c in ["CNTY_FSA_CD", "cnty_fsa_cd", "county_fsa_code"] if c in cols), None)
#     pgm_col = next((c for c in ["ACCT_PGM_CD", "acct_pgm_cd", "accounting_program_code"] if c in cols), None)
#     pgm_yr_col = next((c for c in ["ACCT_PGM_YR", "acct_pgm_yr", "accounting_program_year"] if c in cols), None)
    
#     group_cols = [c for c in [fscl_yr_col, st_col, cnty_col, pgm_col, pgm_yr_col] if c]
#     col_mapping = {fscl_yr_col: "FSCL_YR", st_col: "ST_FSA_CD", cnty_col: "CNTY_FSA_CD", pgm_col: "ACCT_PGM_CD", pgm_yr_col: "PGM_YR"}
    
#     if not group_cols:
#         status.add_warning("NPS: Could not find grouping columns")
#         return None
    
#     agg_exprs = [F.sum(F.col(amt_col).cast("decimal(18,2)")).alias("PYMT_RQST_AMT"), F.count("*").alias("TXN_CT")]
#     if int_col:
#         agg_exprs.append(F.sum(F.col(int_col).cast("decimal(18,2)")).alias("PRMPT_PYMT_INT_AMT"))
    
#     df_agg = df.groupBy(*group_cols).agg(*agg_exprs)
#     df_agg = df_agg.withColumn("SRC_SYS", F.lit("NPS")).withColumn("LOAD_DT", F.current_timestamp())
    
#     for old, new in col_mapping.items():
#         if old and old in df_agg.columns and old != new:
#             df_agg = df_agg.withColumnRenamed(old, new)
    
#     return df_agg

def aggregate_nps():
    print("\n>>> Processing NPS...")
    
    # Read primary payable data
    df = read_source_safe("NPS_PYBL")
    if df is None:
        return None
    
    cols = df.columns
    amt_col = next((c for c in ["PYMT_RQST_AMT", "pymt_rqst_amt", "payment_request_amount"] if c in cols), None)
    
    if not amt_col:
        status.add_warning("NPS: Could not find payment amount column")
        return None
    
    # Column lookups for grouping (with new descriptive names from refactored parquets)
    fscl_yr_col = next((c for c in ["BDGT_FSCL_YR", "bdgt_fscl_yr", "budget_fiscal_year"] if c in cols), None)
    st_col = next((c for c in ["ST_FSA_CD", "st_fsa_cd", "state_fsa_code"] if c in cols), None)
    cnty_col = next((c for c in ["CNTY_FSA_CD", "cnty_fsa_cd", "county_fsa_code"] if c in cols), None)
    pgm_col = next((c for c in ["ACCT_PGM_CD", "acct_pgm_cd", "accounting_program_code"] if c in cols), None)
    pgm_yr_col = next((c for c in ["ACCT_PGM_YR", "acct_pgm_yr", "accounting_program_year"] if c in cols), None)
    
    group_cols = [c for c in [fscl_yr_col, st_col, cnty_col, pgm_col, pgm_yr_col] if c]
    col_mapping = {fscl_yr_col: "FSCL_YR", st_col: "ST_FSA_CD", cnty_col: "CNTY_FSA_CD", pgm_col: "ACCT_PGM_CD", pgm_yr_col: "PGM_YR"}
    
    if not group_cols:
        status.add_warning("NPS: Could not find grouping columns")
        return None
    
    # Aggregate payable amounts
    df_agg = df.groupBy(*group_cols).agg(
        F.sum(F.col(amt_col).cast("decimal(18,2)")).alias("PYMT_RQST_AMT"),
        F.count("*").alias("TXN_CT")
    )
    
    ############################################################################################
    # NEW: Read payment_transaction_fact for prompt payment interest
    ############################################################################################
    df_txn = read_source_safe("NPS_PYMT_TXN_FACT")
    
    if df_txn is not None:
        txn_cols = df_txn.columns
        
        # Find interest column in payment_transaction_fact
        int_col = next((c for c in ["PRMPT_PYMT_INT_AMT", "prmpt_pymt_int_amt", "prompt_payment_interest_amount"] if c in txn_cols), None)
        
        if int_col:
            print(f"    Found interest column '{int_col}' in payment_transaction_fact")
            
            # Find matching grouping columns in txn fact
            txn_fscl_yr = next((c for c in ["BDGT_FSCL_YR", "bdgt_fscl_yr", "budget_fiscal_year"] if c in txn_cols), None)
            txn_st = next((c for c in ["ST_FSA_CD", "st_fsa_cd", "state_fsa_code"] if c in txn_cols), None)
            txn_cnty = next((c for c in ["CNTY_FSA_CD", "cnty_fsa_cd", "county_fsa_code"] if c in txn_cols), None)
            txn_pgm = next((c for c in ["ACCT_PGM_CD", "acct_pgm_cd", "accounting_program_code"] if c in txn_cols), None)
            txn_pgm_yr = next((c for c in ["ACCT_PGM_YR", "acct_pgm_yr", "accounting_program_year"] if c in txn_cols), None)
            
            txn_group_cols = [c for c in [txn_fscl_yr, txn_st, txn_cnty, txn_pgm, txn_pgm_yr] if c]
            
            if txn_group_cols:
                # Aggregate interest amounts from transaction fact
                df_int_agg = df_txn.groupBy(*txn_group_cols).agg(
                    F.sum(F.col(int_col).cast("decimal(18,2)")).alias("PRMPT_PYMT_INT_AMT")
                )
                
                # Rename columns to match standard names for join
                txn_col_mapping = {txn_fscl_yr: "FSCL_YR", txn_st: "ST_FSA_CD", txn_cnty: "CNTY_FSA_CD", txn_pgm: "ACCT_PGM_CD", txn_pgm_yr: "PGM_YR"}
                for old, new in txn_col_mapping.items():
                    if old and old in df_int_agg.columns and old != new:
                        df_int_agg = df_int_agg.withColumnRenamed(old, new)
                
                # Rename payable columns before join
                for old, new in col_mapping.items():
                    if old and old in df_agg.columns and old != new:
                        df_agg = df_agg.withColumnRenamed(old, new)
                
                # Join interest data with payable aggregation
                join_cols = [new for old, new in col_mapping.items() if old]
                df_agg = df_agg.join(df_int_agg, join_cols, "left_outer")
                
                # Fill nulls for records without interest
                df_agg = df_agg.fillna(0, subset=["PRMPT_PYMT_INT_AMT"])
                
                int_total = df_agg.agg(F.sum("PRMPT_PYMT_INT_AMT")).collect()[0][0] or 0
                print(f"    Interest total: ${int_total:,.2f}")
            else:
                status.add_warning("NPS: Could not find grouping columns in payment_transaction_fact")
                # Still rename columns for output
                for old, new in col_mapping.items():
                    if old and old in df_agg.columns and old != new:
                        df_agg = df_agg.withColumnRenamed(old, new)
        else:
            print("    No interest column found in payment_transaction_fact")
            # Still rename columns for output
            for old, new in col_mapping.items():
                if old and old in df_agg.columns and old != new:
                    df_agg = df_agg.withColumnRenamed(old, new)
    else:
        print("    payment_transaction_fact not available - skipping interest")
        # Rename columns for output
        for old, new in col_mapping.items():
            if old and old in df_agg.columns and old != new:
                df_agg = df_agg.withColumnRenamed(old, new)
    
    df_agg = df_agg.withColumn("SRC_SYS", F.lit("NPS")).withColumn("LOAD_DT", F.current_timestamp())
    
    return df_agg


########################################################################################################
# NRRS GL transaction aggregation with reversal logic
########################################################################################################
def aggregate_nrrs():
    print("\n>>> Processing NRRS...")
    
    df = read_source_safe("NRRS_GL_TXN_QUE")
    if df is None:
        df = read_source_safe("NRRS_GL_TXN_QUE_ARCV")
    if df is None:
        return None
    
    cols = df.columns
    amt_col = next((c for c in ["TXN_AMT", "txn_amt"] if c in cols), None)
    rvrs_col = next((c for c in ["RVRS_TXN_IND", "rvrs_txn_ind"] if c in cols), None)
    pgm_col = next((c for c in ["ACCT_PGM_ID", "acct_pgm_id"] if c in cols), None)
    
    if not amt_col:
        status.add_warning("NRRS: Could not find transaction amount column")
        return None
    
    if rvrs_col:
        df = df.withColumn("ADJ_TXN_AMT",
            F.when(F.upper(F.col(rvrs_col)) == "Y", F.col(amt_col).cast("decimal(18,2)") * -1)
             .otherwise(F.col(amt_col).cast("decimal(18,2)")))
    else:
        df = df.withColumn("ADJ_TXN_AMT", F.col(amt_col).cast("decimal(18,2)"))
    
    if pgm_col:
        group_col = pgm_col
    else:
        df = df.withColumn("ACCT_PGM_ID", F.lit("UNKNOWN"))
        group_col = "ACCT_PGM_ID"
    
    df_agg = df.groupBy(group_col).agg(
        F.sum("ADJ_TXN_AMT").alias("TXN_AMT"),
        F.count("*").alias("TXN_CT"),
        F.sum(F.when(F.col("ADJ_TXN_AMT") > 0, F.col("ADJ_TXN_AMT")).otherwise(0)).alias("POSITIVE_TXN_AMT"),
        F.sum(F.when(F.col("ADJ_TXN_AMT") < 0, F.col("ADJ_TXN_AMT")).otherwise(0)).alias("NEGATIVE_TXN_AMT")
    )
    
    if group_col != "ACCT_PGM_ID":
        df_agg = df_agg.withColumnRenamed(group_col, "ACCT_PGM_ID")
    
    df_agg = df_agg.withColumn("SRC_SYS", F.lit("NRRS")).withColumn("LOAD_DT", F.current_timestamp())
    return df_agg

########################################################################################################
# Write Output with status tracking
########################################################################################################

def write_output(df, table_name: str):
    if df is None:
        status.add_table_skipped(table_name, "No source data")
        print(f"  Skip {table_name}: No source data")
        return None
    
    output_path = get_stg_output_path(table_name)
    
    try:
        row_count = df.count()
        if row_count == 0:
            status.add_table_skipped(table_name, "Empty dataframe")
            print(f"  Skip {table_name}: 0 rows")
            return None
        
        df.write.mode("overwrite").parquet(output_path)
        status.add_table_written(table_name, row_count, output_path)
        print(f"{table_name}: {row_count:,} rows → {output_path}")
        return {"table": table_name, "rows": row_count}
    except Exception as e:
        status.add_error(f"Failed to write {table_name}: {str(e)}")
        print(f"{table_name}: {str(e)}")
        return None

########################################################################################################
# Main Execution
########################################################################################################

print("\n" + "=" * 80)
print("PMRDS Stage 1: ODS → STG Aggregation")
print("=" * 80)

try:
    #########################################################################################
    # Run aggregations
    #########################################################################################
    print("\n>>> Running Aggregations...")
    df_cps = aggregate_cps()
    df_sbsd = aggregate_sbsd()
    df_arc = aggregate_arcplc()
    df_nps = aggregate_nps()
    df_nrrs = aggregate_nrrs()
    
    #########################################################################################
    # Write outputs
    #########################################################################################
    print("\n>>> Writing to Cleansed Zone...")
    write_output(df_cps, "CPS_PYMT_SUMM")
    write_output(df_sbsd, "SBSD_PYMT_ATRB_SUMM")
    write_output(df_arc, "ARC_PYMT_SUMM")
    write_output(df_nps, "NPS_PYBL_SUMM")
    write_output(df_nrrs, "NRRS_GL_TXN_SUMM")
    
    #########################################################################################
    # Check if any tables written
    #########################################################################################
    if len(status.tables_written) == 0:
        status.add_error("No tables written - all sources missing or empty")

except Exception as e:
    status.add_error(f"Job exception: {str(e)}")
    raise

finally:
    #########################################################################################
    # Always write status to S3... success & failure both
    #########################################################################################
    print("\n>>> Writing job status to S3...")
    status.write_to_s3(CLEANSED_BUCKET, "PMRDS_STATUS")
    
    #########################################################################################
    # Print summary
    #########################################################################################
    print("\n" + "=" * 80)
    print(f"Stage 1 Status: {status.status}")
    print(f"Sources Found: {len(status.sources_processed)}")
    print(f"Sources Missing: {len(status.sources_missing)}")
    print(f"Tables Written: {len(status.tables_written)}")
    print(f"Tables Skipped: {len(status.tables_skipped)}")
    print(f"Total Input Rows: {status.metrics['total_input_rows']:,}")
    print(f"Total Output Rows: {status.metrics['total_output_rows']:,}")
    print("=" * 80)

#########################################################################################
# Fail job if FAILED status
#########################################################################################
if status.status == "FAILED":
    raise Exception(f"Job failed: {status.errors}")

print("\nStage 1 completed!")
job.commit()
