#######################################################################################################################
# Developer Name: Sajid Inayat
# Date: 01/28/25
# Script Name: FSA PMRDS STG to FINAL
# History:
#   01/28/25 - Sajid -- Initial version for PMRDS pipeline:
#                       Stage 2 Transformation (V5)
#                       Combines STG tables from Cleansed Zone to Final Zone
#                       STATUS TRACKING:
#                       - Writes job status JSON to S3 for Step Function to consume
#                       - No direct SNS - Step Function handles notification
#                       Input: PMRDS_STG tables (Cleansed Zone)
#                       Output: PMRDS summary tables (Final Zone)
#
#  Input tables (cleansed):  see each create_<tablename> per ETL logic and effective input files from cleansed
#  Input:      s3://{CLEANSED_BUCKET}/PMRDS_STG/CPS_PYMT_SUMM/dt={SYSTEM_DATE}/
#  Output:     OVERWRITE 
#  Output to:  s3://c108-{ENV}-fpacfsa-final-zone/PMRDS/{TABLENAME}
#  Output tables (final): 5 possible: fncl_amt_summ, fncl_coll_summ, fncl_otly_summ, fncl_prmpt_pymt_int_summ, fncl_rcv_summ 
#######################################################################################################################

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
from functools import reduce

#######################################################################################################################
# Parse Job Arguments
#######################################################################################################################
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'env',
    'ppln_nm',
    'step_nm',
    'operator_nm',
    'system_date'
])

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

#######################################################################################################################
# Initialize Glue Context
#######################################################################################################################
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, args)

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

if '--system_date' in sys.argv:
    args_with_date = getResolvedOptions(sys.argv, ['system_date'])
    SYSTEM_DATE = args_with_date['system_date']
else:
    SYSTEM_DATE = datetime.now().strftime('%Y%m%d')


job.init(JOB_NAME, args)

#######################################################################################################################
# Job Status Tracker and writes to S3 for Step Function
#######################################################################################################################
class JobStatus:  
    def __init__(self, job_name, stage, env, system_date):
        self.job_name = job_name
        self.stage = stage
        self.env = env
        self.system_date = system_date
        self.start_time = datetime.now()
        self.end_time = None
        self.status = "SUCCESS"
        
        self.stg_tables_read = []
        self.stg_tables_missing = []
        self.tables_written = []
        self.tables_skipped = []
        self.warnings = []
        self.errors = []
        self.row_counts = {}
        self.amount_totals = {}
        
        self.metrics = {
            "total_input_rows": 0,
            "total_output_rows": 0,
            "financial_totals": {}
        }
    
    def add_stg_found(self, table_name, row_count):
        self.stg_tables_read.append({"name": table_name, "rows": row_count})
        self.metrics["total_input_rows"] += row_count
    
    def add_stg_missing(self, table_name, path):
        self.stg_tables_missing.append({"name": table_name, "path": path})
        self.warnings.append(f"STG table missing: {table_name}")
        if self.status == "SUCCESS":
            self.status = "WARNING"

    def add_table_written(self, table_name, row_count, path, total_amt=None):
        self.tables_written.append({"name": table_name, "rows": row_count, "path": path})
        self.row_counts[f"output_{table_name}"] = row_count
        if total_amt is not None:
            self.amount_totals[table_name] = float(total_amt)
    
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
            "stg_tables_read": self.stg_tables_read,
            "stg_tables_missing": self.stg_tables_missing,
            "tables_written": self.tables_written,
            "tables_skipped": self.tables_skipped,
            "metrics": self.metrics,
            "warnings": self.warnings,
            "errors": self.errors
        }
    
    def write_to_s3(self, bucket, prefix="PMRDS_STATUS"):
        s3 = boto3.client('s3')
        key = f"{prefix}/{self.system_date}/{self.stage}_status.json"
        try:
            s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(self.to_dict(), indent=2), ContentType='application/json')
            print(f"Status written to s3://{bucket}/{key}")
            return f"s3://{bucket}/{key}"
        except Exception as e:
            print(f"Failed to write status: {str(e)}")
            return None

status = JobStatus(JOB_NAME, "STAGE2", ENV, SYSTEM_DATE)

print("=" * 80)
print(f"JOB: {JOB_NAME} | ENV: {ENV} | DATE: {SYSTEM_DATE}")
print("=" * 80)

#######################################################################################################################
# Environment Configuration
#######################################################################################################################

BUCKETS = {
    "dev": {"final": "c108-dev-fpacfsa-final-zone", "cleansed": "c108-dev-fpacfsa-cleansed-zone"},
    "cert": {"final": "c108-cert-fpacfsa-final-zone", "cleansed": "c108-cert-fpacfsa-cleansed-zone"},
    "prod": {"final": "c108-prod-fpacfsa-final-zone", "cleansed": "c108-prod-fpacfsa-cleansed-zone"}
}

FINAL_BUCKET = BUCKETS[ENV]["final"]
CLEANSED_BUCKET = BUCKETS[ENV]["cleansed"]

#######################################################################################################################
# STG Input Paths
#######################################################################################################################

STG_PATHS = {
    "CPS_PYMT_SUMM": f"s3://{CLEANSED_BUCKET}/PMRDS_STG/CPS_PYMT_SUMM/dt={SYSTEM_DATE}/",
    "SBSD_PYMT_ATRB_SUMM": f"s3://{CLEANSED_BUCKET}/PMRDS_STG/SBSD_PYMT_ATRB_SUMM/dt={SYSTEM_DATE}/",
    "ARC_PYMT_SUMM": f"s3://{CLEANSED_BUCKET}/PMRDS_STG/ARC_PYMT_SUMM/dt={SYSTEM_DATE}/",
    "NPS_PYBL_SUMM": f"s3://{CLEANSED_BUCKET}/PMRDS_STG/NPS_PYBL_SUMM/dt={SYSTEM_DATE}/",
    "NRRS_GL_TXN_SUMM": f"s3://{CLEANSED_BUCKET}/PMRDS_STG/NRRS_GL_TXN_SUMM/dt={SYSTEM_DATE}/",
}

def get_final_output_path(table_name: str) -> str:
    return f"s3://{FINAL_BUCKET}/PMRDS/{table_name}/dt={SYSTEM_DATE}"

#######################################################################################################################
# Standard Dimensions
#######################################################################################################################

STANDARD_DIMS = ["FSCL_YR", "FSCL_MO", "PGM_YR", "ACCT_PGM_CD", "ST_FSA_CD", "CNTY_FSA_CD"]

def standardize_columns(df, src_sys):
    col_map = {
        "FSCL_YR": ["FSCL_YR", "BDGT_FSCL_YR"],
        "FSCL_MO": ["FSCL_MO"],
        "PGM_YR": ["PGM_YR", "PYMT_LMT_YR"],
        "ACCT_PGM_CD": ["ACCT_PGM_CD", "ACCT_PGM_ID"],
        "ST_FSA_CD": ["ST_FSA_CD"],
        "CNTY_FSA_CD": ["CNTY_FSA_CD"],
    }
    cols = df.columns
    for std, opts in col_map.items():
        for opt in opts:
            if opt in cols and opt != std:
                df = df.withColumnRenamed(opt, std)
                break
    for dim in STANDARD_DIMS:
        if dim not in df.columns:
            df = df.withColumn(dim, F.lit(None).cast("string"))
    if "SRC_SYS" not in df.columns:
        df = df.withColumn("SRC_SYS", F.lit(src_sys))
    return df

#######################################################################################################################
# Safe Read Function
#######################################################################################################################

def read_stg_safe(table_name: str):
    path = STG_PATHS.get(table_name)
    if not path:
        status.add_warning(f"Unknown STG table: {table_name}")
        return None
    try:
        df = spark.read.parquet(path)
        count = df.count()
        if count == 0:
            status.add_stg_missing(table_name, f"{path} (empty)")
            print(f"{table_name}: Empty")
            return None
        status.add_stg_found(table_name, count)
        print(f"{table_name}: {count:,} rows")
        return df
    except Exception as e:
        status.add_stg_missing(table_name, path)
        print(f"{table_name}: Not found")
        return None

#######################################################################################################################
# TRANSFORMATIONS
#######################################################################################################################
#######################################################################################################################
# UNION ALL payment sources into outlay summary
#######################################################################################################################
def create_fncl_otly_summ(stg_data):
    print("\n>>> Creating FNCL_OTLY_SUMM...")
    dfs = []
    
    if stg_data.get("CPS_PYMT_SUMM"):
        df = standardize_columns(stg_data["CPS_PYMT_SUMM"], "CPS")
        df = df.withColumn("OTLY_AMT", F.col("PYMT_AMT").cast("decimal(18,2)")).withColumn("OTLY_CT", F.col("TXN_CT").cast("long"))
        dfs.append(df.select(*STANDARD_DIMS, "OTLY_AMT", "OTLY_CT", "SRC_SYS"))
        print(f"    + CPS: {df.count():,} rows")
    
    if stg_data.get("SBSD_PYMT_ATRB_SUMM"):
        df = standardize_columns(stg_data["SBSD_PYMT_ATRB_SUMM"], "SBSD")
        df = df.withColumn("OTLY_AMT", F.col("NET_PYMT_AMT").cast("decimal(18,2)")).withColumn("OTLY_CT", F.col("TXN_CT").cast("long"))
        dfs.append(df.select(*STANDARD_DIMS, "OTLY_AMT", "OTLY_CT", "SRC_SYS"))
        print(f"    + SBSD: {df.count():,} rows")
    
    if stg_data.get("ARC_PYMT_SUMM"):
        df = standardize_columns(stg_data["ARC_PYMT_SUMM"], "ARCPLC")
        df = df.withColumn("OTLY_AMT", F.col("PYMT_AMT").cast("decimal(18,2)")).withColumn("OTLY_CT", F.col("TXN_CT").cast("long"))
        dfs.append(df.select(*STANDARD_DIMS, "OTLY_AMT", "OTLY_CT", "SRC_SYS"))
        print(f"    + ARCPLC: {df.count():,} rows")
    
    if stg_data.get("NPS_PYBL_SUMM"):
        df = standardize_columns(stg_data["NPS_PYBL_SUMM"], "NPS")
        df = df.withColumn("OTLY_AMT", F.col("PYMT_RQST_AMT").cast("decimal(18,2)")).withColumn("OTLY_CT", F.col("TXN_CT").cast("long"))
        dfs.append(df.select(*STANDARD_DIMS, "OTLY_AMT", "OTLY_CT", "SRC_SYS"))
        print(f"    + NPS: {df.count():,} rows")
    
    if not dfs:
        print("No outlay data available")
        return None
    
    df_combined = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)
    df_combined = df_combined.withColumn("LOAD_DT", F.current_timestamp()).withColumn("PROCESS_DT", F.lit(SYSTEM_DATE))
    print(f"    = Combined: {df_combined.count():,} rows")
    return df_combined

#######################################################################################################################
# Receivables from NRRS positive transactions
#######################################################################################################################
def create_fncl_rcv_summ(stg_data):
    print("\n>>> Creating FNCL_RCV_SUMM...")
    if not stg_data.get("NRRS_GL_TXN_SUMM"):
        print("    No NRRS data - skipping receivables")
        return None
    df = standardize_columns(stg_data["NRRS_GL_TXN_SUMM"], "NRRS")
    df = df.withColumn("RCV_AMT", F.col("POSITIVE_TXN_AMT").cast("decimal(18,2)")).withColumn("RCV_CT", F.col("TXN_CT").cast("long"))
    df = df.filter(F.col("RCV_AMT") > 0)
    df = df.select(*STANDARD_DIMS, "RCV_AMT", "RCV_CT", "SRC_SYS")
    df = df.withColumn("LOAD_DT", F.current_timestamp()).withColumn("PROCESS_DT", F.lit(SYSTEM_DATE))
    print(f"    = Receivables: {df.count():,} rows")
    return df

#######################################################################################################################
# Collections from NRRS negative transactions
#######################################################################################################################
def create_fncl_coll_summ(stg_data):
    print("\n>>> Creating FNCL_COLL_SUMM...")
    if not stg_data.get("NRRS_GL_TXN_SUMM"):
        print("    No NRRS data - skipping collections")
        return None
    df = standardize_columns(stg_data["NRRS_GL_TXN_SUMM"], "NRRS")
    df = df.withColumn("COLL_AMT", F.abs(F.col("NEGATIVE_TXN_AMT")).cast("decimal(18,2)")).withColumn("COLL_CT", F.col("TXN_CT").cast("long"))
    df = df.filter(F.col("NEGATIVE_TXN_AMT") < 0)
    df = df.select(*STANDARD_DIMS, "COLL_AMT", "COLL_CT", "SRC_SYS")
    df = df.withColumn("LOAD_DT", F.current_timestamp()).withColumn("PROCESS_DT", F.lit(SYSTEM_DATE))
    print(f"    = Collections: {df.count():,} rows")
    return df

#######################################################################################################################
# Prompt payment interest from NPS
#######################################################################################################################
def create_fncl_prmpt_pymt_int_summ(stg_data):
    print("\n>>> Creating FNCL_PRMPT_PYMT_INT_SUMM...")
    if not stg_data.get("NPS_PYBL_SUMM"):
        print("    No NPS data - skipping interest")
        return None
    df = standardize_columns(stg_data["NPS_PYBL_SUMM"], "NPS")
    if "PRMPT_PYMT_INT_AMT" not in df.columns:
        print("    No interest column in NPS")
        return None
    df = df.withColumn("INT_AMT", F.col("PRMPT_PYMT_INT_AMT").cast("decimal(18,2)")).withColumn("INT_CT", F.col("TXN_CT").cast("long"))
    df = df.filter(F.col("INT_AMT") > 0)
    df = df.select(*STANDARD_DIMS, "INT_AMT", "INT_CT", "SRC_SYS")
    df = df.withColumn("LOAD_DT", F.current_timestamp()).withColumn("PROCESS_DT", F.lit(SYSTEM_DATE))
    print(f"    = Interest: {df.count():,} rows")
    return df

#######################################################################################################################
# Combined financial summary with calculated totals
#######################################################################################################################
def create_fncl_amt_summ(df_otly, df_rcv, df_coll, df_int):
    print("\n>>> Creating FNCL_AMT_SUMM...")
    dfs = []
    if df_otly:
        dfs.append(("OTLY", df_otly.groupBy(*STANDARD_DIMS).agg(F.sum("OTLY_AMT").alias("OTLY_AMT"), F.sum("OTLY_CT").alias("OTLY_CT"))))
    if df_rcv:
        dfs.append(("RCV", df_rcv.groupBy(*STANDARD_DIMS).agg(F.sum("RCV_AMT").alias("RCV_AMT"), F.sum("RCV_CT").alias("RCV_CT"))))
    if df_coll:
        dfs.append(("COLL", df_coll.groupBy(*STANDARD_DIMS).agg(F.sum("COLL_AMT").alias("COLL_AMT"), F.sum("COLL_CT").alias("COLL_CT"))))
    if df_int:
        dfs.append(("INT", df_int.groupBy(*STANDARD_DIMS).agg(F.sum("INT_AMT").alias("INT_AMT"), F.sum("INT_CT").alias("INT_CT"))))
    
    if not dfs:
        print("    No data for combined summary")
        return None
    
    df_combined = dfs[0][1]
    for name, df in dfs[1:]:
        df_combined = df_combined.join(df, STANDARD_DIMS, "full_outer")
    
    for col in ["OTLY_AMT", "RCV_AMT", "COLL_AMT", "INT_AMT", "OTLY_CT", "RCV_CT", "COLL_CT", "INT_CT"]:
        if col in df_combined.columns:
            df_combined = df_combined.fillna(0, subset=[col])
        else:
            df_combined = df_combined.withColumn(col, F.lit(0))
    
    df_combined = df_combined.withColumn("NET_AMT",
        F.coalesce(F.col("OTLY_AMT"), F.lit(0)) - F.coalesce(F.col("COLL_AMT"), F.lit(0)) + F.coalesce(F.col("INT_AMT"), F.lit(0)))
    df_combined = df_combined.withColumn("OUTSTDG_AMT",
        F.coalesce(F.col("RCV_AMT"), F.lit(0)) - F.coalesce(F.col("COLL_AMT"), F.lit(0)))
    df_combined = df_combined.withColumn("LOAD_DT", F.current_timestamp()).withColumn("PROCESS_DT", F.lit(SYSTEM_DATE))
    
    print(f"    = Combined: {df_combined.count():,} rows")
    return df_combined

#######################################################################################################################
# Write Output
#######################################################################################################################

def write_output(df, table_name: str, amt_col: str = None):
    if df is None:
        status.add_table_skipped(table_name, "No source data")
        print(f"  Skip {table_name}: No data")
        return None
    
    output_path = get_final_output_path(table_name)
    try:
        row_count = df.count()
        if row_count == 0:
            status.add_table_skipped(table_name, "0 rows")
            print(f"  Skip {table_name}: 0 rows")
            return None
        
        total_amt = None
        if amt_col and amt_col in df.columns:
            total_amt = df.agg(F.sum(amt_col)).collect()[0][0] or 0
        
        df.write.mode("overwrite").parquet(output_path)
        status.add_table_written(table_name, row_count, output_path, total_amt)
        
        amt_str = f" | ${total_amt:,.2f}" if total_amt else ""
        print(f"{table_name}: {row_count:,} rows{amt_str}")
        return {"table": table_name, "rows": row_count}
    except Exception as e:
        status.add_error(f"Failed to write {table_name}: {str(e)}")
        print(f"{table_name}: {str(e)}")
        return None

#######################################################################################################################
# Main Execution
#######################################################################################################################

print("\n" + "=" * 80)
print("PMRDS Stage 2: STG → Final Transformation")
print("=" * 80)

try:
    # Load STG tables
    stg_data = {}
    print("\n>>> Loading STG tables...")
    for name in STG_PATHS.keys():
        stg_data[name] = read_stg_safe(name)
    
    if all(v is None for v in stg_data.values()):
        status.add_error("No STG data found. Run Stage 1 first.")
        raise Exception("No STG data found")
    
    # Run transformations
    df_otly = create_fncl_otly_summ(stg_data)
    df_rcv = create_fncl_rcv_summ(stg_data)
    df_coll = create_fncl_coll_summ(stg_data)
    df_int = create_fncl_prmpt_pymt_int_summ(stg_data)
    df_amt = create_fncl_amt_summ(df_otly, df_rcv, df_coll, df_int)
    
    # Write to Final Zone
    print("\n>>> Writing to Final Zone...")
    write_output(df_otly, "FNCL_OTLY_SUMM", "OTLY_AMT")
    write_output(df_rcv, "FNCL_RCV_SUMM", "RCV_AMT")
    write_output(df_coll, "FNCL_COLL_SUMM", "COLL_AMT")
    write_output(df_int, "FNCL_PRMPT_PYMT_INT_SUMM", "INT_AMT")
    write_output(df_amt, "FNCL_AMT_SUMM", "NET_AMT")
    
    if len(status.tables_written) == 0:
        status.add_error("No tables written to Final Zone")

except Exception as e:
    status.add_error(f"Job exception: {str(e)}")
    raise

finally:
    # Always write status to S3
    print("\n>>> Writing job status to S3...")
    status.write_to_s3(FINAL_BUCKET, "PMRDS_STATUS")
    
    print("\n" + "=" * 80)
    print(f"Stage 2 Status: {status.status}")
    print(f"STG Tables Read: {len(status.stg_tables_read)}")
    print(f"STG Tables Missing: {len(status.stg_tables_missing)}")
    print(f"Tables Written: {len(status.tables_written)}")
    print(f"Tables Skipped: {len(status.tables_skipped)}")
    if status.metrics["financial_totals"]:
        print("Financial Totals:")
        for name, amt in status.metrics["financial_totals"].items():
            print(f"  {name}: ${amt:,.2f}")
    print("=" * 80)

if status.status == "FAILED":
    raise Exception(f"Job failed: {status.errors}")

print("\nStage 2 completed!")
job.commit()
