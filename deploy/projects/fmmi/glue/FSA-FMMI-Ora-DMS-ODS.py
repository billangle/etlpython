##
# Author:  Steampunk/Mahender Vulupala
# Purpose: Convert without header .csv files(which are extracted from Oracle though DMS) to  parquets 
# with column headers & audit columns with using source jsons from fmmi/config
# Date:    2026-02-17
# Json schema includes both column names and data types.  
##
import sys
import json
import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, to_date

# ---------------- INIT ----------------
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "env",
    "landing_bucket",
    "final_bucket",
    "run_date"
])

env = args["env"].lower()
landing_bucket = args["landing_bucket"]
final_bucket = args["final_bucket"]
run_date = args["run_date"]

config_prefix = "fmmi/config"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

s3 = boto3.resource("s3")
s3_client = boto3.client("s3")

# ---------------- HELPERS ----------------
def key_exists(bucket, key):
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except:
        return False

def prefix_exists(bucket, prefix):
    objs = list(s3.Bucket(bucket).objects.filter(Prefix=prefix))
    return len(objs) > 0

def load_json(bucket, key):
    print(f"[DEBUG] Loading JSON: s3://{bucket}/{key}")
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))

def delete_prefix(bucket, prefix):
    s3.Bucket(bucket).objects.filter(Prefix=prefix).delete()

def count_files(bucket, prefix):
    return sum(1 for _ in s3.Bucket(bucket).objects.filter(Prefix=prefix)
               if not _.key.endswith("/"))

# ---------------- TABLE DEFINITIONS ----------------
TABLES = [
    # -------- RELOAD TABLES --------
    ("cs_tbi_cmmt_item/",        "CS_TBL_CMMT_ITEM/",        "cs_tbl_cmmt_item",        "reload"),
    ("cs_tbi_cost_center/",      "CS_TBL_COST_CENTER/",      "cs_tbl_cost_center",      "reload"),
    ("cs_tbi_customer/",         "CS_TBL_CUSTOMER/",         "cs_tbl_customer",         "reload"),
    ("cs_tbi_func_area/",        "CS_TBL_FUNC_AREA/",        "cs_tbl_func_area",        "reload"),
    ("cs_tbi_fund/",             "CS_TBL_FUND/",             "cs_tbl_fund",             "reload"),
    ("cs_tbi_funded_program/",   "CS_TBL_FUNDED_PROGRAM/",   "cs_tbl_funded_program",   "reload"),
    ("cs_tbi_fund_center/",      "CS_TBL_FUND_CENTER/",      "cs_tbl_fund_center",      "reload"),
    ("cs_tbi_gl_account/",       "CS_TBL_GL_ACCOUNT/",       "cs_tbl_gl_account",       "reload"),
    ("cs_tbi_vendor/",           "CS_TBL_VENDOR/",           "cs_tbl_vendor",           "reload"),
    ("cs_tbi_wbs/",              "CS_TBL_WBS/",              "cs_tbl_wbs",              "reload"),

    # -------- APPEND TABLES --------
    ("cs_tbi_gl/",               "CS_TBL_GL/",               "cs_tbl_gl",               "append"),
    ("cs_tbi_invoice_dis/",      "CS_TBL_INVOICE_DIS/",      "cs_tbl_invoice_dis",      "append"),
    ("cs_tbi_material_doc/",     "CS_TBL_MATERIAL_DOC/",     "cs_tbl_material_doc",     "append"),
    ("cs_tbi_payroll/",          "CS_TBL_PAYROLL/",          "cs_tbl_payroll",          "append"),
    ("cs_tbi_po_header/",        "CS_TBL_PO_HEADER/",        "cs_tbl_po_header",        "append"),
    ("cs_tbi_po_item/",          "CS_TBL_PO_ITEM/",          "cs_tbl_po_item",          "append"),
    ("cs_tbi_purchasing/",       "CS_TBL_PURCHASING/",       "cs_tbl_purchasing",       "append"),
    ("cs_tbi_system_assurance/", "CS_TBL_SYSTEM_ASSURANCE/", "cs_tbl_system_assurance", "append"),
    ("cs_tbi_commitment/",       "CS_TBL_COMMITMENT/",       "cs_tbl_commitment",       "append")
]

# ---------------- MAIN ----------------

for src, tgt, json_base, mode in TABLES:

    #skip reload tables — DMS job only processes append tables
    if mode == "reload":
        print(f"[SKIP] Reload table {tgt} — DMS job only processes append tables")
        continue

    try:
        print(f"\n=== Processing {src} → {tgt} ===")

        # JSON check
        json_key = f"{config_prefix}/{json_base}_source.json"
        if not key_exists(landing_bucket, json_key):
            print(f"[SKIP] JSON not found: {json_key} for table {tgt}")
            continue

        # Source CSV check
        src_prefix = f"fmmi/fmmi_ods/{src}"
        if not prefix_exists(final_bucket, src_prefix):
            print(f"[SKIP] Source CSV folder missing: {src_prefix} for table {tgt}")
            continue

        # Load JSON schema
        source_json = load_json(landing_bucket, json_key)
        business_cols = [c["Name"].lower() for c in source_json["schema"]]

        # Audit columns
        audit_cols = [
            "fmmi_ods_cs_aud_id",
            "cre_dt",
            "last_chg_dt",
            "last_chg_user_nm",
            "load_bat_id"
        ]
        all_cols = business_cols + audit_cols

        # Read CSV
        df = (
            spark.read
                .option("header", "false")
                .option("delimiter", ",")
                .option("inferSchema", "true")
                .csv(f"s3://{final_bucket}/{src_prefix}")
        )

        df = df.toDF(*all_cols)

        # Enforce audit column types (DATE for cre_dt, last_chg_dt)
        df = df.withColumn("cre_dt", to_date(col("cre_dt").cast("string"))) \
               .withColumn("last_chg_dt", to_date(col("last_chg_dt").cast("string"))) \
               .withColumn("fmmi_ods_cs_aud_id", col("fmmi_ods_cs_aud_id").cast("string")) \
               .withColumn("last_chg_user_nm", col("last_chg_user_nm").cast("string")) \
               .withColumn("load_bat_id", col("load_bat_id").cast("string"))

        # Match number of files to source EXACTLY
        num_files = count_files(final_bucket, src_prefix)
        if num_files == 0:
            print(f"[SKIP] No CSV files found under {src_prefix} for table {tgt}")
            continue

        # Step 1: collapse to 1 partition
        df = df.coalesce(1)

        # Step 2: expand to EXACT number of source CSV files
        df = df.repartition(num_files)

        # Target path with load_date
        target_prefix = f"fmmi/fmmi_ods/{tgt}load_date={run_date}/"

        # Always append — no reload logic
        df.write.mode("append").parquet(f"s3://{final_bucket}/{target_prefix}")

        print(f"[SUCCESS] Loaded → {target_prefix} (files: {num_files})")

    except Exception as e:
        print(f"[ERROR] Failed processing table {tgt}: {str(e)}")
        print("[SKIP] Continuing to next table...\n")
        continue

job.commit()
