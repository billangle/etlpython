####################################################################################################### 
# Developer Name: Mahender Vulupala
# Date: 01/29/2026
# Script Name: FSA-()-FMMI-S3-STG-ODS-parquet
#    - Process the files from s3:landing/fmmi/fmmi files to cleansed/S3:fmmi/fmmi_stg/ 
#    - Process the data to s3:finalzone/fmmi/fmmi_ods from /fmmi_stg/ 
#    - with using transformation & reconciliation checks 
#######################################################################################################

import sys, json, boto3, traceback, logging
from datetime import datetime
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, DecimalType, DateType, TimestampType, NullType
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# ---------------- SCHEMAS FOR GL/SA TMP ----------------
schema_gl = StructType([
    StructField("fiscal_year", StringType()),
    StructField("business_area", StringType()),
    StructField("fiscal_year_period", StringType()),
    StructField("debit_amount", DoubleType()),
    StructField("credit_amount", DoubleType())
])

schema_sa = StructType([
    StructField("fiscal_year", StringType()),
    StructField("business_area", StringType()),
    StructField("fiscal_year_period", StringType()),
    StructField("fg_debit", DoubleType()),
    StructField("fg_credit", DoubleType())
])

# ---------------- INITIALIZE CLIENTS ----------------
s3_client = boto3.client("s3")

# ---------------- ARGUMENTS ----------------
args = getResolvedOptions(sys.argv, [
    "JOB_NAME", "env",
    "job_type",
    "bucket_name", "folder_name",
    "stg_bucket_name", "stg_folder_name",
    "ods_bucket_name", "ods_folder_name"
])

env = args["env"]
job_type = args["job_type"].upper() if args.get("job_type") else "BOTH"

landing_bucket = args["bucket_name"]
landing_prefix = args["folder_name"]          # e.g. fmmi/fmmi_ocfo_files
stg_bucket = args["stg_bucket_name"]
stg_prefix = args["stg_folder_name"]          # e.g. fmmi_stg  (DO NOT include "fmmi/")
ods_bucket = args["ods_bucket_name"]
ods_prefix = args["ods_folder_name"]          # e.g. fmmi_ods  (DO NOT include "fmmi/")

config_prefix = "fmmi/config"

# ---- optional args from sys.argv ----
run_date = None
raw_file_names = None

if "--run_date" in sys.argv:
    run_date = sys.argv[sys.argv.index("--run_date") + 1].strip() or None

if "--file_name" in sys.argv:
    raw_file_names = sys.argv[sys.argv.index("--file_name") + 1].strip() or None

# ---------------- TABLE MAP (SINGLE SOURCE OF TRUTH) ----------------
TABLE_MAP = {
    "FMMI.FSA.MD.CMMITEM": {
        "ods": "CS_TBL_CMMT_ITEM",
        "json": "cs_tbl_cmmt_item",
        "mode": "reload",
        "audit_id": 1
    },
    "FMMI.FSA.CMMT": {
        "ods": "CS_TBL_COMMITMENT",
        "json": "cs_tbl_commitment",
        "mode": "append",
        "audit_id": 2
    },
    "FMMI.FSA.MD_COSTCTR": {
        "ods": "CS_TBL_COST_CENTER",
        "json": "cs_tbl_cost_center",
        "mode": "reload",
        "audit_id": 3
    },
    "FMMI.FSA.MD_CUSTOMER": {
        "ods": "CS_TBL_CUSTOMER",
        "json": "cs_tbl_customer",
        "mode": "reload",
        "audit_id": 4
    },
    "FMMI.FSA.MD_FUNCAREA": {
        "ods": "CS_TBL_FUNC_AREA",
        "json": "cs_tbl_func_area",
        "mode": "reload",
        "audit_id": 5
    },
    "FMMI.FSA.MD_FUND": {
        "ods": "CS_TBL_FUND",
        "json": "cs_tbl_fund",
        "mode": "reload",
        "audit_id": 6
    },
    "FMMI.FSA.FUNDEDPRG": {
        "ods": "CS_TBL_FUNDED_PROGRAM",
        "json": "cs_tbl_funded_program",
        "mode": "reload",
        "audit_id": 8
    },
    "FMMI.FSA.MD_FUNDCTR": {
        "ods": "CS_TBL_FUND_CENTER",
        "json": "cs_tbl_fund_center",
        "mode": "reload",
        "audit_id": 7,
        "special": "fund_center"
    },
    "FMMI.FSA.MD_GLACCT": {
        "ods": "CS_TBL_GL_ACCOUNT",
        "json": "cs_tbl_gl_account",
        "mode": "reload",
        "audit_id": 10
    },
    "FMMI.FSA.MD.VENDOR": {
        "ods": "CS_TBL_VENDOR",
        "json": "cs_tbl_vendor",
        "mode": "reload",
        "audit_id": 21
    },
    "FMMI.FSA.MD_WBS": {
        "ods": "CS_TBL_WBS",
        "json": "cs_tbl_wbs",
        "mode": "reload",
        "audit_id": 22
    },
    "FMMI.FSA.GLITEM": {
        "ods": "CS_TBL_GL",
        "json": "cs_tbl_gl",
        "mode": "append",
        "audit_id": 9,
        "special": "gl"
    },
    "FMMI.FSA.SYSASSURANCE": {
        "ods": "CS_TBL_SYSTEM_ASSURANCE",
        "json": "cs_tbl_system_assurance",
        "mode": "append",
        "audit_id": 19,
        "special": "sa"
    },
    "FMMI.FSA.INV_DIS": {
        "ods": "CS_TBL_INVOICE_DIS",
        "json": "cs_tbl_invoice_dis",
        "mode": "append",
        "audit_id": 11
    },
    "FMMI.FSA.MATDOC": {
        "ods": "CS_TBL_MATERIAL_DOC",
        "json": "cs_tbl_material_doc",
        "mode": "append",
        "audit_id": 12
    },
    "FMMI.FSA.PAYROLL": {
        "ods": "CS_TBL_PAYROLL",
        "json": "cs_tbl_payroll",
        "mode": "append",
        "audit_id": 11
    },
    "FMMI.FSA_POHEAD": {
        "ods": "CS_TBL_PO_HEADER",
        "json": "cs_tbl_po_header",
        "mode": "append",
        "audit_id": 14
    },
    "FMMI.FSA_POITEM": {
        "ods": "CS_TBL_PO_ITEM",
        "json": "cs_tbl_po_item",
        "mode": "append",
        "audit_id": 15
    },
    "FMMI.FSA.PURCH": {
        "ods": "CS_TBL_PURCHASING",
        "json": "cs_tbl_purchasing",
        "mode": "append",
        "audit_id": 16
    }
}

FILE_TO_JSON = {k: v["json"] for k, v in TABLE_MAP.items()}
ods_name_map = {k: v["ods"] for k, v in TABLE_MAP.items()}
AUD_ID_MAP = {v["ods"]: v["audit_id"] for v in TABLE_MAP.values()}
reload_tables = {k for k, v in TABLE_MAP.items() if v["mode"].lower() == "reload"}
append_tables = {k for k, v in TABLE_MAP.items() if v["mode"].lower() == "append"}

gl_table = "FMMI.FSA.GLITEM"
sa_table = "FMMI.FSA.SYSASSURANCE"

# ---------------- HELPERS ----------------
def derive_table_name(file_name: str) -> str:
    base = file_name.split("/")[-1].replace(".csv", "")
    if "_" in base:
        name_part, last = base.rsplit("_", 1)
        if last.isdigit() and len(last) == 8:
            return name_part
    return base

def normalize_table_name(raw_name: str) -> str:
    if raw_name in ods_name_map:
        return ods_name_map[raw_name]
    return raw_name.replace(".", "_")

def get_landing_date():
    prefix = f"{landing_prefix}/"
    paginator = s3_client.get_paginator("list_objects_v2")
    date_folders = set()
    for page in paginator.paginate(Bucket=landing_bucket, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            folder = cp["Prefix"].rstrip("/").split("/")[-1]
            if folder.isdigit():
                date_folders.add(folder)
    if not date_folders:
        raise Exception("[STG] No landing date folders found")
    return max(date_folders)

def detect_latest_run_date():
    prefix = f"{landing_prefix}/"
    print(f"[AUTO] Detecting latest run_date under s3://{landing_bucket}/{prefix}")
    paginator = s3_client.get_paginator("list_objects_v2")
    latest = None
    for page in paginator.paginate(Bucket=landing_bucket, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            folder = cp["Prefix"].rstrip("/").split("/")[-1]
            if folder.isdigit() and len(folder) == 8:
                if latest is None or folder > latest:
                    latest = folder
    print(f"[AUTO] Latest detected run_date = {latest}")
    return latest

def extract_date_from_filename(name):
    base = name.replace(".csv", "")
    parts = base.split("_")
    if len(parts) >= 2 and parts[-1].isdigit() and len(parts[-1]) == 8:
        return parts[-1]
    return None

def json_to_spark_schema(json_schema):
    fields = []
    for col in json_schema["schema"]:
        name = col["Name"].lower()
        dtype = col["Type"].lower()
        if dtype.startswith(("varchar", "char")) or dtype == "string":
            spark_type = StringType()
        elif dtype in ("int", "integer"):
            spark_type = IntegerType()
        elif dtype in ("double", "float"):
            spark_type = DoubleType()
        elif dtype.startswith("decimal"):
            p, s = dtype[dtype.find("(")+1:dtype.find(")")].split(",")
            spark_type = DecimalType(int(p), int(s))
        elif dtype == "date":
            spark_type = DateType()
        elif dtype in ("timestamp", "timestamptz"):
            spark_type = TimestampType()
        else:
            spark_type = StringType()
        fields.append(StructField(name, spark_type, True))
    return StructType(fields)

def load_json_from_s3(bucket, key):
    logger.info(f"[JSON] Loading JSON from s3://{bucket}/{key}")
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))

def delete_s3_prefix(bucket, prefix):
    s3 = boto3.resource("s3")
    bucket_obj = s3.Bucket(bucket)
    try:
        objs = list(bucket_obj.objects.filter(Prefix=prefix))
        if not objs:
            logger.info(f"[S3] No objects found under prefix {prefix}. Nothing to delete.")
            return
        logger.info(f"[S3] Deleting {len(objs)} objects under prefix {prefix}")
        bucket_obj.objects.filter(Prefix=prefix).delete()
    except Exception as e:
        logger.warn(f"[S3] Warning while deleting prefix {prefix}: {e}")

def list_latest_fmmi_files():
    logger.info("[STG] Determining latest file per table (auto mode)")
    prefix = f"{landing_prefix}/"
    paginator = s3_client.get_paginator("list_objects_v2")
    date_folders = set()
    for page in paginator.paginate(Bucket=landing_bucket, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            folder = cp["Prefix"].rstrip("/").split("/")[-1]
            if folder.isdigit():
                date_folders.add(folder)
    if not date_folders:
        logger.info("[STG] No date folders found")
        return [], None
    latest_date = max(date_folders)
    logger.info(f"[STG] Latest date folder detected: {latest_date}")
    base_prefix = f"{landing_prefix}/{latest_date}/"
    logger.info(f"[STG] Scanning landing prefix: s3://{landing_bucket}/{base_prefix}")
    latest_per_table = {}
    for page in paginator.paginate(Bucket=landing_bucket, Prefix=base_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            file_name = key.split("/")[-1]
            if not file_name.lower().endswith(".csv"):
                continue
            base = file_name.replace(".csv", "")
            name_part, _ = base.rsplit("_", 1)
            table_name = name_part
            last_modified = obj["LastModified"]
            if table_name not in latest_per_table or last_modified > latest_per_table[table_name][0]:
                latest_per_table[table_name] = (last_modified, key)
    auto_files = [v[1] for v in latest_per_table.values()]
    return auto_files, latest_date

def transform_fund_center(df):
    logger.info("[ODS][FUND_CENTER] Applying fund center transformation")
    df = df.withColumn(
        "st_nm",
        F.when(~F.upper(F.col("funds_center")).like("FA5S0%"), F.lit(None))
         .when(F.instr("medium_text", ",") > 0,
               F.trim(F.expr("substr(medium_text, instr(medium_text, ',')+1)")))
         .when(F.instr("medium_text", "-") > 0,
               F.trim(F.expr("substr(medium_text, 1, instr(medium_text, '-')-1)")))
         .otherwise(F.trim(F.col("medium_text")))
    )
    df = df.withColumn(
        "ofc_nm",
        F.when(F.col("funds_center").isNull(), F.lit(None))
         .when(~F.upper(F.col("funds_center")).like("FA5S0%"), F.lit(None))
         .when(F.instr("medium_text", ",") > 0,
               F.trim(F.expr("substr(medium_text, 1, instr(medium_text, ',')-1)")))
         .when(
             (F.instr("medium_text", "-") > 0) &
             (F.upper(F.col("funds_center")).like("%AFL")),
             F.concat(
                 F.trim(F.expr("substr(medium_text, 1, instr(medium_text, '-')-1)")),
                 F.lit(" STO "),
                 F.trim(F.expr("substr(medium_text, instr(medium_text, '-')+1)"))
             )
         )
         .otherwise(F.concat(F.trim(F.col("medium_text")), F.lit(" STO")))
    )
    logger.info("[ODS][FUND_CENTER] Fund center transformation complete")
    return df

def add_audit_columns(df, src_table_name):
    ods_name = ods_name_map.get(src_table_name, normalize_table_name(src_table_name))
    aud_id = AUD_ID_MAP.get(ods_name)
    logger.info(f"[AUDIT] Adding audit columns for src_table={src_table_name} ods_table={ods_name} aud_id={aud_id}")
    return (df
        .withColumn("fmmi_ods_cs_aud_id", F.lit(aud_id))
        .withColumn("cre_dt", F.current_date())
        .withColumn("last_chg_dt", F.current_date())
        .withColumn("last_chg_user_nm", F.lit("fsa_prod_rs_usr"))
        .withColumn("load_bat_id", F.lit(load_bat_id))
    )

def skip_first_row(df):
    return (
        df.withColumn("_row_id", F.monotonically_increasing_id())
          .filter(F.col("_row_id") != 0)
          .drop("_row_id")
    )

def read_stg_parquet(stg_name):
    path = f"s3://{stg_bucket}/fmmi/{stg_prefix}/{stg_name}/"
    try:
        return spark.read.parquet(path)
    except Exception:
        logger.warn(f"[STG] Missing STG folder: {path}. Returning empty DataFrame.")
        return spark.createDataFrame([], StructType([]))

def read_ods_if_exists(mapped):
    path = f"s3://{ods_bucket}/fmmi/{ods_prefix}/{mapped}"
    logger.info(f"[ODS] Checking existing ODS data at {path}")
    try:
        return spark.read.parquet(path)
    except Exception:
        logger.info(f"[ODS] No existing ODS data for table={mapped}")
        return None

def write_ods_table(table_name, df, landing_date):
    output_path = f"s3://{ods_bucket}/fmmi/{ods_prefix}/{table_name}/load_date={landing_date}"
    df.repartition(1).write.mode("overwrite").parquet(output_path)
    logger.info(f"[ODS] Wrote table={table_name} partition={landing_date} to {output_path}")

# ---------------- DETERMINE FILE MODE ----------------
if raw_file_names is None:
    file_names = []
    single_tables = set()
else:
    file_names = [f.strip() for f in raw_file_names.split(",") if f.strip()]
    single_tables = {normalize_table_name(derive_table_name(f)) for f in file_names}

# ---------------- RUN DATE RESOLUTION ----------------
if file_names and run_date is None:
    run_date = extract_date_from_filename(file_names[0])
    print(f"[AUTO] Extracted run_date={run_date} from file_name={file_names}")

if run_date is None:
    run_date = detect_latest_run_date()

# ---------------- SPARK / GLUE INIT ----------------
load_bat_id = int(datetime.now().strftime("%Y%m%d%H%M%S"))

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()

spark.sparkContext.setLogLevel("ERROR")
logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("py4j").setLevel(logging.ERROR)

logger.info(f"[DEBUG] landing_bucket={landing_bucket}")
logger.info(f"[DEBUG] landing_prefix={landing_prefix}")
logger.info(f"[DEBUG] run_date={run_date}")
logger.info(f"[DEBUG] scanning path = s3://{landing_bucket}/{landing_prefix}/{run_date}/")

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger.info(f"[JOB] Started job={args['JOB_NAME']} env={env} job_type={job_type} run_date={run_date} file_names={file_names} load_bat_id={load_bat_id}")

# ---------------- STG LOGIC ----------------
def run_stg():
    global landing_date
    logger.info(f"[DEBUG] file_names={file_names}")
    logger.info(f"[DEBUG] single_tables={single_tables}")
    logger.info(f"[DEBUG] FILE_TO_JSON keys={list(FILE_TO_JSON.keys())}")

    logger.info("[STG] Clearing STG zone for allowed tables only")
    for raw_table in FILE_TO_JSON.keys():
        stg_table = normalize_table_name(raw_table)
        stg_prefix_path = f"fmmi/{stg_prefix}/{stg_table}/"
        delete_s3_prefix(stg_bucket, stg_prefix_path)

    logger.info("[STG] Starting STG processing")

    # SINGLE-FILE / MULTI-FILE MODE
    if single_tables:
        logger.info(f"[STG] SINGLE-FILE MODE enabled. Tables={single_tables}")
        for f in file_names:
            file_name = f.split("/")[-1]
            raw_table_name = derive_table_name(file_name)
            table_name = normalize_table_name(raw_table_name)

            if run_date:
                landing_date = run_date
            else:
                landing_date = file_name.split("_")[-1].replace(".csv", "")

            key = f"{landing_prefix}/{landing_date}/{file_name}"
            logger.info(f"[STG] Processing key={key} landing_date={landing_date}")

            if raw_table_name not in FILE_TO_JSON:
                logger.info(f"[STG] Skipping table={raw_table_name} (not in FILE_TO_JSON)")
                continue

            json_base = FILE_TO_JSON[raw_table_name]
            source_json_key = f"{config_prefix}/{json_base}_source.json"
            target_json_key = f"{config_prefix}/{json_base}_target.json"

            try:
                source_json = load_json_from_s3(landing_bucket, source_json_key)
                target_json = load_json_from_s3(landing_bucket, target_json_key)
            except Exception as e:
                logger.error(f"[STG] Missing source/target JSON for table={raw_table_name} (base={json_base}). Skipping. Error: {e}")
                continue

            source_schema = json_to_spark_schema(source_json)

            df = (
                spark.read
                    .option("header", "false")
                    .option("delimiter", "|")
                    .option("quote", "\u0000")
                    .schema(source_schema)
                    .csv(f"s3://{landing_bucket}/{key}")
            )

            df = skip_first_row(df)

            if df.count() == 0:
                logger.info("[STG] No data rows found. Writing EMPTY parquet with schema only.")
                df = spark.createDataFrame([], df.schema)

            df = df.toDF(*[c.lower() for c in df.columns])
            df = add_audit_columns(df, raw_table_name)

            stg_prefix_path = f"fmmi/{stg_prefix}/{table_name}/"
            delete_s3_prefix(stg_bucket, stg_prefix_path)

            df.repartition(1).write.mode("overwrite").parquet(f"s3://{stg_bucket}/{stg_prefix_path}")

        globals()["landing_date"] = landing_date
        logger.info(f"[STG] Stored landing_date for ODS = {landing_date}")
        logger.info("[STG] STG processing complete (single-file mode)")
        return

    # AUTO MODE
    logger.info("[STG] AUTO MODE enabled. Loading all latest files.")
    auto_files, latest_date = list_latest_fmmi_files()
    if not auto_files:
        logger.info("[STG] No landing files found in AUTO MODE.")
        return

    landing_date = latest_date
    logger.info(f"[STG] AUTO MODE landing_date={landing_date}")

    for key in auto_files:
        file_name = key.split("/")[-1]
        raw_table_name = derive_table_name(file_name)
        table_name = normalize_table_name(raw_table_name)

        logger.info(f"[STG] AUTO MODE processing key={key}")

        if raw_table_name not in FILE_TO_JSON:
            logger.info(f"[STG] Skipping table={raw_table_name} (not in FILE_TO_JSON)")
            continue

        json_base = FILE_TO_JSON[raw_table_name]
        source_json_key = f"{config_prefix}/{json_base}_source.json"
        target_json_key = f"{config_prefix}/{json_base}_target.json"

        try:
            source_json = load_json_from_s3(landing_bucket, source_json_key)
            target_json = load_json_from_s3(landing_bucket, target_json_key)
        except Exception as e:
            logger.error(f"[STG] Missing source/target JSON for table={raw_table_name} (base={json_base}). Skipping. Error: {e}")
            continue

        source_schema = json_to_spark_schema(source_json)

        df = (
            spark.read
                .option("header", "false")
                .option("delimiter", "|")
                .option("quote", "\u0000")
                .schema(source_schema)
                .csv(f"s3://{landing_bucket}/{key}")
        )

        df = skip_first_row(df)

        if df.count() == 0:
            logger.info("[STG] No data rows found. Writing EMPTY parquet with schema only.")
            df = spark.createDataFrame([], df.schema)

        df = df.toDF(*[c.lower() for c in df.columns])
        df = add_audit_columns(df, raw_table_name)

        stg_prefix_path = f"fmmi/{stg_prefix}/{table_name}/"
        delete_s3_prefix(stg_bucket, stg_prefix_path)

        df.repartition(1).write.mode("overwrite").parquet(f"s3://{stg_bucket}/{stg_prefix_path}")

    globals()["landing_date"] = landing_date
    logger.info(f"[STG] Stored landing_date for ODS = {landing_date}")
    logger.info("[STG] AUTO MODE STG processing complete")

# ---------------- ODS LOGIC ----------------
def run_ods():
    logger.info("[ODS] Starting ODS processing")

    landing_date = globals().get("landing_date")
    if not landing_date:
        landing_date = get_landing_date()

    logger.info(f"[ODS] Using landing_date={landing_date}")

    systemdate = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    success_count = 0
    failure_count = 0
    success_tables = []
    failed_tables = []

    landing_row_count = {}
    ods_row_count = {}
    ods_table_name_map = {}

    # PRE-SCAN STG
    for t in list(reload_tables) + list(append_tables):
        stg_name = normalize_table_name(t)
        try:
            df = read_stg_parquet(stg_name)
            landing_row_count[t] = df.count()
        except Exception:
            landing_row_count[t] = 0

    # 1. RELOAD TABLES
    logger.info("[ODS] Starting RELOAD tables")
    for t in reload_tables:
        if landing_row_count.get(t, 0) == 0:
            logger.info(f"[ODS] Skipping reload for missing table={t}")
            continue
        if t not in FILE_TO_JSON:
            logger.info(f"[ODS] Skipping reload for table={t} (not in FILE_TO_JSON)")
            continue
        try:
            logger.info(f"[ODS] Reload table={t}")
            stg_name = normalize_table_name(t)
            df = read_stg_parquet(stg_name)
            before_count = df.count()
            logger.info(f"[ODS][RELOAD] STG row count for table={t}: {before_count}")
            landing_row_count[t] = before_count
            if before_count == 0:
                df = spark.createDataFrame([], df.schema)
            ods_table_name = ods_name_map.get(t, normalize_table_name(t))
            ods_table_name_map[t] = ods_table_name
            base_prefix = f"fmmi/{ods_prefix}/{ods_table_name}"
            delete_s3_prefix(ods_bucket, base_prefix)
            write_ods_table(ods_table_name, df, landing_date)
            logger.info(f"[ODS][RELOAD] Wrote table={ods_table_name} with row count={before_count}")
            ods_row_count[t] = before_count
            success_count += 1
            success_tables.append(t)
        except Exception as e:
            logger.error(f"[ODS][WARN] Reload failed for table={t}: {e}")
            logger.error(traceback.format_exc())
            failure_count += 1
            failed_tables.append(t)
            ods_row_count[t] = "Failed"

    # 4. FUND CENTER TRANSFORM (RELOAD ONLY)
    logger.info("[ODS][FUNDCTR] Starting Fund Center transform block")
    fundctr_key = "FMMI.FSA.MD_FUNDCTR"
    if fundctr_key in FILE_TO_JSON:
        # *** FIX: only touch ODS when STG has data for Fund Center in this run ***
        if landing_row_count.get(fundctr_key, 0) == 0:
            logger.info("[ODS][FUNDCTR] No STG data detected for Fund Center; existing ODS data will be left untouched.")
        else:
            try:
                stg_name = normalize_table_name(fundctr_key)
                df = read_stg_parquet(stg_name)
                before_count = df.count()
                logger.info(f"[ODS][FUNDCTR] Row count BEFORE transform: {before_count}")
                if before_count == 0:
                    logger.info("[ODS][FUNDCTR] STG is empty. Writing empty schema.")
                    df = spark.createDataFrame([], df.schema)
                required_cols = {"funds_center", "medium_text"}
                df_cols = set([c.lower() for c in df.columns])
                missing = required_cols - df_cols
                ods_table_name = ods_name_map.get(fundctr_key, normalize_table_name(fundctr_key))
                if missing:
                    logger.error(f"[ODS][FUNDCTR] Missing required columns: {missing}")
                    logger.error("[ODS][FUNDCTR] Skipping Fund Center transform due to schema mismatch.")
                    partition_prefix = f"fmmi/{ods_prefix}/{ods_table_name}/load_date={landing_date}"
                    delete_s3_prefix(ods_bucket, partition_prefix)
                    write_ods_table(ods_table_name, df, landing_date)
                else:
                    df = transform_fund_center(df)
                    after_count = df.count()
                    logger.info(f"[ODS][FUNDCTR] Row count AFTER transform: {after_count}")
                    partition_prefix = f"fmmi/{ods_prefix}/{ods_table_name}/load_date={landing_date}"
                    delete_s3_prefix(ods_bucket, partition_prefix)
                    write_ods_table(ods_table_name, df, landing_date)
                    logger.info("[ODS][FUNDCTR] Fund Center transform complete")
            except Exception as e:
                logger.error(f"[ODS][FUNDCTR] Processing failed: {e}")
                logger.error(traceback.format_exc())

    # 2. APPEND TABLES
    logger.info("[ODS] Starting APPEND tables")
    for t in append_tables:
        if landing_row_count.get(t, 0) == 0:
            logger.info(f"[ODS] Skipping append for missing table={t}")
            continue
        if t in (gl_table, sa_table):
            logger.info(f"[ODS] Skipping GL/SA in append counter loop: {t}")
            continue
        if t not in FILE_TO_JSON:
            logger.info(f"[ODS] Skipping append for table={t} (not in FILE_TO_JSON)")
            continue
        try:
            logger.info(f"[ODS] Append table={t}")
            stg_name = normalize_table_name(t)
            df = read_stg_parquet(stg_name)
            before_count = df.count()
            logger.info(f"[ODS][APPEND] STG row count for table={t}: {before_count}")
            landing_row_count[t] = before_count
            if before_count == 0:
                df = spark.createDataFrame([], df.schema)
            ods_table_name = ods_name_map.get(t, normalize_table_name(t))
            ods_table_name_map[t] = ods_table_name
            partition_prefix = f"fmmi/{ods_prefix}/{ods_table_name}/load_date={landing_date}"
            delete_s3_prefix(ods_bucket, partition_prefix)
            write_ods_table(ods_table_name, df, landing_date)
            logger.info(f"[ODS][APPEND] Wrote table={ods_table_name} partition={landing_date} with row count={before_count}")
            ods_row_count[t] = before_count
            success_count += 1
            success_tables.append(t)
        except Exception as e:
            logger.error(f"[ODS][WARN] Append failed for table={t}: {e}")
            logger.error(traceback.format_exc())
            failure_count += 1
            failed_tables.append(t)
            ods_row_count[t] = "Failed"

    # 3. GL/SA RECONCILIATION
    logger.info("[ODS][GL/SA] Starting GL/SA reconciliation block")
    gl_stg = normalize_table_name(gl_table)
    sa_stg = normalize_table_name(sa_table)
    gl_df = None
    sa_df = None
    gl_count = 0
    sa_count = 0

    try:
        gl_df = read_stg_parquet(gl_stg)
        gl_count = gl_df.count()
    except Exception as e:
        logger.warn(f"[ODS][GL] Failed to read STG for {gl_stg}: {e}")
        gl_df = None
        gl_count = 0

    try:
        sa_df = read_stg_parquet(sa_stg)
        sa_count = sa_df.count()
    except Exception as e:
        logger.warn(f"[ODS][SA] Failed to read STG for {sa_stg}: {e}")
        sa_df = None
        sa_count = 0

    logger.info(f"[ODS][GL] STG row count={gl_count}")
    logger.info(f"[ODS][SA] STG row count={sa_count}")

    landing_row_count[gl_table] = gl_count
    landing_row_count[sa_table] = sa_count

    if gl_df is not None and gl_count > 0:
        gl_df = gl_df.toDF(*[c.lower() for c in gl_df.columns])
        tmp_gl = gl_df.select(
            "fiscal_year", "business_area", "fiscal_year_period",
            "debit_amount", "credit_amount"
        )
    else:
        tmp_gl = spark.createDataFrame([], schema_gl)

    if sa_df is not None and sa_count > 0:
        sa_df = sa_df.toDF(*[c.lower() for c in sa_df.columns])
        tmp_sa = sa_df.select(
            "fiscal_year", "business_area", "fiscal_year_period",
            "fg_debit", "fg_credit"
        )
    else:
        tmp_sa = spark.createDataFrame([], schema_sa)

    try:
        delete_s3_prefix(ods_bucket, f"fmmi/{ods_prefix}/TMP_GL")
    except:
        pass
    try:
        delete_s3_prefix(ods_bucket, f"fmmi/{ods_prefix}/TMP_SA")
    except:
        pass

    write_ods_table("TMP_GL", tmp_gl, landing_date)
    write_ods_table("TMP_SA", tmp_sa, landing_date)

    logger.info(f"[ODS][GL/SA] Reloaded TMP_GL with row count={tmp_gl.count()}")
    logger.info(f"[ODS][GL/SA] Reloaded TMP_SA with row count={tmp_sa.count()}")

    if gl_count == 0 or sa_count == 0:
        logger.error(f"FMMI ODS - Either GL Or SA files did not received on {systemdate}")
        logger.info("[ODS][GL/SA] Skipping GL/SA reconciliation and main ODS loads due to missing file(s).")
        ods_row_count[gl_table] = "Skipped"
        ods_row_count[sa_table] = "Skipped"
        ods_table_name_map[gl_table] = ods_name_map.get(gl_table, gl_stg)
        ods_table_name_map[sa_table] = ods_name_map.get(sa_table, sa_stg)
        diff_cnt = None
    else:
        tmp_gl.createOrReplaceTempView("TMP_GL")
        tmp_sa.createOrReplaceTempView("TMP_SA")
        diff_cnt = spark.sql("""
            SELECT COUNT(*) diff_cnt FROM (
                (SELECT fiscal_year, business_area, fiscal_year_period,
                        SUM(debit_amount), SUM(credit_amount)
                 FROM TMP_GL GROUP BY 1,2,3
                 MINUS
                 SELECT fiscal_year, business_area, fiscal_year_period,
                        SUM(fg_debit), SUM(fg_credit)
                 FROM TMP_SA GROUP BY 1,2,3)
                UNION ALL
                (SELECT fiscal_year, business_area, fiscal_year_period,
                        SUM(fg_debit), SUM(fg_credit)
                 FROM TMP_SA GROUP BY 1,2,3
                 MINUS
                 SELECT fiscal_year, business_area, fiscal_year_period,
                        SUM(debit_amount), SUM(credit_amount)
                 FROM TMP_GL GROUP BY 1,2,3)
            )
        """).collect()[0]["diff_cnt"]

        logger.info(f"[ODS][GL/SA] diff_cnt={diff_cnt}")

        gl_grouped = tmp_gl.groupBy(
            "fiscal_year", "business_area", "fiscal_year_period"
        ).agg(
            F.sum("debit_amount").alias("gl_debit"),
            F.sum("credit_amount").alias("gl_credit")
        ).orderBy("fiscal_year", "business_area", "fiscal_year_period")

        sa_grouped = tmp_sa.groupBy(
            "fiscal_year", "business_area", "fiscal_year_period"
        ).agg(
            F.sum("fg_debit").alias("sa_debit"),
            F.sum("fg_credit").alias("sa_credit")
        ).orderBy("fiscal_year", "business_area", "fiscal_year_period")

        gl_rows = gl_grouped.collect()
        sa_rows = sa_grouped.collect()

        if diff_cnt == 0:
            gl_ods = ods_name_map.get(gl_table, gl_stg)
            sa_ods = ods_name_map.get(sa_table, sa_stg)
            ods_table_name_map[gl_table] = gl_ods
            ods_table_name_map[sa_table] = sa_ods
            try:
                delete_s3_prefix(ods_bucket, f"fmmi/{ods_prefix}/{gl_ods}/load_date={landing_date}")
            except:
                pass
            try:
                delete_s3_prefix(ods_bucket, f"fmmi/{ods_prefix}/{sa_ods}/load_date={landing_date}")
            except:
                pass
            write_ods_table(gl_ods, gl_df, landing_date)
            write_ods_table(sa_ods, sa_df, landing_date)
            ods_row_count[gl_table] = gl_count
            ods_row_count[sa_table] = sa_count
            logger.info(f"FMMI ODS - GL & SA Table Load Completed Successfully on {systemdate}")
            logger.info(f"GL/SA Reconciliation Successful (diff_cnt = {diff_cnt})")
        else:
            ods_table_name_map[gl_table] = ods_name_map.get(gl_table, gl_stg)
            ods_table_name_map[sa_table] = ods_name_map.get(sa_table, sa_stg)
            ods_row_count[gl_table] = "Skipped"
            ods_row_count[sa_table] = "Skipped"
            logger.error(f"The FMMI_ODS - SA & GL load process aborted on {systemdate}")
            logger.error(f"GL/SA Reconciliation FAILED (diff_cnt = {diff_cnt})")

    # FINAL SUMMARY
    success_tables_str = ", ".join(success_tables) if success_tables else "None"
    failed_tables_str = ", ".join(failed_tables) if failed_tables else "None"
    logger.info(
        f"FMMI_ODS : {success_count} Tables ({success_tables_str}) ran successfully "
        f"and {failure_count} Tables failed ({failed_tables_str}) on {systemdate}"
    )

    logger.info("Row Counts (Landing vs ODS):")
    for tbl in success_tables + failed_tables:
        land = landing_row_count.get(tbl, "N/A")
        ods = ods_row_count.get(tbl, "N/A")
        ods_tbl = ods_table_name_map.get(tbl, "N/A")
        logger.info(f"{tbl} → Landing Rows: {land} | FMMI_ODS.{ods_tbl} Rows: {ods}")

    for tbl in (gl_table, sa_table):
        land = landing_row_count.get(tbl, "N/A")
        ods = ods_row_count.get(tbl, "N/A")
        ods_tbl = ods_table_name_map.get(tbl, "N/A")
        logger.info(f"{tbl} → Landing Rows: {land} | FMMI_ODS.{ods_tbl} Rows: {ods}")

    logger.info("[ODS] ODS processing complete")

    # GL_SUMMARY REFRESH
    from pyspark.sql.functions import lit
    try:
        logger.info("[FINAL] Refreshing GL_SUMMARY table...")
        sql_key = f"{config_prefix}/gl_summary_refresh.sql"
        sql_text = s3_client.get_object(
            Bucket=landing_bucket, Key=sql_key)["Body"].read().decode("utf-8")

        gl_path = f"s3://{ods_bucket}/fmmi/{ods_prefix}/CS_TBL_GL"
        fc_path = f"s3://{ods_bucket}/fmmi/{ods_prefix}/CS_TBL_FUND_CENTER"
        cm_path = f"s3://{ods_bucket}/fmmi/{ods_prefix}/CS_TBL_CMMT_ITEM"

        gl_df2 = spark.read.parquet(gl_path)
        fc_df = spark.read.parquet(fc_path)
        cm_df = spark.read.parquet(cm_path)

        gl_df2.createOrReplaceTempView("cs_tbl_gl")
        fc_df.createOrReplaceTempView("cs_tbl_fund_center")
        cm_df.createOrReplaceTempView("cs_tbl_cmmt_item")

        gl_summary_df = spark.sql(sql_text)
        system_date = datetime.now().strftime("%Y%m%d")
        if "load_date" in gl_summary_df.columns:
            gl_summary_df = gl_summary_df.drop("load_date")
        gl_summary_df = gl_summary_df.withColumn("load_date", lit(system_date))
        delete_s3_prefix(ods_bucket, f"fmmi/{ods_prefix}/GL_SUMMARY")
        path = f"s3://{ods_bucket}/fmmi/{ods_prefix}/GL_SUMMARY/"
        gl_summary_df.write.mode("overwrite").parquet(path)
        logger.info("[FINAL] GL_SUMMARY table refreshed successfully.")
    except Exception as e:
        logger.error(f"[FINAL] Failed to refresh GL_SUMMARY: {e}")

    # RESULT MESSAGE
    expected_tables = list(reload_tables) + list(append_tables)
    missing_tables = [t for t in expected_tables if landing_row_count.get(t, 0) == 0]
    failed_tables = [t for t in failed_tables if t not in missing_tables]
    all_missing = len(missing_tables) == len(expected_tables)

    try:
        requested_files_missing = (
            file_names and
            all(tbl not in landing_row_count for tbl in single_tables)
        )
        no_files_received = (
            len(success_tables) == 0 and
            (
                all((v == 0 or v == "N/A" or v is None) for v in landing_row_count.values())
                or requested_files_missing
            )
        )
        if no_files_received or all_missing:
            result_message = {
                "status": "NO_FILES",
                "landing_date": landing_date,
                "success_tables": [],
                "missing_tables": missing_tables,
                "failed_tables": ["NONE"],
                "gl_count": gl_count,
                "sa_count": sa_count,
                "gl_sa_status": "NO_DATA"
            }
        elif len(success_tables) == 0 and len(failed_tables) > 0:
            result_message = {
                "status": "FAILED",
                "landing_date": landing_date,
                "success_tables": [],
                "missing_tables": missing_tables,
                "failed_tables": ["ALL_FAILED"],
                "gl_count": gl_count,
                "sa_count": sa_count,
                "gl_sa_status": (
                    "FILES_MISSING" if gl_count == 0 or sa_count == 0
                    else "MISMATCH - The FMMI_ODS-SA & GL load process aborted"
                )
            }
        else:
            result_message = {
                "status": "SUCCESS" if failure_count == 0 and len(success_tables) > 0 else "PARTIAL",
                "landing_date": landing_date,
                "success_tables": success_tables,
                "missing_tables": missing_tables,
                "failed_tables": failed_tables,
                "gl_count": gl_count,
                "sa_count": sa_count,
                "gl_sa_status": (
                    "MATCH-The FMMI_ODS-SA & GL are in sync"
                    if gl_count > 0 and sa_count > 0 and diff_cnt == 0
                    else "FILES_MISSING" if gl_count == 0 or sa_count == 0
                    else "MISMATCH - The FMMI_ODS-SA & GL load process aborted"
                )
            }
    except Exception as e:
        result_message = {
            "status": "FAILED",
            "error": str(e)
        }

    print(json.dumps(result_message))

# ---------------- DRIVER ----------------
try:
    if job_type == "STG":
        logger.info("[JOB] Running STG only")
        run_stg()
    elif job_type == "ODS":
        logger.info("[JOB] Running ODS only")
        run_ods()
    else:  # BOTH or anything else defaults to BOTH
        logger.info("[JOB] Running STG then ODS")
        run_stg()
        run_ods()

    logger.info("[JOB] Job completed successfully")
    job.commit()

except Exception as e:
    logger.error(f"[JOB] Job failed with exception: {e}")
    logger.error(traceback.format_exc())
    raise
