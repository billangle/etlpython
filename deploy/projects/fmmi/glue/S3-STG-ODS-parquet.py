import sys, json, boto3, traceback
from datetime import datetime
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, DecimalType, DateType, TimestampType, NullType
)

from pyspark.sql.types import StructType, StructField, StringType, DoubleType

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

# Extract bucket/folder variables
landing_bucket = args["bucket_name"]
landing_prefix = args["folder_name"]

stg_bucket = args["stg_bucket_name"]
stg_prefix = args["stg_folder_name"]

ods_bucket = args["ods_bucket_name"]
ods_prefix = args["ods_folder_name"]

# ---- optional args ----
run_date = None
raw_file_names = None

if "--run_date" in sys.argv:
    run_date = sys.argv[sys.argv.index("--run_date") + 1].strip() or None

if "--file_name" in sys.argv:
    raw_file_names = sys.argv[sys.argv.index("--file_name") + 1].strip() or None

# ---------------- ODS NAME MAP ----------------
ods_name_map = {
    # your mappings here
}

# ---------------- HELPERS FOR TABLE NAMES / FILTERING ----------------
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

# ---------------- STRICT CSV LOADER (SKIP FIRST ROW + FAILFAST) ----------------
def load_stg_dataframe(key, source_schema):
    # Read raw text
    raw = spark.read.text(f"s3://{landing_bucket}/{key}")

    # Drop first row (|| count)
    data_rdd = (
        raw.rdd
           .zipWithIndex()
           .filter(lambda x: x[1] > 0)
           .map(lambda x: x[0][0])
    )

    # Convert RDD → DataFrame with a single column "line"
    df_raw = data_rdd.toDF(["line"])

    # Split into columns based on schema length
    expected_cols = len(source_schema)
    col_names = [f"col{i}" for i in range(expected_cols)]

    # Split by pipe
    df_split = df_raw.withColumn("parts", F.split("line", "\\|"))

    # Fix missing last column
    df_split = df_split.withColumn(
        "parts",
        F.when(F.size("parts") == expected_cols - 1,
               F.concat("parts", F.array(F.lit("0"))))
         .otherwise(F.col("parts"))
    )

    # Skip rows with missing middle columns
    df_split = df_split.filter(F.size("parts") >= expected_cols - 1)

    # Skip rows with too many columns
    df_split = df_split.filter(F.size("parts") <= expected_cols)

    # Fill empty last column
    df_split = df_split.withColumn(
        "parts",
        F.when(
            (F.size("parts") == expected_cols) &
            (F.col("parts")[expected_cols - 1] == ""),
            F.expr(f"transform(parts, (x, i) -> CASE WHEN i={expected_cols-1} THEN '0' ELSE x END)")
        ).otherwise(F.col("parts"))
    )

    # Rebuild DataFrame with correct schema columns
    df_final = df_split.select(
        *[F.col("parts")[i].alias(source_schema[i].name) for i in range(expected_cols)]
    )

    return df_final

# ---------------- SCHEMA VALIDATION (VARCHAR + NUMBER(p,s)) ----------------
from pyspark.sql.functions import col, length

def validate_schema(df, source_schema):
    for field in source_schema.fields:
        colname = field.name
        dtype = field.dataType.simpleString().lower()

        # VARCHAR(n)
        if "varchar" in dtype:
            max_len = int(dtype.split("(")[1].split(")")[0])
            bad_len = df.filter(length(col(colname)) > max_len)
            if bad_len.count() > 0:
                raise Exception(
                    f"[STG] Column '{colname}' exceeds varchar({max_len}) length"
                )

        # NUMBER(p,s) → Spark uses decimal(p,s)
        if "decimal" in dtype:
            inside = dtype.split("(")[1].split(")")[0]
            precision, scale = map(int, inside.split(","))

            # Invalid numeric
            bad_numeric = df.filter(
                col(colname).isNotNull() &
                col(colname).cast(dtype).isNull()
            )
            if bad_numeric.count() > 0:
                raise Exception(
                    f"[STG] Column '{colname}' contains invalid NUMBER({precision},{scale}) data"
                )

            # Precision overflow
            bad_precision = df.filter(
                col(colname).cast(dtype).isNotNull() &
                (length(col(colname).cast("string")) > precision + 1)  # +1 for decimal point
            )
            if bad_precision.count() > 0:
                raise Exception(
                    f"[STG] Column '{colname}' exceeds NUMBER({precision},{scale}) precision"
                )

def normalize_stg_dataframe(df, schema_cols, table_name, logger, landing_date, stg_bucket):
    expected_cols = len(schema_cols)
    last_col = schema_cols[-1]

    # Convert row to array of strings
    df = df.withColumn("_arr", F.split(F.concat_ws("|", *schema_cols), "\\|"))
    df = df.withColumn("_col_count", F.size("_arr"))

    # Missing middle columns → skip + log
    bad_middle = df.filter(F.col("_col_count") < expected_cols - 1)
    for r in bad_middle.collect():
        row_text = "|".join([str(x) for x in r["_arr"]])
        logger.warn(f"[STG][{table_name}] Skipping malformed row. Logged to S3.")
        log_skipped_row_to_s3(row_text, table_name, landing_date, stg_bucket)

    df = df.filter(F.col("_col_count") >= expected_cols - 1)

    # Missing ONLY last column → append "0"
    df = df.withColumn(
        "_arr",
        F.when(F.col("_col_count") == expected_cols - 1,
               F.concat(F.col("_arr"), F.array(F.lit("0"))))
         .otherwise(F.col("_arr"))
    )

    # Too many columns → skip + log
    bad_extra = df.filter(F.col("_col_count") > expected_cols)
    for r in bad_extra.collect():
        row_text = "|".join([str(x) for x in r["_arr"]])
        logger.warn(f"[STG][{table_name}] Skipping malformed row. Logged to S3.")
        log_skipped_row_to_s3(row_text, table_name, landing_date, stg_bucket)

    df = df.filter(F.col("_col_count") <= expected_cols)

    # Last column empty → fill "0"
    df = df.withColumn(
        "_arr",
        F.when(
            (F.col("_col_count") == expected_cols) &
            (F.col("_arr")[expected_cols - 1] == ""),
            F.expr(f"transform(_arr, (x, i) -> CASE WHEN i={expected_cols-1} THEN '0' ELSE x END)")
        ).otherwise(F.col("_arr"))
    )

    # Rebuild DataFrame
    for i, col in enumerate(schema_cols):
        df = df.withColumn(col, F.col("_arr")[i])

    return df.drop("_arr", "_col_count")

# ---------------- SAFE SAMPLE LOGGER ----------------
def log_sample(df, name, n=5):
    try:
        sample = df.limit(n).toPandas()
        logger.info(f"[SAMPLE] {name} (showing {len(sample)} rows):\n{sample}")
    except Exception as e:
        logger.warn(f"[SAMPLE] Could not show sample for {name}: {e}")

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

    latest_date = max(date_folders)
    return latest_date

# ---------------- DETERMINE FILE MODE ----------------
if raw_file_names is None:
    file_names = []
    single_tables = set()
else:
    file_names = [f.strip() for f in raw_file_names.split(",") if f.strip()]
    single_tables = {normalize_table_name(derive_table_name(f)) for f in file_names}

# ---------------- END OF BLOCK ----------------

def ensure_schema(df):
    """
    Ensures:
    - header row is removed
    - if no data rows remain, returns an EMPTY DataFrame with the same schema
    """
    # Remove header row
    df = skip_first_row(df)

    # If no data rows remain, return empty DF with same schema
    if df.count() == 0:
        logger.info("[ensure_schema] No data rows found. Returning EMPTY DataFrame with schema only.")
        return spark.createDataFrame([], df.schema)

    return df

def should_process_ods(table_name: str, landing_date: str) -> bool:
    """
    table_name is NORMALIZED (e.g. 'FMMI_FSA_POHEAD').
    Skip if that load_date partition already exists in ODS.
    """
    if not landing_date:
        return True

    prefix = f"fmmi/{ods_prefix}/{table_name}/load_date={landing_date}/"
    if s3_prefix_exists(ods_bucket, prefix):
        logger.info(f"[ODS] Skipping table={table_name} because load_date={landing_date} already exists.")
        return False

    return True

# ---- required args ----
env = args["env"]
job_type = args["job_type"].upper()
landing_bucket = args["bucket_name"]
landing_prefix = args["folder_name"]
stg_bucket = args["stg_bucket_name"]
stg_prefix = args["stg_folder_name"]
ods_bucket = args["ods_bucket_name"]
ods_prefix = args["ods_folder_name"]

s3_client = boto3.client("s3")

# ---------------- AUTO-DETECT LATEST RUN DATE ----------------
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

# ---------------- AUTO-DETECT DATE FROM FILE NAME ----------------
def extract_date_from_filename(name):
    base = name.replace(".csv", "")
    parts = base.split("_")
    if len(parts) >= 2 and parts[-1].isdigit() and len(parts[-1]) == 8:
        return parts[-1]
    return None

# ---------------- RUN DATE RESOLUTION ----------------
if file_names and run_date is None:
    run_date = extract_date_from_filename(file_names[0])

    print(f"[AUTO] Extracted run_date={run_date} from file_name={file_names}")

if run_date is None:
    run_date = detect_latest_run_date()

# ---------------- CONTINUE WITH NORMAL JOB SETUP ----------------
load_bat_id = int(datetime.now().strftime("%Y%m%d%H%M%S"))

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()

logger.info(f"[DEBUG] landing_bucket={landing_bucket}")
logger.info(f"[DEBUG] landing_prefix={landing_prefix}")
logger.info(f"[DEBUG] run_date={run_date}")
logger.info(f"[DEBUG] scanning path = s3://{landing_bucket}/{landing_prefix}/{run_date}/")

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger.info(f"[JOB] Started job={args['JOB_NAME']} env={env} job_type={job_type} run_date={run_date} file_names={file_names} load_bat_id={load_bat_id}")

config_prefix = "fmmi/config"

# ---------------- FILE → JSON BASE MAP ----------------
FILE_TO_JSON = {
    "FMMI.FSA.CMMT": "cs_tbl_commitment",
    "FMMI.FSA.MD.CMMITEM": "cs_tbl_cmmt_item",
    "FMMI.FSA.MD_COSTCTR": "cs_tbl_cost_center",
    "FMMI.FSA.MD_CUSTOMER": "cs_tbl_customer",
    "FMMI.FSA.MD_FUNCAREA": "cs_tbl_func_area",
    "FMMI.FSA.MD_FUND": "cs_tbl_fund",
    "FMMI.FSA.FUNDEDPRG": "cs_tbl_funded_program",
    "FMMI.FSA.MD_GLACCT": "cs_tbl_gl_account",
    "FMMI.FSA.INV_DIS": "cs_tbl_invoice_dis",
    "FMMI.FSA.MATDOC": "cs_tbl_material_doc",
    "FMMI.FSA.PAYROLL": "cs_tbl_payroll",
    "FMMI.FSA_POHEAD": "cs_tbl_po_header",
    "FMMI.FSA_POITEM": "cs_tbl_po_item",
    "FMMI.FSA.PURCH": "cs_tbl_purchasing",
    "FMMI.FSA.MD.VENDOR": "cs_tbl_vendor",
    "FMMI.FSA.MD_WBS": "cs_tbl_wbs",
    "FMMI.FSA.MD_FUNDCTR": "cs_tbl_fund_center",
    "FMMI.FSA.GLITEM": "cs_tbl_gl",
    "FMMI.FSA.SYSASSURANCE": "cs_tbl_system_assurance"
}

# ---------------- ODS TABLE NAME MAP ----------------
ods_name_map = {
    "FMMI.FSA.CMMT": "CS_TBL_COMMITMENT",
    "FMMI.FSA.MD.CMMITEM": "CS_TBL_CMMT_ITEM",
    "FMMI.FSA.MD_COSTCTR": "CS_TBL_COST_CENTER",
    "FMMI.FSA.MD_CUSTOMER": "CS_TBL_CUSTOMER",
    "FMMI.FSA.MD_FUNCAREA": "CS_TBL_FUNC_AREA",
    "FMMI.FSA.MD_FUND": "CS_TBL_FUND",
    "FMMI.FSA.FUNDEDPRG": "CS_TBL_FUNDED_PROGRAM",
    "FMMI.FSA.MD_GLACCT": "CS_TBL_GL_ACCOUNT",
    "FMMI.FSA.INV_DIS": "CS_TBL_INVOICE_DIS",
    "FMMI.FSA.MATDOC": "CS_TBL_MATERIAL_DOC",
    "FMMI.FSA.PAYROLL": "CS_TBL_PAYROLL",
    "FMMI.FSA_POHEAD": "CS_TBL_PO_HEADER",
    "FMMI.FSA_POITEM": "CS_TBL_PO_ITEM",
    "FMMI.FSA.PURCH": "CS_TBL_PURCHASING",
    "FMMI.FSA.MD.VENDOR": "CS_TBL_VENDOR",
    "FMMI.FSA.MD_WBS": "CS_TBL_WBS",
    "FMMI.FSA.MD_FUNDCTR": "CS_TBL_FUND_CENTER",
    "FMMI.FSA.GLITEM": "CS_TBL_GL",
    "FMMI.FSA.SYSASSURANCE": "CS_TBL_SYSTEM_ASSURANCE",
}

def map_ods_name(stg_table_name):
    json_base = FILE_TO_JSON.get(stg_table_name)
    if not json_base:
        logger.error(f"[ODS] No JSON mapping found for table={stg_table_name}")
        return stg_table_name
    return json_base.upper()

# ---------------- AUDIT ID MAP (PER ODS TABLE) ----------------
AUD_ID_MAP = {
    "CS_TBL_CMMT_ITEM": 1,
    "CS_TBL_COMMITMENT": 2,
    "CS_TBL_COST_CENTER": 3,
    "CS_TBL_CUSTOMER": 4,
    "CS_TBL_FUNC_AREA": 5,
    "CS_TBL_FUND": 6,
    "CS_TBL_FUND_CENTER": 7,
    "CS_TBL_FUNDED_PROGRAM": 8,
    "CS_TBL_GL": 9,
    "CS_TBL_GL_ACCOUNT": 10,
    "CS_TBL_INVOICE_DIS": 11,
    "CS_TBL_PAYROLL": 11,
    "CS_TBL_MATERIAL_DOC": 12,
    "CS_TBL_PO_HEADER": 14,
    "CS_TBL_PO_ITEM": 15,
    "CS_TBL_PURCHASING": 16,
    "CS_TBL_SYSTEM_ASSURANCE": 19,
    "CS_TBL_VENDOR": 21,
    "CS_TBL_WBS": 22
}

# ---------------- RELOAD / APPEND CONFIG ----------------
reload_tables = {
    "FMMI.FSA.MD.CMMITEM",
    "FMMI.FSA.MD_COSTCTR",
    "FMMI.FSA.MD_CUSTOMER",
    "FMMI.FSA.MD_FUNCAREA",
    "FMMI.FSA.MD_FUND",
    "FMMI.FSA.FUNDEDPRG",
    "FMMI.FSA.MD_FUNDCTR",
    "FMMI.FSA.MD_GLACCT",
    "FMMI.FSA.MD.VENDOR",
    "FMMI.FSA.MD_WBS"
}

append_tables = {
    "FMMI.FSA.CMMT",
    "FMMI.FSA.GLITEM",
    "FMMI.FSA.INV_DIS",
    "FMMI.FSA.MATDOC",
    "FMMI.FSA.PAYROLL",
    "FMMI.FSA_POHEAD",
    "FMMI.FSA_POITEM",
    "FMMI.FSA.PURCH",
    "FMMI.FSA.SYSASSURANCE"
}

gl_table = "FMMI.FSA.GLITEM"
sa_table = "FMMI.FSA.SYSASSURANCE"

# ---------------- HELPERS ----------------
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
            # Nothing to delete — this is normal
            logger.info(f"[S3] No objects found under prefix {prefix}. Nothing to delete.")
            return

        logger.info(f"[S3] Deleting {len(objs)} objects under prefix {prefix}")
        bucket_obj.objects.filter(Prefix=prefix).delete()

    except Exception as e:
        # Only log the error — do NOT stop ODS
        logger.warn(f"[S3] Warning while deleting prefix {prefix}: {e}")

# ---------------- LATEST FILE PER TABLE (AUTO MODE) ----------------
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

# ---------------- FUND CENTER LOGIC ----------------
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

# ---------------- AUDIT COLUMNS (STG & ODS) ----------------
def add_audit_columns(df, src_table_name):
    ods_name = map_ods_name(src_table_name)
    aud_id = AUD_ID_MAP.get(ods_name)
    logger.info(f"[AUDIT] Adding audit columns for src_table={src_table_name} ods_table={ods_name} aud_id={aud_id}")
    return (df
        .withColumn("fmmi_ods_cs_aud_id", F.lit(aud_id))
        .withColumn("cre_dt", F.current_date())
        .withColumn("last_chg_dt", F.current_date())
        .withColumn("last_chg_user_nm", F.lit("fsa_prod_rs_usr"))
        .withColumn("load_bat_id", F.lit(load_bat_id))
    )

def log_skipped_row_to_s3(row_text, table_name, landing_date, stg_bucket):
    """
    Writes skipped/malformed rows to S3 under:
    s3://<bucket>/fmmi/stg_bad_rows/<table>/<landing_date>/bad_rows.txt
    """
    import boto3
    s3 = boto3.client("s3")

    key = f"fmmi/stg_bad_rows/{table_name}/{landing_date}/bad_rows.txt"

    # Append mode: read existing content if present
    try:
        existing = s3.get_object(Bucket=stg_bucket, Key=key)["Body"].read().decode("utf-8")
    except Exception:
        existing = ""

    new_content = existing + row_text + "\n"

    s3.put_object(
        Bucket=stg_bucket,
        Key=key,
        Body=new_content.encode("utf-8")
    )

# ---------------- STG LOGIC ----------------
def run_stg():
    global landing_date

    logger.info(f"[DEBUG] file_names={file_names}")
    logger.info(f"[DEBUG] single_tables={single_tables}")
    logger.info(f"[DEBUG] FILE_TO_JSON keys={list(FILE_TO_JSON.keys())}")

    # ---------------------------------------------------------
    # CLEAR STG ZONE FOR ALLOWED TABLES ONLY
    # ---------------------------------------------------------
    logger.info("[STG] Clearing STG zone for allowed tables only")

    for raw_table in FILE_TO_JSON.keys():
        stg_table = normalize_table_name(raw_table)
        stg_prefix_path = f"fmmi/{stg_prefix}/{stg_table}/"
        delete_s3_prefix(stg_bucket, stg_prefix_path)

    logger.info("[STG] Starting STG processing")

    # ---------------------------------------------------------
    # SINGLE-FILE / MULTI-FILE MODE
    # ---------------------------------------------------------
    if single_tables:
        logger.info(f"[STG] SINGLE-FILE MODE enabled. Tables={single_tables}")

        for f in file_names:
            file_name = f.split("/")[-1]
            raw_table_name = derive_table_name(file_name)
            table_name = normalize_table_name(raw_table_name)

            # Determine landing_date
            if run_date:
                landing_date = run_date
            else:
                landing_date = file_name.split("_")[-1].replace(".csv", "")

            key = f"{landing_prefix}/{landing_date}/{file_name}"
            logger.info(f"[STG] Processing key={key} landing_date={landing_date}")

            # Skip if not in FILE_TO_JSON
            if raw_table_name not in FILE_TO_JSON:
                logger.info(f"[STG] Skipping table={raw_table_name} (not in FILE_TO_JSON)")
                continue

            json_base = FILE_TO_JSON[raw_table_name]
            source_json_key = f"{config_prefix}/{json_base}_source.json"
            target_json_key = f"{config_prefix}/{json_base}_target.json"

            # ---- NEW: safe JSON loading ----
            try:
                source_json = load_json_from_s3(landing_bucket, source_json_key)
                target_json = load_json_from_s3(landing_bucket, target_json_key)
            except Exception as e:
                logger.error(
                    f"[STG] Missing source/target JSON for table={raw_table_name} "
                    f"(base={json_base}). Skipping. Error: {e}"
                )
                continue
            # --------------------------------

            source_schema = json_to_spark_schema(source_json)

            # CSV LOAD + VALIDATION
            df = load_stg_dataframe(key, source_schema)
            schema_cols = [c["Name"] for c in source_json["schema"]]
            df = normalize_stg_dataframe(df, schema_cols, raw_table_name, logger, landing_date, stg_bucket)

            validate_schema(df, source_schema)
            
            log_sample(df, f"STG sample for {raw_table_name}")

            df = df.toDF(*[c.lower() for c in df.columns])
            df = add_audit_columns(df, raw_table_name)


            #df = skip_first_row(df)

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

    # ---------------------------------------------------------
    # AUTO MODE
    # ---------------------------------------------------------
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

        # ---- NEW: safe JSON loading ----
        try:
            source_json = load_json_from_s3(landing_bucket, source_json_key)
            target_json = load_json_from_s3(landing_bucket, target_json_key)
        except Exception as e:
            logger.error(
                f"[STG] Missing source/target JSON for table={raw_table_name} "
                f"(base={json_base}). Skipping. Error: {e}"
            )
            continue
        # --------------------------------

        source_schema = json_to_spark_schema(source_json)

        # STRICT CSV LOAD + VALIDATION
        df = load_stg_dataframe(key, source_schema)
        schema_cols = [c["Name"] for c in source_json["schema"]]
        df = normalize_stg_dataframe(df, schema_cols, raw_table_name, logger, landing_date, stg_bucket)
        validate_schema(df, source_schema)
        log_sample(df, f"STG sample for {raw_table_name}")

        df = df.toDF(*[c.lower() for c in df.columns])
        df = add_audit_columns(df, raw_table_name)


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

# ---------------- ODS HELPERS ----------------
def read_stg_parquet(stg_name):
    path = f"s3://{stg_bucket}/fmmi/fmmi_stg/{stg_name}/"
    try:
        return spark.read.parquet(path)
    except Exception as e:
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

def skip_first_row(df):
    """
    Removes the first row of the DataFrame by using a monotonically increasing id.
    This guarantees the header row is removed regardless of content.
    """
    return (
        df.withColumn("_row_id", F.monotonically_increasing_id())
          .filter(F.col("_row_id") != 0)
          .drop("_row_id")
    )

def delete_ods_partition(table_name, run_date):
    mapped = map_ods_name(table_name)
    prefix = f"fmmi/{ods_prefix}/{mapped}/cre_dt={run_date}"

    logger.info(f"[ODS] Deleting same-day partition: s3://{ods_bucket}/{prefix}")

    paginator = s3_client.get_paginator("list_objects_v2")
    to_delete = []

    for page in paginator.paginate(Bucket=ods_bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            to_delete.append({"Key": obj["Key"]})
            if len(to_delete) == 1000:
                s3_client.delete_objects(Bucket=ods_bucket, Delete={"Objects": to_delete})
                to_delete = []

    if to_delete:
        s3_client.delete_objects(Bucket=ods_bucket, Delete={"Objects": to_delete})

# ---------------- NEW ODS WRITER (REQUIRED) ----------------

def write_ods_table(table_name, df, landing_date):
    """
    Writes a Spark DataFrame to ODS in partitioned parquet format.
    Partition: load_date=YYYYMMDD
    """
    output_path = f"s3://{ods_bucket}/fmmi/{ods_prefix}/{table_name}/load_date={landing_date}"

    df.repartition(1).write \
        .mode("overwrite") \
        .parquet(output_path)

    logger.info(f"[ODS] Wrote table={table_name} partition={landing_date} to {output_path}")
    
# ---------------- ODS LOGIC ----------------

def run_ods():
    logger.info("[ODS] Starting ODS processing")

    # Use landing_date from STG
    landing_date = globals().get("landing_date")
    if not landing_date:
        landing_date = get_landing_date()

    logger.info(f"[ODS] Using landing_date={landing_date}")

    from datetime import datetime
    systemdate = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # ---------------------------------------------------------
    # SUCCESS / FAILURE COUNTERS FOR 17 NORMAL TABLES
    # ---------------------------------------------------------
    success_count = 0
    failure_count = 0
    success_tables = []
    failed_tables = []

    # Row counts and ODS table name mapping
    landing_row_count = {}     # source table -> landing/STG row count
    ods_row_count = {}         # source table -> ODS row count (or "Skipped")
    ods_table_name_map = {}    # source table -> ODS table name
    # ---------------------------------------------------------
# PRE-SCAN STG TO DETECT WHICH TABLES EXIST
# ---------------------------------------------------------
    landing_row_count = {}

    for t in list(reload_tables) + list(append_tables):
        stg_name = normalize_table_name(t)
        try:
            df = read_stg_parquet(stg_name)
            landing_row_count[t] = df.count()
        except Exception:
            landing_row_count[t] = 0

    # ---------------------------------------------------------
    # 1. RELOAD TABLES
    # ---------------------------------------------------------
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
    # ---------------------------------------------------------
    # 4. FUND CENTER TRANSFORM (RELOAD ONLY)
    # ---------------------------------------------------------
    logger.info("[ODS][FUNDCTR] Starting Fund Center transform block")

    fundctr_key = "FMMI.FSA.MD_FUNDCTR"

    if fundctr_key in FILE_TO_JSON:
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
            if missing:
                logger.error(f"[ODS][FUNDCTR] Missing required columns: {missing}")
                logger.error("[ODS][FUNDCTR] Skipping Fund Center transform due to schema mismatch.")
                ods_table_name = ods_name_map.get(fundctr_key, normalize_table_name(fundctr_key))
                partition_prefix = f"fmmi/{ods_prefix}/{ods_table_name}/load_date={landing_date}"
                delete_s3_prefix(ods_bucket, partition_prefix)
                write_ods_table(ods_table_name, df, landing_date)
                pass

            df = transform_fund_center(df)

            after_count = df.count()
            logger.info(f"[ODS][FUNDCTR] Row count AFTER transform: {after_count}")

            ods_table_name = ods_name_map.get(fundctr_key, normalize_table_name(fundctr_key))

            partition_prefix = f"fmmi/{ods_prefix}/{ods_table_name}/load_date={landing_date}"
            delete_s3_prefix(ods_bucket, partition_prefix)

            write_ods_table(ods_table_name, df, landing_date)

            logger.info("[ODS][FUNDCTR] Fund Center transform complete")

        except Exception as e:
            logger.error(f"[ODS][FUNDCTR] Processing failed: {e}")
            logger.error(traceback.format_exc())

    # ---------------------------------------------------------
    # 2. APPEND TABLES
    # ---------------------------------------------------------
    logger.info("[ODS] Starting APPEND tables")

    for t in append_tables:

        if landing_row_count.get(t, 0) == 0:
            logger.info(f"[ODS] Skipping append for missing table={t}") 
            continue
        # DO NOT COUNT GL/SA HERE
        if t in ("FMMI.FSA.GLITEM", "FMMI.FSA.SYSASSURANCE"):
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

    # ---------------------------------------------------------
    # 3. GL/SA RECONCILIATION (SEPARATE FLOW)
    # ---------------------------------------------------------
    logger.info("[ODS][GL/SA] Starting GL/SA reconciliation block")

    gl_table = "FMMI.FSA.GLITEM"
    sa_table = "FMMI.FSA.SYSASSURANCE"

    gl_stg = normalize_table_name(gl_table)
    sa_stg = normalize_table_name(sa_table)

    gl_df = None
    sa_df = None
    gl_count = 0
    sa_count = 0

    # Try to read GL STG
    try:
        gl_df = read_stg_parquet(gl_stg)
        gl_count = gl_df.count()
    except Exception as e:
        logger.warn(f"[ODS][GL] Failed to read STG for {gl_stg}: {e}")
        gl_df = None
        gl_count = 0

    # Try to read SA STG
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

    # Build TMP tables
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

    # Reload TMP tables
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

    # CASE: Missing files → skip main loads
    if gl_count == 0 or sa_count == 0:
        logger.error(f"FMMI ODS - Either GL Or SA files did not received on {systemdate}")
        logger.info("[ODS][GL/SA] Skipping GL/SA reconciliation and main ODS loads due to missing file(s).")
        ods_row_count[gl_table] = "Skipped"
        ods_row_count[sa_table] = "Skipped"
        ods_table_name_map[gl_table] = ods_name_map.get(gl_table, gl_stg)
        ods_table_name_map[sa_table] = ods_name_map.get(sa_table, sa_stg)

    else:
        # Reconciliation diff count
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

        # Grouped summaries by FY, BA, Period
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

        # Show only first 5 grouped rows for GL
        try:
            gl_sample = gl_grouped.limit(5).toPandas()
            logger.info("[ODS][GL] Sample grouped summary (first 5 rows):")
            logger.info(f"\n{gl_sample}")
        except Exception as e:
            logger.warn(f"[ODS][GL] Could not show GL summary sample: {e}")

    # Show only first 5 grouped rows for SA
        try:
            sa_sample = sa_grouped.limit(5).toPandas()
            logger.info("[ODS][SA] Sample grouped summary (first 5 rows):")
            logger.info(f"\n{sa_sample}")
        except Exception as e:
            logger.warn(f"[ODS][SA] Could not show SA summary sample: {e}")

        #
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

            logger.info("GL Summary (FY | BA | Period | Debit | Credit):")
            for r in gl_rows:
                logger.info(
                    f"{r['fiscal_year']} | {r['business_area']} | {r['fiscal_year_period']} | "
                    f"{r['gl_debit']} | {r['gl_credit']}"
                )

            logger.info("SA Summary (FY | BA | Period | Debit | Credit):")
            for r in sa_rows:
                logger.info(
                    f"{r['fiscal_year']} | {r['business_area']} | {r['fiscal_year_period']} | "
                    f"{r['sa_debit']} | {r['sa_credit']}"
                )

        else:
            ods_table_name_map[gl_table] = ods_name_map.get(gl_table, gl_stg)
            ods_table_name_map[sa_table] = ods_name_map.get(sa_table, sa_stg)
            ods_row_count[gl_table] = "Skipped"
            ods_row_count[sa_table] = "Skipped"

            logger.error(f"The FMMI_ODS - SA & GL load process aborted on {systemdate}")
            logger.error(f"GL/SA Reconciliation FAILED (diff_cnt = {diff_cnt})")

            logger.error("GL Summary (FY | BA | Period | Debit | Credit):")
            for r in gl_rows:
                logger.error(
                    f"{r['fiscal_year']} | {r['business_area']} | {r['fiscal_year_period']} | "
                    f"{r['gl_debit']} | {r['gl_credit']}"
                )

            logger.error("SA Summary (FY | BA | Period | Debit | Credit):")
            for r in sa_rows:
                logger.error(
                    f"{r['fiscal_year']} | {r['business_area']} | {r['fiscal_year_period']} | "
                    f"{r['sa_debit']} | {r['sa_credit']}"
                )

    # ---------------------------------------------------------
    # FINAL SUMMARY MESSAGE FOR 17 NORMAL TABLES
    # ---------------------------------------------------------
    success_tables_str = ", ".join(success_tables) if success_tables else "None"
    failed_tables_str = ", ".join(failed_tables) if failed_tables else "None"

    logger.info(
        f"FMMI_ODS : {success_count} Tables ({success_tables_str}) ran successfully "
        f"and {failure_count} Tables failed ({failed_tables_str}) on {systemdate}"
    )

    # Row counts (Landing vs ODS) for all normal tables + GL/SA
    logger.info("Row Counts (Landing vs ODS):")

    # Normal 17 tables
    for tbl in success_tables + failed_tables:
        land = landing_row_count.get(tbl, "N/A")
        ods = ods_row_count.get(tbl, "N/A")
        ods_tbl = ods_table_name_map.get(tbl, "N/A")
        logger.info(f"{tbl} → Landing Rows: {land} | FMMI_ODS.{ods_tbl} Rows: {ods}")

    # GL/SA
    for tbl in (gl_table, sa_table):
        land = landing_row_count.get(tbl, "N/A")
        ods = ods_row_count.get(tbl, "N/A")
        ods_tbl = ods_table_name_map.get(tbl, "N/A")
        logger.info(f"{tbl} → Landing Rows: {land} | FMMI_ODS.{ods_tbl} Rows: {ods}")

        logger.info("[ODS] ODS processing complete")

        from datetime import datetime
        from pyspark.sql.functions import lit

        logger.info("[FINAL] Refreshing GL_SUMMARY table...")

    try:
        # Load SQL from /config
        sql_key = f"{config_prefix}/gl_summary_refresh.sql"
        sql_text = s3_client.get_object(
        Bucket=landing_bucket, Key=sql_key)["Body"].read().decode("utf-8")

    # ---------------------------------------------------------
    # Register ODS parquet tables as SQL views (REQUIRED)
    # ---------------------------------------------------------
        gl_path = f"s3://{ods_bucket}/fmmi/{ods_prefix}/CS_TBL_GL"
        fc_path = f"s3://{ods_bucket}/fmmi/{ods_prefix}/CS_TBL_FUND_CENTER"
        cm_path = f"s3://{ods_bucket}/fmmi/{ods_prefix}/CS_TBL_CMMT_ITEM"

        gl_df = spark.read.parquet(gl_path)
        fc_df = spark.read.parquet(fc_path)
        cm_df = spark.read.parquet(cm_path)

        gl_df.createOrReplaceTempView("cs_tbl_gl")
        fc_df.createOrReplaceTempView("cs_tbl_fund_center")
        cm_df.createOrReplaceTempView("cs_tbl_cmmt_item")

    # ---------------------------------------------------------
    # Execute SQL using Spark (AFTER views exist)
    # ---------------------------------------------------------
        gl_summary_df = spark.sql(sql_text)

    # Generate system date (YYYYMMDD)
        system_date = datetime.now().strftime("%Y%m%d")

    # Remove any existing load_date column
        if "load_date" in gl_summary_df.columns:
            gl_summary_df = gl_summary_df.drop("load_date")

    # Add ONLY the system date load_date column
        gl_summary_df = gl_summary_df.withColumn("load_date", lit(system_date))

    # Overwrite GL_SUMMARY in ODS (NO PARTITIONING)
        delete_s3_prefix(ods_bucket, f"fmmi/{ods_prefix}/GL_SUMMARY")
        path = f"s3://{ods_bucket}/fmmi/{ods_prefix}/GL_SUMMARY/"
        gl_summary_df.write.mode("overwrite").parquet(path)

        logger.info("[FINAL] GL_SUMMARY table refreshed successfully.")

    except Exception as e:
        logger.error(f"[FINAL] Failed to refresh GL_SUMMARY: {e}")

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
                "FILES_MISSING" if gl_count == 0 or sa_count == 0 else "MISMATCH - The FMMI_ODS-SA & GL load process aborted"
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
                    "MATCH-The FMMI_ODS-SA & GL are in sync" if gl_count > 0 and sa_count > 0 and diff_cnt == 0
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
        logger.info("[DEBUG] ENTERED1 run_stg()")
        run_stg()
        logger.info("[DEBUG] completed1 run_stg()")
    elif job_type == "ODS":
        logger.info("[JOB] Running ODS only")
        logger.info("[DEBUG] ENTERED1 run_ods()")
        run_ods()
        logger.info("[DEBUG] completed1 run_ods()")
    elif job_type == "BOTH":
        logger.info("[JOB] Running STG then ODS")
        logger.info("[DEBUG] ENTERED2 run_stg()")
        run_stg()
        logger.info("[DEBUG] completed2 run_stg()")
        logger.info("[DEBUG] ENTERED2 run_ods()")
        run_ods()
        logger.info("[DEBUG] completed2 run_ods()")
    else:
        raise ValueError(f"Invalid job_type: {job_type}")

    logger.info("[JOB] Job completed successfully")
    job.commit()

except Exception as e:
    logger.error(f"[JOB] Job failed with exception: {e}")
    logger.error(traceback.format_exc())
    raise
