"""TPY-01-SpineBase: build normalized tract/farm structural base rows."""

import sys
from awsglue.utils import getResolvedOptions
from pyspark import SparkConf
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

req = ["JOB_NAME", "env", "iceberg_warehouse"]
args = getResolvedOptions(sys.argv, req)


def _opt(name: str, default: str) -> str:
    flag = f"--{name}"
    if flag in sys.argv:
        idx = sys.argv.index(flag)
        if idx + 1 < len(sys.argv) and not sys.argv[idx + 1].startswith("--"):
            return sys.argv[idx + 1]
    return default


SSS_DB = _opt("sss_database", "athenafarm_prod_raw")
TGT_DB = _opt("target_database", "athenafarm_prod_gold")
OUT_TBL = _opt("out_table", "tpy_spine_base")

conf = SparkConf()
conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
conf.set("spark.sql.catalog.glue_catalog.warehouse", args["iceberg_warehouse"])
conf.set("spark.sql.catalog.glue_catalog.write.spark.fanout.enabled", "true")

sc = SparkContext(conf=conf)
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

for t in ["ibsp", "ibst", "ibib"]:
    spark.table(f"glue_catalog.{SSS_DB}.{t}").createOrReplaceTempView(t)

sql = """
SELECT
  CAST(sp.instance AS STRING) AS instance,
  CAST(sp.client AS STRING) AS client,
  CAST(st.ibase AS STRING) AS f_ibase,
  LPAD(COALESCE(ib.ZZFLD000000, '0'), 7, '0') AS farm_number,
  CASE
    WHEN sp.ZZFLD00000T IS NOT NULL AND sp.ZZFLD00000T <> '0' THEN LPAD(sp.ZZFLD00000T, 7, '0')
    WHEN sp.ZZFLD00001O IS NOT NULL AND TRIM(sp.ZZFLD00001O) <> ' ' THEN LPAD(SPLIT(sp.ZZFLD00001O, '-')[3], 7, '0')
    ELSE '0000000'
  END AS tract_number,
  LPAD(sp.ZZFLD00001Z, 2, '0') AS admin_state,
  LPAD(sp.ZZFLD000020, 3, '0') AS admin_county,
  CASE sp.ZZFLD0000B8 WHEN 'ACTV' THEN 'A' WHEN 'IACT' THEN 'I' WHEN 'DELE' THEN 'D' WHEN 'PEND' THEN 'P' ELSE 'A' END AS data_status_code,
  TO_DATE(date_format(COALESCE(sp.crtim, CURRENT_TIMESTAMP), 'yyyy-MM-dd')) AS creation_date,
  TO_DATE(date_format(COALESCE(sp.UPTIM, CURRENT_TIMESTAMP), 'yyyy-MM-dd')) AS last_change_date,
  CASE
    WHEN sp.upnam IS NOT NULL AND TRIM(sp.upnam) NOT IN ('0', '', ')') THEN TRIM(sp.upnam)
    WHEN sp.crnam IS NOT NULL AND TRIM(sp.crnam) NOT IN ('0', '', ')') THEN TRIM(sp.crnam)
    ELSE 'BLANK'
  END AS last_change_user_name
FROM ibsp sp
JOIN ibst st
  ON st.instance = sp.instance
 AND st.parent = '0'
LEFT JOIN ibib ib
  ON CAST(st.ibase AS STRING) = CAST(ib.ibase AS STRING)
"""

out = spark.sql(sql)
out_fqn = f"glue_catalog.{TGT_DB}.{OUT_TBL}"
out.limit(0).write.format("iceberg").mode("ignore").saveAsTable(out_fqn)
out.write.format("iceberg").mode("overwrite").saveAsTable(out_fqn)

job.commit()
