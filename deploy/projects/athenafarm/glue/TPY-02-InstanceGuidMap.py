"""TPY-02-InstanceGuidMap: expand spine rows to f_ibase/farm_number/in_guid.

Version History:
  - 2026-03-25: Initial split-pipeline implementation.
  - 2026-03-26: Took over heavy structure/farm expansion from TPY-01 as part of
    timeout mitigation driven by new-errors-1.
"""

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
IN_TBL = _opt("in_table", "tpy_spine_base")
OUT_TBL = _opt("out_table", "tpy_instance_guid_map")

conf = SparkConf()
conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
conf.set("spark.sql.catalog.glue_catalog.warehouse", args["iceberg_warehouse"])
conf.set("spark.sql.adaptive.enabled", "true")
conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
conf.set("spark.sql.shuffle.partitions", _opt("shuffle_partitions", "1000"))

sc = SparkContext(conf=conf)
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

spark.table(f"glue_catalog.{TGT_DB}.{IN_TBL}").createOrReplaceTempView("spine")
spark.table(f"glue_catalog.{SSS_DB}.ibst").createOrReplaceTempView("ibst")
spark.table(f"glue_catalog.{SSS_DB}.ibib").createOrReplaceTempView("ibib")
spark.table(f"glue_catalog.{SSS_DB}.ibin").createOrReplaceTempView("ibin")

sql = """
WITH root_struct AS (
  SELECT DISTINCT
    CAST(st.instance AS STRING) AS instance,
    CAST(st.ibase AS STRING) AS f_ibase
  FROM ibst st
  WHERE st.parent = '0'
),
core AS (
  SELECT
    s.instance,
    s.client,
    r.f_ibase,
    s.tract_number,
    s.admin_state,
    s.admin_county,
    s.data_status_code,
    s.creation_date,
    s.last_change_date,
    s.last_change_user_name
  FROM spine s
  JOIN root_struct r
    ON r.instance = s.instance
),
core_keys AS (
  SELECT DISTINCT f_ibase
  FROM core
),
farm_lookup AS (
  SELECT
    CAST(ib.ibase AS STRING) AS f_ibase,
    LPAD(COALESCE(ib.ZZFLD000000, '0'), 7, '0') AS farm_number
  FROM ibib ib
  JOIN core_keys k
    ON CAST(ib.ibase AS STRING) = k.f_ibase
)
SELECT DISTINCT
  c.instance,
  c.client,
  c.f_ibase,
  COALESCE(f.farm_number, '0000000') AS farm_number,
  c.tract_number,
  c.admin_state,
  c.admin_county,
  c.data_status_code,
  c.creation_date,
  c.last_change_date,
  c.last_change_user_name,
  CAST(ibin.in_guid AS STRING) AS in_guid
FROM core c
LEFT JOIN farm_lookup f
  ON f.f_ibase = c.f_ibase
JOIN ibin
  ON CAST(ibin.instance AS STRING) = c.instance
 AND CAST(ibin.client AS STRING) = c.client
 AND CAST(ibin.ibase AS STRING) = c.f_ibase
"""

out = spark.sql(sql)
out_fqn = f"glue_catalog.{TGT_DB}.{OUT_TBL}"
out.limit(0).write.format("iceberg").mode("ignore").saveAsTable(out_fqn)
out.write.format("iceberg").mode("overwrite").saveAsTable(out_fqn)

job.commit()
