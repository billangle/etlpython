"""TPY-05-CocTimeMap: precompute active county-office/time mappings."""

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


REF_DB = _opt("ref_database", "athenafarm_prod_ref")
TGT_DB = _opt("target_database", "athenafarm_prod_gold")
OUT_TBL = _opt("out_table", "tpy_coc_time_map")

conf = SparkConf()
conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
conf.set("spark.sql.catalog.glue_catalog.warehouse", args["iceberg_warehouse"])

sc = SparkContext(conf=conf)
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

for t in ["time_period", "county_office_control"]:
    spark.table(f"glue_catalog.{REF_DB}.{t}").createOrReplaceTempView(t)

sql = """
SELECT
  tp.time_period_identifier,
  tp.time_period_name,
  coc.county_office_control_identifier,
  LPAD(coc.state_fsa_code, 2, '0') AS state_fsa_code,
  LPAD(coc.county_fsa_code, 3, '0') AS county_fsa_code
FROM time_period tp
JOIN county_office_control coc
  ON coc.time_period_identifier = tp.time_period_identifier
WHERE tp.data_status_code = 'A'
  AND tp.time_period_name >= '2014'
"""

out = spark.sql(sql)
out_fqn = f"glue_catalog.{TGT_DB}.{OUT_TBL}"
out.limit(0).write.format("iceberg").mode("ignore").saveAsTable(out_fqn)
out.write.format("iceberg").mode("overwrite").saveAsTable(out_fqn)

job.commit()
