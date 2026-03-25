"""TPY-02-InstanceGuidMap: resolve in_guid for base structure keys."""

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

sc = SparkContext(conf=conf)
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

spark.table(f"glue_catalog.{TGT_DB}.{IN_TBL}").createOrReplaceTempView("spine")
spark.table(f"glue_catalog.{SSS_DB}.ibin").createOrReplaceTempView("ibin")

sql = """
SELECT DISTINCT
  spine.instance,
  spine.client,
  spine.f_ibase,
  CAST(ibin.in_guid AS STRING) AS in_guid
FROM spine
JOIN ibin
  ON CAST(ibin.instance AS STRING) = spine.instance
 AND CAST(ibin.client AS STRING) = spine.client
 AND CAST(ibin.ibase AS STRING) = spine.f_ibase
"""

out = spark.sql(sql)
out_fqn = f"glue_catalog.{TGT_DB}.{OUT_TBL}"
out.limit(0).write.format("iceberg").mode("ignore").saveAsTable(out_fqn)
out.write.format("iceberg").mode("overwrite").saveAsTable(out_fqn)

job.commit()
