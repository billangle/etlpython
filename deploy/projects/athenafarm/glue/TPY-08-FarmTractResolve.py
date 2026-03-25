"""TPY-08-FarmTractResolve: resolve farm/tract identifiers and farm_year_identifier."""

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
IN_TBL = _opt("in_table", "tpy_candidate_with_customer")
OUT_TBL = _opt("out_table", "tpy_resolved_ids_stage")

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

spark.table(f"glue_catalog.{TGT_DB}.{IN_TBL}").createOrReplaceTempView("cand")
for t in ["farm", "tract", "farm_year"]:
    spark.table(f"glue_catalog.{REF_DB}.{t}").createOrReplaceTempView(t)

sql = """
SELECT
  cand.*, 
  f.farm_identifier,
  tr.tract_identifier,
  fy.farm_year_identifier
FROM cand
JOIN farm f
  ON f.county_office_control_identifier = cand.county_office_control_identifier
 AND LPAD(CAST(f.farm_number AS STRING), 7, '0') = LPAD(cand.farm_number, 7, '0')
JOIN tract tr
  ON tr.county_office_control_identifier = cand.county_office_control_identifier
 AND LPAD(CAST(tr.tract_number AS STRING), 7, '0') = LPAD(cand.tract_number, 7, '0')
JOIN farm_year fy
  ON fy.farm_identifier = f.farm_identifier
 AND fy.time_period_identifier = cand.time_period_identifier
"""

out = spark.sql(sql)
out_fqn = f"glue_catalog.{TGT_DB}.{OUT_TBL}"
out.limit(0).write.format("iceberg").mode("ignore").saveAsTable(out_fqn)
out.write.format("iceberg").mode("overwrite").saveAsTable(out_fqn)

job.commit()
