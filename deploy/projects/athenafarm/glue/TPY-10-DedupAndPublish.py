"""TPY-10-DedupAndPublish: deduplicate and publish tract_producer_year."""

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


TGT_DB = _opt("target_database", "athenafarm_prod_gold")
IN_TBL = _opt("in_table", "tpy_resolved_final")
OUT_TBL = _opt("target_table", "tract_producer_year")

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

spark.table(f"glue_catalog.{TGT_DB}.{IN_TBL}").createOrReplaceTempView("resolved")

sql = """
SELECT
  core_customer_identifier,
  tract_year_identifier,
  producer_involvement_start_date,
  producer_involvement_end_date,
  producer_involvement_interrupted_indicator,
  tract_producer_hel_exception_code,
  tract_producer_cw_exception_code,
  tract_producer_pcw_exception_code,
  data_status_code,
  creation_date,
  last_change_date,
  last_change_user_name,
  producer_involvement_code,
  time_period_identifier,
  state_fsa_code,
  county_fsa_code,
  farm_identifier,
  farm_number,
  tract_number,
  hel_appeals_exhausted_date,
  cw_appeals_exhausted_date,
  pcw_appeals_exhausted_date,
  tract_producer_rma_hel_exception_code,
  tract_producer_rma_cw_exception_code,
  tract_producer_rma_pcw_exception_code
FROM (
  SELECT
    r.*,
    ROW_NUMBER() OVER (
      PARTITION BY r.core_customer_identifier, r.tract_year_identifier, r.producer_involvement_code
      ORDER BY r.creation_date DESC, r.last_change_date DESC
    ) AS rownum
  FROM resolved r
) x
WHERE x.rownum = 1
"""

final_df = spark.sql(sql)
out_fqn = f"glue_catalog.{TGT_DB}.{OUT_TBL}"
final_df.limit(0).write.format("iceberg").mode("ignore").saveAsTable(out_fqn)
final_df.write.format("iceberg").mode("overwrite").saveAsTable(out_fqn)

job.commit()
