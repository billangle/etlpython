"""TPY-07-PartnerCustomerResolve: resolve core_customer_identifier via but000."""

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
IN_TBL = _opt("in_table", "tpy_candidate_stage")
OUT_TBL = _opt("out_table", "tpy_candidate_with_customer")

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
spark.table(f"glue_catalog.{REF_DB}.but000").createOrReplaceTempView("but000")

sql = """
SELECT
  CAST(b.bpext AS INT) AS core_customer_identifier,
  cand.time_period_identifier,
  cand.producer_involvement_start_date,
  cand.producer_involvement_end_date,
  cand.producer_involvement_interrupted_indicator,
  cand.tract_producer_hel_exception_code,
  cand.tract_producer_cw_exception_code,
  cand.tract_producer_pcw_exception_code,
  cand.data_status_code,
  cand.creation_date,
  cand.last_change_date,
  cand.last_change_user_name,
  cand.producer_involvement_code,
  cand.state_fsa_code,
  cand.county_fsa_code,
  cand.farm_number,
  cand.tract_number,
  cand.hel_appeals_exhausted_date,
  cand.cw_appeals_exhausted_date,
  cand.pcw_appeals_exhausted_date,
  cand.tract_producer_rma_hel_exception_code,
  cand.tract_producer_rma_cw_exception_code,
  cand.tract_producer_rma_pcw_exception_code,
  cand.county_office_control_identifier
FROM cand
LEFT JOIN but000 b
  ON cand.partner_no = b.partner_guid
 AND cand.ZZK0011 = b.partner
WHERE b.bpext IS NOT NULL
  AND b.bpext NOT IN ('DUPLICATE', '11876423_D')
"""

out = spark.sql(sql)
out_fqn = f"glue_catalog.{TGT_DB}.{OUT_TBL}"
out.limit(0).write.format("iceberg").mode("ignore").saveAsTable(out_fqn)
out.write.format("iceberg").mode("overwrite").saveAsTable(out_fqn)

job.commit()
