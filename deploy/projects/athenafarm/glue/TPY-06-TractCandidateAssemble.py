"""TPY-06-TractCandidateAssemble: assemble tract candidate rows from staged maps."""

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
OUT_TBL = _opt("out_table", "tpy_candidate_stage")

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

for t in ["tpy_spine_base", "tpy_instance_guid_map", "tpy_partner_map", "tpy_zmi_map", "tpy_coc_time_map"]:
    spark.table(f"glue_catalog.{TGT_DB}.{t}").createOrReplaceTempView(t)

sql = """
SELECT
  s.instance,
  s.client,
  s.f_ibase,
  ig.in_guid,
  ctm.county_office_control_identifier,
  ctm.time_period_identifier,
  CAST(NULL AS INT) AS core_customer_identifier,
  zm.producer_involvement_start_date,
  zm.producer_involvement_end_date,
  CAST(NULL AS STRING) AS producer_involvement_interrupted_indicator,
  zm.tract_producer_hel_exception_code,
  zm.tract_producer_cw_exception_code,
  zm.tract_producer_pcw_exception_code,
  s.data_status_code,
  s.creation_date,
  s.last_change_date,
  s.last_change_user_name,
  CASE pm.partner_fct WHEN 'ZFARMONR' THEN 162 WHEN 'ZOTNT' THEN 163 ELSE NULL END AS producer_involvement_code,
  s.admin_state AS state_fsa_code,
  s.admin_county AS county_fsa_code,
  s.farm_number,
  s.tract_number,
  zm.hel_appeals_exhausted_date,
  zm.cw_appeals_exhausted_date,
  zm.pcw_appeals_exhausted_date,
  zm.tract_producer_rma_hel_exception_code,
  zm.tract_producer_rma_cw_exception_code,
  zm.tract_producer_rma_pcw_exception_code,
  pm.partner_no,
  zm.ZZK0011
FROM tpy_spine_base s
JOIN tpy_instance_guid_map ig
  ON ig.instance = s.instance
 AND ig.client = s.client
 AND ig.f_ibase = s.f_ibase
LEFT JOIN tpy_partner_map pm
  ON pm.in_guid = ig.in_guid
LEFT JOIN tpy_zmi_map zm
  ON zm.instance = s.instance
 AND zm.f_ibase = s.f_ibase
JOIN tpy_coc_time_map ctm
  ON ctm.state_fsa_code = LPAD(s.admin_state, 2, '0')
 AND ctm.county_fsa_code = LPAD(s.admin_county, 3, '0')
 AND ctm.time_period_name = CAST(CASE WHEN MONTH(CURRENT_DATE) >= 10 THEN YEAR(CURRENT_DATE) + 1 ELSE YEAR(CURRENT_DATE) END AS STRING)
"""

out = spark.sql(sql)
out_fqn = f"glue_catalog.{TGT_DB}.{OUT_TBL}"
out.limit(0).write.format("iceberg").mode("ignore").saveAsTable(out_fqn)
out.write.format("iceberg").mode("overwrite").saveAsTable(out_fqn)

job.commit()
