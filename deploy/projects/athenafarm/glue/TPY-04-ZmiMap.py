"""TPY-04-ZmiMap: resolve ZMI exception and producer date attributes."""

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
IN_TBL = _opt("in_table", "tpy_instance_guid_map")
OUT_TBL = _opt("out_table", "tpy_zmi_map")

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

spark.table(f"glue_catalog.{TGT_DB}.{IN_TBL}").createOrReplaceTempView("guid_map")
for t in ["z_ibase_comp_detail", "comm_pr_frg_rel", "zmi_farm_partn"]:
    spark.table(f"glue_catalog.{SSS_DB}.{t}").createOrReplaceTempView(t)

sql = """
SELECT DISTINCT
  gm.instance,
  gm.f_ibase,
  zm.ZZK0011,
  CASE TRIM(zm.ZZ0011)
      WHEN 'AE' THEN 41 WHEN 'AR' THEN 40 WHEN 'HA' THEN 40
      WHEN 'NP' THEN 45 WHEN 'NW' THEN 45 WHEN 'TP' THEN 44
      WHEN 'TR' THEN 44 ELSE NULL
  END AS tract_producer_cw_exception_code,
  CASE TRIM(zm.ZZ0010)
      WHEN 'GF' THEN 31 WHEN 'EH' THEN 34 WHEN 'AE' THEN 33
      WHEN 'NAR' THEN 33 WHEN 'AR' THEN 32 WHEN 'HA' THEN 32
      WHEN 'LT' THEN 30 ELSE NULL
  END AS tract_producer_hel_exception_code,
  CASE TRIM(zm.ZZ0012)
      WHEN 'AR' THEN 52 WHEN 'HA' THEN 52 WHEN 'NAR' THEN 50
      WHEN 'AE' THEN 50 WHEN 'GF' THEN 51 ELSE NULL
  END AS tract_producer_pcw_exception_code,
  CASE TRIM(zm.ZZ0017)
      WHEN 'WR' THEN 43 WHEN 'GF' THEN 42 WHEN 'NAR' THEN 41
      WHEN 'AR' THEN 40 WHEN 'NP' THEN 45 WHEN 'TP' THEN 44 ELSE NULL
  END AS tract_producer_rma_cw_exception_code,
  CASE TRIM(zm.ZZ0016)
      WHEN 'GF' THEN 31 WHEN 'EH' THEN 34 WHEN 'NAR' THEN 33
      WHEN 'AR' THEN 32 WHEN 'LT' THEN 30 ELSE NULL
  END AS tract_producer_rma_hel_exception_code,
  CASE TRIM(zm.ZZ0018)
      WHEN 'AR' THEN 52 WHEN 'HA' THEN 52 WHEN 'NAR' THEN 50
      WHEN 'AE' THEN 50 WHEN 'GF' THEN 51 ELSE NULL
  END AS tract_producer_rma_pcw_exception_code,
  CASE WHEN zm.ZZ0013 IS NOT NULL THEN TO_DATE(CAST(zm.ZZ0013 AS STRING), 'yyyyMMddHHmmss') ELSE NULL END AS hel_appeals_exhausted_date,
  CASE WHEN zm.ZZ0014 IS NOT NULL THEN TO_DATE(CAST(zm.ZZ0014 AS STRING), 'yyyyMMddHHmmss') ELSE NULL END AS cw_appeals_exhausted_date,
  CASE WHEN zm.ZZ0015 IS NOT NULL THEN TO_DATE(CAST(zm.ZZ0015 AS STRING), 'yyyyMMddHHmmss') ELSE NULL END AS pcw_appeals_exhausted_date,
  CASE WHEN zm.VALID_FROM IS NOT NULL AND zm.VALID_FROM <> 0 THEN TO_DATE(CAST(zm.VALID_FROM AS STRING), 'yyyyMMddHHmmss') ELSE NULL END AS producer_involvement_start_date,
  CASE WHEN zm.VALID_TO IS NOT NULL AND zm.VALID_TO <> 0 THEN TO_DATE(CAST(zm.VALID_TO AS STRING), 'yyyyMMddHHmmss') ELSE NULL END AS producer_involvement_end_date
FROM guid_map gm
JOIN z_ibase_comp_detail zd
  ON CAST(zd.instance AS STRING) = gm.instance
 AND CAST(zd.ibase AS STRING) = gm.f_ibase
JOIN comm_pr_frg_rel cf
  ON cf.product_guid = zd.prod_objnr
JOIN zmi_farm_partn zm
  ON zm.frg_guid = cf.fragment_guid
"""

out = spark.sql(sql)
out_fqn = f"glue_catalog.{TGT_DB}.{OUT_TBL}"
out.limit(0).write.format("iceberg").mode("ignore").saveAsTable(out_fqn)
out.write.format("iceberg").mode("overwrite").saveAsTable(out_fqn)

job.commit()
