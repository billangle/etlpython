
import sys
import pyspark.sql.functions as F
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME",  "clean_bucket", "final_bucket", "project","bucket_region"])
clean_bucket = args["clean_bucket"]
final_bucket = args["final_bucket"]
project = args["project"]
bucket_region = args["bucket_region"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", f"s3.{bucket_region}.amazonaws.com")

input_path = f"s3a://{clean_bucket}/{project}/etl-jobs/"
output_path = f"s3a://{final_bucket}/{project}/"

df = spark.read.option("header", True).csv(input_path)
if df.rdd.isEmpty():
    raise Exception(f"No data found at {input_path}")

df2 = df.withColumn("processed_at_utc", F.current_timestamp())
(df2.write
    .mode("overwrite")
    .parquet(output_path))

job.commit()
