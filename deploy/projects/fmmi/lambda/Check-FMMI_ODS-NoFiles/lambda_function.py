import os
import boto3
from datetime import datetime

s3 = boto3.client("s3")

BUCKET = os.environ["LANDING_BUCKET"]
PREFIX = os.environ.get("LANDING_PREFIX", "fmmi/fmmi_ocfo_files")

def handler(event, context):
    date_str = datetime.now().strftime("%Y%m%d")
    key = f"{PREFIX}/{date_str}/_NO_FILES"

    resp = s3.list_objects_v2(
        Bucket=BUCKET,
        Prefix=key,
        MaxKeys=1
    )

    return {
        "no_files": "Contents" in resp,
        "checked_key": key
    }
