####################################################################################################### 
# Developer Name: Mahender Vulupala
# Date: 01/26/2026
# Script Name: FSA-()FMMI-LandingFiles
#    - Transfers OCFO/fmmi files from Echo\S_dart_()/fmmi/in/ & to S3:landing/fmmi/fmmi_ocfo_files
#######################################################################################################

import sys
import subprocess
import json
import boto3
import os
import re
import logging
from boto3.s3.transfer import TransferConfig
from datetime import datetime
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

# ---------------------------------------------------------
# LOGGING
# ---------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("echo_to_s3")

# ---------------------------------------------------------
# INIT GLUE JOB
# ---------------------------------------------------------
def init_glue_job():
    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "SecretId", "DestinationBucket", "DestinationPrefix", "EchoFolder", "FileNames"]
    )
    sc = SparkContext()
    glueContext = GlueContext(sc)
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)
    return args, job

# ---------------------------------------------------------
# INIT AWS CLIENTS
# ---------------------------------------------------------
def init_aws_clients(secret_name, region):
    session = boto3.Session(region_name=region)
    s3 = session.client("s3")
    secrets = session.client("secretsmanager")
    sns = session.client("sns")
    cloudwatch = session.client("cloudwatch")
    secret_value = secrets.get_secret_value(SecretId=secret_name)
    secret = json.loads(secret_value["SecretString"])
    return s3, sns, cloudwatch, secret

# ---------------------------------------------------------
# DELETE ENTIRE FOLDER
# ---------------------------------------------------------
def delete_entire_folder(s3, bucket, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/"):
        if "Contents" in page:
            for obj in page["Contents"]:
                s3.delete_object(Bucket=bucket, Key=obj["Key"])
    logger.info(f"Folder rewritten: s3://{bucket}/{prefix}/")

# ---------------------------------------------------------
# CREATE EMPTY FOLDER
# ---------------------------------------------------------
def create_empty_folder(s3, bucket, prefix):
    s3.put_object(Bucket=bucket, Key=f"{prefix}/")

# ---------------------------------------------------------
# WRITE FLAG FILE
# ---------------------------------------------------------
def write_flag(s3, bucket, prefix, name, content):
    s3.put_object(Bucket=bucket, Key=f"{prefix}/{name}", Body=content.encode())

# ---------------------------------------------------------
# SNS ALERT
# ---------------------------------------------------------
def send_no_files_alert(sns, topic, date_str, bucket, prefix):
    if not topic:
        return
    sns.publish(
        TopicArn=topic,
        Subject=f"No Echo Files for {date_str}",
        Message=f"No files found. Folder: s3://{bucket}/{prefix}/{date_str}/"
    )

# ---------------------------------------------------------
# CLOUDWATCH METRIC
# ---------------------------------------------------------
def metric(cloudwatch, name, value):
    cloudwatch.put_metric_data(
        Namespace="EchoIngestion",
        MetricData=[{"MetricName": name, "Value": value, "Unit": "Count"}]
    )

# ---------------------------------------------------------
# FTP LIST
# ---------------------------------------------------------
def list_ftp_files(ftp, folder, pattern):
    host, path, user, pwd = ftp
    cmd = (
        f"curl -s --ftp-ssl -k -u {user}:{pwd} "
        f"ftps://{host}/{path}/{folder}/in/"
    )
    try:
        result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, check=True)
        output = result.stdout.decode()
    except:
        return []

    regex = re.compile(pattern.replace("*", ".*"))
    files = []
    for line in output.splitlines():
        parts = line.split()
        if parts:
            name = parts[-1]
            if regex.match(name):
                files.append(name)
    return files

# ---------------------------------------------------------
# DOWNLOAD FILE
# ---------------------------------------------------------
def download_file(ftp, folder, file_name):
    host, path, user, pwd = ftp
    local = f"/tmp/{file_name}"
    cmd = (
        f"curl -s --ftp-ssl -k -u {user}:{pwd} "
        f"-o {local} "
        f"ftps://{host}/{path}/{folder}/in/{file_name}"
    )
    try:
        subprocess.run(cmd, shell=True, check=True)
        return local
    except:
        return None

# ---------------------------------------------------------
# DELETE FTP FILE
# ---------------------------------------------------------
def delete_ftp_file(ftp, folder, file_name):
    host, path, user, pwd = ftp
    cmd = (
        f"curl -s --ftp-ssl -k -u {user}:{pwd} "
        f"-Q \"CWD {path}/{folder}/in\" "
        f"-Q \"DELE {file_name}\" "
        f"ftps://{host}"
    )
    subprocess.run(cmd, shell=True)

# ---------------------------------------------------------
# PROCESS FILES (SAFE REWRITE)
# ---------------------------------------------------------
def process_files(s3, sns, cloudwatch, ftp, folder, patterns, bucket, prefix, topic):

    date_str = datetime.now().strftime("%Y%m%d")
    day_prefix = f"{prefix}/{date_str}"

    # STEP 1 — Gather all files first (do NOT delete folder yet)
    all_files = []
    for p in patterns:
        all_files.extend(list_ftp_files(ftp, folder, p))

    if not all_files:
        # No files → rewrite folder safely
        delete_entire_folder(s3, bucket, day_prefix)
        create_empty_folder(s3, bucket, day_prefix)
        write_flag(s3, bucket, day_prefix, "_NO_FILES", "No files today")
        send_no_files_alert(sns, topic, date_str, bucket, prefix)
        metric(cloudwatch, "NoFilesReceived", 1)
        return

    # STEP 2 — Download all files locally first
    local_files = []
    for f in all_files:
        local = download_file(ftp, folder, f)
        if not local:
            logger.error(f"Download failed: {f}")
            return  # DO NOT rewrite folder; keep previous data safe
        local_files.append((f, local))

    # STEP 3 — All downloads succeeded → rewrite folder now
    delete_entire_folder(s3, bucket, day_prefix)
    create_empty_folder(s3, bucket, day_prefix)

    # STEP 4 — Upload files
    for original, local in local_files:
        if original.lower().endswith(".csv"):
            base = original[:-4]
            new_name = f"{base}_{date_str}.csv"
        else:
            new_name = f"{original}_{date_str}"

        key = f"{day_prefix}/{new_name}"
        s3.upload_file(local, bucket, key)
        delete_ftp_file(ftp, folder, original)
        os.remove(local)

    # STEP 5 — SUCCESS FLAG
    write_flag(s3, bucket, day_prefix, "_SUCCESS", f"{len(local_files)} files received")
    metric(cloudwatch, "FilesReceived", len(local_files))

# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------
def main():
    args, job = init_glue_job()
    s3, sns, cloudwatch, secret = init_aws_clients(args["SecretId"], "us-east-1")

    ftp = (
        secret["echo_ip"],
        secret["echo_dart_path"],
        secret["echo_dart_username"],
        secret["echo_dart_password"]
    )

    patterns = [p.strip() for p in args["FileNames"].split(",") if p.strip()]
    topic = secret.get("sns_topic_arn")

    process_files(
        s3, sns, cloudwatch,
        ftp,
        args["EchoFolder"],
        patterns,
        args["DestinationBucket"],
        args["DestinationPrefix"],
        topic
    )

    job.commit()

if __name__ == "__main__":
    main()
