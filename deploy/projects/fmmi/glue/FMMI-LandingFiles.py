# source:  Echo Server
# destination:  S3 (c108-dev-fpacfsa-landing-zone/fmmi/fmmi_ocfo_files)
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

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    return args, job

# ---------------------------------------------------------
# INIT AWS CLIENTS
# ---------------------------------------------------------
def init_aws_clients(secret_name, region):
    s3 = boto3.client("s3", region_name=region)
    secrets = boto3.client("secretsmanager", region_name=region)

    secret_value = secrets.get_secret_value(SecretId=secret_name)
    secret = json.loads(secret_value["SecretString"])

    return s3, secret

# ---------------------------------------------------------
# LIST FILES ON FTP
# ---------------------------------------------------------
def list_ftp_files_with_curl(ftp_details, folder, pattern):
    ftp_host, ftp_path, ftp_username, ftp_password = ftp_details

    command = (
        f"curl -v --ftp-ssl -k --max-time 180 "
        f"-u {ftp_username}:{ftp_password} "
        f"ftps://{ftp_host}/{ftp_path}/{folder}/in/"
    )

    try:
        result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        output = result.stdout.decode("utf-8")
        logger.info("FTP LIST successful.")
    except subprocess.CalledProcessError as e:
        logger.error(f"FTP LIST failed: {e.stderr.decode('utf-8')}")
        return []

    file_names = []
    for line in output.splitlines():
        parts = line.split()
        if len(parts) > 0:
            file_name = parts[-1]
            if re.match(pattern.replace("*", ".*"), file_name):
                file_names.append(file_name)

    return file_names

# ---------------------------------------------------------
# DOWNLOAD FROM FTP → UPLOAD TO S3
# ---------------------------------------------------------
def download_and_upload_file(s3, ftp_details, folder, file_name, destination_bucket, destination_prefix):
    ftp_host, ftp_path, ftp_username, ftp_password = ftp_details

    # -----------------------------------------------------
    # DATE FOLDER + DATE SUFFIX
    # -----------------------------------------------------
    date_str = datetime.now().strftime("%Y%m%d")

    # Create landing folder structure:
    # fmmi/fmmi_ocfo_files/<YYYYMMDD>/
    s3_folder_path = f"{destination_prefix}/{date_str}/"

    # Add date suffix to filename
    if file_name.lower().endswith(".csv"):
        base = file_name[:-4]
        new_name = f"{base}_{date_str}.csv"
    else:
        new_name = f"{file_name}_{date_str}"

    # Final S3 key
    s3_file_key = f"{s3_folder_path}{new_name}"

    logger.info(f"Uploading to S3: bucket={destination_bucket}, key={s3_file_key}")

    # -----------------------------------------------------
    # DOWNLOAD FROM FTP
    # -----------------------------------------------------
    local_file_path = f"/tmp/{file_name}"

    download_cmd = (
        f"curl -v --ftp-ssl -k "
        f"-u {ftp_username}:{ftp_password} "
        f"-o {local_file_path} "
        f"ftps://{ftp_host}/{ftp_path}/{folder}/in/{file_name}"
    )

    try:
        subprocess.run(download_cmd, shell=True, check=True)
        logger.info(f"Downloaded {file_name} from FTP.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Download failed for {file_name}: {e}")
        return

    # -----------------------------------------------------
    # UPLOAD TO S3
    # -----------------------------------------------------
    try:
        transfer_config = TransferConfig(
            multipart_threshold=128 * 1024 * 1024,
            multipart_chunksize=128 * 1024 * 1024,
            max_concurrency=10,
            use_threads=True
        )

        s3.upload_file(local_file_path, destination_bucket, s3_file_key, Config=transfer_config)
        logger.info(f"Uploaded to S3: {s3_file_key}")

    except Exception as e:
        logger.error(f"S3 upload failed: {e}")
    finally:
        if os.path.exists(local_file_path):
            os.remove(local_file_path)

    # -----------------------------------------------------
    # DELETE FILE FROM FTP
    # -----------------------------------------------------
    delete_cmd = (
        f"curl -v --ftp-ssl -k "
        f"-u {ftp_username}:{ftp_password} "
        f"-Q \"CWD {ftp_path}/{folder}/in\" "
        f"-Q \"DELE {file_name}\" "
        f"ftps://{ftp_host}"
    )

    try:
        subprocess.run(delete_cmd, shell=True, check=True)
        logger.info(f"Deleted file on Echo FTP: {file_name}")
    except Exception as e:
        logger.error(f"Failed to delete file on FTP: {e}")

# ---------------------------------------------------------
# PROCESS FILES
# ---------------------------------------------------------
def process_files(s3, ftp_details, folder, file_patterns, destination_bucket, destination_prefix):
    for pattern in file_patterns:
        matching_files = list_ftp_files_with_curl(ftp_details, folder, pattern)

        if not matching_files:
            logger.info(f"No files found for pattern: {pattern}")
            continue

        for file_name in matching_files:
            download_and_upload_file(
                s3, ftp_details, folder, file_name,
                destination_bucket, destination_prefix
            )

# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------
def main():
    args, job = init_glue_job()

    s3, secret = init_aws_clients(args["SecretId"], "us-east-1")

    ftp_details = (
        secret["echo_ip"],
        secret["echo_dart_path"],
        secret["echo_dart_username"],
        secret["echo_dart_password"]
    )

    process_files(
        s3,
        ftp_details,
        args["EchoFolder"],
        args["FileNames"].split(","),
        args["DestinationBucket"],
        args["DestinationPrefix"]
    )

    job.commit()

if __name__ == "__main__":
    main()
