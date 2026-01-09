import os
import json
import boto3
import botocore
import smart_open
import polars as pl
import multiprocessing
import dateutil.tz as tz
from ftplib import all_errors
from datetime import timedelta, datetime
from types import SimpleNamespace
from concurrent.futures import ThreadPoolExecutor, as_completed
import re

from dart.session import DARTSession
from dart.database import DARTDatabase
from ftps_client import iFTP_TLS, FTPSWriter
        

@pl.Config(
    tbl_formatting="ASCII_MARKDOWN",
    tbl_hide_column_data_types=True,
    tbl_hide_dataframe_shape=True,
    fmt_str_lengths=1000,
    tbl_width_chars=1000,
    tbl_rows=-1,
)
def frame_to_markdown(df: pl.DataFrame) -> str:
    return str(df)

def fetch_secret(secret_id, secret_key):
    client = boto3.client("secretsmanager")
    secrets = client.get_secret_value(SecretId=secret_id)
    return json.loads(secrets[secret_key])

def lambda_handler(event, context):
    """Sample Event
    {
	    "file_pattern": "(mo\\.moyr540)\\.data$",            
	    "echo_folder": "plas",                             
	    "pipeline": "FLPIDS_RC540 Monthly",                 
	    "step": "FLPIDS_RC540 Monthly Landing Zone",                                     
	    "header": false                                     
    }
    """
    print("Input event:\n", event)
    env = os.environ["env"]
    
    s3_client = boto3.client("s3")
    meta_client = DARTDatabase.client(
        service="lambda",
        glue_connection=f"FSA-PROD-PG-MDART", 
        database="metadata_edw",
    )
    
    filters = {
        "file_pattern": event.get("file_pattern"),
        "targets": event.get("targets", []),
    }
    header = event.get("header", True)
    to_queue = event.get("to_queue", False)
    print({"header": header,"to_queue": to_queue, "filters": filters})
    
    secret = fetch_secret(
        secret_id=os.environ["SecretId"], 
        secret_key=os.environ["SecretKey"],
    )

    echo = {
        "connection": {
            "host": secret["echo_ip"],
            "port": 990,
            "username": secret["echo_dart_username"],
            "password": secret["echo_dart_password"],
            "log_level": 0,
        },
        "path": "/{root}/{folder}/in/{subfolder}".format(
            root=secret["echo_dart_path"],
            folder=event["echo_folder"],
            subfolder=event.get("echo_subfolder","")
        ),
    }

    
    def list_files():
        with iFTP_TLS(timeout=10) as ftps:
            ftps.make_connection(**echo["connection"])
            ftps.cwd(echo["path"])
            files = ftps.filter_entries(**filters)
            print(frame_to_markdown(files))
            return files.rows(named=True)
    
    def echo_file_transfer(file):
        s3_uri, echo_uri = None, None
        
        with iFTP_TLS(timeout=600) as ftps:
            ftps.make_connection(**echo["connection"])
            ftps.cwd(echo["path"])
            print("Connected to Echo server...")

            if re.match("^\d{14}$", file["name"].split(".")[-1]):
                data_obj_type_nm = file["name"].split(".")[-2].upper()
            elif "." in file["name"]:
                data_obj_type_nm = file["name"].split(".")[-1].upper()
            else:
                data_obj_type_nm = "Flat File"

            print("Entering session...")
            with DARTSession(
                client=meta_client,
                job_id=context.log_stream_name,
                ppln_nm=event["pipeline"],
                step_nm=event["step"],
                tgt_nm=file["target"],
                data_eng_oper_nm="Lambda",
                data_obj_type_nm=data_obj_type_nm,
                data_obj_proc_rtn_nm=context.function_name,
                env=env,
                system_date=file["system_date"],
                to_queue=to_queue,
                override_prev_run=True
            ) as session:
                print("Entered session...")
                file_date_format = "%Y%m%d%H%M%S" if event["echo_folder"] == "plas" and "MFO900" in event["file_pattern"] else "%Y%m%d"
                
                dataset = session.dataset.properties._asdict().copy()
                bucket = dataset["data_bkt_nm"]
                key = "{prefix}/{object}".format(
                    prefix=dataset["data_bkt_pfx_nm"],
                    object=dataset["data_bkt_obj_tmpl_txt"].format(
                        YYYYMMDD=file["system_date"].strftime(file_date_format)
                    )
                )
                
                s3_uri = f"s3://{bucket}/{key}"
                print(f"s3_uri: {s3_uri}")
                # if greater than 500 MB, use Glue
                if ftps.size(file["name"]) > 500000000:
                    print(f"Found larger file and triggering the glue job: FSA-{env}-GLS-LandingFiles")
                    load_oper_tgt_id = session.dataset.properties.tgt_id
                    
                    job_name = f"FSA-{env}-GLS-LandingFiles"
                    arguments = {
                        "--FileName": file["name"],
                        "--load_oper_tgt_id": str(load_oper_tgt_id),
                        "--job_id": context.log_stream_name
                    }

                    client = boto3.client('glue')

                    response = client.start_job_run(
                        JobName=job_name,
                        Arguments=arguments
                    )
                    
                    record_count = -1       # count - 1 if header else count
                    
                else:
                    with smart_open.open(s3_uri, "wb") as f:
                        writer = FTPSWriter(f)
                        if file["name"] in ["TNA_LOANSTATRPT", "TNA_COMMENT"]:
                            total_size = ftps.size(file["name"])
                            print(f"Size of {file['name']}:", total_size)
                            
                        else:
                            ftps.retrbinary("RETR " + file["name"], writer)
                            
                        record_count = writer.count - 1 if header else writer.count
                    
                ftps.delete(file["name"])
                echo_uri = "{}{}".format(echo["path"], file["name"])
                print(f"echo_uri: {echo_uri}")

                session.metadata.data_obj_nm = s3_uri
                session.metadata.rcd_ct = record_count
                session.complete = True
        
        return s3_uri, echo_uri
        
    files = list_files()
    
    if files:
        max_workers = multiprocessing.cpu_count() * 10
        print(f"Max Number of Workers: {max_workers}")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(echo_file_transfer, file)
                for file in files
            ]
            for future in as_completed(futures):
                s3_uri, echo_uri = future.result()
                print(f"S3 and Echo Paths: {s3_uri}, {echo_uri}")
                if s3_uri:
                    print(f"uploaded: {s3_uri}")
                if echo_uri:
                    print(f"deleted: {echo_uri}")

    return {"statusCode": 200, "body": json.dumps("success")}
