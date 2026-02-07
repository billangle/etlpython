import json
import boto3
import os
import psycopg2
from datetime import datetime

SECRET_NAME = "FSA-CERT-Secrets"
REGION = os.getenv("AWS_REGION")

def handler(event, context):

    # get secrets
    secrets = boto3.client("secretsmanager", region_name=REGION)
    secret_value = secrets.get_secret_value(SecretId=SECRET_NAME)
    secret = json.loads(secret_value["SecretString"])
    
    # configure db connection creds
    db_host = secret["edv_postgres_hostname"]
    db_port = secret["postgres_port"]
    db_name = secret["postgres_prcs_ctrl_dbname"]
    db_user = secret["edv_postgres_username"]
    db_password = secret["edv_postgres_password"]
    
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        user=db_user,
        password=db_password,
        dbname=db_name,
        sslmode="require"
    )
    
    conn.autocommit=True
    cursor = conn.cursor()

    data_ppln_job_id = event["JobId"]
    
    
    if "JobId" in event:# and 'ExecutedVersion' in event["Payload"]:
        job_stat_nm = 'In Progress'
        data_ppln_job_id = event["JobId"]
        err_msg_txt = ''
    else:
        job_stat_nm = 'Failed'
        data_ppln_job_id = event["JobId"]
        err_msg_txt = 'errors occured when executing state machine: FSA-CERT-CNSV-Contr-Maintenance-RAW-DM-Step2' 

    # update data pipeline job
    data_ppln_job_update_on_complete_sql = f"""
        update dart_process_control.data_ppln_job
        set job_end_dt = %s,
            last_cplt_data_load_tgt_nm = %s,
            job_stat_nm = %s,err_msg_txt = %s
        where data_ppln_job_id = %s
        """
        
    cursor.execute(data_ppln_job_update_on_complete_sql, (  datetime.utcnow(), 
                                                            "FSA-CERT-CNSV-CNTR-MAINT-RAW-DM",
                                                            job_stat_nm,err_msg_txt,
                                                            data_ppln_job_id))
                                            
    conn.close()

    return 