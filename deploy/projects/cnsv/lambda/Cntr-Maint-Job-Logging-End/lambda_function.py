import json
import boto3
import os
import psycopg2
import subprocess
import logging
from datetime import datetime

SECRET_NAME = os.environ["SecretId"]
REGION = os.getenv("AWS_REGION")
CRAWLER_NAME = os.environ["CRAWLER_NAME"]

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)
    
def monitor_crawler(crawler_name):
    glue = boto3.client('glue')
    try:
        response = glue.get_crawler(Name=crawler_name)

        crawler_status = response['Crawler']['LastCrawl']['Status']
        crawler_start_time = response['Crawler']['LastCrawl']['StartTime'].isoformat()
        database_name = response['Crawler']['DatabaseName']
        
        # Get the list of tables in the database
        tables_response = glue.get_tables(DatabaseName=database_name)
        # Extract the table names and UpdateTimes
        table_list = tables_response['TableList']
        tables_and_update_times = [{'Name': table['Name'], 'UpdateTime': table['UpdateTime'].isoformat()} for table in table_list]

        extracted_data = {
            'Status': crawler_status,
            'StartTime': crawler_start_time,
            'DatabaseName': database_name,
            'Tables': tables_and_update_times
        }

        # Print the extracted data
        print(json.dumps(extracted_data, indent=2))
        # Return the extracted data
        return extracted_data
    
    except glue.exceptions.EntityNotFoundException:
        print(f"Crawler {crawler_name} not found.")
        return {"error": "Crawler not found"}
    
    except Exception as e:
        print(f"Error: {str(e)}")
        return {"error": str(e)}
    
def handler(event, context):
    print("Lambda function started.")
    
    # Call the monitor_crawler function with the specified crawler name
    crawler_name = CRAWLER_NAME
    print(f"Monitoring crawler: {crawler_name}")
    crawler_data = monitor_crawler(crawler_name)
    print("Crawler data retrieved.")
    ## Return the crawler_data as the response
    crawler_status = crawler_data['Status']
    print(f"Crawler status: {crawler_status}")
    print('crawler_status -', crawler_status)
        
    if crawler_status:
            print("Crawler status is not None.") # Debugging print statement
            # get secrets
            secrets = boto3.client("secretsmanager", region_name=REGION)
            print("SecretsManager client created.") # Debugging print statement
            secret_value = secrets.get_secret_value(SecretId=SECRET_NAME)
            print("Secret value retrieved.") # Debugging print statement
            secret = json.loads(secret_value["SecretString"])
            
            # configure db connection creds
            db_host = secret["edv_postgres_hostname"]
            db_port = secret["postgres_port"]
            db_name = secret["postgres_prcs_ctrl_dbname"]
            db_user = secret["edv_postgres_username"]
            db_password = secret["edv_postgres_password"]
            
            conn = psycopg2.connect(host=db_host, port=db_port, user=db_user, password=db_password, dbname=db_name, sslmode="require")
            print("Database connection established.") # Debugging print statement
            
            conn.autocommit=True
            cursor = conn.cursor()
        
            data_ppln_job_id = event["JobId"]
            
            # get the status from previous step in data pipeline job table
            data_ppln_job_select_sql = f"""
                select job_stat_nm, last_cplt_data_load_tgt_nm from dart_process_control.data_ppln_job
                where data_ppln_job_id  = {data_ppln_job_id}
                """
            print("Executing SQL query to select job status.") # Debugging print statement
            cursor.execute(data_ppln_job_select_sql)
            
            rows = cursor.fetchall()
            print(f"Fetched {len(rows)} rows from the database.") # Debugging print statement
            
            for row in rows:
                job_stat_nm = row[0]
                last_cplt_data_load_tgt_nm = row[1]                                        
        
            
            if job_stat_nm == 'In Progress' and last_cplt_data_load_tgt_nm == 'ccms_ingest' and crawler_status == 'SUCCEEDED':
                job_stat_nm = 'Complete'
                err_msg_txt = ''
                last_cplt_data_load_tgt_nm = 'Glue Data Catalog tables'
                
            else:
                job_stat_nm = 'Failed'
                err_msg_txt = 'Crawler Failed'
                last_cplt_data_load_tgt_nm = 'Glue Data Catalog tables'
                
            
            # update data pipeline job inacase everything was fine
            data_ppln_job_update_on_complete_sql = f"""
                update dart_process_control.data_ppln_job
                set job_end_dt = %s,
                    last_cplt_data_load_tgt_nm = %s,
                    job_stat_nm = %s,err_msg_txt = %s
                where data_ppln_job_id = %s
                """
            print("Executing SQL query to update job status.") # Debugging print statement
                
            cursor.execute(data_ppln_job_update_on_complete_sql, (datetime.utcnow(), 
                                                             last_cplt_data_load_tgt_nm,
                                                                job_stat_nm,
                                                                err_msg_txt,
                                                                data_ppln_job_id))
        
    else:
            print("Crawler status is None.") # Debugging print statement
            pass
    
    print("Lambda function execution completed.")

    return {"JobId": data_ppln_job_id}
    