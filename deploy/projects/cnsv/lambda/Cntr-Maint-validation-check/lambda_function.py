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
        
        #print(database_name) 
        
        # Get the list of tables in the database
        tables_response = glue.get_tables(DatabaseName = database_name)
        # Extract the table names and UpdateTimes
        table_list = tables_response['TableList']
        tables_and_update_times = [{'Name': table['Name'], 'UpdateTime': table['UpdateTime'].isoformat()} for table in table_list if not table['Name'].startswith("v_")]
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

    # Call the monitor_crawler function with the specified crawler name
    crawler_name = CRAWLER_NAME
    crawler_data = monitor_crawler(crawler_name)
    ## Return the crawler_data as the response
    crawler_status = crawler_data['Status']
    
    crawler_table_names = sorted([tables['Name'] for tables in crawler_data['Tables']])
    

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
    
    # Source counts from data pipeline operation table from landing zone
    data_ppln_oper_select_source_sql = f"""
        select substring(data_obj_nm,(length(data_obj_nm) - position('/' in reverse(data_obj_nm)) + 2),
        (length(data_obj_nm) - position('_' in reverse(data_obj_nm)) + 1) - (length(data_obj_nm) - position('/' in reverse(data_obj_nm)) + 2))
         as table_name 
        ,rcd_ct record_count from dart_process_control.data_ppln_oper
        where data_eng_oper_nm = 'Lambda' and  data_obj_type_nm = 'Parquet' and
        data_ppln_job_id  = {data_ppln_job_id} and data_ppln_oper_stat_nm = 'Complete'
        """
    print(data_ppln_oper_select_source_sql) #remove comment for julia testing 
    cursor.execute(data_ppln_oper_select_source_sql)
    
    rows = cursor.fetchall()

    column_name = [desc[0] for desc in cursor.description]
    
    source_counts = []
    for row in rows:
        row_dict = {column_name[i]: row[i] for i in range(len(column_name))}
        source_counts.append(row_dict)

    ### Target counts from data pipeline operation table from Cleansed zone
    data_ppln_oper_select_target_sql = f"""
        select
            case
                -- When path ends with -LOAD/, remove it
                when right(rtrim(data_obj_nm, '/'), 5) = '-LOAD' then
                    substring(
                        rtrim(data_obj_nm, '/'),
                        length(rtrim(data_obj_nm, '/')) - position('/' in reverse(rtrim(data_obj_nm, '/'))) + 2,
                        position('/' in reverse(rtrim(data_obj_nm, '/'))) - 1 - 5
                    )
                -- When path has .parquet extension (original format)
                when position('.' in data_obj_nm) > 0 then
                    substring(
                        data_obj_nm,
                        length(data_obj_nm) - position('/' in reverse(data_obj_nm)) + 2,
                        position('.' in data_obj_nm) - (length(data_obj_nm) - position('/' in reverse(data_obj_nm)) + 2)
                    )
                -- When path ends with / but no -LOAD
                else
                    substring(
                        rtrim(data_obj_nm, '/'),
                        length(rtrim(data_obj_nm, '/')) - position('/' in reverse(rtrim(data_obj_nm, '/'))) + 2,
                        position('/' in reverse(rtrim(data_obj_nm, '/'))) - 1
                    )
            end as table_name,
            rcd_ct as record_count
        from dart_process_control.data_ppln_oper
        where data_eng_oper_nm = 'Glue'
            and data_ppln_job_id = {data_ppln_job_id}
            and data_ppln_oper_stat_nm = 'Complete'
        """

    print(f"data_ppln_oper_select_target_sql: {data_ppln_oper_select_target_sql}")
    cursor.execute(data_ppln_oper_select_target_sql)
    
    rows = cursor.fetchall()
    print(rows)
    
    column_name = [desc[0] for desc in cursor.description]
    
    target_counts = []
    for row in rows:
        row_dict = {column_name[i]: row[i] for i in range(len(column_name))}
        target_counts.append(row_dict)    
    

    source_tables_names = sorted([tables['table_name'] for tables in target_counts])
    
    ### validating Source Vs Target
    output = {}
    
    sorted_source_count = sorted(source_counts, key = lambda x: x['table_name'])
    sorted_target_count = sorted(target_counts, key = lambda x: x['table_name'])
    
    if source_tables_names == crawler_table_names:
        print("The Tables are matching")
        for dict1, dict2 in zip(sorted_source_count,sorted_target_count ):
            source_table_name = dict1['table_name']
            source_record_count = dict1['record_count']
            target_table_name = dict2['table_name']
            target_record_count = dict2['record_count']
            
            if source_table_name == target_table_name:
                if source_record_count == target_record_count:
                    # print(f"{table_name} has count matched")
                    output[source_table_name] = f"Source_Count- {source_record_count} |Target_Count- {target_record_count} | Matched"
                
                elif source_record_count != target_record_count:
                    # print(f"{table_name} has count not matched")
                    output[source_table_name] = f"Source_Count- {source_record_count} |Target_Count- {target_record_count} |Not_Matched"
            else:
                pass
    else:
        print("The Tables are not matching")
        diff_tables = set(crawler_table_names) - set(source_tables_names)
        print("missed tables by crawler are -", diff_tables)
        output = {"missed tables by crawler are": list(diff_tables)}

    conn.close()
    return output

