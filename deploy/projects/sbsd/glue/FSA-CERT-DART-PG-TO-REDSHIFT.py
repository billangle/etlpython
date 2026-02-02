"""
Glue Job: FSA-DART-PG-TO-REDSHIFT
Purpose: Move data from PostgreSQL to Redshift 

Parameters (Required):
    --table_name:       Table name to process
    --source_schema:    Source PostgreSQL schema name
    --target_schema:    Target Redshift schema name
    --env:              Environment (dev, cert, prod)
    --run_type:         'initial' or 'incremental'
    
Parameters (Optional):
    --start_date:       Start date for incremental load (YYYY-MM-DD) - required for incremental
    --data_src_nm:      Application name for logging (cars, cnsv, cvs)

Load Behavior:
    - initial:     Truncates target Redshift table, loads ALL data from PostgreSQL
    - incremental: No truncate, loads only records where load_dt >= start_date
"""

import sys
import json
import boto3
from datetime import datetime
import traceback
from urllib.parse import urlparse

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql.functions import col
from py4j.java_gateway import java_import


#CONFIGURATION

#Column used for incremental filtering
INCREMENTAL_DATE_COLUMN = "data_eff_strt_dt"


def get_environment_config(env: str) -> dict:
    """Get environment-specific configuration for Redshift connection."""
    env_lower = env.lower()
    env_upper = env.upper()
    
    configs = {
        "dev": {
            "rs_catalog_connection": f"FSA-{env_upper}-redshift-conn",
            "arn_role": "arn:aws:iam::241533156429:role/FSA_Redshift_Role",
            "secret_name": f"FSA-{env_upper}-secrets",
            "jdbc_url": "jdbc:redshift://disc-fsa-dev-redshift.ckjzj4bsjear.us-east-1.redshift.amazonaws.com:8200/redshift_db",
            "redshift_tmp_bucket": f"c108-{env_lower}-fpacfsa-final-zone",
        },
        "cert": {
            "rs_catalog_connection": f"FSA-{env_upper}-redshift-conn",
            "arn_role": "arn:aws:iam::241533156429:role/FSA_Redshift_Role",
            "secret_name": f"FSA-{env_upper}-Secrets",
            "jdbc_url": "jdbc:redshift://disc-fsa-cert-redshift.ckjzj4bsjear.us-east-1.redshift.amazonaws.com:5439/redshift_db",
            "redshift_tmp_bucket": f"c108-{env_lower}-fpacfsa-final-zone",
        },
        "prod": {
            "rs_catalog_connection": f"FSA-{env_upper}-redshift-conn",
            "arn_role": "arn:aws:iam::253490756794:role/FSA_Redshift_Role",
            "secret_name": f"FSA-{env_upper}-secrets",
            "jdbc_url": "jdbc:redshift://disc-fsa-prod-redshift.co7jv5kzm7ac.us-east-1.redshift.amazonaws.com:5439/redshift_db",
            "redshift_tmp_bucket": f"c108-{env_lower}-fpacfsa-final-zone",
        },
    }
    
    if env_lower not in configs:
        raise ValueError(f"Invalid environment: {env}. Must be one of: {list(configs.keys())}")
    
    return configs[env_lower]


# DATABASE CONNECTIONS

class PostgresConnection:
    """PostgreSQL database connection for reading source data."""
    
    def __init__(self, spark_context, glue_context, env: str, database: str = "EDV"):
        self.sc = spark_context
        self.glue_context = glue_context
        self.database = database
        self.env = env.upper()
        
        #Glue connection name
        self.glue_connection = f"FSA-{self.env}-PG-DART114"
        
        #Get connection properties
        self.jdbc_conf = glue_context.extract_jdbc_conf(self.glue_connection)
        self.connection_properties = self._get_connection_properties()
        
        #For Spark DataFrame reads
        self.jdbc_url = self.connection_properties["jdbc_url"]
        self.spark_properties = {
            "user": self.connection_properties["user"],
            "password": self.connection_properties["password"],
            "driver": "org.postgresql.Driver",
        }
    
    def _get_connection_properties(self) -> dict:
        """Parse connection properties from Glue connection."""
        url = self.jdbc_conf.get("fullUrl", "").removeprefix("jdbc:")
        jdbc_url = f"jdbc:{url.removesuffix(urlparse(url).path)}/{self.database}"
        
        return {
            "jdbc_url": jdbc_url,
            "user": self.jdbc_conf.get("user"),
            "password": self.jdbc_conf.get("password"),
        }
    
    def read_table(self, spark, schema: str, table: str):
        """Read entire table from PostgreSQL into a Spark DataFrame."""
        full_table = f"{schema}.{table}"
        print(f"Reading from PostgreSQL: {full_table}")
        
        df = spark.read \
            .format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("dbtable", full_table) \
            .option("user", self.spark_properties["user"]) \
            .option("password", self.spark_properties["password"]) \
            .option("driver", self.spark_properties["driver"]) \
            .option("fetchsize", "10000") \
            .load()
        
        return df
    
    def read_table_incremental(self, spark, schema: str, table: str, 
                                date_column: str, start_date: str):
        """Read table from PostgreSQL with date filter for incremental load."""
        query = f"""
            (SELECT * FROM {schema}.{table} 
             WHERE {date_column} >= '{start_date}'::date) as incremental_query
        """
        print(f"Reading from PostgreSQL (incremental): {schema}.{table}")
        print(f"Filter: {date_column} >= '{start_date}'")
        
        df = spark.read \
            .format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("dbtable", query) \
            .option("user", self.spark_properties["user"]) \
            .option("password", self.spark_properties["password"]) \
            .option("driver", self.spark_properties["driver"]) \
            .option("fetchsize", "10000") \
            .load()
        
        return df

class RedshiftConnection:
    """Redshift database connection for writing data."""
    
    def __init__(self, glue_context, env: str):
        self.glue_context = glue_context
        self.env = env.lower()
        self.config = get_environment_config(env)
        
        #Get credentials from Secrets Manager
        self.credentials = self._get_credentials()
        
        self.catalog_connection = self.config["rs_catalog_connection"]
        self.arn_role = self.config["arn_role"]
        self.tmp_bucket = self.config["redshift_tmp_bucket"]
    
    def _get_credentials(self) -> dict:
        secrets_client = boto3.client('secretsmanager')
        response = secrets_client.get_secret_value(SecretId=self.config["secret_name"])
        secret = json.loads(response['SecretString'])
        
        return {
            "user": secret['user_db_redshift'],
            "password": secret['pass_db_redshift'],
        }
    
    def write_table(self, dynamic_frame, schema: str, table: str, truncate: bool = False):
        """Write a DynamicFrame to Redshift."""
        full_table = f"{schema}.{table}"
        print(f"Writing to Redshift: {full_table}")
        print(f"Truncate before load: {truncate}")
        
        #Generate temp path for Redshift COPY command
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        tmp_path = f"s3://{self.tmp_bucket}/_redshift_tmp/{schema}/{table}/{timestamp}/"
        
        conn_options = {
            "dbtable": full_table,
            "database": "redshift_db",
            "aws_iam_role": self.arn_role,
        }
        
        #Add truncate preaction if requested (for initial load)
        if truncate:
            conn_options["preactions"] = f"TRUNCATE TABLE {full_table};"
        
        #Write to Redshift
        self.glue_context.write_dynamic_frame.from_jdbc_conf(
            frame=dynamic_frame,
            catalog_connection=self.catalog_connection,
            connection_options=conn_options,
            redshift_tmp_dir=tmp_path
        )
        
        print(f"Write complete to {full_table}")


# =============================================================================
# PROCESS CONTROL LOGGING
# =============================================================================

class ProcessControlLogger:
    """Log operations to dart_process_control."""
    
    def __init__(self, spark_context, glue_context, env: str):
        self.sc = spark_context
        self.env = env.upper()
        
        #Import Java JDBC
        java_import(spark_context._gateway.jvm, "java.sql.DriverManager")
        
        #Get connection
        glue_connection = f"FSA-{self.env}-PG-DART114"
        jdbc_conf = glue_context.extract_jdbc_conf(glue_connection)
        
        self.conn = spark_context._gateway.jvm.DriverManager.getConnection(
            jdbc_conf.get("url") + "/EDV",
            jdbc_conf.get("user"),
            jdbc_conf.get("password")
        )
        self.conn.setAutoCommit(False)
        print("Process control DB connected")
    
    def log_operation(self, table_name: str, layer: str, system_date: str,
                      start_time: datetime, rows_affected: int, job_run_id: str,
                      status: str, error_message: str = None):
        """Log operation to dart_process_control.data_ppln_oper."""
        try:
            end_time = datetime.utcnow()
            load_oper_tgt_id = self._get_load_oper_tgt_id(table_name)
            
            if not load_oper_tgt_id:
                print("load_oper_tgt_id not found, skipping process control log")
                return
            
            query = """
                INSERT INTO dart_process_control.data_ppln_oper (
                    load_oper_tgt_id, oper_strt_dt, oper_end_dt,
                    data_obj_type_nm, data_obj_nm, rcd_ct,
                    data_obj_proc_rtn_nm, oper_stat_cd, err_msg_txt,
                    aws_job_id, oper_dt, crt_dt, last_chg_dt, data_stat_cd
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'A')
            """
            
            pstmt = self.conn.prepareStatement(query)
            pstmt.setInt(1, load_oper_tgt_id)
            pstmt.setTimestamp(2, self._to_sql_timestamp(start_time))
            pstmt.setTimestamp(3, self._to_sql_timestamp(end_time))
            pstmt.setString(4, f"{layer} Table")
            pstmt.setString(5, table_name)
            pstmt.setInt(6, rows_affected)
            pstmt.setString(7, "FSA-DART-PG-TO-REDSHIFT")
            pstmt.setInt(8, 1 if status == "SUCCESS" else -1)
            pstmt.setString(9, error_message[:1000] if error_message else None)
            pstmt.setString(10, job_run_id)
            pstmt.setDate(11, self._to_sql_date(system_date))
            pstmt.setTimestamp(12, self._to_sql_timestamp(end_time))
            pstmt.setTimestamp(13, self._to_sql_timestamp(end_time))
            
            pstmt.executeUpdate()
            self.conn.commit()
            pstmt.close()
            
            print("Logged to process control")
            
        except Exception as e:
            print(f"Could not log to process control: {e}")
            try:
                self.conn.rollback()
            except:
                pass
    
    def _get_load_oper_tgt_id(self, table_name: str):
        """Get load_oper_tgt_id from dart_process_control."""
        try:
            query = """
                SELECT load_oper_tgt_id
                FROM dart_process_control.load_oper_tgt
                WHERE LOWER(db_tbl_nm) = LOWER(?)
                  AND data_stat_cd = 'A'
                LIMIT 1
            """
            pstmt = self.conn.prepareStatement(query)
            pstmt.setString(1, table_name)
            results = pstmt.executeQuery()
            
            if results.next():
                return results.getInt("load_oper_tgt_id")
            return None
        except:
            return None
    
    def _to_sql_timestamp(self, dt: datetime):
        """Convert Python datetime to Java SQL Timestamp."""
        return self.sc._gateway.jvm.java.sql.Timestamp(int(dt.timestamp() * 1000))
    
    def _to_sql_date(self, date_str: str):
        """Convert date string to Java SQL Date."""
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return self.sc._gateway.jvm.java.sql.Date(int(dt.timestamp() * 1000))
    
    def close(self):
        """Close the database connection."""
        try:
            self.conn.close()
        except:
            pass


# UTILITY FUNCTIONS

def debug_df(df, label: str):
    """Print schema and sample rows for debugging."""
    print(f"\n--- {label} ---")
    print("Schema:")
    df.printSchema()
    print(f"Row count: {df.count()}")
    print("Sample rows:")
    df.show(5, truncate=False)
    print(f"Partition count: {df.rdd.getNumPartitions()}")


# MAIN

def main():
    #Parse required arguments
    args = getResolvedOptions(sys.argv, [
        "JOB_NAME",
        "table_name",
        "source_schema",
        "target_schema",
        "env",
        "run_type"
    ])
    
    #Parse optional arguments with defaults
    optional_params = {
        "start_date": datetime.utcnow().strftime("%Y-%m-%d"),
        "data_src_nm": "",
    }
    
    for param in optional_params.keys():
        try:
            param_args = getResolvedOptions(sys.argv, [param])
            optional_params[param] = param_args[param]
        except:
            pass
    
    #Extract parameters
    table_name = args["table_name"]
    source_schema = args["source_schema"]
    target_schema = args["target_schema"]
    env = args["env"].lower()
    run_type = args["run_type"].lower()
    start_date = optional_params["start_date"]
    data_src_nm = optional_params["data_src_nm"]
    
    #Validate run_type
    if run_type not in ["initial", "incremental"]:
        raise ValueError(f"Invalid run_type: {run_type}. Must be 'initial' or 'incremental'")
    
    #For incremental, start_date is required
    if run_type == "incremental" and not start_date:
        raise ValueError("start_date is required for incremental run_type")
    
    job_run_id = args.get("JOB_RUN_ID", datetime.utcnow().strftime("%Y%m%d%H%M%S"))
    
    #Initialize Spark/Glue
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)
    
    #Determine behavior based on run_type
    truncate_target = (run_type == "initial")
    
    print("=" * 70)
    print("FSA-DART-PG-TO-REDSHIFT")
    print("=" * 70)
    print(f"  Table:          {table_name}")
    print(f"  Source Schema:  {source_schema} (PostgreSQL)")
    print(f"  Target Schema:  {target_schema} (Redshift)")
    print(f"  Environment:    {env}")
    print(f"  Run Type:       {run_type}")
    print(f"  Start Date:     {start_date}")
    print(f"  Data Source:    {data_src_nm}")
    print(f"  Truncate:       {truncate_target}")
    if run_type == "incremental":
        print(f"  Filter Column:  {INCREMENTAL_DATE_COLUMN}")
        print(f"  Filter:         {INCREMENTAL_DATE_COLUMN} >= '{start_date}'")
    print("=" * 70)
    
    process_start_time = datetime.utcnow()
    total_rows = 0
    pc_logger = None
    
    try:
        #Initialize connections
        pg_conn = PostgresConnection(
            spark_context=sc,
            glue_context=glue_context,
            env=env,
            database="EDV"
        )
        print("PostgreSQL connection established")
        
        rs_conn = RedshiftConnection(
            glue_context=glue_context,
            env=env
        )
        print("Redshift connection established")
        
        #Initialize process control logger (optional)
        try:
            pc_logger = ProcessControlLogger(
                spark_context=sc,
                glue_context=glue_context,
                env=env
            )
        except Exception as e:
            print(f"Process control logger not available: {e}")
            pc_logger = None
        
        #Read data from PostgreSQL based on run_type
        if run_type == "initial":
            # Initial: Read all data
            print(f"\nINITIAL LOAD: Reading ALL data from {source_schema}.{table_name}")
            source_df = pg_conn.read_table(spark, source_schema, table_name)
        else:
            # Incremental: Read filtered data
            print(f"\nINCREMENTAL LOAD: Reading data where {INCREMENTAL_DATE_COLUMN} >= '{start_date}'")
            source_df = pg_conn.read_table_incremental(
                spark=spark,
                schema=source_schema,
                table=table_name,
                date_column=INCREMENTAL_DATE_COLUMN,
                start_date=start_date
            )
        
        #Cache for performance
        source_df.cache()
        total_rows = source_df.count()
        print(f"Read {total_rows} rows from PostgreSQL")
        
        if total_rows == 0:
            print("No data to transfer")
        else:
            #Debug output
            debug_df(source_df, f"Source: {source_schema}.{table_name}")
            
            #Convert to DynamicFrame for Glue write
            dynamic_frame = DynamicFrame.fromDF(source_df, glue_context, "source_df")
            
            #Write to Redshift
            print(f"\nWriting to Redshift: {target_schema}.{table_name}")
            rs_conn.write_table(
                dynamic_frame=dynamic_frame,
                schema=target_schema,
                table=table_name,
                truncate=truncate_target
            )
        
        #Unpersist cached DataFrame
        source_df.unpersist()
        
        #Log success
        if pc_logger:
            pc_logger.log_operation(
                table_name=table_name,
                layer="REDSHIFT",
                system_date=start_date,
                start_time=process_start_time,
                rows_affected=total_rows,
                job_run_id=job_run_id,
                status="SUCCESS"
            )
        
        duration = (datetime.utcnow() - process_start_time).total_seconds()
        
        print(f"\n{'=' * 70}")
        print(f"SUCCESS: {source_schema}.{table_name} -> {target_schema}.{table_name}")
        print(f"Run Type: {run_type.upper()}")
        print(f"Rows Transferred: {total_rows}")
        print(f"Duration: {duration:.2f}s")
        print("=" * 70)
        
    except Exception as e:
        error_message = str(e)
        error_tb = traceback.format_exc()
        
        print(f"\n{'=' * 70}")
        print(f"FAILED: {table_name}")
        print(f"Error: {error_message}")
        print(f"Traceback:\n{error_tb}")
        print("=" * 70)
        
        #Log failure
        if pc_logger:
            try:
                pc_logger.log_operation(
                    table_name=table_name,
                    layer="REDSHIFT",
                    system_date=start_date,
                    start_time=process_start_time,
                    rows_affected=0,
                    job_run_id=job_run_id,
                    status="FAILED",
                    error_message=error_message
                )
            except:
                pass
        
        raise e
    
    finally:
        if pc_logger:
            pc_logger.close()
        job.commit()


if __name__ == "__main__":
    main()