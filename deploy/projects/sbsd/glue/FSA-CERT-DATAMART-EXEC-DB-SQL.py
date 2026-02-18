"""
Glue Job: FSA-DART-EXEC-DB-SQL
Purpose: Universal SQL executor for all EDW layers (STG, EDV, EBV, DM)

Parameters (Required):
    --table_name:   Table name to process
    --data_src_nm:  Application name (cars, cnsv, cvs)
    --run_type:     'initial' or 'incremental'
    --start_date:   Start date for processing (YYYY-MM-DD)
    --env:          Environment (dev, cert, prod)
    --layer:        'STG', 'EDV', 'EBV', or 'DM'Reading:

Single day processing: ETL_START_DATE = ETL_END_DATE = start_date
"""

import sys
import re
import json
import boto3
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import List, Dict, Optional
import traceback
from urllib.parse import urlparse

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from py4j.java_gateway import java_import
from py4j.protocol import Py4JJavaError


# =============================================================================
# SQL PARAMETERS
# =============================================================================

class SQLParameters:
    """Generate parameters for SQL template substitution."""

    def __init__(self, start_date_str: str, end_date_str: str = None, env: str = None):
        self.start_date = datetime.strptime(start_date_str, "%Y-%m-%d")

        # End date equals start date (single day processing)
        if end_date_str:
            self.end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        else:
            self.end_date = self.start_date

        self.parameters = {
            # Date range
            "ETL_START_DATE": str(self.start_date.date()),
            "ETL_END_DATE": str(self.end_date.date()),
            "ETL_DATE": str(self.end_date.date()),
            "SYSTEM_DATE": str(self.end_date.date()),

            # Timestamps
            "ETL_START_TIMESTAMP": self._format_timestamp(self.start_date),
            "ETL_END_TIMESTAMP": self._format_timestamp(self.end_date + timedelta(days=1)),

            # Additional formats
            "ETL_DATE_YYYYMMDD": self.end_date.strftime("%Y%m%d"),
            "ETL_START_DATE_YYYYMMDD": self.start_date.strftime("%Y%m%d"),
            "ETL_YEAR": str(self.end_date.year),
            "ETL_MONTH": str(self.end_date.month).zfill(2),
            "ETL_DAY": str(self.end_date.day).zfill(2),
        }

        # Add environment parameter
        if env:
            self.parameters["ENV"] = env
            self.parameters["env"] = env

    def _format_timestamp(self, dt):
        return dt.strftime("%Y-%m-%d %H:%M:%S.000000")


# =============================================================================
# SQLFS - SQL File System
# =============================================================================

class SQLFS:
    """
    SQL File System - Reads SQL files from S3.

    S3 Structure:
        s3://c108-{env}-fpacfsa-final-zone/{app}/_configs/STG/{table.upper()}/initial/{table}.sql
        s3://c108-{env}-fpacfsa-final-zone/{app}/_configs/STG/{table.upper()}/incremental/{table}.sql
        s3://c108-{env}-fpacfsa-final-zone/{app}/_configs/EDV/{table.upper()}/*.sql
        s3://c108-{env}-fpacfsa-final-zone/{app}/_configs/{APP}_DM/{table.upper()}/*.sql
    """

    def __init__(self, env: str, application: str):
        self.env = env.lower()
        self.application = application.lower()
        self.s3_client = boto3.client("s3")

        self.bucket = f"c108-{self.env}-fpacfsa-final-zone"
        self.base_prefix = f"{self.application}/_configs"

        self.layer_folders = {
            "STG": "STG",
            "EDV": "EDV",
            "EBV": "EBV",
            "DM": "DM",
            #"DM": f"{self.application.upper()}_DM",
        }

    def get_stage_sql(self, table_name: str, run_type: str,
                      start_date: str, end_date: str = None) -> Optional[str]:
        """Get stage SQL for a table."""
        if not start_date:
            raise ValueError("start_date is required")

        stg_prefix = f"{self.base_prefix}/STG/"

        table_folder = self._resolve_s3_path_case(stg_prefix, table_name)
        table_prefix = f"{stg_prefix}{table_folder}/"

        run_type_folder = self._resolve_s3_path_case(table_prefix, run_type)
        run_prefix = f"{table_prefix}{run_type_folder}/"

        file_name = self._resolve_s3_path_case(run_prefix, f"{table_name}.sql")

        key = f"{run_prefix}{file_name}"

        return self._read_sql_file(key, start_date, end_date)

    def get_layer_sqls(self, layer: str, table_name: str,
                       start_date: str, end_date: str = None) -> List[Dict]:
        """Get SQLs for a layer (EDV, EBV, DM), ordered by numeric prefix."""
        if not start_date:
            raise ValueError("start_date is required")
            
        #added more changes here to fix case sensitivity issues when reading s3 bucket

        layer_upper = layer.upper()

        if layer_upper == "DM":
            layer_folder = "PYMT_DM"
        
        # Resolve layer folder case (EDV, EBV, DM)
        else:
            layer_folder = self._resolve_s3_path_case(
                f"{self.base_prefix}/",
                self.layer_folders.get(layer_upper, layer_upper)
            )
        
        # Resolve table folder case
        table_folder = self._resolve_s3_path_case(
            f"{self.base_prefix}/{layer_folder}/",
            table_name
        )
        
        prefix = f"{self.base_prefix}/{layer_folder}/{table_folder}/"



        return self._read_ordered_sqls(prefix, start_date, end_date)

    def _read_sql_file(self, key: str, start_date: str, end_date: str = None) -> Optional[str]:
        """Read a single SQL file from S3."""
        try:
            print(f"Reading: s3://{self.bucket}/{key}")
            response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
            sql_content = response["Body"].read().decode("utf-8")
            return self._substitute_parameters(sql_content, start_date, end_date)
        except self.s3_client.exceptions.NoSuchKey:
            print(f"SQL file not found: s3://{self.bucket}/{key}")
            return None
        except Exception as e:
            print(f"Error reading SQL: {e}")
            raise

    def _read_ordered_sqls(self, prefix: str, start_date: str, end_date: str = None) -> List[Dict]:
        """Read multiple SQL files from a folder, ordered by numeric prefix."""
        print(f"Listing: s3://{self.bucket}/{prefix}")

        try:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
            objects = response.get("Contents", [])
        except Exception as e:
            print(f"Error listing S3: {e}")
            raise

        if not objects:
            print(f"No SQL files found at {prefix}")
            return []

        scripts = []
        for obj in objects:
            key = obj["Key"]
            if not key.endswith(".sql"):
                continue

            filename = Path(key).name
            stem = Path(key).stem

            if "_" in stem and stem.split("_", 1)[0].isdigit():
                position = int(stem.split("_", 1)[0])
                operation = stem.split("_", 1)[1]
            else:
                position = 999
                operation = stem

            sql_content = self._read_sql_file(key, start_date, end_date)
            if sql_content:
                scripts.append({
                    "position": position,
                    "operation": operation,
                    "query": sql_content,
                    "filename": filename
                })

        scripts = sorted(scripts, key=lambda x: x["position"])
        print(f" Found {len(scripts)} SQL files")
        
        separator = "\n\n" + "-" * 60 + "\n\n"

        print("scripts:\n")
        for i, script in enumerate(scripts, start=1):
            print(f"[Script {i}]\n{script}")
            if i != len(scripts):
                print(separator)

        return scripts
    
    #added code to find s3 file ignoring case sensitivity

    def _resolve_s3_path_case(self, prefix: str, target: str) -> str:
        """
        returns S3 name with correct case sensitivity
        """
        response = self.s3_client.list_objects_v2(
            Bucket=self.bucket,
            Prefix=prefix
        )

        for obj in response.get("Contents", []):
            parts = obj["Key"].split("/")
            for part in parts:
                if part.lower() == target.lower():
                    return part

        raise FileNotFoundError(
            f"Could not resolve S3 casing for '{target}' under '{prefix}'"
        )
    
    def _resolve_dm_folder(self, table_name: str) -> str:
        resp = self.s3_client.list_objects_v2(
            Bucket=self.bucket,
            Delimiter="/"
        )

        dm_folders = [
            p["Prefix"].rstrip("/")
            for p in resp.get("CommonPrefixes", [])
            if p["Prefix"].rstrip("/").endswith("_DM")
        ]

        for dm in dm_folders:
            table_prefix = f"{dm}/{table_name.upper()}/"
            full_path = f"s3://{self.bucket}/{table_prefix}"
            print(full_path)

            check = self.s3_client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=table_prefix,
                MaxKeys=1
            )

            if "Contents" in check:
                return dm

        raise FileNotFoundError(
            f"No DM folder found for table {table_name}"
        )

    def _substitute_parameters(self, sql_content: str, start_date: str, end_date: str = None) -> str:
        """Substitute parameters in SQL."""
        params = SQLParameters(
            start_date_str=start_date,
            end_date_str=end_date,
            env=self.env
        ).parameters

        result = sql_content
        for key, value in params.items():
            result = result.replace(f"{{{key}}}", value)

        return result


# =============================================================================
# GLUEDB - Database Connection
# =============================================================================

class GlueDB:
    """
    PostgreSQL database connection for Glue jobs.

    Usage:
        db = GlueDB(
            spark_context=sc,
            glue_context=glue_context,
            glue_connection="FSA-CERT-PG-DART114",
            database="EDV"
        )
    """

    def __init__(self, spark_context, glue_context, glue_connection, database="EDV"):
        self.database = database
        self.sc = spark_context
        self.glue_context = glue_context
        self.glue_connection = glue_connection

        # Import Java libraries
        self._import_java_libraries()

        # Get connection properties from Glue
        self.jdbc_conf = glue_context.extract_jdbc_conf(glue_connection)
        self.connection_properties = self._get_connection_properties()

        # For Spark DataFrame writes
        self.jdbc_url = self.connection_properties["jdbc_url"]
        self.spark_connection_properties = {
            "user": self.connection_properties["user"],
            "password": self.connection_properties["password"],
            "driver": "org.postgresql.Driver",
        }

    def _import_java_libraries(self):
        """Import Java JDBC libraries."""
        java_import(self.sc._gateway.jvm, "java.sql.Connection")
        java_import(self.sc._gateway.jvm, "java.sql.DatabaseMetaData")
        java_import(self.sc._gateway.jvm, "java.sql.DriverManager")
        java_import(self.sc._gateway.jvm, "java.sql.SQLException")

    def _get_connection_properties(self):
        """Parse connection properties from Glue connection."""
        url = self.jdbc_conf.get("fullUrl", "").removeprefix("jdbc:")
        jdbc_url = f"jdbc:{url.removesuffix(urlparse(url).path)}/{self.database}"

        return {
            "jdbc_url": jdbc_url,
            "user": self.jdbc_conf.get("user"),
            "password": self.jdbc_conf.get("password"),
        }

    def execute(self, query: str, params: list = None, data: bool = False, commit: bool = False):
        """
        Execute a SQL query.

        Args:
            query: SQL query with ? placeholders for params
            params: List of parameter values
            data: If True, return result set as list of dicts
            commit: If True, commit transaction

        Returns:
            If data=True: List of dicts
            If data=False: Row count affected
        """
        params = params or []
        records = []

        conn = self._get_connection()
        conn.setAutoCommit(False)

        try:
            pstmt = conn.prepareCall(query)

            if params:
                for i, param in enumerate(params):
                    pstmt.setObject(i + 1, param)

            if data:
                results = pstmt.executeQuery()
                records = self._extract_records(results)
                results.close()
            else:
                records = pstmt.executeUpdate()

            if commit:
                conn.commit()
            else:
                conn.rollback()

        except Py4JJavaError as e:
            conn.rollback()
            raise
        finally:
            pstmt.close()
            conn.close()

        return records

    def _get_connection(self):
        """Get JDBC connection."""
        return self.sc._gateway.jvm.DriverManager.getConnection(
            self.connection_properties["jdbc_url"],
            self.connection_properties["user"],
            self.connection_properties["password"]
        )

    def _extract_records(self, results):
        """Extract records from JDBC ResultSet."""
        records = []
        columns = self._extract_columns(results)

        while results.next():
            record = {}
            for column in columns:
                record[column] = results.getString(column)
            records.append(record)

        return records

    def _extract_columns(self, results):
        """Extract column names from ResultSet metadata."""
        metadata = results.getMetaData()
        num_columns = metadata.getColumnCount()
        return [metadata.getColumnName(i) for i in range(1, num_columns + 1)]

    def truncate_table(self, schema: str, table: str):
        """Truncate a table."""
        query = f"TRUNCATE TABLE {schema}.{table};"
        self.execute(query=query, data=False, commit=True)


# =============================================================================
# CONFIGURATION
# =============================================================================

LAYER_CONFIG = {
    "STG": {
        "source_type": "athena",
        "target_schema_template": "{app}_stg",
    },
    "EDV": {
        "source_type": "postgres",
        "target_schema": "edv",
    },
    "EBV": {
        "source_type": "postgres",
        "target_schema": "ebv",
    },
    "DM": {
        "source_type": "postgres",
        "target_schema_template": "{app}_dm",
    },
}

# Tables to skip processing (no table in S3 final zone/sql file in S3)
EXCLUSION_LIST = {
    "cars": ["ACRSI_FARM",
                "ACRSI_PRDR",
                ],
    "cnsv": [#"ACRSI_FARM",
                #"tablename"
                ],
    "sbsd": [#"ACRSI_FARM",
                #"tablename"
                ],
}

# =============================================================================
# MAIN
# =============================================================================

def main():
    # Parse required arguments
    args = getResolvedOptions(sys.argv, [
        "JOB_NAME",
        "table_name",
        "data_src_nm",
        "run_type",
        "start_date",
        "env",
        "layer"
    ])

    # Initialize Spark/Glue
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    # Extract parameters
    table_name = args["table_name"]
    data_src_nm = args["data_src_nm"].lower()
    #run_type = args["run_type"].lower()
    run_type = args["run_type"]
    start_date = args["start_date"]
    env = args["env"].lower()
    layer = args["layer"].upper()
    
    # Check if table is in exclusion list
    excluded_tables = EXCLUSION_LIST.get(data_src_nm, [])
    print(f" EXCLUSION_LIST  is: {EXCLUSION_LIST}")
    if table_name.lower() in [t.lower() for t in excluded_tables]:
        print(f" Table {table_name} is in exclusion list for {data_src_nm}. Skipping.")
        job.commit()
        return

    # End date equals start date (single day processing)
    end_date = start_date

    job_run_id = args.get("JOB_RUN_ID", datetime.utcnow().strftime("%Y%m%d%H%M%S"))

    # Validate layer
    if layer not in LAYER_CONFIG:
        raise ValueError(f"Invalid layer: {layer}. Must be one of: {list(LAYER_CONFIG.keys())}")

    layer_cfg = LAYER_CONFIG[layer]

    # Derive target schema
    if "target_schema" in layer_cfg:
        tgt_schema = layer_cfg["target_schema"]
    else:
        tgt_schema = layer_cfg["target_schema_template"].format(app=data_src_nm)

    print("=" * 70)
    print(f"FSA-DART-EXEC-DB-SQL")
    print("=" * 70)
    print(f"  Layer:          {layer}")
    print(f"  Table:          {table_name}")
    print(f"  Application:    {data_src_nm}")
    print(f"  Run Type:       {run_type}")
    print(f"  Date:           {start_date}")
    print(f"  Environment:    {env}")
    print(f"  Target Schema:  {tgt_schema}")
    print("=" * 70)

    process_start_time = datetime.utcnow()
    total_rows = 0
    db = None

    try:
        # Initialize database connection
        glue_connection = f"FSA-{env.upper()}-PG-DART114"
        db = GlueDB(
            spark_context=sc,
            glue_context=glue_context,
            glue_connection=glue_connection,
            database="EDV"
        )
        print("Database connection established")

        # Initialize SQL file system
        sqlfs = SQLFS(env=env, application=data_src_nm)
        print("SQL file system initialized")
        print(f"  Bucket: {sqlfs.bucket}")

        # Process based on layer
        if layer == "STG":
            total_rows = process_stage_table(
                spark=spark,
                glue_context=glue_context,
                db=db,
                sqlfs=sqlfs,
                table_name=table_name,
                data_src_nm=data_src_nm,
                run_type=run_type,
                start_date=start_date,
                end_date=end_date,
                env=env,
                tgt_schema=tgt_schema
            )
        else:
            # EDV, EBV, DM - PostgreSQL SQL execution
            total_rows = process_postgres_layer(
                db=db,
                run_type=run_type,
                sqlfs=sqlfs,
                table_name=table_name,
                layer=layer,
                start_date=start_date,
                end_date=end_date
            )

        # Log success
        log_operation(
            db=db,
            table_name=table_name,
            layer=layer,
            system_date=end_date,
            start_time=process_start_time,
            rows_affected=total_rows,
            job_run_id=job_run_id,
            status="SUCCESS"
        )

        duration = (datetime.utcnow() - process_start_time).total_seconds()

        print(f"\n{'=' * 70}")
        print(f"SUCCESS: {layer}.{tgt_schema}.{table_name}")
        print(f"Rows Affected: {total_rows}")
        print(f"Duration: {duration:.2f}s")
        print("=" * 70)

    except Exception as e:
        error_message = str(e)
        error_tb = traceback.format_exc()

        print(f"\n{'=' * 70}")
        print(f" FAILED: {layer}.{table_name}")
        print(f"  Error: {error_message}")
        print(f"  Traceback:\n{error_tb}")
        print("=" * 70)

        if db:
            try:
                log_operation(
                    db=db,
                    table_name=table_name,
                    layer=layer,
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
        job.commit()


# =============================================================================
# STAGE LAYER PROCESSING (Athena SQL → PostgreSQL)
# =============================================================================

def process_stage_table(spark, glue_context, db, sqlfs, table_name, data_src_nm,
                        run_type, start_date, end_date, env, tgt_schema):
    """
    Process stage table:
    1. Read SQL from S3
    2. Execute SQL via Spark SQL (queries Glue Catalog directly)
    3. Align columns to target PostgreSQL schema
    4. Write results to PostgreSQL
    """
    print(f"\n--- Processing Stage Table: {table_name} ({run_type}) ---")

    # Get SQL from S3
    sql = sqlfs.get_stage_sql(
        table_name=table_name.upper(),
        run_type=run_type,
        start_date=start_date,
        end_date=end_date
    )
    print(f"\nsql: {sql}")
    if not sql:
        raise Exception(
            f"Stage SQL not found: s3://{sqlfs.bucket}/{sqlfs.base_prefix}/STG/{table_name.upper()}/{run_type}/{table_name.upper()}.sql"
        )

    print(f"SQL loaded from S3 ({len(sql)} chars)")
    print(f"SQL Preview: {sql[:200]}...")

    # Execute SQL via Spark SQL (uses Glue Data Catalog)
    print(f"\nExecuting SQL via Spark SQL...")
    try:
        result_df = spark.sql(sql)
        
        # Cache teh dataframe to avoid recomputation
        result_df.cache()
        
        row_count = result_df.count()
        print(f"Query returned {row_count} rows")
    except Exception as e:
        print(f"Spark SQL Error: {e}")
        print(f"Full SQL:\n{sql}")
        raise

    if row_count == 0:
        print("No data to process")
        return 0

    # Get target columns and align schema
    target_columns = get_postgres_columns(db, tgt_schema, table_name)
    print(f"Target has {len(target_columns)} columns")

    result_df = align_columns(result_df, target_columns, db, tgt_schema, table_name)

    # Truncate data in PostgreSQL staging tables before loading new data.
    print(f"\nTruncating: {tgt_schema}.{table_name}")
    db.truncate_table(tgt_schema, table_name)

    # Write to PostgreSQL
    print(f"\nWriting to PostgreSQL: {tgt_schema}.{table_name}")
    write_to_postgres(db, result_df, tgt_schema, table_name)
    
    # Unpersist cached dataframe
    result_df.unpersist()

    print(f"Wrote {row_count} rows")
    return row_count


def get_postgres_columns(db, schema, table_name):
    """Get column names from PostgreSQL target table."""
    query = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE LOWER(table_schema) = LOWER('{schema}')
          AND LOWER(table_name) = LOWER('{table_name}')
        ORDER BY ordinal_position
    """
    result = db.execute(query=query, data=True)
    return [row["column_name"] for row in result]


def get_postgres_column_types(db, schema, table_name):
    """Get column types from PostgreSQL target table."""
    query = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE LOWER(table_schema) = LOWER('{schema}')
          AND LOWER(table_name) = LOWER('{table_name}')
        ORDER BY ordinal_position
    """
    result = db.execute(query=query, data=True)
    return {row["column_name"].lower(): row["data_type"] for row in result}


def align_columns(source_df, target_columns, db, schema, table_name):
    """Align source DataFrame columns to match target PostgreSQL table."""
    from pyspark.sql.functions import col, lit, current_timestamp
    from pyspark.sql.types import (
        StringType, IntegerType, LongType, DoubleType,
        TimestampType, DateType, BooleanType, DecimalType
    )

    source_col_map = {c.lower(): c for c in source_df.columns}
    source_cols_lower = set(source_col_map.keys())

    target_types = get_postgres_column_types(db, schema, table_name)

    type_map = {
        "integer": IntegerType(),
        "bigint": LongType(),
        "smallint": IntegerType(),
        "numeric": DecimalType(38, 10),
        "decimal": DecimalType(38, 10),
        "real": DoubleType(),
        "double precision": DoubleType(),
        "character varying": StringType(),
        "varchar": StringType(),
        "character": StringType(),
        "char": StringType(),
        "text": StringType(),
        "timestamp without time zone": TimestampType(),
        "timestamp with time zone": TimestampType(),
        "date": DateType(),
        "boolean": BooleanType(),
    }

    audit_defaults = {
        "load_dt": current_timestamp(),
        "crt_dt": current_timestamp(),
        "last_chg_dt": current_timestamp(),
        "etl_load_dt": current_timestamp(),
        "cre_dt": current_timestamp(),
        "crt_user_nm": lit("DART_ETL"),
        "last_chg_user_nm": lit("DART_ETL"),
        "cre_user_nm": lit("DART_ETL"),
    }

    select_exprs = []
    matched = []
    defaulted = []

    for target_col in target_columns:
        target_col_lower = target_col.lower()
        pg_type = target_types.get(target_col_lower, "text")
        spark_type = type_map.get(pg_type, StringType())

        if target_col_lower in source_cols_lower:
            src_col = source_col_map[target_col_lower]
            select_exprs.append(col(f"`{src_col}`").cast(spark_type).alias(target_col))
            matched.append(target_col)
        elif target_col_lower in audit_defaults:
            select_exprs.append(audit_defaults[target_col_lower].cast(spark_type).alias(target_col))
            defaulted.append(target_col)

    print(f"  Columns matched: {len(matched)}, defaulted: {len(defaulted)}")

    if not select_exprs:
        raise ValueError("No matching columns between source and target")

    return source_df.select(select_exprs)


def write_to_postgres(db, df, schema, table_name):
    """Write DataFrame to PostgreSQL via JDBC."""
    
    # Repartition for parallel writes 
    num_partitions = 20
    df_partitioned = df.repartition(num_partitions)
    
    df_partitioned.write \
        .format("jdbc") \
        .option("url", db.jdbc_url) \
        .option("dbtable", f"{schema}.{table_name}") \
        .option("user", db.spark_connection_properties["user"]) \
        .option("password", db.spark_connection_properties["password"]) \
        .option("driver", "org.postgresql.Driver") \
        .option("batchsize", 50000) \
        .option("numPartitions", num_partitions) \
        .option("rewriteBatchedStatements", "true") \
        .mode("append") \
        .save()

    return df.count()


# =============================================================================
# POSTGRES LAYER PROCESSING (EDV, EBV, DM)
# =============================================================================

#added new chunking function here

def execute_insert_in_chunks(
    db,
    base_insert_sql: str,
    chunk_size: int = 50000,
    order_by: str = None
):
    """
    Executes INSERT ... SELECT in chunks using LIMIT / OFFSET.
    Assumes base_insert_sql is an INSERT INTO ... SELECT ... statement.
    """

    offset = 0
    total_rows = 0

    while True:
        paged_sql = f"""
        {base_insert_sql}
        {f"ORDER BY {order_by}" if order_by else ""}
        LIMIT {chunk_size} OFFSET {offset}
        """

        rows = db.execute(query=paged_sql, data=False, commit=True)

        if rows == 0:
            break

        total_rows += rows
        offset += chunk_size

        print(f"Inserted {rows} rows (total: {total_rows})")

    return total_rows

def process_postgres_layer(db, run_type, sqlfs, table_name, layer, start_date, end_date):
    """
    Process PostgreSQL-based layer (EDV, EBV, DM).
    Reads SQL files from S3 and executes in order.
    """
    print(f"\n--- Processing {layer} Table: {table_name} ---")
    print(f"Date: {start_date}")
    print(f"Load Type: {run_type}")

    # Get ordered SQLs from S3
    sqls = sqlfs.get_layer_sqls(
        layer=layer,
        table_name=table_name,
        start_date=start_date,
        end_date=end_date
    )

    if not sqls:
        layer_folder = sqlfs.layer_folders.get(layer.upper(), layer)
        raise Exception(
            f"{layer} SQLs not found: s3://{sqlfs.bucket}/{sqlfs.base_prefix}/{layer}/{table_name}/"
        )

    print(f"Found {len(sqls)} SQL files")

    total_rows = 0

    for idx, sql_item in enumerate(sqls, 1):
        operation = sql_item["operation"]
        query = sql_item["query"]
        filename = sql_item["filename"]

        print(f"\n[{idx}/{len(sqls)}] {filename}")

        op_start = datetime.utcnow()
        #if run_type == 'initial':
            #db.truncate_table(layer.lower(), table_name)
            
        rows = db.execute(query=query, data=False, commit=True)
        op_duration = (datetime.utcnow() - op_start).total_seconds()

        total_rows += rows
        print(f" {rows} rows in {op_duration:.2f}s")

    print(f"\nTotal rows affected: {total_rows}")
    return total_rows


# =============================================================================
# PROCESS CONTROL LOGGING
# =============================================================================

def log_operation(db, table_name, layer, system_date, start_time,
                  rows_affected, job_run_id, status, error_message=None):
    """Log operation to dart_process_control.data_ppln_oper."""
    try:
        end_time = datetime.utcnow()
        load_oper_tgt_id = get_load_oper_tgt_id(db, table_name)

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

        db.execute(
            query=query,
            params=[
                load_oper_tgt_id,
                start_time,
                end_time,
                f"{layer} Table",
                table_name,
                rows_affected,
                "FSA-DART-EXEC-DB-SQL",
                1 if status == "SUCCESS" else -1,
                error_message[:1000] if error_message else None,
                job_run_id,
                datetime.strptime(system_date, "%Y-%m-%d").date(),
                end_time,
                end_time
            ],
            commit=True
        )
        print("Logged to process control")

    except Exception as e:
        print(f"Could not log to process control: {e}")


def get_load_oper_tgt_id(db, table_name):
    """Get load_oper_tgt_id from dart_process_control."""
    try:
        query = """
            SELECT load_oper_tgt_id
            FROM dart_process_control.load_oper_tgt
            WHERE LOWER(db_tbl_nm) = LOWER(?)
              AND data_stat_cd = 'A'
            LIMIT 1
        """
        result = db.execute(query=query, params=[table_name], data=True)
        return int(result[0]["load_oper_tgt_id"]) if result else None
    except:
        return None


if __name__ == "__main__":
    main()