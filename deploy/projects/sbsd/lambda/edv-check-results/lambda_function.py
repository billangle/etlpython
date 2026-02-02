"""
Lambda: edv-check-results
Purpose: Aggregate results from Map state executions

Input:
{
    "layer": "STG" | "EDV",
    "results": [
        {"table_name": "t1", "status": "SUCCESS", "layer": "STG"},
        {"table_name": "t2", "status": "FAILED", "layer": "STG", "error": {...}},
        ...
    ]
}

For nested EDV results (groups):
{
    "layer": "EDV",
    "results": [
        {"groupResults": [{"table_name": "t1", "status": "SUCCESS"}, ...]},
        {"groupResults": [{"table_name": "t2", "status": "FAILED"}, ...]}
    ]
}

Output:
{
    "successCount": N,
    "failedCount": M,
    "failedTables": ["t1", "t2"],
    "canContinue": true | false
}

Environment Variables:
    REGION: AWS region (default: us-east-1)
    SECRET_NAME: Secrets Manager secret name
"""

import os
import json
import boto3
import logging
from datetime import datetime

import psycopg2

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variables
REGION = os.environ.get("REGION", "us-east-1")
SECRET_NAME = os.environ.get("SECRET_NAME", "FSA-CERT-Secrets")


def handler(event, context):
    logger.info(f"Check results input: {json.dumps(event)}")

    layer = event.get("layer", "UNKNOWN")
    results = event.get("results", [])

    # Flatten nested results (for EDV groups)
    flat_results = flatten_results(results)

    # Count successes and failures
    success_count = 0
    failed_count = 0
    failed_tables = []

    for result in flat_results:
        table_name = result.get("table_name", "unknown")
        status = result.get("status", "UNKNOWN")

        if status == "SUCCESS":
            success_count += 1
        else:
            failed_count += 1
            failed_tables.append(table_name)

            # Log failure details
            error = result.get("error", {})
            logger.error(f"Table {table_name} failed: {error}")

    # Determine if pipeline can continue
    # Continue if at least some tables succeeded
    can_continue = success_count > 0

    # Log summary to database if there are failures
    if failed_count > 0:
        try:
            log_failures_to_db(layer, failed_tables)
        except Exception as e:
            logger.warning(f"Could not log failures to DB: {e}")

    output = {
        "successCount": success_count,
        "failedCount": failed_count,
        "failedTables": failed_tables,
        "canContinue": can_continue
    }

    logger.info(f"Check results output: {json.dumps(output)}")
    return output


def flatten_results(results):
    """Flatten nested Map state results."""
    flat = []

    for item in results:
        if isinstance(item, dict):
            # Check if this is a group result (nested Map)
            if "groupResults" in item:
                # EDV group - flatten inner results
                for inner in item["groupResults"]:
                    if isinstance(inner, dict):
                        flat.append(inner)
                    elif isinstance(inner, list):
                        flat.extend(inner)
            else:
                # Direct result
                flat.append(item)
        elif isinstance(item, list):
            # Nested list - flatten recursively
            flat.extend(flatten_results(item))

    return flat


def get_database_connection():
    """Get PostgreSQL connection using Secrets Manager."""
    secrets = boto3.client("secretsmanager", region_name=REGION)
    secret_value = secrets.get_secret_value(SecretId=SECRET_NAME)
    secret = json.loads(secret_value["SecretString"])

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
    conn.autocommit = True
    cursor = conn.cursor()

    return conn, cursor


def log_failures_to_db(layer, failed_tables):
    """Log failed tables to process control."""
    conn = None
    cursor = None

    try:
        conn, cursor = get_database_connection()

        for table_name in failed_tables:
            cursor.execute("""
                INSERT INTO dart_process_control.data_ppln_oper_log (
                    log_type, log_message, data_obj_nm, crt_dt
                ) VALUES (%s, %s, %s, %s)
            """, (
                "ERROR",
                f"{layer} table processing failed",
                table_name,
                datetime.utcnow()
            ))

        logger.info(f"Logged {len(failed_tables)} failures to database")

    except Exception as e:
        logger.warning(f"Could not log to database: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()