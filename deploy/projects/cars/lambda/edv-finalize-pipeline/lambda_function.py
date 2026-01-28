"""
Lambda: edv-finalize-pipeline
Purpose: Determine final pipeline status and log completion

Input:
{
    "data_src_nm": "CARS",
    "run_type": "initial",
    "start_date": "2025-01-12",
    "stageCheck": {"successCount": 10, "failedCount": 0, "failedTables": [], "canContinue": true},
    "edvCheck": {"successCount": 5, "failedCount": 1, "failedTables": ["sat_x"], "canContinue": true},
    "counts": {"stg": 10, "dv": 6, "dvGroups": 2}
}

Output:
{
    "status": "SUCCESS" | "PARTIAL_SUCCESS" | "FAILED",
    "summary": {...}
}

Environment Variables:
    REGION: AWS region (default: us-east-1)
    SECRET_NAME: Secrets Manager secret name
"""

import os
import json
import boto3
import logging
from datetime import datetime, date

import psycopg2

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variables
REGION = os.environ.get("REGION", "us-east-1")
SECRET_NAME = os.environ.get("SECRET_NAME", "fsa-dart-edv-secrets")


def lambda_handler(event, context):
    logger.info(f"Finalize pipeline input: {json.dumps(event)}")

    data_src_nm = event.get("data_src_nm", "UNKNOWN")
    run_type = event.get("run_type", "incremental")
    start_date = event.get("start_date", str(date.today()))

    stage_check = event.get("stageCheck", {})
    edv_check = event.get("edvCheck", {})
    counts = event.get("counts", {})

    # Calculate totals
    stg_success = stage_check.get("successCount", 0)
    stg_failed = stage_check.get("failedCount", 0)
    edv_success = edv_check.get("successCount", 0)
    edv_failed = edv_check.get("failedCount", 0)

    total_success = stg_success + edv_success
    total_failed = stg_failed + edv_failed
    total_tables = counts.get("stg", 0) + counts.get("dv", 0)

    # Determine overall status
    if total_failed == 0:
        status = "SUCCESS"
    elif total_success > 0:
        status = "PARTIAL_SUCCESS"
    else:
        status = "FAILED"

    # Build summary
    summary = {
        "data_src_nm": data_src_nm,
        "run_type": run_type,
        "start_date": start_date,
        "end_date": str(date.today()),
        "stage": {
            "total": counts.get("stg", 0),
            "success": stg_success,
            "failed": stg_failed,
            "failedTables": stage_check.get("failedTables", [])
        },
        "edv": {
            "total": counts.get("dv", 0),
            "groups": counts.get("dvGroups", 0),
            "success": edv_success,
            "failed": edv_failed,
            "failedTables": edv_check.get("failedTables", [])
        },
        "totals": {
            "tables": total_tables,
            "success": total_success,
            "failed": total_failed,
            "successRate": f"{(total_success / total_tables * 100):.1f}%" if total_tables > 0 else "N/A"
        }
    }

    # Log completion to database
    try:
        log_completion_to_db(data_src_nm, run_type, start_date, status, summary)
    except Exception as e:
        logger.warning(f"Could not log completion to DB: {e}")

    output = {
        "status": status,
        "summary": summary
    }

    logger.info(f"Pipeline finalized: {status}")
    logger.info(f"Summary: {json.dumps(summary)}")

    return output


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


def log_completion_to_db(data_src_nm, run_type, start_date, status, summary):
    """Log pipeline completion to process control."""
    conn = None
    cursor = None

    try:
        conn, cursor = get_database_connection()

        cursor.execute("""
            INSERT INTO dart_process_control.data_ppln_run_log (
                data_src_nm, run_type, start_date, end_date,
                status, stg_success_ct, stg_failed_ct,
                edv_success_ct, edv_failed_ct,
                summary_json, crt_dt
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            data_src_nm,
            run_type,
            start_date,
            str(date.today()),
            status,
            summary["stage"]["success"],
            summary["stage"]["failed"],
            summary["edv"]["success"],
            summary["edv"]["failed"],
            json.dumps(summary),
            datetime.utcnow()
        ))

        logger.info("Logged completion to database")

    except Exception as e:
        logger.warning(f"Could not log to database: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()