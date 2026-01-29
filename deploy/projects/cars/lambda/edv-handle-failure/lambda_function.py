"""
Lambda: edv-handle-failure
Purpose: Handle pipeline failures, log errors, and send notifications

Input:
{
    "error": {
        "Error": "States.TaskFailed",
        "Cause": "..."
    },
    "data_src_nm": "CARS",
    "run_type": "initial",
    "start_date": "2025-01-12"
}

Output:
{
    "handled": true,
    "error_logged": true,
    "notification_sent": true
}

Environment Variables:
    REGION: AWS region (default: us-east-1)
    SECRET_NAME: Secrets Manager secret name
    SNS_TOPIC_ARN: SNS topic ARN for notifications (optional)
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
SECRET_NAME = os.environ.get("SECRET_NAME", "FSA-CERT-Secrets")
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN", "")


def handler(event, context):
    logger.info(f"Handle failure input: {json.dumps(event)}")

    error_info = event.get("error", {})
    data_src_nm = event.get("data_src_nm", "UNKNOWN")
    run_type = event.get("run_type", "incremental")
    start_date = event.get("start_date", str(date.today()))

    # Extract error details
    error_type = error_info.get("Error", "Unknown")
    error_cause = error_info.get("Cause", "No cause provided")

    # Try to parse Cause if it's JSON
    try:
        cause_obj = json.loads(error_cause)
        error_message = cause_obj.get("errorMessage", error_cause)
    except:
        error_message = error_cause

    logger.error(f"Pipeline failure - Type: {error_type}, Message: {error_message}")

    output = {
        "handled": True,
        "error_logged": False,
        "notification_sent": False
    }

    # Log error to database
    try:
        log_error_to_db(data_src_nm, run_type, start_date, error_type, error_message)
        output["error_logged"] = True
    except Exception as e:
        logger.warning(f"Could not log error to DB: {e}")

    # Send SNS notification
    if SNS_TOPIC_ARN:
        try:
            send_notification(data_src_nm, run_type, start_date, error_type, error_message)
            output["notification_sent"] = True
        except Exception as e:
            logger.warning(f"Could not send notification: {e}")

    logger.info(f"Handle failure output: {json.dumps(output)}")
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


def log_error_to_db(data_src_nm, run_type, start_date, error_type, error_message):
    """Log pipeline error to process control."""
    conn = None
    cursor = None

    try:
        conn, cursor = get_database_connection()

        cursor.execute("""
            INSERT INTO dart_process_control.data_ppln_error_log (
                data_src_nm, run_type, start_date,
                error_type, error_message, crt_dt
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            data_src_nm,
            run_type,
            start_date,
            error_type,
            error_message[:4000] if error_message else None,
            datetime.utcnow()
        ))

        logger.info("Error logged to database")

    except Exception as e:
        logger.warning(f"Could not log to database: {e}")
        raise

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def send_notification(data_src_nm, run_type, start_date, error_type, error_message):
    """Send SNS notification for pipeline failure."""
    sns = boto3.client("sns", region_name=REGION)

    subject = f"EDV Pipeline FAILED - {data_src_nm} ({run_type})"

    message = f"""
EDV Pipeline Failure Alert
===========================

Data Source: {data_src_nm}
Run Type: {run_type}
Start Date: {start_date}
Failure Time: {datetime.utcnow().isoformat()}

Error Type: {error_type}

Error Message:
{error_message[:1000]}

Please check the Step Functions console and CloudWatch logs for details.
"""

    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject[:100],  # SNS subject limit
        Message=message
    )

    logger.info(f"Notification sent to {SNS_TOPIC_ARN}")