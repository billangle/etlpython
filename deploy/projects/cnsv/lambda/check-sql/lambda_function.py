import os
import json
import boto3
from datetime import datetime
from pathlib import Path

def _resolve_s3_path_case(s3_client, bucket, prefix, target):
    """
    Returns S3 object name with correct case sensitivity.
    """
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    for obj in response.get("Contents", []):
        parts = obj["Key"].split("/")
        for part in parts:
            if part.lower() == target.lower():
                return part
    raise FileNotFoundError(f"Could not resolve S3 bucket: '{bucket}' casing for '{target}' under '{prefix}'")

def lambda_handler(event, context):
    # Accepts same arguments as EXEC-SQL Glue job
    # event should contain: env, application (data_src_nm), run_type, start_date, stgTables, etc.
    env = event.get('env')
    application = event.get('data_src_nm')
    run_type = event.get('run_type')
    start_date = event.get('start_date')
    stg_tables = event.get('plan', {}).get('stgTables') or event.get('stgTables')
    if not (env and application and run_type and start_date and stg_tables):
        return {"error": "Missing required parameters"}

    s3 = boto3.client('s3')
    bucket = f"c108-{env.lower()}-fpacfsa-final-zone"
    base_prefix = f"{application.lower()}/_configs/STG/"

    found = []
    missing = []
    for table in stg_tables:
        try:
            table_folder = _resolve_s3_path_case(s3, bucket, base_prefix, table.upper())
            table_prefix = f"{base_prefix}{table_folder}/"
            run_type_folder = _resolve_s3_path_case(s3, bucket, table_prefix, run_type)
            run_prefix = f"{table_prefix}{run_type_folder}/"
            file_name = _resolve_s3_path_case(s3, bucket, run_prefix, f"{table.upper()}.sql")
            key = f"{run_prefix}{file_name}"
            # Check if file exists
            s3.head_object(Bucket=bucket, Key=key)
            found.append(key)
        except Exception as e:
            missing.append(f"{table} ({str(e)})")

    total = len(stg_tables)
    found_pct = (len(found) / total * 100) if total else 0
    now = datetime.utcnow().strftime('%Y-%m-%dT%H%M%S')
    report = [
        f"# EXEC-SQL Preflight Report",
        f"Checked {total} tables for required SQL files.",
        f"\n## Found ({len(found)})",
        *(f"- {k}" for k in found),
        f"\n## Missing ({len(missing)})",
        *(f"- {k}" for k in missing),
        f"\n**Percent found:** {found_pct:.1f}%"
    ]
    md_report = "\n".join(report)

    # Write to S3 artifact bucket
    # artifactBucket from config: fsa-dev-ops, prefix: report/cnsv/exec-sql-<datetime>.md
    artifact_bucket = os.environ.get('ARTIFACT_BUCKET', 'fsa-dev-ops')
    s3_key = f"report/cnsv/exec-sql-{now}.md"
    s3.put_object(Bucket=artifact_bucket, Key=s3_key, Body=md_report.encode('utf-8'))

    return {
        "report_s3": f"s3://{artifact_bucket}/{s3_key}",
        "found": found,
        "missing": missing,
        "percent_found": found_pct
    }
