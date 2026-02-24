import os
import json
import boto3
from datetime import datetime, timezone

sns = boto3.client("sns")
SNS_TOPIC_ARN = os.environ["SNS_ARN"]

def handler(event, context):
    """
    event = {
      "JobId": 12345,
      "noFiles": true|false,
      "files": [ {"file_name": "...", "folder": "..."}, ... ]  # only when noFiles==false
    }
    """
    job_id  = event["JobId"]
    no_files = event.get("noFiles", False)
    files = event.get('FileList', [])

    # get today's date (UTC) as YYYY-MM-DD
    today = datetime.now(timezone.utc).strftime('%Y-%m-d')

    if no_files:
        subject =f"No Incremental Files available to Process for Conservation Contract Maintenance Raw-DM on {today} in CERT"
        message = (
            f"No incremental files found for job {job_id} on {today}.\n\n"
            "Please check your source if you were expecting data."
        )
    else:
        subject = f"Conservation Contract Maintenance Raw-DM Processed {len(files)} file(s) processed on {today} in CERT"
        file_list_str = "\n".join(files)
        message = (
            f"Conservation Contract Maintenance RAW-DM Job {job_id} on {today} successfully processed {len(files)} file(s):\n\n"
            f"{file_list_str}"
        )

    resp = sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject,
        Message=message
    )
    
    return {
        'statusCode': 200,
        'messageId': resp.get('MessageId'),
        'JobId': job_id,
        'noFiles': no_files,
        'fileCount': len(files),
        'date': today
    }
