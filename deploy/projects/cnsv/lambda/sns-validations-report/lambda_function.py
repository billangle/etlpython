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
    
    print("Incoming event:", json.dumps(event, default=str))
    
    job_id   = event.get("JobId")
    no_files = event.get("noFiles", False)
    files    = event.get("files", [])  # matches Step Functions payload
    
    def file_to_str(x):
        if isinstance(x, str):
            return x
        if isinstance(x, dict):
            name   = x.get("file_name") or x.get("name") or ""
            folder = x.get("folder")    or x.get("path") or ""
            return "/".join([p for p in [folder, name] if p])
        return str(x)
        
    display_files = [file_to_str(x) for x in files]
    file_list_str = "\n".join(display_files)
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    
    if no_files:
        subject = f"No Incremental Files for CNSV Raw-DM on {today} CERT"
        message = (
            f"No incremental files found for job {job_id} on {today}.\n\n"
            "Please check your source if you were expecting data."
        )
        
    else:
        subject = f"CNSV Raw-DM: {len(display_files)} file(s) processed on {today} CERT"
        message = (
            f"CNSV RAW-DM Job {job_id} on {today} successfully processed "
            f"{len(display_files)} file(s):\n\n{file_list_str}"
        )
        
    resp = sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject,
        Message=message
    )
    
    return {
        "statusCode": 200,
        "messageId": resp.get("MessageId"),
        "JobId": job_id,
        "noFiles": no_files,
        "fileCount": len(display_files),
        "date": today
    }