import boto3

def lambda_handler(event, context):
    glue = boto3.client('glue')
    try:
        glue.start_crawler(Name='FSA-DEV-FMMI-ODS')
        return {"status": "crawler_started"}
    except glue.exceptions.CrawlerRunningException:
        return {"status": "crawler_already_running"}
