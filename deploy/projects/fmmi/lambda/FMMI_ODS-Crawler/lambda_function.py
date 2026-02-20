import os
import boto3

CRAWLER_NAME = os.environ["CRAWLER_NAME"]

def handler(event, context):
    glue = boto3.client('glue')
    try:
        glue.start_crawler(Name=CRAWLER_NAME)
        return {"status": "crawler_started", "crawler": CRAWLER_NAME}
    except glue.exceptions.CrawlerRunningException:
        return {"status": "crawler_already_running", "crawler": CRAWLER_NAME}
