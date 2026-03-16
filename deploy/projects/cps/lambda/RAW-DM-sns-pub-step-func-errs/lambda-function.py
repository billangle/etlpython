'''
Function to publish errors incase of any glue job failed .
'''


import json
import boto3
import os


REGION = os.getenv("AWS_REGION")
SNS_ARN = os.environ["SNS_ARN"]


def lambda_handler(event, context):
    # parse, format and return a single error message
    def msg_formatter(content):
        cause_json = json.loads(content)
        # table_name = cause_json['Arguments']['--TableName']
        cause_info = (f"Error Number: {counter} \n"
                      f"JobName: {cause_json.get('JobName', 'Unknown')} \n"
                      f"TableName: {cause_json.get('Arguments', {}).get('--TableName', 'Unknown')} \n"
                      f"JobRunState: {cause_json.get('JobRunState', 'Unknown')} \n"
                      f"StartedOn: {cause_json.get('StartedOn', 'Unknown')} \n"
                      f"ErrorMessage: {cause_json.get('ErrorMessage', 'Unknown')} \n"
                      "---------------------------------- \n")
        return cause_info

    # iterate over input to extract and process all errors    
    counter = 1
    sf_name = event.get('SfName', '')
    all_fails = ''
    for key, val in event.items():

        if isinstance(val, dict):
            for item in [event[key]]:
                if 'Cause' in item:
                    cause = item["Cause"]
                    message = msg_formatter(cause)
                    all_fails += message
                    counter += 1
        else:
            for item in event[key]:
                if 'Cause' in item:
                    cause = item["Cause"]
                    message = msg_formatter(cause)
                    all_fails += message
                    counter += 1
                     
    # if there were any failures in the preceeding steps send sns notification and fail state function
    if all_fails:
    
        # concatenate the final sns message
        sns_msg = f'One or multiple errors occured when executing state machine: {sf_name}. \n\n' + all_fails

        sns_client = boto3.client('sns', region_name=REGION)
        response = sns_client.publish(
            TopicArn = SNS_ARN,
            Message = sns_msg,
            Subject= 'FSA-DEV-CPS-NOTIFICATIONS'
        )
        
        
        raise Exception(sns_msg)
    
        
    else:
        return