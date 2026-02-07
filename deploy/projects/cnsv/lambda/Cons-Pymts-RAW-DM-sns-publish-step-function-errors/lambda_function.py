'''
Function to publish errors incase of any glue job failed .
'''


import json
import boto3
import os
from sns_topics import sns_topics


REGION = os.getenv("AWS_REGION")
#SNS_ARN = "arn:aws:sns:us-east-1:241533156429:FSA-CERT-NAP"


def handler(event, context):
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

    
    def check_for_input_errors(input_dict, all_fails, counter):
        
        for key, val in input_dict.items():
            if isinstance(val, (int, float)):
                pass
            
            elif isinstance(val, dict):
                for item in [input_dict[key]]:
                    if "Cause" in item:
                        cause = item["Cause"]
                        print(cause)
                        message = msg_formatter(cause)
                        all_fails += message
                        counter += 1
            else:
                for item in input_dict[key]:
                    if "Cause" in item:
                        cause = item["Cause"]
                        print(cause)
                        message = msg_formatter(cause)
                        all_fails += message
                        counter += 1
            
        return all_fails
        
    error = event.get('Error')
    print(f"error: {error}")
    error_json = json.loads(error['Cause'])
    print(f"error_json: {error_json}")
    target = error_json["Arguments"]["--target_prefix"]
    print(f"target: {target}")
    env = error_json["Arguments"]["--env"]
    print(f"env: {env}")

    # iterate over input to extract and process all errors    
    counter = 1
    #sf_name = event.get('SfName', '')
    sf_name = 'FSA-CERT-Cnsv-Cons-Pymts-S3Landing-to-S3Final-Raw-DM'
    all_fails = ''

    if isinstance(event, list):
        for input in event:
            if isinstance(input, dict):
                all_fails = check_for_input_errors(input, all_fails, counter)
    
    else:
        all_fails = check_for_input_errors(event, all_fails, counter)

    print(f"All Fails: {all_fails}")  
                     
    #Assign SNS Topic
    SNS_ARN = sns_topics[target.upper()].format(env.upper())
    print("SNS_ARN: " + SNS_ARN)
    # if there were any failures in the preceeding steps send sns notification and fail state function
    if all_fails:
        
        # concatenate the final sns message
        sns_msg = f'One or multiple errors occured when executing state machine: {sf_name}. \n\n' + all_fails

        sns_client = boto3.client('sns', region_name=REGION)
        response = sns_client.publish(
            TopicArn = SNS_ARN,
            Message = sns_msg,
            Subject= 'FSA-CERT-CNSV-Cons-Pymts-RAW-DM-NOTIFICATIONS'
        )
        
        
        #raise Exception(sns_msg)

    else:
        return