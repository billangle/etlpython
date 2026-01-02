from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict


@dataclass(frozen=True)
class FpacStateMachineInputs:
    """
    Everything needed to build the ASL definitions.
    """
    validate_lambda_arn: str
    create_id_lambda_arn: str
    log_results_lambda_arn: str

    glue_step1_job_name: str
    glue_step2_job_name: str
    glue_step3_job_name: str

    crawler_name: str


class FpacStateMachineBuilder:
    """
    Central place for Step Functions ASL definitions for the FPAC pipeline.
    Keeps deployer focused on 'wiring' + resource creation.
    """

    @staticmethod
    def step1_asl(inputs: FpacStateMachineInputs) -> Dict[str, Any]:
        return {
            "StartAt": "ValidateInput",
            "States": {
                "ValidateInput": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::lambda:invoke",
                    "Parameters": {"FunctionName": inputs.validate_lambda_arn, "Payload.$": "$"},
                    "OutputPath": "$.Payload",
                    "Next": "CreateNewId",
                },
                "CreateNewId": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::lambda:invoke",
                    "Parameters": {"FunctionName": inputs.create_id_lambda_arn, "Payload.$": "$"},
                    "OutputPath": "$.Payload",
                    "Next": "Step1GlueJob",
                },
                "Step1GlueJob": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::glue:startJobRun.sync",
                    "Parameters": {"JobName": inputs.glue_step1_job_name},
                    "ResultPath": "$.glueResult",
                    "End": True,
                },
            },
        }

    @staticmethod
    def step2_asl(inputs: FpacStateMachineInputs) -> Dict[str, Any]:
        return {
            "StartAt": "Step2GlueJob",
            "States": {
                "Step2GlueJob": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::glue:startJobRun.sync",
                    "Parameters": {"JobName": inputs.glue_step2_job_name},
                    "ResultPath": "$.glueResult",
                    "Next": "Step3GlueJob",
                },
                "Step3GlueJob": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::glue:startJobRun.sync",
                    "Parameters": {"JobName": inputs.glue_step3_job_name},
                    "ResultPath": "$.glueResult",
                    "Next": "LogGlueResults",
                },
                "LogGlueResults": {
                    "Type": "Pass",
                    "Parameters": {
                        "jobDetails.$": "$.glueResult",
                        "timestamp.$": "$$.State.EnteredTime",
                    },
                    "ResultPath": "$.logged",
                    "Next": "FinalLogResults",
                },
                "FinalLogResults": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::lambda:invoke",
                    "Parameters": {"FunctionName": inputs.log_results_lambda_arn, "Payload.$": "$"},
                    "OutputPath": "$.Payload",
                    "Next": "StartCrawler",
                },
                "StartCrawler": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::aws-sdk:glue:startCrawler",
                    "Parameters": {"Name": inputs.crawler_name},
                    "ResultPath": "$.crawlerResult",
                    "Next": "WasGlueSuccessful",
                },
                "WasGlueSuccessful": {
                    "Type": "Choice",
                    "Choices": [
                        {
                            "Variable": "$.logged.jobDetails.JobRunState",
                            "StringEquals": "SUCCEEDED",
                            "Next": "Success",
                        }
                    ],
                    "Default": "Fail",
                },
                "Success": {"Type": "Succeed"},
                "Fail": {"Type": "Fail"},
            },
        }

    @staticmethod
    def parent_asl(step1_sm_arn: str, step2_sm_arn: str) -> Dict[str, Any]:
        return {
            "StartAt": "Run Step1",
            "States": {
                "Run Step1": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::states:startExecution.sync:2",
                    "ResultPath": "$.step1Result",
                    "Parameters": {"Input.$": "$", "StateMachineArn": step1_sm_arn},
                    "Next": "Run Step2",
                },
                "Run Step2": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::states:startExecution.sync:2",
                    "ResultPath": "$.step2Result",
                    "Parameters": {"Input.$": "$", "StateMachineArn": step2_sm_arn},
                    "End": True,
                },
            },
        }
