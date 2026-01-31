# deploy/projects/cars/states/carsdm_stepfunction.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict


@dataclass(frozen=True)
class CarsDmStateMachineInputs:
    """
    Provide the exact resource names/arns used by the ASL.

    If you pass the same literal strings you currently have in the JSON,
    the output ASL will be logically identical to your original definition.
    """

    # Lambdas
    get_incremental_tables_fn_arn: str  # e.g. "arn:aws:lambda:us-east-1:253490756794:function:FSA-PROD-CARS-get-incremental-tables"
    sns_publish_errors_fn_arn: str      # e.g. "arn:aws:lambda:us-east-1:253490756794:function:FSA-PROD-CARS-RAW-DM-sns-publish-step-function-errors"
    etl_workflow_update_job_fn_arn: str # e.g. "arn:aws:lambda:us-east-1:253490756794:function:FSA-PROD-CARS-RAW-DM-etl-workflow-update-data-ppln-job"

    # Glue Job
    raw_dm_glue_job_name: str           # e.g. "FSA-PROD-Cars-Raw-DM"

    # Static arguments used by the Glue job in the ASL
    env: str                            # e.g. "prod"
    postgres_prcs_ctrl_dbname: str      # e.g. "metadata_edw"
    region_name: str                    # e.g. "us-east-1"
    secret_name: str                    # e.g. "FSA-PROD-secrets"
    target_bucket: str                  # e.g. "c108-prod-fpacfsa-final-zone"


class CarsDmStateMachineBuilder:
    """
    Builder for the CARS DM (S3 Landing -> S3 Final Raw DM) state machine ASL.

    IMPORTANT:
    - This is a structural refactor only (JSON -> Python dict builder).
    - The emitted ASL preserves the original state machine behavior.
    - Crawler is intentionally left "as-is" per instructions.
    """

    @staticmethod
    def carsdm_asl(inputs: CarsDmStateMachineInputs) -> Dict[str, Any]:
        return {
            "Comment": "FSA-PROD-Cars-S3Landing-to-S3Final-Raw-DM",
            "StartAt": "Get Incremental Table list From S3",
            "States": {
                "Get Incremental Table list From S3": {
                    "Type": "Task",
                    # NOTE: Preserving the *original* direct Lambda ARN task resource (not lambda:invoke integration)
                    "Resource": inputs.get_incremental_tables_fn_arn,
                    "Parameters": {
                        "JobId.$": "$.JobId",
                        "SfName.$": "$.SfName",
                    },
                    "ResultPath": "$.incremental_tables",
                    "Next": "CARS Tables",
                },

                "CARS Tables": {
                    "Type": "Pass",
                    "Next": "Map",
                    "Parameters": {
                        "JobId.$": "$.JobId",
                        "SfName.$": "$.SfName",
                        "Target": "cars",
                        "tables.$": "$.incremental_tables.tables",
                    },
                    "ResultPath": "$.input",
                },

                "Map": {
                    "Type": "Map",
                    "ItemProcessor": {
                        "ProcessorConfig": {"Mode": "INLINE"},
                        "StartAt": "Pass (1)",
                        "States": {
                            "Pass (1)": {
                                "Type": "Pass",
                                "Next": "Move to Final Zone",
                                "ResultPath": "$.output",
                                "Parameters": {
                                    "JobId.$": "$.JobId",
                                    "table.$": "$.app_name.tablename",
                                    "Target.$": "$.Target",
                                },
                            },

                            "Move to Final Zone": {
                                "Type": "Task",
                                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                                "Parameters": {
                                    "JobName": inputs.raw_dm_glue_job_name,
                                    "Arguments": {
                                        "--JobId.$": "States.Format('{}', $.JobId)",
                                        "--TableName.$": "$.output.table",
                                        "--env": inputs.env,
                                        "--postgres_prcs_ctrl_dbname": inputs.postgres_prcs_ctrl_dbname,
                                        "--region_name": inputs.region_name,
                                        "--secret_name": inputs.secret_name,
                                        "--target_bucket": inputs.target_bucket,
                                        "--target_prefix.$": "$.Target",
                                    },
                                },
                                "Catch": [
                                    {
                                        "ErrorEquals": ["States.ALL"],
                                        "Next": "SNS Lambda RAW DM",
                                        "ResultPath": "$.Error",
                                    }
                                ],
                                "End": True,
                            },

                            "SNS Lambda RAW DM": {
                                "Type": "Task",
                                "Resource": "arn:aws:states:::lambda:invoke",
                                "Parameters": {
                                    "FunctionName": inputs.sns_publish_errors_fn_arn,
                                    "Payload.$": "$",
                                },
                                "Retry": [
                                    {
                                        "ErrorEquals": [
                                            "Lambda.ServiceException",
                                            "Lambda.AWSLambdaException",
                                            "Lambda.SdkClientException",
                                            "Lambda.TooManyRequestsException",
                                        ],
                                        "IntervalSeconds": 1,
                                        "MaxAttempts": 3,
                                        "BackoffRate": 2,
                                    }
                                ],
                                "Next": "Pass",
                                "Catch": [
                                    {
                                        "ErrorEquals": ["States.ALL"],
                                        "Next": "Pass",
                                    }
                                ],
                            },

                            "Pass": {
                                "Type": "Pass",
                                "ResultPath": "$.status",
                                "Result": "Error",
                                "End": True,
                            },
                        },
                    },
                    "Next": "data_ppl_job_update RAW DM",
                    "MaxConcurrency": 12,
                    "ItemsPath": "$.input.tables",
                    "ItemSelector": {
                        "JobId.$": "$.JobId",
                        "app_name.$": "$$.Map.Item.Value",
                        "Target.$": "$.input.Target",
                    },
                    "ResultPath": "$.MapOut",
                },

                "data_ppl_job_update RAW DM": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::lambda:invoke",
                    "Parameters": {
                        "Payload.$": "$",
                        "FunctionName": inputs.etl_workflow_update_job_fn_arn,
                    },
                    "Retry": [
                        {
                            "ErrorEquals": [
                                "Lambda.ServiceException",
                                "Lambda.AWSLambdaException",
                                "Lambda.SdkClientException",
                                "Lambda.TooManyRequestsException",
                            ],
                            "IntervalSeconds": 1,
                            "BackoffRate": 2,
                            "MaxAttempts": 3,
                        }
                    ],
                    "ResultPath": "$.Update",
                    "Next": "FSA-PROD-CARS Crawler",
                },

                # Per instructions: leave crawler alone (name + integration preserved)
                "FSA-PROD-CARS Crawler": {
                    "Type": "Task",
                    "Parameters": {"Name": "FSA-PROD-CARS-CRAWLER"},
                    "Resource": "arn:aws:states:::aws-sdk:glue:startCrawler",
                    "ResultPath": "$.Crawler",
                    "OutputPath": "$.Crawler",
                    "End": True,
                },
            },
        }
