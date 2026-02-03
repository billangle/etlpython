# deploy/projects/cars/states/carsdm_stepfunction.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict


@dataclass(frozen=True)
class CarsDmStateMachineInputs:
    """
    Variable-driven inputs for the CARS DM Step Function.

    - Lambdas + Glue jobs remain parameterized
    - Crawlers are assumed to exist
    - deploy_env is used to build crawler *state names* (no hard-coded CERT)
    """

    # Environment
    deploy_env: str                  # e.g. "cert", "prod", "dev"

    # Lambdas
    get_incremental_tables_fn_arn: str
    sns_publish_errors_fn_arn: str
    etl_workflow_update_job_fn_arn: str

    # Glue Job
    raw_dm_glue_job_name: str

    # Static Glue args
    env: str
    postgres_prcs_ctrl_dbname: str
    region_name: str
    secret_name: str
    target_bucket: str

    # Crawlers (must already exist)
    final_zone_crawler_name: str     # e.g. "FSA-CERT-CARS-CRAWLER"
    cdc_crawler_name: str            # e.g. "FSA-CERT-CARS-cdc"

    # State machine comment
    comment: str


class CarsDmStateMachineBuilder:
    """
    Builder for the CARS DM (S3 Landing -> S3 Final Raw DM) ASL.

    Diff vs previous version:
    - Crawler *state names* are derived from deploy_env
    - No hard-coded CERT anywhere
    """

    @staticmethod
    def carsdm_asl(inputs: CarsDmStateMachineInputs) -> Dict[str, Any]:
        env_upper = inputs.deploy_env.upper()

        final_crawler_state = f"FSA-{env_upper}-CARS Final Zone Crawler"
        cdc_crawler_state = f"FSA-{env_upper}-CARS CDC Crawler"

        return {
            "Comment": inputs.comment,
            "StartAt": "Get Incremental Table list From S3",
            "States": {
                "Get Incremental Table list From S3": {
                    "Type": "Task",
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
                                    {"ErrorEquals": ["States.ALL"], "Next": "Pass"}
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
                    "Next": "Run Both Crawlers",
                },

                "Run Both Crawlers": {
                    "Type": "Parallel",
                    "Branches": [
                        {
                            "StartAt": final_crawler_state,
                            "States": {
                                final_crawler_state: {
                                    "Type": "Task",
                                    "Parameters": {
                                        "Name": inputs.final_zone_crawler_name
                                    },
                                    "Resource": "arn:aws:states:::aws-sdk:glue:startCrawler",
                                    "ResultPath": "$.CrawlerResult",
                                    "End": True,
                                    "Catch": [
                                        {
                                            "ErrorEquals": ["States.ALL"],
                                            "ResultPath": "$.CrawlerError",
                                            "Next": "Final Crawler Failed",
                                        }
                                    ],
                                },
                                "Final Crawler Failed": {
                                    "Type": "Pass",
                                    "Result": "Final Zone Crawler Failed",
                                    "ResultPath": "$.CrawlerStatus",
                                    "End": True,
                                },
                            },
                        },
                        {
                            "StartAt": cdc_crawler_state,
                            "States": {
                                cdc_crawler_state: {
                                    "Type": "Task",
                                    "Parameters": {
                                        "Name": inputs.cdc_crawler_name
                                    },
                                    "Resource": "arn:aws:states:::aws-sdk:glue:startCrawler",
                                    "ResultPath": "$.CrawlerResult",
                                    "End": True,
                                    "Catch": [
                                        {
                                            "ErrorEquals": ["States.ALL"],
                                            "ResultPath": "$.CrawlerError",
                                            "Next": "CDC Crawler Failed",
                                        }
                                    ],
                                },
                                "CDC Crawler Failed": {
                                    "Type": "Pass",
                                    "Result": "CDC Crawler Failed",
                                    "ResultPath": "$.CrawlerStatus",
                                    "End": True,
                                },
                            },
                        },
                    ],
                    "ResultPath": "$.Crawlers",
                    "End": True,
                },
            },
        }
