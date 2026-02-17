from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict


@dataclass(frozen=True)
class FsaFileChecksStateMachineInputs:
    set_running_lambda_arn: str
    transfer_file_lambda_arn: str
    finalize_job_lambda_arn: str


class FsaFileChecksStateMachineBuilder:
    """
    Pattern A (arn:aws:states:::lambda:invoke) state machine builder.

    Fix included:
      - DynamoDB Streams via EventBridge Pipes can deliver a batch (array) even with batchSize=1.
      - We add an UnwrapRecord Pass state that converts [ {...} ] -> {...}
        so downstream JSONPaths like $.jobId work reliably.
    """

    @staticmethod
    def filechecks_asl(inputs: FsaFileChecksStateMachineInputs) -> Dict[str, Any]:
        return {
            "Comment": "FSA FileChecks: set RUNNING -> transfer FTPS file to S3 -> finalize COMPLETED/ERROR",
            "StartAt": "UnwrapRecord",
            "States": {
                "UnwrapRecord": {
                    "Type": "Pass",
                    "InputPath": "$[0]",
                    "ResultPath": "$",
                    "Next": "SetRunning",
                },
                "SetRunning": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::lambda:invoke",
                    "Parameters": {
                        "FunctionName": inputs.set_running_lambda_arn,
                        "Payload": {
                            "jobId.$": "$.jobId",
                            "project.$": "$.project",
                            "table_name.$": "$.table_name",
                            "debug.$": "$.debug",
                        },
                    },
                    "ResultPath": "$.setRunning",
                    "OutputPath": "$",
                    "Next": "TransferFile",
                },
                "TransferFile": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::lambda:invoke",
                    "Parameters": {
                        "FunctionName": inputs.transfer_file_lambda_arn,
                        "Payload": {
                            "jobId.$": "$.jobId",
                            "project.$": "$.project",
                            "table_name.$": "$.table_name",
                            "bucket.$": "$.bucket",
                            "secret_id.$": "$.secret_id",
                            "verify_tls.$": "$.verify_tls",
                            "timeout_seconds.$": "$.timeout_seconds",
                            "debug.$": "$.debug",
                        },
                    },
                    "ResultPath": "$.transfer",
                    "OutputPath": "$",
                    "Next": "FinalizeJob",
                    "Catch": [
                        {
                            "ErrorEquals": ["States.ALL"],
                            "ResultPath": "$.transferError",
                            "Next": "FinalizeJobOnCatch",
                        }
                    ],
                },
                "FinalizeJob": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::lambda:invoke",
                    "Parameters": {
                        "FunctionName": inputs.finalize_job_lambda_arn,
                        "Payload": {
                            "jobId.$": "$.jobId",
                            "project.$": "$.project",
                            "table_name.$": "$.table_name",
                            "transfer.$": "$.transfer.Payload",
                            "debug.$": "$.debug",
                        },
                    },
                    "ResultPath": "$.finalize",
                    "OutputPath": "$",
                    "End": True,
                },
                "FinalizeJobOnCatch": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::lambda:invoke",
                    "Parameters": {
                        "FunctionName": inputs.finalize_job_lambda_arn,
                        "Payload": {
                            "jobId.$": "$.jobId",
                            "project.$": "$.project",
                            "table_name.$": "$.table_name",
                            "transfer": {
                                "transferStatus": "FAILURE",
                                "error.$": "$.transferError",
                            },
                            "debug.$": "$.debug",
                        },
                    },
                    "ResultPath": "$.finalize",
                    "OutputPath": "$",
                    "End": True,
                },
            },
        }
