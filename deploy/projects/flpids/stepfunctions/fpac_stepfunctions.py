from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict


@dataclass(frozen=True)
class FsaFileChecksStateMachineInputs:
    set_running_lambda_arn: str
    transfer_file_lambda_arn: str  # kept for backward compatibility / defaults
    finalize_job_lambda_arn: str


class FsaFileChecksStateMachineBuilder:
    """
    Pattern A (arn:aws:states:::lambda:invoke) state machine builder.

    Fix included:
      - DynamoDB Streams via EventBridge Pipes can deliver a batch (array) even with batchSize=1.
      - We add an UnwrapRecord Pass state that converts [ {...} ] -> {...}
        so downstream JSONPaths like $.jobId work reliably.

    Change included:
      - SetRunning returns: pipeline, echo_folder, project_name, file_pattern, echo_subfolder, lambda_arn
      - TransferFile runs the Lambda defined at runtime: $.setRunning.Payload.lambda_arn
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
                    "Next": "TransferAndProcessFile",
                },
                "TransferAndProcessFile": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::lambda:invoke",
                    "Parameters": {
                        # ✅ runtime-selected Lambda ARN (returned by SetRunning)
                        "FunctionName.$": "$.setRunning.Payload.lambda_arn",
                        "Payload": {
                            # base context (from the original event)
                            "jobId.$": "$.jobId",
                            "project.$": "$.project",
                            "table_name.$": "$.table_name",
                            "bucket.$": "$.bucket",
                            "debug.$": "$.debug",

                            # connection settings (top-level in your current event)
                            "secret_id.$": "$.secret_id",
                            "verify_tls.$": "$.verify_tls",
                            "timeout_seconds.$": "$.timeout_seconds",

                            # returned by SetRunning.Payload
                            "pipeline.$": "$.setRunning.Payload.pipeline",
                            "echo_folder.$": "$.setRunning.Payload.echo_folder",
                            "project_name.$": "$.setRunning.Payload.project_name",
                            "file_pattern.$": "$.setRunning.Payload.file_pattern",
                            "echo_subfolder.$": "$.setRunning.Payload.echo_subfolder",
                            "step.$": "$.setRunning.Payload.step",
                            "header.$": "$.setRunning.Payload.header",
                            "to_queue.$": "$.setRunning.Payload.to_queue",
                            "env.$": "$.setRunning.Payload.env",

                            # optional: pass through the chosen lambda arn for logging/auditing
                            "lambda_arn.$": "$.setRunning.Payload.lambda_arn",
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
                            "path": "SUCCESS",
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
                            "path": "CATCH",
                        },
                    },
                    "ResultPath": "$.finalize",
                    "OutputPath": "$",
                    "End": True,
                },
            },
        }

    @staticmethod
    def filechecks_stepfn_target_asl(inputs: FsaFileChecksStateMachineInputs) -> Dict[str, Any]:
        """
        Pattern B state machine builder.

        This variant is used when SetRunning returns a Step Functions ARN in
        $.setRunning.Payload.lambda_arn. It starts that child state machine
        and then finalizes the job using the same FinalizeJob lambda path.
        """
        return {
            "Comment": "FSA FileChecks (StepFn target): set RUNNING -> start child Step Function -> finalize COMPLETED/ERROR",
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
                    "Next": "StartChildStepFunction",
                },
                "StartChildStepFunction": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::states:startExecution.sync:2",
                    "Parameters": {
                        "StateMachineArn.$": "$.setRunning.Payload.lambda_arn",
                        "Input": {
                            "jobId.$": "$.jobId",
                            "project.$": "$.project",
                            "table_name.$": "$.table_name",
                            "bucket.$": "$.bucket",
                            "debug.$": "$.debug",
                            "secret_id.$": "$.secret_id",
                            "verify_tls.$": "$.verify_tls",
                            "timeout_seconds.$": "$.timeout_seconds",
                            "pipeline.$": "$.setRunning.Payload.pipeline",
                            "echo_folder.$": "$.setRunning.Payload.echo_folder",
                            "project_name.$": "$.setRunning.Payload.project_name",
                            "file_pattern.$": "$.setRunning.Payload.file_pattern",
                            "echo_subfolder.$": "$.setRunning.Payload.echo_subfolder",
                            "step.$": "$.setRunning.Payload.step",
                            "header.$": "$.setRunning.Payload.header",
                            "to_queue.$": "$.setRunning.Payload.to_queue",
                            "env.$": "$.setRunning.Payload.env",
                            "step_function_arn.$": "$.setRunning.Payload.lambda_arn"
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
                            "transfer.$": "$.transfer.Output",
                            "debug.$": "$.debug",
                            "path": "SUCCESS",
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
                            "path": "CATCH",
                        },
                    },
                    "ResultPath": "$.finalize",
                    "OutputPath": "$",
                    "End": True,
                },
            },
        }
