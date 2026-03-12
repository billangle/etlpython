from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict


@dataclass(frozen=True)
class EdvPipelineStateMachineInputs:
    """
    Provide the exact resource names/arns used by the ASL.

    If you pass the same literal strings you currently have in the JSON,
    the output ASL will be logically identical to your original definition.
    """
    # Lambdas
    build_processing_plan_fn: str  # e.g. "FSA-CERT-edv-build-processing-plan"
    check_results_fn: str          # e.g. "FSA-CERT-edv-check-results"
    finalize_pipeline_fn: str      # e.g. "FSA-CERT-edv-finalize-pipeline"
    handle_failure_fn: str         # e.g. "FSA-CERT-edv-handle-failure"

    # Glue
    exec_sql_glue_job_name: str    # e.g. "FSA-CERT-DATAMART-EXEC-DB-SQL"


class EdvPipelineStateMachineBuilder:
    """
    Builder for the EDV Pipeline state machine ASL.

    IMPORTANT:
    - This is a structural refactor only (JSON -> Python dict builder).
    - The emitted ASL preserves the original state machine behavior.
    - No UnwrapRecord, no new states, no rewiring.
    """

    @staticmethod
    def edv_pipeline_asl(inputs: EdvPipelineStateMachineInputs) -> Dict[str, Any]:
        return {
            "Comment": "EDV Pipeline: S3 Parquet → Stage → DataVault. Input: start_date",
            "StartAt": "BuildProcessingPlan",
            "States": {
                "BuildProcessingPlan": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::lambda:invoke",
                    "Parameters": {
                        "FunctionName": inputs.build_processing_plan_fn,
                        "Payload": {
                            "data_src_nm.$": "$.data_src_nm",
                            "run_type.$": "$.run_type",
                            "start_date.$": "$.start_date",
                            "env.$": "$.env",
                        },
                    },
                    "ResultSelector": {
                        "stgTables.$": "$.Payload.stgTables",
                        "dvGroups.$": "$.Payload.dvGroups",
                        "counts.$": "$.Payload.counts",
                        "data_src_nm.$": "$.Payload.data_src_nm",
                        "run_type.$": "$.Payload.run_type",
                        "start_date.$": "$.Payload.start_date",
                        "env.$": "$.Payload.env",
                    },
                    "ResultPath": "$.plan",
                    "Next": "CheckTablesExist",
                    "Catch": [
                        {
                            "ErrorEquals": ["States.ALL"],
                            "Next": "HandlePipelineFailure",
                            "ResultPath": "$.error",
                        }
                    ],
                },

                "CheckTablesExist": {
                    "Type": "Choice",
                    "Choices": [
                        {
                            "Variable": "$.plan.counts.stg",
                            "NumericEquals": 0,
                            "Next": "NoTablesToProcess",
                        }
                    ],
                    "Default": "ProcessStageTables",
                },

                "NoTablesToProcess": {
                    "Type": "Succeed",
                    "Comment": "No tables found for processing",
                },

                "ProcessStageTables": {
                    "Type": "Map",
                    "ItemsPath": "$.plan.stgTables",
                    "MaxConcurrency": 20,
                    "Parameters": {
                        "table_name.$": "$$.Map.Item.Value",
                        "data_src_nm.$": "$.plan.data_src_nm",
                        "run_type.$": "$.plan.run_type",
                        "start_date.$": "$.plan.start_date",
                        "env.$": "$.plan.env",
                        "layer": "STG",
                    },
                    "Iterator": {
                        "StartAt": "ExecuteStageSQL",
                        "States": {
                            "ExecuteStageSQL": {
                                "Type": "Task",
                                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                                "Parameters": {
                                    "JobName": inputs.exec_sql_glue_job_name,
                                    "Arguments": {
                                        "--JOB_NAME": inputs.exec_sql_glue_job_name,
                                        "--table_name.$": "$.table_name",
                                        "--data_src_nm.$": "$.data_src_nm",
                                        "--run_type.$": "$.run_type",
                                        "--start_date.$": "$.start_date",
                                        "--env.$": "$.env",
                                        "--layer.$": "$.layer",
                                    },
                                },
                                "ResultPath": "$.glueResult",
                                "Next": "StageTableSuccess",
                                "Catch": [
                                    {
                                        "ErrorEquals": ["States.ALL"],
                                        "Next": "StageTableFailed",
                                        "ResultPath": "$.error",
                                    }
                                ],
                            },

                            "StageTableSuccess": {
                                "Type": "Pass",
                                "Parameters": {
                                    "table_name.$": "$.table_name",
                                    "status": "SUCCESS",
                                    "layer": "STG",
                                },
                                "End": True,
                            },

                            "StageTableFailed": {
                                "Type": "Pass",
                                "Parameters": {
                                    "table_name.$": "$.table_name",
                                    "status": "FAILED",
                                    "layer": "STG",
                                    "error.$": "$.error",
                                },
                                "End": True,
                            },
                        },
                    },
                    "ResultPath": "$.stageResults",
                    "Next": "CheckStageResults",
                    "Catch": [
                        {
                            "ErrorEquals": ["States.ALL"],
                            "Next": "HandlePipelineFailure",
                            "ResultPath": "$.error",
                        }
                    ],
                },

                "CheckStageResults": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::lambda:invoke",
                    "Parameters": {
                        "FunctionName": inputs.check_results_fn,
                        "Payload": {
                            "layer": "STG",
                            "results.$": "$.stageResults",
                        },
                    },
                    "ResultSelector": {
                        "successCount.$": "$.Payload.successCount",
                        "failedCount.$": "$.Payload.failedCount",
                        "failedTables.$": "$.Payload.failedTables",
                        "canContinue.$": "$.Payload.canContinue",
                    },
                    "ResultPath": "$.stageCheck",
                    "Next": "ShouldProcessEDV",
                },

                "ShouldProcessEDV": {
                    "Type": "Choice",
                    "Choices": [
                        {
                            "And": [
                                {"Variable": "$.plan.counts.dv", "NumericGreaterThan": 0},
                                {"Variable": "$.stageCheck.canContinue", "BooleanEquals": True},
                            ],
                            "Next": "ProcessEDVGroups",
                        }
                    ],
                    "Default": "FinalizePipeline",
                },

                "ProcessEDVGroups": {
                    "Type": "Map",
                    "ItemsPath": "$.plan.dvGroups",
                    "MaxConcurrency": 1,
                    "Parameters": {
                        "tables.$": "$$.Map.Item.Value",
                        "groupIndex.$": "$$.Map.Item.Index",
                        "data_src_nm.$": "$.plan.data_src_nm",
                        "run_type.$": "$.plan.run_type",
                        "start_date.$": "$.plan.start_date",
                        "env.$": "$.plan.env",
                    },
                    "Iterator": {
                        "StartAt": "ProcessEDVTablesInGroup",
                        "States": {
                            "ProcessEDVTablesInGroup": {
                                "Type": "Map",
                                "ItemsPath": "$.tables",
                                "MaxConcurrency": 10,
                                "Parameters": {
                                    "table_name.$": "$$.Map.Item.Value",
                                    "groupIndex.$": "$.groupIndex",
                                    "data_src_nm.$": "$.data_src_nm",
                                    "run_type.$": "$.run_type",
                                    "start_date.$": "$.start_date",
                                    "env.$": "$.env",
                                    "layer": "EDV",
                                },
                                "Iterator": {
                                    "StartAt": "ExecuteEDVSQL",
                                    "States": {
                                        "ExecuteEDVSQL": {
                                            "Type": "Task",
                                            "Resource": "arn:aws:states:::glue:startJobRun.sync",
                                            "Parameters": {
                                                "JobName": inputs.exec_sql_glue_job_name,
                                                "Arguments": {
                                                    "--JOB_NAME": inputs.exec_sql_glue_job_name,
                                                    "--table_name.$": "$.table_name",
                                                    "--data_src_nm.$": "$.data_src_nm",
                                                    "--run_type.$": "$.run_type",
                                                    "--start_date.$": "$.start_date",
                                                    "--env.$": "$.env",
                                                    "--layer.$": "$.layer",
                                                },
                                            },
                                            "ResultPath": "$.glueResult",
                                            "Next": "EDVTableSuccess",
                                            "Catch": [
                                                {
                                                    "ErrorEquals": ["States.ALL"],
                                                    "Next": "EDVTableFailed",
                                                    "ResultPath": "$.error",
                                                }
                                            ],
                                        },

                                        "EDVTableSuccess": {
                                            "Type": "Pass",
                                            "Parameters": {
                                                "table_name.$": "$.table_name",
                                                "groupIndex.$": "$.groupIndex",
                                                "status": "SUCCESS",
                                                "layer": "EDV",
                                            },
                                            "End": True,
                                        },

                                        "EDVTableFailed": {
                                            "Type": "Pass",
                                            "Parameters": {
                                                "table_name.$": "$.table_name",
                                                "groupIndex.$": "$.groupIndex",
                                                "status": "FAILED",
                                                "layer": "EDV",
                                                "error.$": "$.error",
                                            },
                                            "End": True,
                                        },
                                    },
                                },
                                "ResultPath": "$.groupResults",
                                "End": True,
                            }
                        },
                    },
                    "ResultPath": "$.edvResults",
                    "Next": "CheckEDVResults",
                    "Catch": [
                        {
                            "ErrorEquals": ["States.ALL"],
                            "Next": "HandlePipelineFailure",
                            "ResultPath": "$.error",
                        }
                    ],
                },

                "CheckEDVResults": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::lambda:invoke",
                    "Parameters": {
                        "FunctionName": inputs.check_results_fn,
                        "Payload": {
                            "layer": "EDV",
                            "results.$": "$.edvResults",
                        },
                    },
                    "ResultSelector": {
                        "successCount.$": "$.Payload.successCount",
                        "failedCount.$": "$.Payload.failedCount",
                        "failedTables.$": "$.Payload.failedTables",
                        "canContinue.$": "$.Payload.canContinue",
                    },
                    "ResultPath": "$.edvCheck",
                    "Next": "FinalizePipeline",
                },

                "FinalizePipeline": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::lambda:invoke",
                    "Parameters": {
                        "FunctionName": inputs.finalize_pipeline_fn,
                        "Payload": {
                            "data_src_nm.$": "$.plan.data_src_nm",
                            "run_type.$": "$.plan.run_type",
                            "start_date.$": "$.plan.start_date",
                            "stageCheck.$": "$.stageCheck",
                            "edvCheck.$": "$.edvCheck",
                            "counts.$": "$.plan.counts",
                        },
                    },
                    "ResultPath": "$.finalResult",
                    "Next": "PipelineSuccess",
                    "Catch": [
                        {
                            "ErrorEquals": ["States.ALL"],
                            "Next": "HandlePipelineFailure",
                            "ResultPath": "$.error",
                        }
                    ],
                },

                "PipelineSuccess": {
                    "Type": "Succeed"
                },

                "HandlePipelineFailure": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::lambda:invoke",
                    "Parameters": {
                        "FunctionName": inputs.handle_failure_fn,
                        "Payload": {
                            "error.$": "$.error",
                            "data_src_nm.$": "$.data_src_nm",
                            "run_type.$": "$.run_type",
                            "start_date.$": "$.start_date",
                        },
                    },
                    "Next": "PipelineFailed",
                },

                "PipelineFailed": {
                    "Type": "Fail",
                    "Error": "PipelineExecutionFailed",
                    "Cause": "Pipeline execution failed - check logs for details",
                },
            },
        }
