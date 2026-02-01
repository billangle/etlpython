from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict


@dataclass(frozen=True)
class DatamartEtlStateMachineInputs:
    """
    Provide the exact Glue Job names/arns used by the ASL.

    If you pass literal strings that match your deployed resources,
    the output ASL will be logically identical to your original definition.

    IMPORTANT:
    - These values are used as literal JobName / --JOB_NAME values (NOT States.Format).
    - Your execution input no longer needs to carry $.env just to derive the Glue job name.
    """
    exec_db_sql_glue_job_name: str      # e.g. "FSA-CERT-DATAMART-EXEC-DB-SQL"
    pg_to_redshift_glue_job_name: str   # e.g. "FSA-CERT-DART-PG-TO-REDSHIFT"


class DatamartEtlStateMachineBuilder:
    """
    Builder for the Datamart ETL Pipeline ASL.

    IMPORTANT:
    - Structural refactor only (JSON -> Python dict builder).
    - Same Map/Choice/Task/Catch/Pass flow.
    - FIXED: now uses `inputs.exec_db_sql_glue_job_name` and
      `inputs.pg_to_redshift_glue_job_name` for Glue JobName/--JOB_NAME.
    """

    @staticmethod
    def datamart_etl_asl(inputs: DatamartEtlStateMachineInputs) -> Dict[str, Any]:
        exec_sql_job = inputs.exec_db_sql_glue_job_name
        pg_to_rs_job = inputs.pg_to_redshift_glue_job_name

        return {
            "Comment": (
                "Datamart ETL Pipeline: Athena -> PostgreSQL -> Redshift. "
                "Level 1 & 2: Athena to PostgreSQL. Level 3: PostgreSQL to Redshift."
            ),
            "StartAt": "ProcessLevel1Tables",
            "States": {
                "ProcessLevel1Tables": {
                    "Type": "Map",
                    "ItemsPath": "$.level1_tables",
                    "MaxConcurrency": 5,
                    "Parameters": {
                        "table_name.$": "$$.Map.Item.Value",
                        "data_src_nm.$": "$.data_src_nm",
                        "env.$": "$.env",
                        "layer.$": "$.layer",
                        "run_type.$": "$.run_type",
                        "start_date.$": "$.start_date",
                        "source_schema.$": "$.source_schema",
                        "target_schema.$": "$.target_schema",
                    },
                    "Iterator": {
                        "StartAt": "ExecuteLevel1SQL",
                        "States": {
                            "ExecuteLevel1SQL": {
                                "Type": "Task",
                                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                                "Parameters": {
                                    # ✅ USE INPUTS (literal job name)
                                    "JobName": exec_sql_job,
                                    "Arguments": {
                                        "--JOB_NAME": exec_sql_job,
                                        "--table_name.$": "$.table_name",
                                        "--data_src_nm.$": "$.data_src_nm",
                                        "--env.$": "$.env",
                                        "--layer.$": "$.layer",
                                        "--run_type.$": "$.run_type",
                                        "--start_date.$": "$.start_date",
                                    },
                                },
                                "ResultPath": "$.glueResult",
                                "Next": "Level1TableSuccess",
                                "Catch": [
                                    {
                                        "ErrorEquals": ["States.ALL"],
                                        "Next": "Level1TableFailed",
                                        "ResultPath": "$.error",
                                    }
                                ],
                            },
                            "Level1TableSuccess": {
                                "Type": "Pass",
                                "Parameters": {
                                    "table_name.$": "$.table_name",
                                    "status": "SUCCESS",
                                    "level": 1,
                                },
                                "End": True,
                            },
                            "Level1TableFailed": {
                                "Type": "Pass",
                                "Parameters": {
                                    "table_name.$": "$.table_name",
                                    "status": "FAILED",
                                    "level": 1,
                                    "error.$": "$.error",
                                },
                                "End": True,
                            },
                        },
                    },
                    "ResultPath": "$.level1Results",
                    "Next": "CheckLevel2Tables",
                    "Catch": [
                        {
                            "ErrorEquals": ["States.ALL"],
                            "Next": "PipelineFailed",
                            "ResultPath": "$.error",
                        }
                    ],
                },

                "CheckLevel2Tables": {
                    "Type": "Choice",
                    "Choices": [
                        {
                            "Variable": "$.level2_tables[0]",
                            "IsPresent": True,
                            "Next": "ProcessLevel2Tables",
                        }
                    ],
                    "Default": "ProcessLevel3RedshiftTables",
                },

                "ProcessLevel2Tables": {
                    "Type": "Map",
                    "ItemsPath": "$.level2_tables",
                    "MaxConcurrency": 5,
                    "Parameters": {
                        "table_name.$": "$$.Map.Item.Value",
                        "data_src_nm.$": "$.data_src_nm",
                        "env.$": "$.env",
                        "layer.$": "$.layer",
                        "run_type.$": "$.run_type",
                        "start_date.$": "$.start_date",
                        "source_schema.$": "$.source_schema",
                        "target_schema.$": "$.target_schema",
                    },
                    "Iterator": {
                        "StartAt": "ExecuteLevel2SQL",
                        "States": {
                            "ExecuteLevel2SQL": {
                                "Type": "Task",
                                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                                "Parameters": {
                                    # ✅ USE INPUTS
                                    "JobName": exec_sql_job,
                                    "Arguments": {
                                        "--JOB_NAME": exec_sql_job,
                                        "--table_name.$": "$.table_name",
                                        "--data_src_nm.$": "$.data_src_nm",
                                        "--env.$": "$.env",
                                        "--layer.$": "$.layer",
                                        "--run_type.$": "$.run_type",
                                        "--start_date.$": "$.start_date",
                                    },
                                },
                                "ResultPath": "$.glueResult",
                                "Next": "Level2TableSuccess",
                                "Catch": [
                                    {
                                        "ErrorEquals": ["States.ALL"],
                                        "Next": "Level2TableFailed",
                                        "ResultPath": "$.error",
                                    }
                                ],
                            },
                            "Level2TableSuccess": {
                                "Type": "Pass",
                                "Parameters": {
                                    "table_name.$": "$.table_name",
                                    "status": "SUCCESS",
                                    "level": 2,
                                },
                                "End": True,
                            },
                            "Level2TableFailed": {
                                "Type": "Pass",
                                "Parameters": {
                                    "table_name.$": "$.table_name",
                                    "status": "FAILED",
                                    "level": 2,
                                    "error.$": "$.error",
                                },
                                "End": True,
                            },
                        },
                    },
                    "ResultPath": "$.level2Results",
                    "Next": "ProcessLevel3RedshiftTables",
                    "Catch": [
                        {
                            "ErrorEquals": ["States.ALL"],
                            "Next": "PipelineFailed",
                            "ResultPath": "$.error",
                        }
                    ],
                },

                "ProcessLevel3RedshiftTables": {
                    "Type": "Map",
                    "ItemsPath": "$.level1_tables",
                    "MaxConcurrency": 5,
                    "Parameters": {
                        "table_name.$": "$$.Map.Item.Value",
                        "data_src_nm.$": "$.data_src_nm",
                        "env.$": "$.env",
                        "run_type.$": "$.run_type",
                        "start_date.$": "$.start_date",
                        "source_schema.$": "$.source_schema",
                        "target_schema.$": "$.target_schema",
                    },
                    "Iterator": {
                        "StartAt": "ExecuteLevel3PGToRedshift",
                        "States": {
                            "ExecuteLevel3PGToRedshift": {
                                "Type": "Task",
                                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                                "Parameters": {
                                    # ✅ USE INPUTS
                                    "JobName": pg_to_rs_job,
                                    "Arguments": {
                                        "--JOB_NAME": pg_to_rs_job,
                                        "--table_name.$": "$.table_name",
                                        "--source_schema.$": "$.source_schema",
                                        "--target_schema.$": "$.target_schema",
                                        "--env.$": "$.env",
                                        "--run_type.$": "$.run_type",
                                        "--start_date.$": "$.start_date",
                                        "--data_src_nm.$": "$.data_src_nm",
                                    },
                                },
                                "ResultPath": "$.glueResult",
                                "Next": "Level3TableSuccess",
                                "Catch": [
                                    {
                                        "ErrorEquals": ["States.ALL"],
                                        "Next": "Level3TableFailed",
                                        "ResultPath": "$.error",
                                    }
                                ],
                            },
                            "Level3TableSuccess": {
                                "Type": "Pass",
                                "Parameters": {
                                    "table_name.$": "$.table_name",
                                    "status": "SUCCESS",
                                    "level": 3,
                                },
                                "End": True,
                            },
                            "Level3TableFailed": {
                                "Type": "Pass",
                                "Parameters": {
                                    "table_name.$": "$.table_name",
                                    "status": "FAILED",
                                    "level": 3,
                                    "error.$": "$.error",
                                },
                                "End": True,
                            },
                        },
                    },
                    "ResultPath": "$.level3Results",
                    "Next": "CheckLevel2ForRedshift",
                    "Catch": [
                        {
                            "ErrorEquals": ["States.ALL"],
                            "Next": "PipelineFailed",
                            "ResultPath": "$.error",
                        }
                    ],
                },

                "CheckLevel2ForRedshift": {
                    "Type": "Choice",
                    "Choices": [
                        {
                            "Variable": "$.level2_tables[0]",
                            "IsPresent": True,
                            "Next": "ProcessLevel3RedshiftLevel2Tables",
                        }
                    ],
                    "Default": "PipelineSuccess",
                },

                "ProcessLevel3RedshiftLevel2Tables": {
                    "Type": "Map",
                    "ItemsPath": "$.level2_tables",
                    "MaxConcurrency": 5,
                    "Parameters": {
                        "table_name.$": "$$.Map.Item.Value",
                        "data_src_nm.$": "$.data_src_nm",
                        "env.$": "$.env",
                        "run_type.$": "$.run_type",
                        "start_date.$": "$.start_date",
                        "source_schema.$": "$.source_schema",
                        "target_schema.$": "$.target_schema",
                    },
                    "Iterator": {
                        "StartAt": "ExecuteLevel3PGToRedshiftLevel2",
                        "States": {
                            "ExecuteLevel3PGToRedshiftLevel2": {
                                "Type": "Task",
                                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                                "Parameters": {
                                    # ✅ USE INPUTS
                                    "JobName": pg_to_rs_job,
                                    "Arguments": {
                                        "--JOB_NAME": pg_to_rs_job,
                                        "--table_name.$": "$.table_name",
                                        "--source_schema.$": "$.source_schema",
                                        "--target_schema.$": "$.target_schema",
                                        "--env.$": "$.env",
                                        "--run_type.$": "$.run_type",
                                        "--start_date.$": "$.start_date",
                                        "--data_src_nm.$": "$.data_src_nm",
                                    },
                                },
                                "ResultPath": "$.glueResult",
                                "Next": "Level3Level2TableSuccess",
                                "Catch": [
                                    {
                                        "ErrorEquals": ["States.ALL"],
                                        "Next": "Level3Level2TableFailed",
                                        "ResultPath": "$.error",
                                    }
                                ],
                            },
                            "Level3Level2TableSuccess": {
                                "Type": "Pass",
                                "Parameters": {
                                    "table_name.$": "$.table_name",
                                    "status": "SUCCESS",
                                    "level": "3-L2",
                                },
                                "End": True,
                            },
                            "Level3Level2TableFailed": {
                                "Type": "Pass",
                                "Parameters": {
                                    "table_name.$": "$.table_name",
                                    "status": "FAILED",
                                    "level": "3-L2",
                                    "error.$": "$.error",
                                },
                                "End": True,
                            },
                        },
                    },
                    "ResultPath": "$.level3Level2Results",
                    "Next": "PipelineSuccess",
                    "Catch": [
                        {
                            "ErrorEquals": ["States.ALL"],
                            "Next": "PipelineFailed",
                            "ResultPath": "$.error",
                        }
                    ],
                },

                "PipelineSuccess": {"Type": "Succeed"},
                "PipelineFailed": {
                    "Type": "Fail",
                    "Error": "PipelineExecutionFailed",
                    "Cause": "Pipeline execution failed - check logs for details",
                },
            },
        }
