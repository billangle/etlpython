from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict


@dataclass(frozen=True)
class DatamartEtlStateMachineInputs:
    """
    Provide the exact Glue Job names used by the ASL.

    IMPORTANT:
    - These values are used as literal JobName / --JOB_NAME values (NOT States.Format).
    - Your execution input does not need $.env to derive the Glue job name.
    """
    exec_db_sql_glue_job_name: str      # e.g. "FSA-PROD-DATAMART-EXEC-DB-SQL"
    pg_to_redshift_glue_job_name: str   # e.g. "FSA-PROD-DART-PG-TO-REDSHIFT"


class DatamartEtlStateMachineBuilder:
    """
    Builder for the Datamart ETL Pipeline ASL.

    Structural refactor only (JSON -> Python dict builder).
    FIXED:
      - JSON booleans `true/false` -> Python `True/False`
      - JobName / --JOB_NAME now use the provided inputs (no hard-coded env strings)
    """

    @staticmethod
    def datamart_etl_asl(inputs: DatamartEtlStateMachineInputs) -> Dict[str, Any]:
        exec_sql_job = inputs.exec_db_sql_glue_job_name
        pg_to_rs_job = inputs.pg_to_redshift_glue_job_name

        return {
            "Comment": (
                "Datamart ETL Pipeline: DM SQL Execution (4 levels) then PostgreSQL to Redshift (4 levels). "
                "Order: DIM L1 → DIM L2 → FACT → SUMM/PRMPT. Supports per-table date_column and run_type "
                "for incremental loads."
            ),
            "StartAt": "ProcessDimTables",
            "States": {
                "ProcessDimTables": {
                    "Type": "Map",
                    "ItemsPath": "$.dim_tables",
                    "MaxConcurrency": 5,
                    "Parameters": {
                        "table_name.$": "$$.Map.Item.Value.table_name",
                        "date_column.$": "$$.Map.Item.Value.date_column",
                        "data_src_nm.$": "$.data_src_nm",
                        "env.$": "$.env",
                        "layer.$": "$.layer",
                        "run_type.$": "$.run_type",
                        "start_date.$": "$.start_date",
                        "source_schema.$": "$.source_schema",
                        "target_schema.$": "$.target_schema",
                    },
                    "Iterator": {
                        "StartAt": "ExecuteDimSQL",
                        "States": {
                            "ExecuteDimSQL": {
                                "Type": "Task",
                                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                                "Parameters": {
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
                                "Next": "DimSuccess",
                                "Catch": [
                                    {
                                        "ErrorEquals": ["States.ALL"],
                                        "Next": "DimFailed",
                                        "ResultPath": "$.error",
                                    }
                                ],
                            },
                            "DimSuccess": {
                                "Type": "Pass",
                                "Parameters": {
                                    "table_name.$": "$.table_name",
                                    "status": "SUCCESS",
                                    "group": "PG-DIM-L1",
                                },
                                "End": True,
                            },
                            "DimFailed": {
                                "Type": "Pass",
                                "Parameters": {
                                    "table_name.$": "$.table_name",
                                    "status": "FAILED",
                                    "group": "PG-DIM-L1",
                                    "error.$": "$.error",
                                },
                                "End": True,
                            },
                        },
                    },
                    "ResultPath": "$.dimResults",
                    "Next": "CheckDimLevel2",
                    "Catch": [
                        {
                            "ErrorEquals": ["States.ALL"],
                            "Next": "PipelineFailed",
                            "ResultPath": "$.error",
                        }
                    ],
                },

                "CheckDimLevel2": {
                    "Type": "Choice",
                    "Choices": [
                        {
                            "Variable": "$.dim_tables_level2[0]",
                            "IsPresent": True,
                            "Next": "ProcessDimTablesLevel2",
                        }
                    ],
                    "Default": "CheckFactTables",
                },

                "ProcessDimTablesLevel2": {
                    "Type": "Map",
                    "ItemsPath": "$.dim_tables_level2",
                    "MaxConcurrency": 5,
                    "Parameters": {
                        "table_name.$": "$$.Map.Item.Value.table_name",
                        "date_column.$": "$$.Map.Item.Value.date_column",
                        "data_src_nm.$": "$.data_src_nm",
                        "env.$": "$.env",
                        "layer.$": "$.layer",
                        "run_type.$": "$.run_type",
                        "start_date.$": "$.start_date",
                        "source_schema.$": "$.source_schema",
                        "target_schema.$": "$.target_schema",
                    },
                    "Iterator": {
                        "StartAt": "ExecuteDimL2SQL",
                        "States": {
                            "ExecuteDimL2SQL": {
                                "Type": "Task",
                                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                                "Parameters": {
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
                                "Next": "DimL2Success",
                                "Catch": [
                                    {
                                        "ErrorEquals": ["States.ALL"],
                                        "Next": "DimL2Failed",
                                        "ResultPath": "$.error",
                                    }
                                ],
                            },
                            "DimL2Success": {
                                "Type": "Pass",
                                "Parameters": {
                                    "table_name.$": "$.table_name",
                                    "status": "SUCCESS",
                                    "group": "PG-DIM-L2",
                                },
                                "End": True,
                            },
                            "DimL2Failed": {
                                "Type": "Pass",
                                "Parameters": {
                                    "table_name.$": "$.table_name",
                                    "status": "FAILED",
                                    "group": "PG-DIM-L2",
                                    "error.$": "$.error",
                                },
                                "End": True,
                            },
                        },
                    },
                    "ResultPath": "$.dimL2Results",
                    "Next": "CheckFactTables",
                    "Catch": [
                        {
                            "ErrorEquals": ["States.ALL"],
                            "Next": "PipelineFailed",
                            "ResultPath": "$.error",
                        }
                    ],
                },

                "CheckFactTables": {
                    "Type": "Choice",
                    "Choices": [
                        {
                            "Variable": "$.fact_tables[0]",
                            "IsPresent": True,
                            "Next": "ProcessFactTables",
                        }
                    ],
                    "Default": "CheckSummaryTables",
                },

                "ProcessFactTables": {
                    "Type": "Map",
                    "ItemsPath": "$.fact_tables",
                    "MaxConcurrency": 5,
                    "Parameters": {
                        "table_name.$": "$$.Map.Item.Value.table_name",
                        "date_column.$": "$$.Map.Item.Value.date_column",
                        "data_src_nm.$": "$.data_src_nm",
                        "env.$": "$.env",
                        "layer.$": "$.layer",
                        "run_type.$": "$.run_type",
                        "start_date.$": "$.start_date",
                        "source_schema.$": "$.source_schema",
                        "target_schema.$": "$.target_schema",
                    },
                    "Iterator": {
                        "StartAt": "ExecuteFactSQL",
                        "States": {
                            "ExecuteFactSQL": {
                                "Type": "Task",
                                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                                "Parameters": {
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
                                "Next": "FactSuccess",
                                "Catch": [
                                    {
                                        "ErrorEquals": ["States.ALL"],
                                        "Next": "FactFailed",
                                        "ResultPath": "$.error",
                                    }
                                ],
                            },
                            "FactSuccess": {
                                "Type": "Pass",
                                "Parameters": {
                                    "table_name.$": "$.table_name",
                                    "status": "SUCCESS",
                                    "group": "PG-FACT",
                                },
                                "End": True,
                            },
                            "FactFailed": {
                                "Type": "Pass",
                                "Parameters": {
                                    "table_name.$": "$.table_name",
                                    "status": "FAILED",
                                    "group": "PG-FACT",
                                    "error.$": "$.error",
                                },
                                "End": True,
                            },
                        },
                    },
                    "ResultPath": "$.factResults",
                    "Next": "CheckSummaryTables",
                    "Catch": [
                        {
                            "ErrorEquals": ["States.ALL"],
                            "Next": "PipelineFailed",
                            "ResultPath": "$.error",
                        }
                    ],
                },

                "CheckSummaryTables": {
                    "Type": "Choice",
                    "Choices": [
                        {
                            "Variable": "$.summary_tables[0]",
                            "IsPresent": True,
                            "Next": "ProcessSummaryTables",
                        }
                    ],
                    "Default": "RedshiftDimTables",
                },

                "ProcessSummaryTables": {
                    "Type": "Map",
                    "ItemsPath": "$.summary_tables",
                    "MaxConcurrency": 5,
                    "Parameters": {
                        "table_name.$": "$$.Map.Item.Value.table_name",
                        "date_column.$": "$$.Map.Item.Value.date_column",
                        "data_src_nm.$": "$.data_src_nm",
                        "env.$": "$.env",
                        "layer.$": "$.layer",
                        "run_type.$": "$.run_type",
                        "start_date.$": "$.start_date",
                        "source_schema.$": "$.source_schema",
                        "target_schema.$": "$.target_schema",
                    },
                    "Iterator": {
                        "StartAt": "ExecuteSummarySQL",
                        "States": {
                            "ExecuteSummarySQL": {
                                "Type": "Task",
                                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                                "Parameters": {
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
                                "Next": "SummarySuccess",
                                "Catch": [
                                    {
                                        "ErrorEquals": ["States.ALL"],
                                        "Next": "SummaryFailed",
                                        "ResultPath": "$.error",
                                    }
                                ],
                            },
                            "SummarySuccess": {
                                "Type": "Pass",
                                "Parameters": {
                                    "table_name.$": "$.table_name",
                                    "status": "SUCCESS",
                                    "group": "PG-SUMMARY",
                                },
                                "End": True,
                            },
                            "SummaryFailed": {
                                "Type": "Pass",
                                "Parameters": {
                                    "table_name.$": "$.table_name",
                                    "status": "FAILED",
                                    "group": "PG-SUMMARY",
                                    "error.$": "$.error",
                                },
                                "End": True,
                            },
                        },
                    },
                    "ResultPath": "$.summaryResults",
                    "Next": "RedshiftDimTables",
                    "Catch": [
                        {
                            "ErrorEquals": ["States.ALL"],
                            "Next": "PipelineFailed",
                            "ResultPath": "$.error",
                        }
                    ],
                },

                "RedshiftDimTables": {
                    "Type": "Map",
                    "ItemsPath": "$.dim_tables",
                    "MaxConcurrency": 5,
                    "Parameters": {
                        "table_name.$": "$$.Map.Item.Value.table_name",
                        "date_column.$": "$$.Map.Item.Value.date_column",
                        "table_run_type.$": "$$.Map.Item.Value.run_type",
                        "data_src_nm.$": "$.data_src_nm",
                        "env.$": "$.env",
                        "run_type.$": "$.run_type",
                        "start_date.$": "$.start_date",
                        "source_schema.$": "$.source_schema",
                        "target_schema.$": "$.target_schema",
                    },
                    "Iterator": {
                        "StartAt": "CheckDimRunType",
                        "States": {
                            "CheckDimRunType": {
                                "Type": "Choice",
                                "Choices": [
                                    {
                                        "Variable": "$.table_run_type",
                                        "IsPresent": True,
                                        "Next": "LoadDimToRedshiftTableRunType",
                                    }
                                ],
                                "Default": "LoadDimToRedshiftGlobalRunType",
                            },
                            "LoadDimToRedshiftTableRunType": {
                                "Type": "Task",
                                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                                "Parameters": {
                                    "JobName": pg_to_rs_job,
                                    "Arguments": {
                                        "--JOB_NAME": pg_to_rs_job,
                                        "--table_name.$": "$.table_name",
                                        "--source_schema.$": "$.source_schema",
                                        "--target_schema.$": "$.target_schema",
                                        "--env.$": "$.env",
                                        "--run_type.$": "$.table_run_type",
                                        "--start_date.$": "$.start_date",
                                        "--data_src_nm.$": "$.data_src_nm",
                                        "--date_column.$": "$.date_column",
                                    },
                                },
                                "ResultPath": "$.glueResult",
                                "Next": "RsDimSuccess",
                                "Catch": [
                                    {
                                        "ErrorEquals": ["States.ALL"],
                                        "Next": "RsDimFailed",
                                        "ResultPath": "$.error",
                                    }
                                ],
                            },
                            "LoadDimToRedshiftGlobalRunType": {
                                "Type": "Task",
                                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                                "Parameters": {
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
                                        "--date_column.$": "$.date_column",
                                    },
                                },
                                "ResultPath": "$.glueResult",
                                "Next": "RsDimSuccess",
                                "Catch": [
                                    {
                                        "ErrorEquals": ["States.ALL"],
                                        "Next": "RsDimFailed",
                                        "ResultPath": "$.error",
                                    }
                                ],
                            },
                            "RsDimSuccess": {
                                "Type": "Pass",
                                "Parameters": {
                                    "table_name.$": "$.table_name",
                                    "status": "SUCCESS",
                                    "group": "RS-DIM-L1",
                                },
                                "End": True,
                            },
                            "RsDimFailed": {
                                "Type": "Pass",
                                "Parameters": {
                                    "table_name.$": "$.table_name",
                                    "status": "FAILED",
                                    "group": "RS-DIM-L1",
                                    "error.$": "$.error",
                                },
                                "End": True,
                            },
                        },
                    },
                    "ResultPath": "$.rsDimResults",
                    "Next": "CheckDimL2ForRedshift",
                    "Catch": [
                        {
                            "ErrorEquals": ["States.ALL"],
                            "Next": "PipelineFailed",
                            "ResultPath": "$.error",
                        }
                    ],
                },

                "CheckDimL2ForRedshift": {
                    "Type": "Choice",
                    "Choices": [
                        {
                            "Variable": "$.dim_tables_level2[0]",
                            "IsPresent": True,
                            "Next": "RedshiftDimTablesLevel2",
                        }
                    ],
                    "Default": "CheckFactForRedshift",
                },

                "RedshiftDimTablesLevel2": {
                    "Type": "Map",
                    "ItemsPath": "$.dim_tables_level2",
                    "MaxConcurrency": 5,
                    "Parameters": {
                        "table_name.$": "$$.Map.Item.Value.table_name",
                        "date_column.$": "$$.Map.Item.Value.date_column",
                        "table_run_type.$": "$$.Map.Item.Value.run_type",
                        "data_src_nm.$": "$.data_src_nm",
                        "env.$": "$.env",
                        "run_type.$": "$.run_type",
                        "start_date.$": "$.start_date",
                        "source_schema.$": "$.source_schema",
                        "target_schema.$": "$.target_schema",
                    },
                    "Iterator": {
                        "StartAt": "CheckDimL2RunType",
                        "States": {
                            "CheckDimL2RunType": {
                                "Type": "Choice",
                                "Choices": [
                                    {
                                        "Variable": "$.table_run_type",
                                        "IsPresent": True,
                                        "Next": "LoadDimL2ToRedshiftTableRunType",
                                    }
                                ],
                                "Default": "LoadDimL2ToRedshiftGlobalRunType",
                            },
                            "LoadDimL2ToRedshiftTableRunType": {
                                "Type": "Task",
                                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                                "Parameters": {
                                    "JobName": pg_to_rs_job,
                                    "Arguments": {
                                        "--JOB_NAME": pg_to_rs_job,
                                        "--table_name.$": "$.table_name",
                                        "--source_schema.$": "$.source_schema",
                                        "--target_schema.$": "$.target_schema",
                                        "--env.$": "$.env",
                                        "--run_type.$": "$.table_run_type",
                                        "--start_date.$": "$.start_date",
                                        "--data_src_nm.$": "$.data_src_nm",
                                        "--date_column.$": "$.date_column",
                                    },
                                },
                                "ResultPath": "$.glueResult",
                                "Next": "RsDimL2Success",
                                "Catch": [
                                    {
                                        "ErrorEquals": ["States.ALL"],
                                        "Next": "RsDimL2Failed",
                                        "ResultPath": "$.error",
                                    }
                                ],
                            },
                            "LoadDimL2ToRedshiftGlobalRunType": {
                                "Type": "Task",
                                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                                "Parameters": {
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
                                        "--date_column.$": "$.date_column",
                                    },
                                },
                                "ResultPath": "$.glueResult",
                                "Next": "RsDimL2Success",
                                "Catch": [
                                    {
                                        "ErrorEquals": ["States.ALL"],
                                        "Next": "RsDimL2Failed",
                                        "ResultPath": "$.error",
                                    }
                                ],
                            },
                            "RsDimL2Success": {
                                "Type": "Pass",
                                "Parameters": {
                                    "table_name.$": "$.table_name",
                                    "status": "SUCCESS",
                                    "group": "RS-DIM-L2",
                                },
                                "End": True,
                            },
                            "RsDimL2Failed": {
                                "Type": "Pass",
                                "Parameters": {
                                    "table_name.$": "$.table_name",
                                    "status": "FAILED",
                                    "group": "RS-DIM-L2",
                                    "error.$": "$.error",
                                },
                                "End": True,
                            },
                        },
                    },
                    "ResultPath": "$.rsDimL2Results",
                    "Next": "CheckFactForRedshift",
                    "Catch": [
                        {
                            "ErrorEquals": ["States.ALL"],
                            "Next": "PipelineFailed",
                            "ResultPath": "$.error",
                        }
                    ],
                },

                "CheckFactForRedshift": {
                    "Type": "Choice",
                    "Choices": [
                        {
                            "Variable": "$.fact_tables[0]",
                            "IsPresent": True,
                            "Next": "RedshiftFactTables",
                        }
                    ],
                    "Default": "CheckSummaryForRedshift",
                },

                "RedshiftFactTables": {
                    "Type": "Map",
                    "ItemsPath": "$.fact_tables",
                    "MaxConcurrency": 5,
                    "Parameters": {
                        "table_name.$": "$$.Map.Item.Value.table_name",
                        "date_column.$": "$$.Map.Item.Value.date_column",
                        "table_run_type.$": "$$.Map.Item.Value.run_type",
                        "data_src_nm.$": "$.data_src_nm",
                        "env.$": "$.env",
                        "run_type.$": "$.run_type",
                        "start_date.$": "$.start_date",
                        "source_schema.$": "$.source_schema",
                        "target_schema.$": "$.target_schema",
                    },
                    "Iterator": {
                        "StartAt": "CheckFactRunType",
                        "States": {
                            "CheckFactRunType": {
                                "Type": "Choice",
                                "Choices": [
                                    {
                                        "Variable": "$.table_run_type",
                                        "IsPresent": True,
                                        "Next": "LoadFactToRedshiftTableRunType",
                                    }
                                ],
                                "Default": "LoadFactToRedshiftGlobalRunType",
                            },
                            "LoadFactToRedshiftTableRunType": {
                                "Type": "Task",
                                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                                "Parameters": {
                                    "JobName": pg_to_rs_job,
                                    "Arguments": {
                                        "--JOB_NAME": pg_to_rs_job,
                                        "--table_name.$": "$.table_name",
                                        "--source_schema.$": "$.source_schema",
                                        "--target_schema.$": "$.target_schema",
                                        "--env.$": "$.env",
                                        "--run_type.$": "$.table_run_type",
                                        "--start_date.$": "$.start_date",
                                        "--data_src_nm.$": "$.data_src_nm",
                                        "--date_column.$": "$.date_column",
                                    },
                                },
                                "ResultPath": "$.glueResult",
                                "Next": "RsFactSuccess",
                                "Catch": [
                                    {
                                        "ErrorEquals": ["States.ALL"],
                                        "Next": "RsFactFailed",
                                        "ResultPath": "$.error",
                                    }
                                ],
                            },
                            "LoadFactToRedshiftGlobalRunType": {
                                "Type": "Task",
                                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                                "Parameters": {
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
                                        "--date_column.$": "$.date_column",
                                    },
                                },
                                "ResultPath": "$.glueResult",
                                "Next": "RsFactSuccess",
                                "Catch": [
                                    {
                                        "ErrorEquals": ["States.ALL"],
                                        "Next": "RsFactFailed",
                                        "ResultPath": "$.error",
                                    }
                                ],
                            },
                            "RsFactSuccess": {
                                "Type": "Pass",
                                "Parameters": {
                                    "table_name.$": "$.table_name",
                                    "status": "SUCCESS",
                                    "group": "RS-FACT",
                                },
                                "End": True,
                            },
                            "RsFactFailed": {
                                "Type": "Pass",
                                "Parameters": {
                                    "table_name.$": "$.table_name",
                                    "status": "FAILED",
                                    "group": "RS-FACT",
                                    "error.$": "$.error",
                                },
                                "End": True,
                            },
                        },
                    },
                    "ResultPath": "$.rsFactResults",
                    "Next": "CheckSummaryForRedshift",
                    "Catch": [
                        {
                            "ErrorEquals": ["States.ALL"],
                            "Next": "PipelineFailed",
                            "ResultPath": "$.error",
                        }
                    ],
                },

                "CheckSummaryForRedshift": {
                    "Type": "Choice",
                    "Choices": [
                        {
                            "Variable": "$.summary_tables[0]",
                            "IsPresent": True,
                            "Next": "RedshiftSummaryTables",
                        }
                    ],
                    "Default": "PipelineSuccess",
                },

                "RedshiftSummaryTables": {
                    "Type": "Map",
                    "ItemsPath": "$.summary_tables",
                    "MaxConcurrency": 5,
                    "Parameters": {
                        "table_name.$": "$$.Map.Item.Value.table_name",
                        "date_column.$": "$$.Map.Item.Value.date_column",
                        "table_run_type.$": "$$.Map.Item.Value.run_type",
                        "data_src_nm.$": "$.data_src_nm",
                        "env.$": "$.env",
                        "run_type.$": "$.run_type",
                        "start_date.$": "$.start_date",
                        "source_schema.$": "$.source_schema",
                        "target_schema.$": "$.target_schema",
                    },
                    "Iterator": {
                        "StartAt": "CheckSummaryRunType",
                        "States": {
                            "CheckSummaryRunType": {
                                "Type": "Choice",
                                "Choices": [
                                    {
                                        "Variable": "$.table_run_type",
                                        "IsPresent": True,
                                        "Next": "LoadSummaryToRedshiftTableRunType",
                                    }
                                ],
                                "Default": "LoadSummaryToRedshiftGlobalRunType",
                            },
                            "LoadSummaryToRedshiftTableRunType": {
                                "Type": "Task",
                                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                                "Parameters": {
                                    "JobName": pg_to_rs_job,
                                    "Arguments": {
                                        "--JOB_NAME": pg_to_rs_job,
                                        "--table_name.$": "$.table_name",
                                        "--source_schema.$": "$.source_schema",
                                        "--target_schema.$": "$.target_schema",
                                        "--env.$": "$.env",
                                        "--run_type.$": "$.table_run_type",
                                        "--start_date.$": "$.start_date",
                                        "--data_src_nm.$": "$.data_src_nm",
                                        "--date_column.$": "$.date_column",
                                    },
                                },
                                "ResultPath": "$.glueResult",
                                "Next": "RsSummarySuccess",
                                "Catch": [
                                    {
                                        "ErrorEquals": ["States.ALL"],
                                        "Next": "RsSummaryFailed",
                                        "ResultPath": "$.error",
                                    }
                                ],
                            },
                            "LoadSummaryToRedshiftGlobalRunType": {
                                "Type": "Task",
                                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                                "Parameters": {
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
                                        "--date_column.$": "$.date_column",
                                    },
                                },
                                "ResultPath": "$.glueResult",
                                "Next": "RsSummarySuccess",
                                "Catch": [
                                    {
                                        "ErrorEquals": ["States.ALL"],
                                        "Next": "RsSummaryFailed",
                                        "ResultPath": "$.error",
                                    }
                                ],
                            },
                            "RsSummarySuccess": {
                                "Type": "Pass",
                                "Parameters": {
                                    "table_name.$": "$.table_name",
                                    "status": "SUCCESS",
                                    "group": "RS-SUMMARY",
                                },
                                "End": True,
                            },
                            "RsSummaryFailed": {
                                "Type": "Pass",
                                "Parameters": {
                                    "table_name.$": "$.table_name",
                                    "status": "FAILED",
                                    "group": "RS-SUMMARY",
                                    "error.$": "$.error",
                                },
                                "End": True,
                            },
                        },
                    },
                    "ResultPath": "$.rsSummaryResults",
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
