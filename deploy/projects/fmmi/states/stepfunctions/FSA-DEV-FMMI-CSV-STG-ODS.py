{
  "Comment": "FMMI Pipeline with safe defaults and preserved input (PROD)",
  "StartAt": "SaveInput",
  "States": {
    "SaveInput": {
      "Type": "Pass",
      "ResultPath": "$.OriginalInput",
      "Next": "CheckSkipLanding"
    },
    "CheckSkipLanding": {
      "Type": "Choice",
      "Choices": [
        {
          "And": [
            {
              "Variable": "$.OriginalInput.use_existing_landing",
              "IsPresent": true
            },
            {
              "Variable": "$.OriginalInput.use_existing_landing",
              "BooleanEquals": true
            }
          ],
          "Next": "CheckFileInputs"
        }
      ],
      "Default": "RunEchoLanding"
    },
    "RunEchoLanding": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": {
        "JobName": "FSA-PROD-FMMI-LandingFiles"
      },
      "ResultPath": "$.LandingResult",
      "Next": "CheckNoFilesLambda",
      "Retry": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "IntervalSeconds": 60,
          "MaxAttempts": 3,
          "BackoffRate": 2
        }
      ],
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "Next": "FailureSNS"
        }
      ]
    },
    "CheckNoFilesLambda": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:us-east-1:253490756794:function:FSA-PROD-FMMI-Check-ODS-NoFiles",
      "ResultPath": "$.NoFilesCheck",
      "Next": "CheckNoFilesChoice",
      "Retry": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "IntervalSeconds": 30,
          "MaxAttempts": 3,
          "BackoffRate": 2
        }
      ],
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "Next": "FailureSNS"
        }
      ]
    },
    "CheckNoFilesChoice": {
      "Type": "Choice",
      "Choices": [
        {
          "Variable": "$.NoFilesCheck.no_files",
          "BooleanEquals": true,
          "Next": "SuccessSNS_NoFiles"
        }
      ],
      "Default": "CheckFileInputs"
    },
    "CheckFileInputs": {
      "Type": "Choice",
      "Choices": [
        {
          "Variable": "$.OriginalInput.file_name",
          "IsPresent": true,
          "Next": "InitArgsWithFileName"
        }
      ],
      "Default": "InitArgsNoFiles"
    },
    "InitArgsWithFileName": {
      "Type": "Pass",
      "ResultPath": "$.GlueArgs",
      "Parameters": {
        "JobName": "FSA-PROD-FMMI-S3-STG-ODS-parquet",
        "Arguments": {
          "--job_type.$": "$.OriginalInput.job_type",
          "--env": "PROD",
          "--bucket_name": "c108-prod-fpacfsa-landing-zone",
          "--folder_name": "fmmi/fmmi_ocfo_files",
          "--stg_bucket_name": "c108-prod-fpacfsa-cleansed-zone",
          "--stg_folder_name": "fmmi_stg",
          "--ods_bucket_name": "c108-prod-fpacfsa-final-zone",
          "--ods_folder_name": "fmmi_ods",
          "--run_date.$": "$.OriginalInput.run_date",
          "--file_name.$": "$.OriginalInput.file_name"
        }
      },
      "Next": "RunGlue"
    },
    "InitArgsNoFiles": {
      "Type": "Pass",
      "ResultPath": "$.GlueArgs",
      "Parameters": {
        "JobName": "FSA-PROD-FMMI-S3-STG-ODS-parquet",
        "Arguments": {
          "--job_type": "BOTH",
          "--env": "PROD",
          "--bucket_name": "c108-prod-fpacfsa-landing-zone",
          "--folder_name": "fmmi/fmmi_ocfo_files",
          "--stg_bucket_name": "c108-prod-fpacfsa-cleansed-zone",
          "--stg_folder_name": "fmmi_stg",
          "--ods_bucket_name": "c108-prod-fpacfsa-final-zone",
          "--ods_folder_name": "fmmi_ods"
        }
      },
      "Next": "RunGlue"
    },
    "RunGlue": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": {
        "JobName.$": "$.GlueArgs.JobName",
        "Arguments.$": "$.GlueArgs.Arguments"
      },
      "ResultPath": "$.GlueResult",
      "Next": "RunCrawler",
      "Retry": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "IntervalSeconds": 60,
          "MaxAttempts": 3,
          "BackoffRate": 2
        }
      ],
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "Next": "FailureSNS"
        }
      ]
    },
    "RunCrawler": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:us-east-1:253490756794:function:FSA-PROD-FMMI-ODS-Crawler",
      "ResultPath": "$.CrawlerResult",
      "Next": "SuccessSNS",
      "Retry": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "IntervalSeconds": 60,
          "MaxAttempts": 3,
          "BackoffRate": 2
        }
      ],
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "Next": "FailureSNS"
        }
      ]
    },
    "SuccessSNS_NoFiles": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "arn:aws:sns:us-east-1:253490756794:FSA-PROD-FMMI",
        "Message": {
          "status": "SUCCESS",
          "env": "PROD",
          "message": "No files found. Pipeline skipped."
        }
      },
      "End": true
    },
    "SuccessSNS": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "arn:aws:sns:us-east-1:253490756794:FSA-PROD-FMMI",
        "Message": {
          "status": "SUCCESS",
          "env": "PROD",
          "glue_result.$": "$.GlueResult"
        }
      },
      "End": true
    },
    "FailureSNS": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "arn:aws:sns:us-east-1:253490756794:FSA-PROD-FMMI",
        "Message": {
          "status": "FAILED",
          "env": "PROD",
          "error.$": "$.Error"
        }
      },
      "End": true
    }
  }
}
