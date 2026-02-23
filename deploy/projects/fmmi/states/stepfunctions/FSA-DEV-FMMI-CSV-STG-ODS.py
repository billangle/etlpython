{
  "Comment": "FMMI Pipeline with safe defaults and preserved input (DEV)",
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
      "Default": "InitLandingArgs"
    },
    "InitLandingArgs": {
      "Type": "Pass",
      "ResultPath": "$.LandingArgs",
      "Parameters": {
        "JobName": "FSA-DEV-FMMI-LandingFiles",
        "Arguments": {
          "--JOB_NAME": "FSA-DEV-FMMI-LandingFiles",
          "--SecretId": "FSA-DEV-secrets",
          "--PipelineName": "fmmi",
          "--DestinationBucket": "c108-dev-fpacfsa-landing-zone",
          "--DestinationPrefix": "fmmi/fmmi_ocfo_files",
          "--EchoFolder": "fmmi",
          "--FileNames": "FMMI.FSA*"
        }
      },
      "Next": "RunEchoLanding"
    },
    "RunEchoLanding": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": {
        "JobName.$": "$.LandingArgs.JobName",
        "Arguments.$": "$.LandingArgs.Arguments"
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
      "Resource": "arn:aws:lambda:us-east-1:241533156429:function:Check-FMMI_ODS-NoFiles",
      "ResultPath": "$.NoFilesCheck",
      "Next": "CheckNoFilesChoice",
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
        "JobName": "FSA-DEV-FMMI-S3-STG-ODS-parquet",
        "Arguments": {
          "--JOB_NAME": "FSA-DEV-FMMI-S3-STG-ODS-parquet",
          "--SecretId": "FSA-DEV-secrets",
          "--PipelineName": "fmmi",
          "--job_type.$": "$.OriginalInput.job_type",
          "--env": "dev",
          "--bucket_name": "c108-dev-fpacfsa-landing-zone",
          "--folder_name": "fmmi/fmmi_ocfo_files",
          "--stg_bucket_name": "c108-dev-fpacfsa-cleansed-zone",
          "--stg_folder_name": "fmmi_stg",
          "--ods_bucket_name": "c108-dev-fpacfsa-final-zone",
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
        "JobName": "FSA-DEV-FMMI-S3-STG-ODS-parquet",
        "Arguments": {
          "--JOB_NAME": "FSA-DEV-FMMI-S3-STG-ODS-parquet",
          "--SecretId": "FSA-DEV-secrets",
          "--PipelineName": "fmmi",
          "--job_type": "BOTH",
          "--env": "dev",
          "--bucket_name": "c108-dev-fpacfsa-landing-zone",
          "--folder_name": "fmmi/fmmi_ocfo_files",
          "--stg_bucket_name": "c108-dev-fpacfsa-cleansed-zone",
          "--stg_folder_name": "fmmi_stg",
          "--ods_bucket_name": "c108-dev-fpacfsa-final-zone",
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
      "Resource": "arn:aws:lambda:us-east-1:241533156429:function:FMMI_ODS-Crawler",
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
        "TopicArn": "arn:aws:sns:us-east-1:241533156429:FSA-DEV-FMMI",
        "Message": {
          "status": "SUCCESS",
          "message": "No files found. Pipeline skipped."
        }
      },
      "End": true
    },
    "SuccessSNS": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "arn:aws:sns:us-east-1:241533156429:FSA-DEV-FMMI",
        "Message": {
          "status": "SUCCESS",
          "glue_result.$": "$.GlueResult"
        }
      },
      "End": true
    },
    "FailureSNS": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "arn:aws:sns:us-east-1:241533156429:FSA-DEV-FMMI",
        "Message": {
          "status": "FAILED",
          "error.$": "$.Error"
        }
      },
      "End": true
    }
  }
}