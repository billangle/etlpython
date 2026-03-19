# CPS

CPS pipeline deploy project for Glue, Lambda, and Step Functions resources.

This project uses a deploy-time substitution and normalization approach similar to CNSV and Athenafarm:
- Glue jobs are deployed from local scripts with names derived from deployEnv and project.
- Lambda functions are deployed from folders under lambda.
- Step Functions definitions are loaded from ASL JSON files and rewritten at deploy time to point to deployed Lambda ARNs, Glue job names, child state machine ARNs, crawler names, and bucket/key values.

## Naming Convention

Resources are named using:

FSA-{deployEnv}-{project}-{ResourceName}

Examples:
- FSA-FPACDEV-CPS-LandingFiles
- FSA-FPACDEV-CPS-Raw-DM
- FSA-FPACDEV-CPS-MAIN

## Wiring Model

CPS Step Functions are wired using explicit __PLACEHOLDER__ tokens in ASL files.
At deployment, deploy.py replaces each token with environment-specific values (Lambda ARNs, Glue names, crawler names, bucket/key values, and child state machine ARNs).

## Project Layout

- deploy.py: CPS deployer
- glue/: Glue scripts
- lambda/: Lambda function source folders
- states/: ASL state machine definitions
- scripts/: local utility scripts
- deploy_config.sh: config upload helper

## Config Source of Truth

Use the config files under:

- config/cps/dev.json
- config/cps/steamdev.json
- config/cps/prod.json

The CPS deployer reads resource settings directly from these configs, especially:
- artifacts.artifactBucket and artifacts.prefix
- strparams role/bucket parameters
- stepFunctions.roleArn
- GlueConfig entries for LandingFiles and Raw-DM

## ASL Placeholder Tokens

CPS Step Function ASL files under states/ now use explicit placeholder tokens that are replaced in deploy.py at deployment time.

These tokens are intentionally visible in ASL JSON files (CNSV-style) so runtime wiring is explicit.

Common tokens currently supported:

- __LANDING_FILES_GLUE_JOB_NAME__: Glue job name for LandingFiles
- __RAW_DM_GLUE_JOB_NAME__: Glue job name for Raw-DM
- __LANDING_BUCKET__: Landing bucket from strparams.landingBucketNameParam
- __FINAL_BUCKET__: Final bucket from strparams.finalBucketNameParam
- __JOB_ID_KEY__: S3 key for job ID from strparams.jobIdKeyParam
- __DEPLOY_ENV_LOWER__: Lowercase deploy environment (for Raw-DM --env)
- __REGION__: Deployment region argument
- __SECRET_ID__: Secret id from config.secretId
- __RAW_DM_POSTGRES_PRCS_CTRL_DBNAME__: Raw-DM JobParameters[--postgres_prcs_ctrl_dbname]
- __GET_INCREMENTAL_TABLES_FN_ARN__: ARN of get-incremental-tables Lambda
- __RAW_DM_SNS_ERRORS_FN_ARN__: ARN of RAW-DM-sns-pub-step-func-errs Lambda
- __RAW_DM_ETL_WORKFLOW_UPDATE_FN_ARN__: ARN of RAW-DM-etl-workflow-update Lambda
- __JOB_LOGGING_END_FN_ARN__: ARN of Job-Logging-End Lambda
- __VALIDATION_CHECK_FN_ARN__: ARN of validation-check Lambda
- __SNS_PUBLISH_VALIDATIONS_REPORT_FN_ARN__: ARN of sns-publish-validations-report Lambda
- __MAIN_CRAWLER_NAME__: Deployed main crawler name
- __CDC_CRAWLER_NAME__: Deployed CDC crawler name
- __INCREMENTAL_TO_LANDING_SM_ARN__: ARN of Incremental-to-S3Landing state machine
- __S3LANDING_TO_RAWDM_SM_ARN__: ARN of S3Landing-to-S3Final-Raw-DM state machine
- __PROCESS_CONTROL_UPDATE_SM_ARN__: ARN of Process-Control-Update state machine
- __S3LANDING_TO_RAWDM_SM_NAME__: Name of S3Landing-to-S3Final-Raw-DM state machine

Deploy-time safety check:

- deploy.py raises an error if any __TOKEN__ placeholder remains unresolved after substitution.

## Deploy Commands

From deploy/:

Development:
python deploy.py --config config/cps/dev.json --region us-east-1 --project-type cps

Steamdev:
python deploy.py --config config/cps/steamdev.json --region us-east-1 --project-type cps

Production:
python deploy.py --config config/cps/prod.json --region us-east-1 --project-type cps

## Unit Tests

Run CPS deploy regression tests:

python -m unittest deploy.projects.cps.tests.test_deploy -v
