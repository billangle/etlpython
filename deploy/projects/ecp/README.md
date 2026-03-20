# ECP

ECP pipeline deploy project for Glue, Lambda, and Step Functions resources.

This project uses a deploy-time substitution and normalization approach similar to CNSV and Athenafarm:
- Glue jobs are deployed from local scripts with names derived from deployEnv and project.
- Lambda functions are deployed from folders under lambda.
- Step Functions definitions are loaded from ASL JSON files and rewritten at deploy time to point to deployed Lambda ARNs, Glue job names, child state machine ARNs, crawler names, and bucket/key values.

## Naming Convention

Resources are named using:

FSA-{deployEnv}-{project}-{ResourceName}

Examples:
- FSA-FPACDEV-ECP-LandingFiles
- FSA-FPACDEV-ECP-Raw-DM
- FSA-FPACDEV-ECP-MAIN

## Wiring Model

ECP Step Functions are wired using explicit __PLACEHOLDER__ tokens in ASL files.
At deployment, deploy.py replaces each token with environment-specific values (Lambda ARNs, Glue names, crawler names, bucket/key values, and child state machine ARNs).

## Project Layout

- deploy.py: ECP deployer
- glue/: Glue scripts
- lambda/: Lambda function source folders
- states/: ASL state machine definitions
- scripts/: local utility scripts
- deploy_config.sh: config upload helper

## Master vs Local Deployer

There are two deploy entry points involved for ECP:

- Master deployer: deploy/deploy.py
- ECP local deployer: deploy/projects/ecp/deploy.py

Relationship:

- The master deployer is a router/orchestrator for all projects.
- When called with --project-type ecp, it imports and calls projects.ecp.deploy.deploy(cfg, region).
- The ECP local deployer performs all ECP-specific work (Glue, Lambda, Crawler, and Step Function deployment, including ASL token replacement).

Practical usage:

- Standard entrypoint (recommended):
	python deploy.py --config config/ecp/steamdev.json --region us-east-1 --project-type ecp
- Direct ECP module execution (for focused debugging):
	python projects/ecp/deploy.py --config config/ecp/steamdev.json --region us-east-1

## Config Source of Truth

Use the config files under:

- config/ecp/dev.json
- config/ecp/steamdev.json
- config/ecp/prod.json

The ECP deployer reads resource settings directly from these configs, especially:
- artifacts.artifactBucket and artifacts.prefix
- strparams role/bucket parameters
- stepFunctions.roleArn
- GlueConfig entries for LandingFiles and Raw-DM

## Config Requirements (dev.json and prod.json)

The following requirements apply to both:

- config/ecp/dev.json
- config/ecp/prod.json

Required top-level keys:

- deployEnv: environment identifier used in resource naming (for example FPACDEV or PROD)
- project: must resolve to ECP naming context
- artifacts.artifactBucket: S3 bucket for deployed Glue scripts
- artifacts.prefix: object prefix for deployment artifacts (for example ecp/)
- stepFunctions.roleArn: IAM role ARN for Step Functions state machines
- secretId: secret used by ECP lambdas/Glue job arguments

Required strparams keys:

- strparams.glueJobRoleArnParam: IAM role ARN for Glue jobs
- strparams.etlRoleArnParam: IAM role ARN for Lambda functions
- strparams.landingBucketNameParam: landing-zone bucket used in Step Functions inputs
- strparams.finalBucketNameParam: final-zone bucket used by Raw-DM Glue job args
- strparams.jobIdKeyParam: S3 object key for the job id file (for example ecp/etl-jobs/job_id.json)

Strongly recommended strparams keys:

- strparams.snsArnParam: used by sns-related lambdas

Required GlueConfig structure:

- GlueConfig must include LandingFiles and Raw-DM entries.
- Each entry should define operational settings (worker type/count, timeout, retries, connections) and JobParameters.
- Raw-DM JobParameters should include values consumed by ASL token replacement, especially:
	- --postgres_prcs_ctrl_dbname
	- --env
	- --region_name
	- --secret_name
	- --target_bucket

Required LambdaConfig entries:

- Job-Logging-End
- RAW-DM-etl-workflow-update
- RAW-DM-sns-pub-step-func-errs
- get-incremental-tables
- sns-publish-validations-report
- validation-check

Each LambdaConfig block should define, at minimum:

- runtime
- timeoutSeconds
- memoryMb
- layers (can be empty list)

Recommended LambdaConfig options (per function as needed):

- environmentVariables (for SNS_ARN, SecretId, CRAWLER_NAME, source_folder)
- networking.subnetIds and networking.securityGroupIds for VPC-enabled lambdas

Required crawlers structure:

- crawlers should define both main and cdc crawler entries.
- Each crawler should provide:
	- crawlerName (supports tokenized form such as FSA-{deployEnv}-{projectName})
	- databaseName
	- recrawlBehavior
	- target paths via s3TargetsByBucket or targetS3Path

Validation behavior in deploy.py:

- Deployment fails fast if required keys are missing (artifact bucket, role ARNs, landing/final buckets, job id key).
- ASL deployment fails if any __TOKEN__ placeholder remains unresolved.

## ASL Placeholder Tokens

ECP Step Function ASL files under states/ now use explicit placeholder tokens that are replaced in deploy.py at deployment time.

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
- __INCREMENTAL_TO_LANDING_SM_NAME__: Name of Incremental-to-S3Landing state machine (used for MAIN StartAt/state label)
- __S3LANDING_TO_RAWDM_SM_ARN__: ARN of S3Landing-to-S3Final-Raw-DM state machine
- __PROCESS_CONTROL_UPDATE_SM_ARN__: ARN of Process-Control-Update state machine
- __S3LANDING_TO_RAWDM_SM_NAME__: Name of S3Landing-to-S3Final-Raw-DM state machine

Deploy-time safety check:

- deploy.py raises an error if any __TOKEN__ placeholder remains unresolved after substitution.

## Deploy Commands

From deploy/:

Development:
python deploy.py --config config/ecp/dev.json --region us-east-1 --project-type ecp

Steamdev:
python deploy.py --config config/ecp/steamdev.json --region us-east-1 --project-type ecp

Production:
python deploy.py --config config/ecp/prod.json --region us-east-1 --project-type ecp

## Unit Tests

Run ECP deploy regression tests:

python -m unittest deploy.projects.ecp.tests.test_deploy -v
