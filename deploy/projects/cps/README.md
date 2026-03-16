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

## Important CPS Matching Rule

CPS Lambda folder names are shortened and may not match the full historical names embedded in ASL files.

During deploy-time ASL rewrite, matching is performed after removing the FSA-{env}-{project}- prefix and normalizing punctuation/case. If no exact normalized match exists, prefix matching is used with the first 6 characters (fallback 5).

This allows mappings such as:
- RAW-DM-sns-publish-step-function-errors -> RAW-DM-sns-pub-step-func-errs
- RAW-DM-etl-workflow-update-data-ppln-job -> RAW-DM-etl-workflow-update

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
