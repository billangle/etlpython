# ECP Lambda Configuration Remediation Summary

This document captures the hardcoded configuration remediation completed for ECP Lambda functions.

## Goal

Identify hardcoded environment-specific values in ECP Lambda handlers and remediate them so runtime values are supplied through Lambda environment variables.

## Scope

The following Lambda handlers were reviewed and updated:

- RAW-DM-sns-pub-step-func-errs
- RAW-DM-etl-workflow-update
- Job-Logging-End
- get-incremental-tables
- sns-publish-validations-report

## Hardcoded Values Found and Remediation

| Lambda function | Value found | Remediation |
|---|---|---|
| RAW-DM-sns-pub-step-func-errs | arn:aws:sns:us-east-1:241533156429:FSA-DEV-ECP | Replaced with env var SNS_ARN, hardcoded per environment in config files. |
| RAW-DM-sns-pub-step-func-errs | FSA-DEV-ECP-S3Landing-to-S3Final-Raw-DM | Replaced with env var STATE_MACHINE_NAME (event SfName still takes precedence when present). |
| RAW-DM-sns-pub-step-func-errs | FSA-DEV-ECP-RAW-DM-NOTIFICATIONS | Replaced with env var SNS_SUBJECT, hardcoded per environment in config files. |
| RAW-DM-etl-workflow-update | FSA-DEV-secrets | Replaced with env var SecretId, hardcoded per environment in config files. |
| RAW-DM-etl-workflow-update | FSA-DEV-ECP (last_cplt_data_load_tgt_nm marker) | Replaced with env var LAST_COMPLETE_TARGET, hardcoded per environment in config files. |
| RAW-DM-etl-workflow-update | FSA-DEV-ECP-Step2 (error text) | Replaced with env var STEP2_NAME, hardcoded per environment in config files. |
| Job-Logging-End | FSA-DEV-ECP (comparison marker in status check) | Replaced with env var LAST_COMPLETE_TARGET, hardcoded per environment in config files. |
| get-incremental-tables | Dynamic bucket formula c108-{env}-fpacfsa-landing-zone | Added env var LANDING_BUCKET as primary source. Legacy derivation is retained only as fallback when env var is missing. |
| sns-publish-validations-report | PROD suffix in SNS subject | Replaced with env var REPORT_ENV_LABEL, hardcoded per environment in config files. |

## Config Files Updated

Environment-specific Lambda environment variables were explicitly set in:

- dev.json
- steamdev.json
- prod.json

These values are intentionally hardcoded per environment (no token placeholders).

## Deployment Wiring Update

ECP deploy logic was updated to support and default these environment variables at deployment time, while allowing explicit values from LambdaConfig to remain authoritative.

## Notes

- This remediation focused on Lambda runtime configuration values, not Glue job parameter cleanup.
- Existing runtime behavior was preserved where possible; only configuration sourcing was changed.
