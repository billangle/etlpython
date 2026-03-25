# Nightly AWS DMS CDC to S3 at 1:00 AM Eastern

## Overview

This package implements a practical pattern for producing a **single nightly file of incremental changes** from an AWS DMS task targeting Amazon S3.

The design assumes:

- the AWS DMS task remains in **CDC mode**
- **EventBridge Scheduler** triggers execution every day at **1:00 AM America/New_York**
- a **Lambda wrapper** starts the DMS task and dynamically computes the **next 1:00 AM Eastern cutoff**
- the DMS task stops at that cutoff by using **`CdcStopPosition`**
- the S3 target endpoint is configured so **file size does not split the output early**

## Solution contents

- `lambda_function.py` – Lambda wrapper that starts or resumes a DMS CDC task and sets the next stop time
- `create_schedule.sh` – AWS CLI example to create the EventBridge Scheduler schedule
- `iam_scheduler_trust.json` – trust policy for the Scheduler execution role
- `iam_scheduler_invoke_lambda_policy.json` – permissions policy for Scheduler to invoke the Lambda
- `iam_lambda_execution_policy.json` – permissions policy for the Lambda to control the DMS task
- `s3_endpoint_settings.json` – recommended S3 endpoint settings example for nightly CDC batching
- `first_run_example.sh` – example of the initial DMS task bootstrap run

## Architecture

1. **EventBridge Scheduler** fires every day at **1:00 AM Eastern**.
2. Scheduler invokes the **Lambda wrapper**.
3. Lambda computes the **next 1:00 AM Eastern** in UTC.
4. Lambda calls AWS DMS `StartReplicationTask`:
   - first run: `start-replication` with `CdcStartPosition`
   - later runs: `resume-processing`
5. Lambda passes `CdcStopPosition` for the next cutoff.
6. DMS captures incremental changes during the day.
7. At the stop point, DMS ends the run and flushes the CDC batch to S3.

## Key configuration notes

### 1. DMS task type

Use a **CDC-only** task if the initial full load is already complete or not needed.

### 2. Time zone handling

The schedule is defined with:

- `cron(0 1 * * ? *)`
- `America/New_York`

This keeps the schedule aligned to Eastern time, including daylight saving transitions.

### 3. S3 output control

To maximize the chance of getting **one nightly output file**, set:

- `CdcMaxBatchInterval = 86400`
- `CdcMinFileSize` large enough that the daily output volume will **not** exceed it

If the daily change volume exceeds `CdcMinFileSize`, DMS may still create more than one file.

### 4. First-time bootstrap

The first execution of a CDC-only task needs an explicit starting point such as a timestamp or log position.
After the first execution, subsequent runs should use `resume-processing`.

## Required environment variables for the Lambda

- `DMS_TASK_ARN` – ARN of the replication task
- `INITIAL_CDC_START_POSITION` – required only for the first run if no recovery checkpoint exists
- `USE_COMMIT_TIME` – optional, `true` by default

Example:

```bash
export DMS_TASK_ARN="arn:aws:dms:us-east-1:123456789012:task:YOUR_TASK_ID"
export INITIAL_CDC_START_POSITION="2026-03-25T05:00:00"
export USE_COMMIT_TIME="true"
```

## Deployment steps

1. Create the Lambda function from `lambda_function.py`.
2. Attach the Lambda execution policy from `iam_lambda_execution_policy.json`.
3. Create the Scheduler execution role using:
   - `iam_scheduler_trust.json`
   - `iam_scheduler_invoke_lambda_policy.json`
4. Create the EventBridge Scheduler schedule using `create_schedule.sh`.
5. Run the first-time bootstrap example in `first_run_example.sh` if the task has never run before.
6. Confirm that the DMS S3 endpoint has settings aligned with `s3_endpoint_settings.json`.

## Validation checklist

- DMS task is in CDC mode
- Scheduler uses `America/New_York`
- Lambda has `dms:DescribeReplicationTasks` and `dms:StartReplicationTask`
- S3 endpoint has a sufficiently large `CdcMinFileSize`
- Task is not still running when the next nightly schedule fires
- DMS target pathing does not introduce unexpected per-table file splits

## Operational caution

This pattern is the best fit for nightly incremental output from DMS to S3, but **exactly one file per night** still depends on practical limits such as:

- daily CDC volume
- S3 endpoint batch and file-size settings
- target formatting behavior
- whether the target pathing or table layout causes multiple output objects

## Customization ideas

You can extend the Lambda to:

- write execution details to CloudWatch in a more structured format
- notify SNS on failures
- block a new start if the task is still running too close to the next window
- validate that the previous run completed successfully
- compute different stop windows for weekends or business calendars

