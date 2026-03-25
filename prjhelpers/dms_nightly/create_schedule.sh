#!/bin/sh
set -e

# Replace the values below before running.
SCHEDULE_NAME="dms-nightly-cdc-1am-et"
GROUP_NAME="default"
LAMBDA_ARN="arn:aws:lambda:us-east-1:123456789012:function:start-dms-nightly-cdc"
SCHEDULER_ROLE_ARN="arn:aws:iam::123456789012:role/EventBridgeSchedulerInvokeLambdaRole"

aws scheduler create-schedule \
  --name "$SCHEDULE_NAME" \
  --group-name "$GROUP_NAME" \
  --schedule-expression "cron(0 1 * * ? *)" \
  --schedule-expression-timezone "America/New_York" \
  --flexible-time-window '{"Mode":"OFF"}' \
  --target "{\"Arn\":\"$LAMBDA_ARN\",\"RoleArn\":\"$SCHEDULER_ROLE_ARN\",\"Input\":\"{\\\"task\\\":\\\"daily-cdc\\\"}\",\"RetryPolicy\":{\"MaximumEventAgeInSeconds\":3600,\"MaximumRetryAttempts\":2}}"
