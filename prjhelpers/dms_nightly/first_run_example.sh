#!/bin/sh
set -e

# Replace with your task ARN and desired initial CDC start position.
TASK_ARN="arn:aws:dms:us-east-1:123456789012:task:YOUR_TASK_ID"
CDC_START_POSITION="2026-03-25T05:00:00"
CDC_STOP_POSITION="commit_time:2026-03-26T05:00:00"

aws dms start-replication-task \
  --replication-task-arn "$TASK_ARN" \
  --start-replication-task-type start-replication \
  --cdc-start-position "$CDC_START_POSITION" \
  --cdc-stop-position "$CDC_STOP_POSITION"
