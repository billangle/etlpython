"""
================================================================================
Lambda Function: NotifyPipeline
================================================================================

PURPOSE:
    Receives pipeline completion/failure events from the athenafarm Step
    Functions state machine and publishes a human-readable notification.

    The Step Functions Notify state invokes this Lambda with:
      {
        "env":            "<deploy-env>",        # e.g. "PROD"
        "pipeline":       "athenafarm",
        "executionState": { ...full SFN output... }
      }

    The Lambda inspects the executionState for any Error keys written by
    Catch blocks, determines success vs failure, then:
      - Always logs the full event (visible in CloudWatch Logs)
      - If SNS_TOPIC_ARN is set, publishes a brief notification to that topic

ENVIRONMENT VARIABLES:
    SNS_TOPIC_ARN  (optional) — ARN of the SNS topic to notify.
                                If absent, only CloudWatch Logs are written.

================================================================================
"""

import json
import logging
import os

import boto3

log = logging.getLogger()
log.setLevel(logging.INFO)

SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN", "")
REGION        = os.environ.get("AWS_REGION", "us-east-1")


def handler(event, context):
    """Entry point invoked by the Step Functions Notify state."""
    log.info("NotifyPipeline invoked: %s", json.dumps(event, default=str))

    env             = event.get("env", "unknown")
    pipeline        = event.get("pipeline", "unknown")
    execution_state = event.get("executionState", {})

    is_failure = _has_error(execution_state)
    status     = "FAILED" if is_failure else "SUCCEEDED"

    subject = f"[{env.upper()}] {pipeline.upper()} pipeline {status}"
    body = (
        f"Pipeline : {pipeline}\n"
        f"Env      : {env}\n"
        f"Status   : {status}\n"
        f"\nExecution state:\n{json.dumps(execution_state, indent=2, default=str)}"
    )

    log.info("Notification — %s", subject)

    if SNS_TOPIC_ARN:
        try:
            sns = boto3.client("sns", region_name=REGION)
            sns.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject=subject[:100],
                Message=body,
            )
            log.info("Published notification to %s", SNS_TOPIC_ARN)
        except Exception as exc:  # noqa: BLE001
            log.warning("SNS publish failed (non-fatal): %s", exc)
    else:
        log.info("SNS_TOPIC_ARN not set — skipping SNS publish")

    return {"status": status, "subject": subject}


def _has_error(obj, _depth=0):
    """Return True when any nested dict contains an 'Error' or 'error' key."""
    if _depth > 12:
        return False
    if isinstance(obj, dict):
        if "Error" in obj or "error" in obj:
            return True
        return any(_has_error(v, _depth + 1) for v in obj.values())
    if isinstance(obj, list):
        return any(_has_error(i, _depth + 1) for i in obj)
    return False
