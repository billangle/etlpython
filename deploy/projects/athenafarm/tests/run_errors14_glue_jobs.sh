#!/usr/bin/env bash
set -euo pipefail

# Runs only the Glue jobs updated for errors-14 fixes:
#   - Transform-Tract-Producer-Year
#   - Transform-Farm-Producer-Year
#
# CloudShell usage examples:
#   bash run_errors14_glue_jobs.sh
#   bash run_errors14_glue_jobs.sh --env PROD --region us-east-1 --warehouse s3://c108-prod-fpacfsa-final-zone/athenafarm/iceberg --full-load true

ENV_NAME="PROD"
REGION="us-east-1"
FULL_LOAD="true"
WAREHOUSE="s3://c108-prod-fpacfsa-final-zone/athenafarm/iceberg"
PREFIX_TEMPLATE="FSA-%s-ATHENAFARM"
POLL_SECONDS=15

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env)
      ENV_NAME="$2"; shift 2 ;;
    --region)
      REGION="$2"; shift 2 ;;
    --warehouse)
      WAREHOUSE="$2"; shift 2 ;;
    --full-load)
      FULL_LOAD="$2"; shift 2 ;;
    --poll-seconds)
      POLL_SECONDS="$2"; shift 2 ;;
    -h|--help)
      sed -n '1,40p' "$0"
      exit 0 ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 2 ;;
  esac
done

PREFIX=$(printf "$PREFIX_TEMPLATE" "$ENV_NAME")
TRACT_JOB="${PREFIX}-Transform-Tract-Producer-Year"
FARM_JOB="${PREFIX}-Transform-Farm-Producer-Year"

run_and_wait() {
  local job_name="$1"
  echo "--------------------------------------------------------------------------------"
  echo "Starting: ${job_name}"

  local run_id
  run_id=$(aws glue start-job-run \
    --region "$REGION" \
    --job-name "$job_name" \
    --arguments "--env=${ENV_NAME},--full_load=${FULL_LOAD},--iceberg_warehouse=${WAREHOUSE}" \
    --query 'JobRunId' \
    --output text)

  if [[ -z "$run_id" || "$run_id" == "None" ]]; then
    echo "Failed to start job: ${job_name}" >&2
    return 1
  fi

  echo "RunId: ${run_id}"

  while true; do
    local state
    state=$(aws glue get-job-run \
      --region "$REGION" \
      --job-name "$job_name" \
      --run-id "$run_id" \
      --query 'JobRun.JobRunState' \
      --output text)

    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ${job_name} -> ${state}"

    case "$state" in
      SUCCEEDED)
        return 0 ;;
      FAILED|TIMEOUT|ERROR|STOPPED)
        aws glue get-job-run \
          --region "$REGION" \
          --job-name "$job_name" \
          --run-id "$run_id" \
          --query 'JobRun.{State:JobRunState,Error:ErrorMessage,Started:StartedOn,Completed:CompletedOn}' \
          --output table || true
        return 1 ;;
      STARTING|RUNNING|STOPPING|WAITING|EXPIRED)
        sleep "$POLL_SECONDS" ;;
      *)
        sleep "$POLL_SECONDS" ;;
    esac
  done
}

echo "Region       : ${REGION}"
echo "Environment  : ${ENV_NAME}"
echo "Warehouse    : ${WAREHOUSE}"
echo "Full Load    : ${FULL_LOAD}"
echo "Jobs         : ${TRACT_JOB}, ${FARM_JOB}"

overall_rc=0
run_and_wait "$TRACT_JOB" || overall_rc=1
run_and_wait "$FARM_JOB" || overall_rc=1

if [[ "$overall_rc" -eq 0 ]]; then
  echo "--------------------------------------------------------------------------------"
  echo "SUCCESS: All errors-14 target Glue jobs completed successfully."
else
  echo "--------------------------------------------------------------------------------"
  echo "FAILURE: One or more errors-14 target Glue jobs failed." >&2
fi

exit "$overall_rc"
