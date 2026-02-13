#!/bin/sh
set -euo pipefail

# ----------------------------
# Test invoke for:
#   FSA-steam-dev-FpacFLPIDS-DynaCheckFile
#
# Jenkins Job: FSA-PROD-DART-ECHO-FETCH-FLPIDS-NATS
#
# Works on macOS + Amazon CloudShell (POSIX sh).
#
# Overrides (optional):
#   AWS_REGION, FUNCTION_NAME, FTPS_PORT, LAMBDA_ARN,
#   DDB_TABLE_NAME, DDB_PROJECT_GSI_NAME,
#   SECRET_ID, STEP,
#   FILE_PATTERN, ECHO_FOLDER, ECHO_SUBFOLDER, PIPELINE,
#   MIN_SIZE_BYTES, VERIFY_TLS, CURL_TIMEOUT_SECONDS, DEBUG, HEADER, TO_QUEUE,
#   RESP_FILE
# ----------------------------

: "${AWS_REGION:=us-east-1}"
: "${FUNCTION_NAME:=FSA-steam-dev-FpacFLPIDS-DynaCheckFile}"
: "${FTPS_PORT:=21}"
: "${LAMBDA_ARN:=arn:aws:lambda:us-east-1:335965711887:function:FSA-steam-dev-FpacFLPIDS-TransferFile}"

: "${DDB_TABLE_NAME:=FSA-FileChecks}"
: "${DDB_PROJECT_GSI_NAME:=project_gsi_name}"
: "${SECRET_ID:=FSA-CERT-Secrets}"

JENKINS_JOB_NAME="FSA-PROD-DART-ECHO-FETCH-FLPIDS-NATS"
PROJECT_NAME="${JENKINS_JOB_NAME#*ECHO-FETCH-}"
: "${STEP:=FSA-PROD-DART-ECHO-FETCH-FLPIDS-NATS}"
export PROJECT_NAME

# Defaults (regex can contain braces, so avoid shell parameter default syntax with embedded braces)
if [ -z "${FILE_PATTERN:-}" ]; then FILE_PATTERN='^(FILE_NATS_\w+)\.(\d{8})\.csv$'; fi
if [ -z "${ECHO_FOLDER:-}" ]; then ECHO_FOLDER='nats'; fi
if [ -z "${ECHO_SUBFOLDER:-}" ]; then ECHO_SUBFOLDER=''; fi
if [ -z "${PIPELINE:-}" ]; then PIPELINE='FLPIDS_ODS'; fi

if [ -z "${MIN_SIZE_BYTES:-}" ]; then MIN_SIZE_BYTES="1"; fi
if [ -z "${VERIFY_TLS:-}" ]; then VERIFY_TLS="false"; fi
if [ -z "${CURL_TIMEOUT_SECONDS:-}" ]; then CURL_TIMEOUT_SECONDS="30"; fi
if [ -z "${DEBUG:-}" ]; then DEBUG="true"; fi
if [ -z "${HEADER:-}" ]; then HEADER="false"; fi
if [ -z "${TO_QUEUE:-}" ]; then TO_QUEUE="true"; fi

export FILE_PATTERN ECHO_FOLDER ECHO_SUBFOLDER PIPELINE
export MIN_SIZE_BYTES VERIFY_TLS CURL_TIMEOUT_SECONDS DEBUG HEADER TO_QUEUE
export AWS_REGION FUNCTION_NAME FTPS_PORT DDB_TABLE_NAME DDB_PROJECT_GSI_NAME SECRET_ID LAMBDA_ARN STEP

PAYLOAD_FILE="$(mktemp /tmp/dynacheckfile-payload.XXXXXX.json)"
RESP_FILE="${RESP_FILE:-response.json}"
trap 'rm -f "$PAYLOAD_FILE"' EXIT

# Build JSON payload using python3 for proper JSON escaping (POSIX-safe)
python3 - <<'PY' > "$PAYLOAD_FILE"
import os, json

payload = {
  "file_pattern": os.environ.get("FILE_PATTERN", ""),
  "echo_folder": os.environ.get("ECHO_FOLDER", ""),
  "echo_subfolder": os.environ.get("ECHO_SUBFOLDER", ""),
  "pipeline": os.environ.get("PIPELINE", ""),
  "project_name": os.environ.get("PROJECT_NAME", ""),
  "secret_id": os.environ.get("SECRET_ID", ""),
  "lambda_arn": os.environ.get("LAMBDA_ARN", ""),
  "step": os.environ.get("STEP", ""),
  "ftps_port": int(os.environ.get("FTPS_PORT", "21")),
  "min_size_bytes": int(os.environ.get("MIN_SIZE_BYTES", "1")),
  "verify_tls": (os.environ.get("VERIFY_TLS", "false").lower() == "true"),
  "curl_timeout_seconds": int(os.environ.get("CURL_TIMEOUT_SECONDS", "30")),
  "debug": (os.environ.get("DEBUG", "true").lower() == "true"),
  "header": (os.environ.get("HEADER", "false").lower() == "true"),
  "to_queue": (os.environ.get("TO_QUEUE", "true").lower() == "true"),
  "dynamodb": {
    "table_name": os.environ.get("DDB_TABLE_NAME", ""),
    "partition_key": "jobId",
    "sort_key": "project",
    "project_gsi_name": os.environ.get("DDB_PROJECT_GSI_NAME", ""),
  },
}
print(json.dumps(payload))
PY

echo "Invoking Lambda: ${FUNCTION_NAME} (region=${AWS_REGION})"
echo "  Jenkins job:     ${JENKINS_JOB_NAME}"
echo "  project_name:    ${PROJECT_NAME}"
echo "  ftps_port:       ${FTPS_PORT}"
echo "  echo folder:     ${ECHO_FOLDER}"
echo "  echo subfolder:  ${ECHO_SUBFOLDER}"
echo "  file_pattern:    ${FILE_PATTERN}"
echo "  pipeline:        ${PIPELINE}"
echo "  ddb table:       ${DDB_TABLE_NAME} (gsi=${DDB_PROJECT_GSI_NAME})"
echo

aws lambda invoke \
  --region "${AWS_REGION}" \
  --function-name "${FUNCTION_NAME}" \
  --cli-binary-format raw-in-base64-out \
  --payload "file://${PAYLOAD_FILE}" \
  "${RESP_FILE}" >/dev/null

echo "Response written to ${RESP_FILE}"
echo "----"
cat "${RESP_FILE}"
echo
