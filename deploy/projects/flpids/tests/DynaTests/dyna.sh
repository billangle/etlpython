#!/bin/sh
set -euo pipefail

# ----------------------------
# Test invoke for:
#   FSA-steam-dev-FpacFLPIDS-DynaCheckFile
# Uses CLI binary format for raw JSON payload.
# ----------------------------

: "${AWS_REGION:=us-east-1}"

FUNCTION_NAME="FSA-steam-dev-FpacFLPIDS-DynaCheckFile"

PAYLOAD_FILE="$(mktemp /tmp/dynacheckfile-payload.XXXXXX.json)"
RESP_FILE="response.json"
trap 'rm -f "$PAYLOAD_FILE"' EXIT

cat > "$PAYLOAD_FILE" <<'EOF'
{
  "file_pattern": "^(wk\\.moyr540)\\.data$",
  "echo_folder": "plas",
  "echo_subfolder": "weekly",
  "pipeline": "FLPIDS_RC540 Weekly",
  "project_name": "RC540-WEEKLY",
  "secret_id": "FSA-CERT-Secrets",
  "ftps_port": 21,
  "min_size_bytes": 1,
  "verify_tls": false,
  "curl_timeout_seconds": 30,
  "debug": true,

  "dynamodb": {
    "table_name": "FSA-FileChecks",
    "partition_key": "jobId",
    "sort_key": "project",
    "project_gsi_name": "project_gsi_name"
  }
}
EOF

echo "Invoking Lambda: ${FUNCTION_NAME} (region=${AWS_REGION})"
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
