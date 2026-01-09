#! /bin/sh
set -euo pipefail

# PLAS weekly subfolder example: /plas/in/weekly, file wk.moyr540.data

# Required environment variables:
#   FUNCTION   - Lambda function name
#   FTPS_PORT  - FTPS port (21 or 990)
: "${FUNCTION:?FUNCTION environment variable not set}"
: "${FTPS_PORT:?FTPS_PORT environment variable not set (use 21 or 990)}"


PAYLOAD_FILE="$(mktemp /tmp/lambda-payload.XXXXXX.json)"
trap 'rm -f "$PAYLOAD_FILE"' EXIT

# Use a *quoted* heredoc delimiter to prevent backslash processing (keeps regex JSON valid)
cat > "$PAYLOAD_FILE" <<'EOF'
{
  "file_pattern": "^(wk.moyr540)\\.data$",
  "echo_folder": "plas",
  "echo_subfolder": "weekly",
  "pipeline": "FLPIDS_RC540 Weekly",
  "step": "FLPIDS_RC540 Weekly Landing Zone",
  "header": false,
  "jenkins_url": "https://u9b0bk5pxi.execute-api.us-east-1.amazonaws.com/prod/jenkins-webhook",
  "secret_id": "FSA-CERT-Secrets",
  "ftps_port": __FTPS_PORT__
}
EOF

# Substitute port placeholder (portable on macOS/BSD sed)
sed -i '' "s/__FTPS_PORT__/${FTPS_PORT}/g" "$PAYLOAD_FILE"

aws lambda invoke \
  --no-verify-ssl \
  --function-name "${FUNCTION}" \
  --cli-binary-format raw-in-base64-out \
  --payload "file://${PAYLOAD_FILE}" \
  response.json

echo "Wrote response.json"
cat response.json
echo ""
