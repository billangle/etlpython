#!/usr/bin/env bash
set -euo pipefail

# PLAS SCIMS report file: MOYWRSC_ABCD.txt

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
  "file_pattern": "(MOYWRSC_[A-Z]{4})\\.txt$",
  "echo_folder": "plas",
  "pipeline": "FLPIDS_RPT (SCIMS)",
  "step": "FLPIDS_RPT (SCIMS) Landing Zone",
  "header": false,
  "to_queue": true,
  "jenkins_url": "https://ka6kbk373a.execute-api.us-east-1.amazonaws.com/prod/jenkins-hook",
  "secret_id": "FSA-CERT-Secrets",
  "ftps_port": __FTPS_PORT__
}
EOF

sed_inplace() {
  # macOS (BSD sed) needs: -i ''
  # Linux (GNU sed) needs: -i
  if sed --version >/dev/null 2>&1; then
    sed -i "$@"
  else
    sed -i '' "$@"
  fi
}

sed_inplace "s/__FTPS_PORT__/${FTPS_PORT}/g" "$PAYLOAD_FILE"

aws lambda invoke \
  --no-verify-ssl \
  --function-name "${FUNCTION}" \
  --cli-binary-format raw-in-base64-out \
  --payload "file://${PAYLOAD_FILE}" \
  response.json

echo "Wrote response.json"
cat response.json
echo ""
