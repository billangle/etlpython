#!/usr/bin/env bash
set -euo pipefail

: "${FUNCTION:?FUNCTION environment variable not set}"
: "${FTPS_PORT:?FTPS_PORT environment variable not set (use 21 or 990)}"

PAYLOAD_FILE="$(mktemp -t lambda-payload.XXXXXX.json)"
trap 'rm -f "$PAYLOAD_FILE"' EXIT

cat > "$PAYLOAD_FILE" <<'EOF'
{
  "file_pattern": "^(\\w+)$",
  "echo_folder": "gls",
  "pipeline": "FLPIDS_ODS",
  "step": "FLPIDS_ODS Landing Zone",
  "header": false,
  "jenkins_url":  "https://jenkinsfsa-dev.dl.usda.gov/job/BuildProcess/job/TestFlpidsHook/build?token=72305AD4-A19D-462C-9CEB-275E01BDFAFD",
  "jenkins_login": "28722025092407116016427",
  "jenkins_password": "11895b798e52daaad50bc841a07484faea",
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
  --function-name "${FUNCTION}" \
  --cli-binary-format raw-in-base64-out \
  --payload "file://${PAYLOAD_FILE}" \
  response.json

echo "Wrote response.json"
cat response.json
echo
