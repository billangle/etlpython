#! /bin/sh
set -euo pipefail

# PLAS RC540 monthly file: mo.moyr540.data
# Lambda: FSA-steam-dev-FpacFLPIDS-CheckFile

aws lambda invoke \
  --no-verify-ssl \
  --function-name FSA-steam-dev-FpacFLPIDS-CheckFile \
  --cli-binary-format raw-in-base64-out \
  --payload '{"file_pattern": "^(mo.moyr540)\\.data$", "echo_folder": "plas", "pipeline": "FLPIDS_RC540 Monthly", "step": "FLPIDS_RC540 Monthly Landing Zone", "header": false, "jenkins_url": "https://u9b0bk5pxi.execute-api.us-east-1.amazonaws.com/prod/jenkins-webhook", "secret_id": "FSA-CERT-Secrets", "ftps_mode": "explicit", "ftps_port": 21}' \
  response.json

echo "Wrote response.json"
cat response.json
echo ""
