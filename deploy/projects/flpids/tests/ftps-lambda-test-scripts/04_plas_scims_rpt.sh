#! /bin/sh
set -euo pipefail

# PLAS SCIMS report file: MOYWRSC_ABCD.txt (ABCD = any 4 uppercase letters).
# Lambda: FSA-steam-dev-FpacFLPIDS-CheckFile

aws lambda invoke \
  --no-verify-ssl \
  --function-name FSA-steam-dev-FpacFLPIDS-CheckFile \
  --cli-binary-format raw-in-base64-out \
  --payload '{"file_pattern": "(MOYWRSC_[A-Z]{4})\\.txt$", "echo_folder": "plas", "pipeline": "FLPIDS_RPT (SCIMS)", "step": "FLPIDS_RPT (SCIMS) Landing Zone", "to_queue": true, "header": false, "jenkins_url": "https://u9b0bk5pxi.execute-api.us-east-1.amazonaws.com/prod/jenkins-webhook", "secret_id": "FSA-CERT-Secrets", "ftps_mode": "explicit", "ftps_port": 21}' \
  response.json

echo "Wrote response.json"
cat response.json
echo ""
