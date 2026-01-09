#! /bin/sh
set -euo pipefail

# PLAS MFO900 weekly report with 14-digit timestamp: MFO900.MOYRPT.<name>.DATA.<YYYYMMDDHHMMSS>
# Lambda: FSA-steam-dev-FpacFLPIDS-CheckFile

aws lambda invoke \
  --no-verify-ssl \
  --function-name FSA-steam-dev-FpacFLPIDS-CheckFile \
  --cli-binary-format raw-in-base64-out \
  --payload '{"file_pattern": "(MFO900\\.MOYRPT\\.\\w{1,128})\\.DATA\\.(\\d{14})$", "echo_folder": "plas", "pipeline": "FLPIDS_RPT", "step": "flpids_rpt Landing Zone (Weekly)", "header": false, "jenkins_url": "https://u9b0bk5pxi.execute-api.us-east-1.amazonaws.com/prod/jenkins-webhook", "secret_id": "FSA-CERT-Secrets", "ftps_mode": "explicit", "ftps_port": 21}' \
  response.json

echo "Wrote response.json"
cat response.json
echo ""
