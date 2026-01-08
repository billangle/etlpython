#! /bin/sh


aws lambda invoke \
  --no-verify-ssl \
  --function-name FSA-steam-dev-FpacFLPIDS-CheckFile \
  --cli-binary-format raw-in-base64-out \
  --payload '{
    "file_pattern": "^(\\w+)$",
    "echo_folder": "gls",
    "pipeline": "FLPIDS_ODS",
    "step": "FLPIDS_ODS Landing Zone",
    "header": false,
    "jenkins_url": "https://u9b0bk5pxi.execute-api.us-east-1.amazonaws.com/prod/jenkins-webhook",
    "secret_id": "FSA-CERT-Secrets"
  }' \
  response.json
