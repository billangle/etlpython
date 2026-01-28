#! /bin/sh


aws lambda invoke \
  --no-verify-ssl \
  --function-name FSA-dev-FpacFLPIDSCHK-CheckFileNotSecure \
  --cli-binary-format raw-in-base64-out \
  --payload '{
    "file_pattern": "^(\\w+)$",
    "echo_folder": "gls",
    "pipeline": "FLPIDS_ODS",
    "step": "FLPIDS_ODS Landing Zone",
    "header": false,
    "jenkins_url": "https://10.219.30.23/job/BuildProcess/job/TestFlpidsHook/build?token=72305AD4-A19D-462C-9CEB-275E01BDFAFD",
    "jenkins_tls_mode": "insecure",
    "jenkins_login": "28722025092407116016427",
    "jenkins_password": "11895b798e52daaad50bc841a07484faea",
    "secret_id": "FSA-CERT-Secrets",
    "ftps_mode": "explicit",
    "ftps_port": 990
  }' \
  response.json
