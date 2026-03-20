#! /bin/sh


aws lambda invoke \
  --no-verify-ssl \
  --function-name FSA-dev-FpacTSTHOOKS-JenkinsHook \
  --cli-binary-format raw-in-base64-out \
  --payload '{
    "jenkins_url": "https://10.219.30.23/job/BuildProcess/job/TestFlpidsHook/build?token=72305AD4-A19D-462C-9CEB-275E01BDFAFD",
    "jenkins_user": "28722025092407116016427",
    "jenkins_password": "11895b798e52daaad50bc841a07484faea",
    "debug": true
  }' \
  response.json
