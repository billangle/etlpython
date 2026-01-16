#! /bin/sh


aws lambda invoke \
  --no-verify-ssl \
  --function-name FSA-steam-dev-FpacTSTHOOKS-JenkinsHook \
  --cli-binary-format raw-in-base64-out \
  --payload '{
    "jenkins_url": "https://u9b0bk5pxi.execute-api.us-east-1.amazonaws.com/prod/jenkins-webhook"
  }' \
  response.json
