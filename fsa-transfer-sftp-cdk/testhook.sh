#! /bin/sh

curl  -X POST \
  -H "Content-Type: application/json" \
  -d '{"hello":"jenkins"}' \
  "https://u9b0bk5pxi.execute-api.us-east-1.amazonaws.com/prod/jenkins-webhook"
