#! /bin/sh

aws lambda invoke \
  --function-name FSA-dev-FpacFLPIDSCHK-TestFileLoader  \
  --cli-binary-format raw-in-base64-out \
  --payload fileb://rmdev.json \
  response.json
