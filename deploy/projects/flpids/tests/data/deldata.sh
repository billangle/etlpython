#! /bin/sh

aws lambda invoke \
  --function-name FSA-steam-dev-FpacFLPIDS-TestFileLoader \
  --cli-binary-format raw-in-base64-out \
  --payload fileb://rmpunk.json \
  response.json
