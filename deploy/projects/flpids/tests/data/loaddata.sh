#! /bin/sh
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
S3_BUCKET="dev-cars-artifacts"
S3_PREFIX="testdata/data"

echo "Syncing test data to s3://${S3_BUCKET}/${S3_PREFIX} ..."

# plas-in
aws s3 sync "${SCRIPT_DIR}/plas-in" "s3://${S3_BUCKET}/${S3_PREFIX}/plas-in" --no-progress

# nats-in
aws s3 sync "${SCRIPT_DIR}/nats-in" "s3://${S3_BUCKET}/${S3_PREFIX}/nats-in" --no-progress

# gls-in
aws s3 sync "${SCRIPT_DIR}/gls-in" "s3://${S3_BUCKET}/${S3_PREFIX}/gls-in" --no-progress

# mrtxdb-in (new: caorpt daily + weekly)
aws s3 sync "${SCRIPT_DIR}/mrtxdb-in" "s3://${S3_BUCKET}/${S3_PREFIX}/mrtxdb-in" --no-progress

echo "Sync complete. Invoking TestFileLoader lambda ..."

aws lambda invoke \
  --function-name FSA-steam-dev-FpacFLPIDS-TestFileLoader \
  --cli-binary-format raw-in-base64-out \
  --payload fileb://loadpunk.json \
  response.json

echo "Done."
cat response.json
echo
