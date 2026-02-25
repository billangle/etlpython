#!/bin/sh
set -euo pipefail

: "${AWS_REGION:=us-east-1}"
: "${FUNCTION_NAME:=FSA-PROD-Postgres-Proc-Status}"

PROC="${1:-}"
SECRET_NAME="${2:-}"
SECRET_PREFIX="${3:-}"
MATCH="${4:-call}"

if [ -z "$PROC" ] || [ -z "$SECRET_NAME" ] || [ -z "$SECRET_PREFIX" ]; then
  echo "Usage: $0 <proc_name> <secret_name> <secret_prefix> [match_mode]"
  echo "Example: $0 my_schema.my_proc FSA-PROD-secrets edv call"
  exit 2
fi

PAYLOAD_FILE="payload.json"
RESP_FILE="lambda_response.json"

cat > "$PAYLOAD_FILE" <<EOF
{"proc":"$PROC","match":"$MATCH","secret_name":"$SECRET_NAME","secret_prefix":"$SECRET_PREFIX"}
EOF

echo "[INFO] Invoking Lambda: $FUNCTION_NAME"
aws lambda invoke \
  --region "$AWS_REGION" \
  --function-name "$FUNCTION_NAME" \
  --cli-binary-format raw-in-base64-out \
  --payload fileb://"$PAYLOAD_FILE" \
  "$RESP_FILE" >/dev/null

cat "$RESP_FILE"

BUCKET="$(jq -r '.s3_bucket' < "$RESP_FILE")"
KEY="$(jq -r '.s3_key' < "$RESP_FILE")"
STATUS="$(jq -r '.status' < "$RESP_FILE")"

if [ "$STATUS" != "ok" ] || [ -z "$BUCKET" ] || [ -z "$KEY" ] || [ "$BUCKET" = "null" ] || [ "$KEY" = "null" ]; then
  echo "[ERROR] Lambda did not return expected s3_bucket/s3_key"
  exit 1
fi

echo "[INFO] Waiting for: s3://$BUCKET/$KEY"
i=0
while :; do
  if aws s3api head-object --region "$AWS_REGION" --bucket "$BUCKET" --key "$KEY" >/dev/null 2>&1; then
    break
  fi
  i=$((i+1))
  [ "$i" -ge 30 ] && { echo "[ERROR] Timed out waiting for S3 object"; exit 1; }
  sleep 2
done

OUTFILE="$(basename "$KEY")"
aws s3 cp --region "$AWS_REGION" "s3://$BUCKET/$KEY" "./$OUTFILE" >/dev/null
echo "[OK] Downloaded: $OUTFILE"