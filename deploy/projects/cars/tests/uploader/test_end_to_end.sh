#!/usr/bin/env bash
set -euo pipefail

REGION="${REGION_OVERRIDE:-us-east-1}"
OUTPUT_REGION="${OUTPUT_REGION_OVERRIDE:-$REGION}"

FUNCTION_NAME="${FUNCTION_NAME_OVERRIDE:-FSA-steam-dev-UploadConfig}"

INPUT_BUCKET="${INPUT_BUCKET_OVERRIDE:-dev-cars-artifacts}"
INPUT_PREFIX="${INPUT_PREFIX_OVERRIDE:-input/zips}"

OUTPUT_BUCKET="${OUTPUT_BUCKET_OVERRIDE:-punkdev-fpacfsa-final-zone}"
OUTPUT_PREFIX="${OUTPUT_PREFIX_OVERRIDE:-car2}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../../" && pwd)"

WORK="$ROOT_DIR/car"
OUT_JSON="$WORK/upload.json"

# 1) Zip + upload
S3URI="$("$ROOT_DIR/scripts/make_and_upload_zip.sh" \
  --src "$WORK" \
  --bucket "$INPUT_BUCKET" \
  --prefix "$INPUT_PREFIX" \
  --region "$REGION" \
  --out-json "$OUT_JSON")"

INPUT_KEY="$(python3 -c 'import json; print(json.load(open("'"$OUT_JSON"'"))["key"])')"
echo "[e2e] uploaded zip: $S3URI"
echo "[e2e] input key: $INPUT_KEY"

# 2) Invoke lambda to expand
"$ROOT_DIR/scripts/invoke_expand_lambda.sh" \
  --function-name "$FUNCTION_NAME" \
  --region "$REGION" \
  --input-bucket "$INPUT_BUCKET" \
  --input-key "$INPUT_KEY" \
  --output-bucket "$OUTPUT_BUCKET" \
  --output-prefix "$OUTPUT_PREFIX" \
  --debug \
  --out "$WORK/lambda_response.json" >/dev/null

echo "[e2e] verify extracted objects exist under s3://$OUTPUT_BUCKET/$OUTPUT_PREFIX/ (region=$OUTPUT_REGION)"

# 3) List output objects robustly
set +e
LIST_OUT="$(aws s3api list-objects-v2 \
  --bucket "$OUTPUT_BUCKET" \
  --prefix "$OUTPUT_PREFIX/" \
  --region "$OUTPUT_REGION" \
  --query 'KeyCount' \
  --output text 2>&1)"
RC=$?
set -e

if [[ $RC -ne 0 ]]; then
  echo "ERROR: aws s3api list-objects-v2 failed (rc=$RC)" >&2
  echo "$LIST_OUT" >&2
  exit 1
fi

# LIST_OUT should be a number; sometimes it can be "None"
if [[ ! "$LIST_OUT" =~ ^[0-9]+$ ]]; then
  echo "ERROR: Expected numeric KeyCount, got: '$LIST_OUT'" >&2
  echo "Most likely wrong region, bucket name, prefix, or missing permissions." >&2
  exit 1
fi

COUNT="$LIST_OUT"

if [[ "$COUNT" -eq 0 ]]; then
  echo "ERROR: no output objects found" >&2
  exit 1
fi

if [[ "$COUNT" -lt 2 ]]; then
  echo "ERROR: expected >=2 objects, got $COUNT" >&2
  exit 1
fi

echo "[e2e] PASS end-to-end (KeyCount=$COUNT)"
