\
#!/usr/bin/env bash
set -euo pipefail

REGION="${REGION_OVERRIDE:-us-east-1}"
FUNCTION_NAME="${FUNCTION_NAME_OVERRIDE:-FSA-steam-dev-UploadConfig}"

INPUT_BUCKET="${INPUT_BUCKET_OVERRIDE:-dev-cars-artifacts}"
INPUT_KEY="${INPUT_KEY_OVERRIDE:-input/zips/car-20260129-161547-9dbc10c9ac9c.zip}"   # REQUIRED
OUTPUT_BUCKET="${OUTPUT_BUCKET_OVERRIDE:-punkdev-fpacfsa-final-zone}"
OUTPUT_PREFIX="${OUTPUT_PREFIX_OVERRIDE:-car}"

if [[ -z "$INPUT_KEY" ]]; then
  echo "ERROR: set INPUT_KEY_OVERRIDE to the S3 key of an input zip object" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../../" && pwd)"

"$ROOT_DIR/scripts/invoke_expand_lambda.sh" \
  --function-name "$FUNCTION_NAME" \
  --region "$REGION" \
  --input-bucket "$INPUT_BUCKET" \
  --input-key "$INPUT_KEY" \
  --output-bucket "$OUTPUT_BUCKET" \
  --output-prefix "$OUTPUT_PREFIX" \
  --debug \
  --out /tmp/cars-zip-test-lambda-response.json >/dev/null

echo "[test] verifying output has objects under s3://$OUTPUT_BUCKET/$OUTPUT_PREFIX/"
COUNT="$(aws s3api list-objects-v2 --bucket "$OUTPUT_BUCKET" --prefix "$OUTPUT_PREFIX/" --region "$REGION" --query 'KeyCount' --output text)"
if [[ "$COUNT" == "0" ]]; then
  echo "ERROR: no output objects found" >&2
  exit 1
fi

echo "[test] PASS lambda expand (KeyCount=$COUNT)"
