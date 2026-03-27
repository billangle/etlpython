#!/usr/bin/env bash
set -euo pipefail

CONFIG_FILE="${1:-}"
if [[ -z "$CONFIG_FILE" ]]; then
  echo "Usage: $0 <config.json>" >&2
  exit 1
fi
if [[ ! -f "$CONFIG_FILE" ]]; then
  echo "ERROR: config file not found: $CONFIG_FILE" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="."

# Helper: read a key from JSON using python, returning empty string if missing
json_get() {
  local expr="$1"
  python3 - <<PY
import json, sys
cfg=json.load(open("$CONFIG_FILE"))
def get(path):
    cur=cfg
    for p in path.split("."):
        if isinstance(cur, dict) and p in cur:
            cur=cur[p]
        else:
            return ""
    return cur
v=get("$expr")
if isinstance(v, bool):
    print("true" if v else "false")
elif v is None:
    print("")
else:
    print(v)
PY
}

# Regions: allow overrides but default from config
CFG_REGION="$(json_get "region")"
CFG_BUCKET_REGION="$(json_get "bucketRegion")"

REGION="${REGION_OVERRIDE:-${CFG_REGION:-us-east-1}}"
OUTPUT_REGION="${OUTPUT_REGION_OVERRIDE:-${CFG_BUCKET_REGION:-$REGION}}"

FUNCTION_NAME="$(json_get "configUpload.functionName")"
INPUT_BUCKET="$(json_get "configUpload.inputBucket")"
INPUT_PREFIX="$(json_get "configUpload.inputPrefix")"
OUTPUT_BUCKET="$(json_get "configUpload.outputBucket")"
OUTPUT_PREFIX="$(json_get "configUpload.outputPrefix")"
SOURCE_DIR="$(json_get "configUpload.sourceDir")"
MIN_EXPECTED="$(json_get "configUpload.minExpectedObjects")"
DEBUG_FLAG="$(json_get "configUpload.debug")"

# Defaults if not present
FUNCTION_NAME="${FUNCTION_NAME:-FSA-steam-dev-UploadConfig}"
INPUT_BUCKET="${INPUT_BUCKET:-dev-cars-artifacts}"
INPUT_PREFIX="${INPUT_PREFIX:-input/zips}"
OUTPUT_BUCKET="${OUTPUT_BUCKET:-punkdev-fpacfsa-final-zone}"
OUTPUT_PREFIX="${OUTPUT_PREFIX:-car2}"
SOURCE_DIR="${SOURCE_DIR:-car}"
MIN_EXPECTED="${MIN_EXPECTED:-2}"
DEBUG_FLAG="${DEBUG_FLAG:-true}"

WORK="$ROOT_DIR/$SOURCE_DIR"
OUT_JSON="$WORK/upload.json"

if [[ ! -d "$WORK" ]]; then
  echo "ERROR: sourceDir does not exist: $WORK" >&2
  exit 1
fi

echo "[cfg] REGION=$REGION OUTPUT_REGION=$OUTPUT_REGION"
echo "[cfg] FUNCTION_NAME=$FUNCTION_NAME"
echo "[cfg] INPUT=s3://$INPUT_BUCKET/$INPUT_PREFIX/"
echo "[cfg] OUTPUT=s3://$OUTPUT_BUCKET/$OUTPUT_PREFIX/"
echo "[cfg] WORK=$WORK"
echo

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
INVOKE_ARGS=(
  --function-name "$FUNCTION_NAME"
  --region "$REGION"
  --input-bucket "$INPUT_BUCKET"
  --input-key "$INPUT_KEY"
  --output-bucket "$OUTPUT_BUCKET"
  --output-prefix "$OUTPUT_PREFIX"
  --out "$WORK/lambda_response.json"
)
if [[ "$DEBUG_FLAG" == "true" ]]; then
  INVOKE_ARGS+=( --debug )
fi

"$ROOT_DIR/scripts/invoke_expand_lambda.sh" "${INVOKE_ARGS[@]}" >/dev/null

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



echo "[e2e] PASS end-to-end "
