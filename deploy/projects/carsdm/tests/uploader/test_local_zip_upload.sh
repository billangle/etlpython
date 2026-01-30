\
#!/usr/bin/env bash
set -euo pipefail

REGION="${REGION_OVERRIDE:-us-east-1}"
INPUT_BUCKET="${INPUT_BUCKET_OVERRIDE:-dev-cars-artifacts}"
INPUT_PREFIX="${INPUT_PREFIX_OVERRIDE:-input/zips}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../../" && pwd)"

WORK="$ROOT_DIR/car"

OUT_JSON="$WORK/upload.json"
S3URI="$("$ROOT_DIR/scripts/make_and_upload_zip.sh" --src "$WORK" --bucket "$INPUT_BUCKET" --prefix "$INPUT_PREFIX" --region "$REGION" --out-json "$OUT_JSON")"

BUCKET="$(python3 -c 'import json; print(json.load(open("'"$OUT_JSON"'"))["bucket"])')"
KEY="$(python3 -c 'import json; print(json.load(open("'"$OUT_JSON"'"))["key"])')"

echo "[test] verifying object exists: s3://$BUCKET/$KEY"
aws s3api head-object --bucket "$BUCKET" --key "$KEY" --region "$REGION" >/dev/null

echo "[test] PASS local zip+upload"
echo "[test] Uploaded: $S3URI"
