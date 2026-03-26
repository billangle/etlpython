\
#!/usr/bin/env bash
set -euo pipefail

FUNCTION_NAME=""
REGION="us-east-1"
INPUT_BUCKET=""
INPUT_KEY=""
OUTPUT_BUCKET=""
OUTPUT_PREFIX=""
OVERWRITE="false"
DEBUG="false"
OUT_FILE="response.json"

usage() {
  cat <<EOF
Usage: $0 --function-name NAME --input-bucket B --input-key K --output-bucket B2 --output-prefix P [options]

Required:
  --function-name NAME
  --input-bucket BUCKET
  --input-key KEY
  --output-bucket BUCKET
  --output-prefix PREFIX

Options:
  --region REGION   (default: us-east-1)
  --overwrite       (default: false)
  --debug           (default: false)
  --out FILE        (default: response.json)
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --function-name) FUNCTION_NAME="$2"; shift 2 ;;
    --region) REGION="$2"; shift 2 ;;
    --input-bucket) INPUT_BUCKET="$2"; shift 2 ;;
    --input-key) INPUT_KEY="$2"; shift 2 ;;
    --output-bucket) OUTPUT_BUCKET="$2"; shift 2 ;;
    --output-prefix) OUTPUT_PREFIX="$2"; shift 2 ;;
    --overwrite) OVERWRITE="true"; shift ;;
    --debug) DEBUG="true"; shift ;;
    --out) OUT_FILE="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

if [[ -z "$FUNCTION_NAME" || -z "$INPUT_BUCKET" || -z "$INPUT_KEY" || -z "$OUTPUT_BUCKET" || -z "$OUTPUT_PREFIX" ]]; then
  echo "ERROR: missing required args" >&2
  usage
  exit 1
fi

PAYLOAD=$(cat <<EOF
{
  "input": { "bucket": "$INPUT_BUCKET", "key": "$INPUT_KEY" },
  "output": { "bucket": "$OUTPUT_BUCKET", "prefix": "$OUTPUT_PREFIX" },
  "overwrite": $OVERWRITE,
  "debug": $DEBUG
}
EOF
)

aws lambda invoke \
  --region "$REGION" \
  --function-name "$FUNCTION_NAME" \
  --cli-binary-format raw-in-base64-out \
  --payload "$PAYLOAD" \
  "$OUT_FILE" >/dev/null

if command -v jq >/dev/null 2>&1; then
  jq . "$OUT_FILE"
else
  cat "$OUT_FILE"
fi
