#!/usr/bin/env bash
set -euo pipefail

# Invoke UploadConfig Lambda to expand an already-existing zip in S3.
# Defaults are loaded from parms.json in this directory.

usage() {
  cat <<EOF
Usage:
  $0 [options]

Defaults are loaded from parms.json in this same directory.

Options:
  --parms-file <path>      Path to JSON parms file (default: ./parms.json)
  --function-name <name>   Lambda function name override
  --region <name>          AWS region override
  --source-bucket <name>   Source S3 bucket override
  --source-key <key>       Source S3 key override (zip object)
  --target-bucket <name>   Target S3 bucket override
  --target-key <key>       Target S3 key/prefix override (destination folder)
  --debug                  Force debug=true
  --out <file>             Lambda response output file override
  -h, --help               Show help

Expected parms.json shape:
{
  "function_name": "FSA-FPACDEV-UploadConfig",
  "region": "us-east-1",
  "source": { "bucket": "fsa-dev-ops", "key": "input/zips/myfile.zip" },
  "target": { "bucket": "c108-dev-fpacfsa-final-zone", "key": "car" },
  "debug": true,
  "out": "lambda_response_existing_zip.json"
}
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PARMS_FILE="$SCRIPT_DIR/parms.json"

FUNCTION_NAME=""
REGION=""
SOURCE_BUCKET=""
SOURCE_KEY=""
TARGET_BUCKET=""
TARGET_KEY=""
DEBUG=""
OUT_FILE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --parms-file)
      PARMS_FILE="$2"
      shift 2
      ;;
    --function-name)
      FUNCTION_NAME="$2"
      shift 2
      ;;
    --region)
      REGION="$2"
      shift 2
      ;;
    --source-bucket)
      SOURCE_BUCKET="$2"
      shift 2
      ;;
    --source-key)
      SOURCE_KEY="$2"
      shift 2
      ;;
    --target-bucket)
      TARGET_BUCKET="$2"
      shift 2
      ;;
    --target-key)
      TARGET_KEY="$2"
      shift 2
      ;;
    --debug)
      DEBUG="true"
      shift
      ;;
    --out)
      OUT_FILE="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "ERROR: unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ ! -f "$PARMS_FILE" ]]; then
  echo "ERROR: parms file not found: $PARMS_FILE" >&2
  exit 1
fi

if ! command -v aws >/dev/null 2>&1; then
  echo "ERROR: aws CLI not found in PATH" >&2
  exit 1
fi
if ! command -v python3 >/dev/null 2>&1; then
  echo "ERROR: python3 not found in PATH" >&2
  exit 1
fi

eval "$(python3 - "$PARMS_FILE" <<'PY'
import json
import shlex
import sys

path = sys.argv[1]
with open(path, "r", encoding="utf-8") as f:
    d = json.load(f)

def q(v):
    return shlex.quote(str(v if v is not None else ""))

src = d.get("source", {}) if isinstance(d.get("source"), dict) else {}
tgt = d.get("target", {}) if isinstance(d.get("target"), dict) else {}

print("DEF_FUNCTION_NAME=" + q(d.get("function_name", "")))
print("DEF_REGION=" + q(d.get("region", "us-east-1")))
print("DEF_SOURCE_BUCKET=" + q(src.get("bucket", "")))
print("DEF_SOURCE_KEY=" + q(src.get("key", "")))
print("DEF_TARGET_BUCKET=" + q(tgt.get("bucket", "")))
print("DEF_TARGET_KEY=" + q(tgt.get("key", "")))

debug = d.get("debug", False)
print("DEF_DEBUG=" + q("true" if bool(debug) else "false"))
print("DEF_OUT_FILE=" + q(d.get("out", "lambda_response_existing_zip.json")))
PY
)"

[[ -z "$FUNCTION_NAME" ]] && FUNCTION_NAME="$DEF_FUNCTION_NAME"
[[ -z "$REGION" ]] && REGION="$DEF_REGION"
[[ -z "$SOURCE_BUCKET" ]] && SOURCE_BUCKET="$DEF_SOURCE_BUCKET"
[[ -z "$SOURCE_KEY" ]] && SOURCE_KEY="$DEF_SOURCE_KEY"
[[ -z "$TARGET_BUCKET" ]] && TARGET_BUCKET="$DEF_TARGET_BUCKET"
[[ -z "$TARGET_KEY" ]] && TARGET_KEY="$DEF_TARGET_KEY"
[[ -z "$DEBUG" ]] && DEBUG="$DEF_DEBUG"
[[ -z "$OUT_FILE" ]] && OUT_FILE="$DEF_OUT_FILE"

if [[ "$OUT_FILE" != /* ]]; then
  OUT_FILE="$SCRIPT_DIR/$OUT_FILE"
fi

if [[ -z "$FUNCTION_NAME" || -z "$SOURCE_BUCKET" || -z "$SOURCE_KEY" || -z "$TARGET_BUCKET" || -z "$TARGET_KEY" ]]; then
  echo "ERROR: missing required values after merging parms/overrides" >&2
  usage
  exit 1
fi

if [[ "$DEBUG" != "true" && "$DEBUG" != "false" ]]; then
  echo "ERROR: debug must resolve to true or false (got: $DEBUG)" >&2
  exit 1
fi

export AWS_PAGER=""

PAYLOAD=$(cat <<EOF
{
  "input": { "bucket": "$SOURCE_BUCKET", "key": "$SOURCE_KEY" },
  "output": { "bucket": "$TARGET_BUCKET", "prefix": "$TARGET_KEY" },
  "overwrite": false,
  "debug": $DEBUG
}
EOF
)

echo "Invoking function: $FUNCTION_NAME"
echo "Region: $REGION"
echo "Source: s3://$SOURCE_BUCKET/$SOURCE_KEY"
echo "Target: s3://$TARGET_BUCKET/$TARGET_KEY/"

aws lambda invoke \
  --region "$REGION" \
  --function-name "$FUNCTION_NAME" \
  --cli-read-timeout 0 \
  --cli-connect-timeout 60 \
  --cli-binary-format raw-in-base64-out \
  --payload "$PAYLOAD" \
  "$OUT_FILE" >/dev/null

echo
echo "Lambda response ($OUT_FILE):"
if command -v jq >/dev/null 2>&1; then
  jq . "$OUT_FILE"
else
  cat "$OUT_FILE"
fi
