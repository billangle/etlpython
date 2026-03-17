#!/usr/bin/env bash
set -euo pipefail

# CloudShell helper to invoke the CARS DownloadZip Lambda.
#
# Default function naming convention:
#   FSA-<DEPLOY_ENV>-DownloadZip
#
# Example:
#   ./run_downloadzip_cloudshell.sh \
#     --env FPACDEV \
#     --region us-east-1 \
#     --source-bucket c108-dev-fpacfsa-landing-zone \
#     --source-folder cars/etl-jobs \
#     --destination-bucket dev-cars-artifacts

usage() {
  cat <<EOF
Usage:
  $0 [options]

Defaults are loaded from parms.json in this same directory.
If parms.json contains valid values, the script can run with no arguments.

Optional:
  --env <name>                   Deploy environment used in function name (default: FPACDEV)
  --project <name>               Project token in function name (default: CARS)
  --function-name <name>         Explicit Lambda function name (overrides env/project naming)
  --region <name>                AWS region (default: us-east-1)
  --cli-read-timeout <seconds>   AWS CLI read timeout; 0 disables timeout (default: 0)
  --cli-connect-timeout <sec>    AWS CLI connect timeout (default: 60)
  --destination-key <key>        Destination S3 key for output zip
  --ignore-patterns <csv>        Comma-separated patterns (default: *.zip)
  --include-source-folder-in-zip <true|false>  (default: true)
  --parms-file <path>            Optional path to parms.json override
  --output <file>                Output file for lambda response (default: lambda_output.json)
  -h, --help                     Show help
EOF
}

DEPLOY_ENV="FPACDEV"
PROJECT="CARS"
FUNCTION_NAME="FSA-FPACDEV-DownloadZip"
REGION="us-east-1"
CLI_READ_TIMEOUT="0"
CLI_CONNECT_TIMEOUT="60"
SOURCE_BUCKET=""
SOURCE_FOLDER=""
DEST_BUCKET=""
DEST_KEY=""
IGNORE_PATTERNS="*.zip"
INCLUDE_SOURCE_FOLDER_IN_ZIP="true"
PARMS_FILE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/parms.json"
OUT_FILE="lambda_output.json"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env)
      DEPLOY_ENV="$2"
      shift 2
      ;;
    --project)
      PROJECT="$2"
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
    --cli-read-timeout)
      CLI_READ_TIMEOUT="$2"
      shift 2
      ;;
    --cli-connect-timeout)
      CLI_CONNECT_TIMEOUT="$2"
      shift 2
      ;;
    --source-bucket)
      SOURCE_BUCKET="$2"
      shift 2
      ;;
    --source-folder)
      SOURCE_FOLDER="$2"
      shift 2
      ;;
    --destination-bucket)
      DEST_BUCKET="$2"
      shift 2
      ;;
    --destination-key)
      DEST_KEY="$2"
      shift 2
      ;;
    --ignore-patterns)
      IGNORE_PATTERNS="$2"
      shift 2
      ;;
    --include-source-folder-in-zip)
      INCLUDE_SOURCE_FOLDER_IN_ZIP="$2"
      shift 2
      ;;
    --parms-file)
      PARMS_FILE="$2"
      shift 2
      ;;
    --output)
      OUT_FILE="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -f "$PARMS_FILE" ]]; then
  eval "$(python3 - "$PARMS_FILE" <<'PY'
import json
import shlex
import sys

path = sys.argv[1]
with open(path, "r", encoding="utf-8") as f:
    d = json.load(f)

ignore = d.get("ignore_patterns", ["*.zip"])
if isinstance(ignore, list):
    ignore_csv = ",".join(str(x) for x in ignore)
else:
    ignore_csv = "*.zip"

include = d.get("include_source_folder_in_zip", True)
if isinstance(include, bool):
    include_str = "true" if include else "false"
else:
    include_str = str(include).strip().lower() or "true"

print("DEF_SOURCE_BUCKET=" + shlex.quote(str(d.get("source_bucket", ""))))
print("DEF_SOURCE_FOLDER=" + shlex.quote(str(d.get("source_folder", ""))))
print("DEF_DEST_BUCKET=" + shlex.quote(str(d.get("destination_bucket", ""))))
print("DEF_DEST_KEY=" + shlex.quote(str(d.get("destination_key", ""))))
print("DEF_IGNORE_PATTERNS=" + shlex.quote(ignore_csv))
print("DEF_INCLUDE_SOURCE_FOLDER_IN_ZIP=" + shlex.quote(include_str))
PY
)"

  [[ -z "$SOURCE_BUCKET" ]] && SOURCE_BUCKET="$DEF_SOURCE_BUCKET"
  [[ -z "$SOURCE_FOLDER" ]] && SOURCE_FOLDER="$DEF_SOURCE_FOLDER"
  [[ -z "$DEST_BUCKET" ]] && DEST_BUCKET="$DEF_DEST_BUCKET"
  [[ -z "$DEST_KEY" ]] && DEST_KEY="$DEF_DEST_KEY"
  [[ "$IGNORE_PATTERNS" == "*.zip" ]] && IGNORE_PATTERNS="$DEF_IGNORE_PATTERNS"
  [[ "$INCLUDE_SOURCE_FOLDER_IN_ZIP" == "true" ]] && INCLUDE_SOURCE_FOLDER_IN_ZIP="$DEF_INCLUDE_SOURCE_FOLDER_IN_ZIP"
fi

# Built-in fallbacks so every variable has a default even when parms.json is missing/incomplete.
[[ -z "$SOURCE_BUCKET" ]] && SOURCE_BUCKET="my-source-bucket"
[[ -z "$SOURCE_FOLDER" ]] && SOURCE_FOLDER="incoming/data"
[[ -z "$DEST_BUCKET" ]] && DEST_BUCKET="my-destination-bucket"
[[ -z "$DEST_KEY" ]] && DEST_KEY="exports/incoming-data.zip"

if [[ -z "$FUNCTION_NAME" ]]; then
  FUNCTION_NAME="FSA-${DEPLOY_ENV}-DownloadZip"
fi

TMP_PAYLOAD="$(mktemp /tmp/downloadzip_payload.XXXXXX.json)"
trap 'rm -f "$TMP_PAYLOAD"' EXIT

# Convert comma-separated patterns into JSON array.
IFS=',' read -r -a _PAT_ARR <<< "$IGNORE_PATTERNS"
PAT_JSON=""
for p in "${_PAT_ARR[@]}"; do
  p_trim="$(echo "$p" | sed 's/^ *//; s/ *$//')"
  [[ -z "$p_trim" ]] && continue
  if [[ -n "$PAT_JSON" ]]; then
    PAT_JSON+=" , "
  fi
  PAT_JSON+="\"$p_trim\""
done
[[ -z "$PAT_JSON" ]] && PAT_JSON='"*.zip"'

cat > "$TMP_PAYLOAD" <<EOF
{
  "source_bucket": "$SOURCE_BUCKET",
  "source_folder": "$SOURCE_FOLDER",
  "destination_bucket": "$DEST_BUCKET",
  "ignore_patterns": [ $PAT_JSON ],
  "include_source_folder_in_zip": $INCLUDE_SOURCE_FOLDER_IN_ZIP$( [[ -n "$DEST_KEY" ]] && printf ',\n  "destination_key": "%s"' "$DEST_KEY" )
}
EOF

echo "Invoking function: $FUNCTION_NAME"
echo "Region: $REGION"
echo "Payload file: $TMP_PAYLOAD"

aws lambda invoke \
  --function-name "$FUNCTION_NAME" \
  --region "$REGION" \
  --cli-read-timeout "$CLI_READ_TIMEOUT" \
  --cli-connect-timeout "$CLI_CONNECT_TIMEOUT" \
  --cli-binary-format raw-in-base64-out \
  --payload "fileb://$TMP_PAYLOAD" \
  "$OUT_FILE" \
  --output json > /tmp/downloadzip_invoke_meta.json

echo
echo "Invoke metadata:"
cat /tmp/downloadzip_invoke_meta.json

echo
echo "Lambda response body ($OUT_FILE):"
cat "$OUT_FILE"
echo
