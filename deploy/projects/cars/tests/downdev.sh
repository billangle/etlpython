#!/bin/sh
set -euo pipefail

###############################################################################
# Defaults (can be overridden by env vars or flags)
###############################################################################

LAMBDA_NAME="${LAMBDA_NAME:-FSA-FPACDEV-DownloadZip}"
REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-us-east-1}}"

SOURCE_BUCKET="${SOURCE_BUCKET:-c108-cert-fpacfsa-final-zone}"
SOURCE_FOLDER="${SOURCE_FOLDER:-car}"                 # empty = bucket root
DEST_BUCKET="${DEST_BUCKET:-fsa-dev-ops}"
DEST_KEY="${DEST_KEY:-}"                           # auto if empty

IGNORE_CSV="${IGNORE_CSV:-*.zip}"
INCLUDE_SOURCE_FOLDER_IN_ZIP="${INCLUDE_SOURCE_FOLDER_IN_ZIP:-true}"

###############################################################################
# Usage
###############################################################################

usage() {
  cat <<EOF
Invoke the DownloadZip Lambda.

All parameters are OPTIONAL.

Flags override environment variables, which override defaults.

Optional flags:
  -s  Source S3 bucket
  -f  Source folder/prefix
  -d  Destination S3 bucket
  -k  Destination key for zip
  -i  Ignore patterns (comma-separated, default "*.zip")
  --strip-prefix   Strip source folder from paths inside zip
  -r  AWS region
  -n  Lambda name
  -h  Help

Examples:
  ./invoke_downloadzip.sh
  ./invoke_downloadzip.sh -s my-src -f incoming/data
  ./invoke_downloadzip.sh -i "*.zip,*.tmp,*/_SUCCESS" --strip-prefix
EOF
}

###############################################################################
# Parse arguments (all optional)
###############################################################################

while [ $# -gt 0 ]; do
  case "$1" in
    -s) SOURCE_BUCKET="$2"; shift 2 ;;
    -f) SOURCE_FOLDER="$2"; shift 2 ;;
    -d) DEST_BUCKET="$2"; shift 2 ;;
    -k) DEST_KEY="$2"; shift 2 ;;
    -i) IGNORE_CSV="$2"; shift 2 ;;
    --strip-prefix) INCLUDE_SOURCE_FOLDER_IN_ZIP="false"; shift ;;
    -r) REGION="$2"; shift 2 ;;
    -n) LAMBDA_NAME="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage; exit 2 ;;
  esac
done

###############################################################################
# Build ignore_patterns JSON array
###############################################################################

IFS=',' read -r -a IGNORE_ARR <<EOF
$IGNORE_CSV
EOF
unset IFS

IGNORE_JSON="["
first=1
for pat in "${IGNORE_ARR[@]}"; do
  pat="$(printf "%s" "$pat" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
  [ -n "$pat" ] || continue

  [ $first -eq 1 ] || IGNORE_JSON="${IGNORE_JSON},"
  first=0

  esc="$(printf "%s" "$pat" | sed 's/\\/\\\\/g; s/"/\\"/g')"
  IGNORE_JSON="${IGNORE_JSON}\"${esc}\""
done
IGNORE_JSON="${IGNORE_JSON}]"

###############################################################################
# Payload
###############################################################################

PAYLOAD_FILE="$(mktemp -t downloadzip_payload.XXXXXX.json 2>/dev/null || mktemp /tmp/downloadzip_payload.XXXXXX.json)"
RESP_FILE="response.json"
trap 'rm -f "$PAYLOAD_FILE"' EXIT

cat > "$PAYLOAD_FILE" <<EOF
{
  "source_bucket": "$(printf "%s" "$SOURCE_BUCKET" | sed 's/"/\\"/g')",
  "source_folder": "$(printf "%s" "$SOURCE_FOLDER" | sed 's/"/\\"/g')",
  "destination_bucket": "$(printf "%s" "$DEST_BUCKET" | sed 's/"/\\"/g')",
  "ignore_patterns": $IGNORE_JSON,
  "include_source_folder_in_zip": $INCLUDE_SOURCE_FOLDER_IN_ZIP$( [ -n "$DEST_KEY" ] && printf ',\n  "destination_key": "%s"' "$(printf "%s" "$DEST_KEY" | sed 's/"/\\"/g')" )
}
EOF

###############################################################################
# Invoke Lambda
###############################################################################

echo "Invoking Lambda"
echo "  Lambda:      $LAMBDA_NAME"
echo "  Region:      $REGION"
echo "  Source:      s3://$SOURCE_BUCKET/$SOURCE_FOLDER"
echo "  Destination: s3://$DEST_BUCKET/${DEST_KEY:-<auto>}"
echo "  Ignore:      $IGNORE_CSV"
echo ""

aws lambda invoke \
  --region "$REGION" \
  --function-name "$LAMBDA_NAME" \
  --cli-binary-format raw-in-base64-out \
  --payload file://"$PAYLOAD_FILE" \
  "$RESP_FILE" >/dev/null

echo "Lambda response:"
cat "$RESP_FILE"
echo ""
