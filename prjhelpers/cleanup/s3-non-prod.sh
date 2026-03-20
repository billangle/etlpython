#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
DEFAULT_DAYS="${DAYS:-60}"
DEFAULT_IGNORE_BUCKETS=("cdk" "dms" "cdo" "aws" "fsa-dev-ops")

has_days=0
has_execute=0
for arg in "$@"; do
  if [[ "$arg" == "--days" ]]; then
    has_days=1
  fi
  if [[ "$arg" == "--execute" ]]; then
    has_execute=1
  fi
done

if [[ $has_execute -eq 1 ]]; then
  echo "Mode: EXECUTE (deletions enabled)"
else
  echo "Mode: DRY RUN (default, nothing deleted)"
fi

if [[ $has_days -eq 1 ]]; then
  exec "$PYTHON_BIN" "$SCRIPT_DIR/s3_cleanup.py" \
    --ignore-bucket "${DEFAULT_IGNORE_BUCKETS[0]}" \
    --ignore-bucket "${DEFAULT_IGNORE_BUCKETS[1]}" \
    --ignore-bucket "${DEFAULT_IGNORE_BUCKETS[2]}" \
    --ignore-bucket "${DEFAULT_IGNORE_BUCKETS[3]}" \
    --ignore-bucket "${DEFAULT_IGNORE_BUCKETS[4]}" \
    --rules-json "$SCRIPT_DIR/S3-NON-PROD.json" \
    "$@"
else
  exec "$PYTHON_BIN" "$SCRIPT_DIR/s3_cleanup.py" \
    --days "$DEFAULT_DAYS" \
    --ignore-bucket "${DEFAULT_IGNORE_BUCKETS[0]}" \
    --ignore-bucket "${DEFAULT_IGNORE_BUCKETS[1]}" \
    --ignore-bucket "${DEFAULT_IGNORE_BUCKETS[2]}" \
    --ignore-bucket "${DEFAULT_IGNORE_BUCKETS[3]}" \
    --ignore-bucket "${DEFAULT_IGNORE_BUCKETS[4]}" \
    --rules-json "$SCRIPT_DIR/S3-NON-PROD.json" \
    "$@"
fi
