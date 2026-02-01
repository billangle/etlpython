\
#!/usr/bin/env bash
set -euo pipefail

SRC_DIR=""
BUCKET=""
PREFIX="input/zips"
NAME=""
REGION="us-east-1"
KEEP_LOCAL_ZIP="false"
OUT_JSON=""

usage() {
  cat <<EOF
Usage: $0 --src DIR --bucket BUCKET [options]

Required:
  --src DIR              Source directory to zip (recursive)
  --bucket BUCKET        S3 bucket to upload the zip to

Options:
  --prefix PREFIX        S3 key prefix (default: input/zips)
  --name NAME            Zip filename (default: <src>-<timestamp>-<sha12>.zip)
  --region REGION        AWS region (default: us-east-1)
  --keep                 Keep the local zip file (default: false)
  --out-json FILE        Write details to FILE (json)
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --src) SRC_DIR="$2"; shift 2 ;;
    --bucket) BUCKET="$2"; shift 2 ;;
    --prefix) PREFIX="$2"; shift 2 ;;
    --name) NAME="$2"; shift 2 ;;
    --region) REGION="$2"; shift 2 ;;
    --keep) KEEP_LOCAL_ZIP="true"; shift ;;
    --out-json) OUT_JSON="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

if [[ -z "$SRC_DIR" || -z "$BUCKET" ]]; then
  echo "ERROR: --src and --bucket are required" >&2
  usage
  exit 1
fi

if [[ ! -d "$SRC_DIR" ]]; then
  echo "ERROR: Source directory does not exist: $SRC_DIR" >&2
  exit 1
fi

if ! command -v zip >/dev/null 2>&1; then
  echo "ERROR: zip command not found. Install zip." >&2
  exit 1
fi

TS="$(date -u +%Y%m%d-%H%M%S)"
BASENAME="$(basename "$SRC_DIR")"

TMP_ZIP="$(mktemp -t "${BASENAME}.XXXXXX.zip")"
rm -f "$TMP_ZIP" 2>/dev/null || true
TMP_ZIP="${TMP_ZIP%.zip}.zip"

(cd "$SRC_DIR" && zip -qr "$TMP_ZIP" .)

if command -v shasum >/dev/null 2>&1; then
  SHA="$(shasum -a 256 "$TMP_ZIP" | awk '{print $1}')"
elif command -v sha256sum >/dev/null 2>&1; then
  SHA="$(sha256sum "$TMP_ZIP" | awk '{print $1}')"
else
  echo "ERROR: need shasum or sha256sum to compute checksum" >&2
  exit 1
fi

SHA12="${SHA:0:12}"

if [[ -z "$NAME" ]]; then
  NAME="${BASENAME}-${TS}-${SHA12}.zip"
fi

PREFIX="${PREFIX#/}"
PREFIX="${PREFIX%/}"

KEY="${PREFIX}/${NAME}"
S3URI="s3://${BUCKET}/${KEY}"

echo "[local] Created zip: $TMP_ZIP"
echo "[local] Uploading to: $S3URI"
aws s3 cp "$TMP_ZIP" "$S3URI" --region "$REGION" >/dev/null
echo "[local] Uploaded: $S3URI"

if [[ -n "$OUT_JSON" ]]; then
  cat > "$OUT_JSON" <<EOF
{
  "src_dir": "$(cd "$SRC_DIR" && pwd)",
  "zip_path": "$TMP_ZIP",
  "bucket": "$BUCKET",
  "key": "$KEY",
  "s3_uri": "$S3URI",
  "sha256": "$SHA"
}
EOF
  echo "[local] Wrote: $OUT_JSON"
fi

if [[ "$KEEP_LOCAL_ZIP" != "true" ]]; then
  rm -f "$TMP_ZIP"
else
  echo "[local] Kept zip: $TMP_ZIP"
fi

echo "$S3URI"
