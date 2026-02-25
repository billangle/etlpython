#!/usr/bin/env bash
# =============================================================================
# load_test_data.sh — Upload CNSV test fixture data to S3.
#
# Uploads job_id.json fixtures to an S3 bucket so that the deployers can
# locate them at runtime.  Safe to run against dev/steamdev; pass an
# --endpoint-url to target localstack instead.
#
# Usage:
#   ./load_test_data.sh [OPTIONS]
#
# Options:
#   --bucket   <name>   Target S3 bucket  (default: punkdev-fpacfsa-landing-zone)
#   --region   <name>   AWS region        (default: us-east-1)
#   --profile  <name>   AWS profile       (default: from environment)
#   --endpoint-url <u>  Override endpoint (e.g. http://localhost:4566 for localstack)
#   --dry-run           Print aws s3 cp commands without executing them
#   -h, --help          Show this help
#
# Example (steamdev):
#   ./load_test_data.sh --bucket punkdev-fpacfsa-landing-zone
#
# Example (localstack):
#   ./load_test_data.sh --bucket tst-landing-zone --endpoint-url http://localhost:4566
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FIXTURES_DIR="${SCRIPT_DIR}/fixtures"

# ---- defaults ---------------------------------------------------------------
BUCKET="punkdev-fpacfsa-landing-zone"
REGION="us-east-1"
PROFILE=""
ENDPOINT_URL=""
DRY_RUN=false

# ---- arg parse --------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --bucket)       BUCKET="$2";       shift 2 ;;
        --region)       REGION="$2";       shift 2 ;;
        --profile)      PROFILE="$2";      shift 2 ;;
        --endpoint-url) ENDPOINT_URL="$2"; shift 2 ;;
        --dry-run)      DRY_RUN=true;      shift   ;;
        -h|--help)
            grep '^#' "$0" | sed 's/^# \?//'
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            exit 1
            ;;
    esac
done

# ---- build base aws command --------------------------------------------------
AWS_CMD="aws"
[[ -n "${PROFILE}" ]]       && AWS_CMD="${AWS_CMD} --profile ${PROFILE}"
[[ -n "${ENDPOINT_URL}" ]]  && AWS_CMD="${AWS_CMD} --endpoint-url ${ENDPOINT_URL}"
AWS_CMD="${AWS_CMD} --region ${REGION}"

# ---- helper -----------------------------------------------------------------
s3_cp () {
    local src="$1"
    local dest="$2"
    if [[ "${DRY_RUN}" == "true" ]]; then
        echo "[DRY-RUN] ${AWS_CMD} s3 cp '${src}' '${dest}'"
    else
        echo "Uploading: ${src}  →  ${dest}"
        ${AWS_CMD} s3 cp "${src}" "${dest}"
    fi
}

# ---- fixture uploads --------------------------------------------------------
echo "=== CNSV test fixture upload ==="
echo "  Bucket : s3://${BUCKET}"
echo "  Region : ${REGION}"
[[ -n "${ENDPOINT_URL}" ]] && echo "  Endpoint: ${ENDPOINT_URL}"
[[ "${DRY_RUN}" == "true" ]] && echo "  Mode   : DRY-RUN (no actual uploads)"
echo ""

# Base pipeline job_id
s3_cp "${FIXTURES_DIR}/job_id.json" \
      "s3://${BUCKET}/cnsv/etl-jobs/job_id.json"

# Contract Maintenance pipeline job_id
s3_cp "${FIXTURES_DIR}/job_id.json" \
      "s3://${BUCKET}/contract_maintenance/etl-jobs/job_id.json"

# Conservation Payments pipeline job_id
s3_cp "${FIXTURES_DIR}/job_id.json" \
      "s3://${BUCKET}/conservation_payments/etl-jobs/job_id.json"

echo ""
echo "Done. Test fixtures loaded to s3://${BUCKET}"
