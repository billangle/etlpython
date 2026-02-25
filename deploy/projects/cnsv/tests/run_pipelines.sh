#!/usr/bin/env bash
# =============================================================================
# run_pipelines.sh — Deploy all three CNSV pipelines against a target env.
#
# Runs the deployers in sequence:
#   1. cnsvbase  — base CNSV pipeline
#   2. cnsvmaint — Contract Maintenance pipeline
#   3. cnsvpymts — Conservation Payments pipeline
#
# Requires valid AWS credentials and a config file in deploy/config/cnsv/.
#
# Usage:
#   ./run_pipelines.sh [OPTIONS]
#
# Options:
#   --env      <env>     Config environment: steamdev | dev | prod
#                        (default: steamdev)
#   --region   <region>  AWS region  (default: us-east-1)
#   --profile  <name>    AWS CLI profile to use
#   --pipeline <name>    Run only one pipeline: base | maint | pymts
#                        (default: run all three)
#   --dry-run            Print the commands without executing them
#   -h, --help           Show this help
#
# Examples:
#   # Deploy all pipelines to steamdev
#   ./run_pipelines.sh --env steamdev
#
#   # Deploy only the base pipeline to prod
#   ./run_pipelines.sh --env prod --pipeline base
#
#   # See what would run without executing
#   ./run_pipelines.sh --env dev --dry-run
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CNSV_DIR="$(dirname "${SCRIPT_DIR}")"           # deploy/projects/cnsv/
PROJECTS_DIR="$(dirname "${CNSV_DIR}")"         # deploy/projects/
DEPLOY_ROOT="$(dirname "${PROJECTS_DIR}")"      # deploy/
CONFIG_DIR="${DEPLOY_ROOT}/config/cnsv"
DEPLOY_PY="${DEPLOY_ROOT}/deploy.py"

# ---- defaults ---------------------------------------------------------------
ENV="steamdev"
REGION="us-east-1"
PROFILE=""
PIPELINE="all"
DRY_RUN=false

# ---- arg parse --------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --env)      ENV="$2";      shift 2 ;;
        --region)   REGION="$2";   shift 2 ;;
        --profile)  PROFILE="$2";  shift 2 ;;
        --pipeline) PIPELINE="$2"; shift 2 ;;
        --dry-run)  DRY_RUN=true;  shift   ;;
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

# ---- resolve config file ----------------------------------------------------
CONFIG_FILE="${CONFIG_DIR}/${ENV}.json"
if [[ ! -f "${CONFIG_FILE}" ]]; then
    echo "ERROR: Config file not found: ${CONFIG_FILE}" >&2
    echo "  Available configs:" >&2
    ls "${CONFIG_DIR}"/*.json 2>/dev/null | sed 's/^/    /' >&2
    exit 1
fi

# ---- AWS profile ------------------------------------------------------------
if [[ -n "${PROFILE}" ]]; then
    export AWS_PROFILE="${PROFILE}"
fi

# ---- python -----------------------------------------------------------------
PYTHON="${PYTHON:-python3}"

run_pipeline () {
    local project_type="$1"
    local label="$2"

    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  Deploying: ${label}  (--project-type ${project_type})"
    echo "  Config   : ${CONFIG_FILE}"
    echo "  Region   : ${REGION}"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    local cmd="${PYTHON} ${DEPLOY_PY} --config ${CONFIG_FILE} --region ${REGION} --project-type ${project_type}"

    if [[ "${DRY_RUN}" == "true" ]]; then
        echo "[DRY-RUN]  ${cmd}"
    else
        ${cmd}
    fi
}

# ---- header -----------------------------------------------------------------
echo "=== CNSV pipeline deployer ==="
echo "  Env        : ${ENV}"
echo "  Config     : ${CONFIG_FILE}"
echo "  Region     : ${REGION}"
echo "  Pipeline   : ${PIPELINE}"
[[ "${DRY_RUN}" == "true" ]] && echo "  Mode       : DRY-RUN (no actual deployments)"

# ---- run pipelines ----------------------------------------------------------
case "${PIPELINE}" in
    all)
        run_pipeline "cnsvbase"  "CNSV Base"
        run_pipeline "cnsvmaint" "Contract Maintenance"
        run_pipeline "cnsvpymts" "Conservation Payments"
        ;;
    base)
        run_pipeline "cnsvbase"  "CNSV Base"
        ;;
    maint)
        run_pipeline "cnsvmaint" "Contract Maintenance"
        ;;
    pymts)
        run_pipeline "cnsvpymts" "Conservation Payments"
        ;;
    *)
        echo "ERROR: Unknown pipeline: ${PIPELINE}" >&2
        echo "  Valid values: all | base | maint | pymts" >&2
        exit 1
        ;;
esac

echo ""
echo "=== Done ==="
