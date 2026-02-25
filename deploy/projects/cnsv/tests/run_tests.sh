#!/usr/bin/env bash
# =============================================================================
# run_tests.sh — Run all CNSV deployer unit tests (no AWS credentials needed).
#
# Runs the three pytest test modules:
#   test_deploy_base.py   — CNSV base pipeline
#   test_deploy_maint.py  — Contract Maintenance pipeline
#   test_deploy_pymts.py  — Conservation Payments pipeline
#
# Usage:
#   ./run_tests.sh [PYTEST_ARGS...]
#
# Examples:
#   ./run_tests.sh                      # run all tests
#   ./run_tests.sh -k TestDeployBase    # run only base tests
#   ./run_tests.sh -v --tb=short        # verbose with short tracebacks
#   ./run_tests.sh test_deploy_base.py  # run a single file
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CNSV_DIR="$(dirname "${SCRIPT_DIR}")"           # deploy/projects/cnsv/
PROJECTS_DIR="$(dirname "${CNSV_DIR}")"         # deploy/projects/
DEPLOY_ROOT="$(dirname "${PROJECTS_DIR}")"      # deploy/

# Ensure deploy/ is on PYTHONPATH so 'from common.aws_common import ...' resolves.
export PYTHONPATH="${DEPLOY_ROOT}:${PYTHONPATH:-}"

echo "=== CNSV deployer unit tests ==="
echo "  DEPLOY_ROOT : ${DEPLOY_ROOT}"
echo "  PYTHONPATH  : ${PYTHONPATH}"
echo ""

# ---- locate pytest ----------------------------------------------------------
if command -v pytest &>/dev/null; then
    PYTEST_CMD="pytest"
elif python3 -m pytest --version &>/dev/null 2>&1; then
    PYTEST_CMD="python3 -m pytest"
else
    echo "ERROR: pytest is not installed. Run:  pip install pytest" >&2
    exit 1
fi

# ---- run --------------------------------------------------------------------
cd "${SCRIPT_DIR}"

if [[ $# -gt 0 ]]; then
    # Forward any extra args (file filter, -k, etc.) directly to pytest.
    ${PYTEST_CMD} -v "$@"
else
    # Default: run all three test modules.
    ${PYTEST_CMD} -v \
        test_deploy_base.py \
        test_deploy_maint.py \
        test_deploy_pymts.py
fi
