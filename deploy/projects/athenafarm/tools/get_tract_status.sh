#!/usr/bin/env bash
set -euo pipefail

# CloudShell wrapper for GetTractStatus.py
# Defaults are PROD unless overridden via args or env vars.
#
# Positional args:
#   1: ENV            (default: PROD)
#   2: REGION         (default: us-east-1)
#   3: PROJECT        (default: ATHENAFARM)
#   4: JOB_NAME       (default: derived from ENV/PROJECT)
#   5: MAX_RESULTS    (default: 25)

TRACT_ENV="${1:-${TRACT_ENV:-PROD}}"
TRACT_REGION="${2:-${TRACT_REGION:-${AWS_REGION:-us-east-1}}}"
TRACT_PROJECT="${3:-${TRACT_PROJECT:-ATHENAFARM}}"
TRACT_MAX_RESULTS="${5:-${TRACT_MAX_RESULTS:-25}}"

TRACT_ENV_UPPER="$(printf '%s' "$TRACT_ENV" | tr '[:lower:]' '[:upper:]')"
TRACT_PROJECT_UPPER="$(printf '%s' "$TRACT_PROJECT" | tr '[:lower:]' '[:upper:]')"
TRACT_JOB_NAME_DEFAULT="FSA-${TRACT_ENV_UPPER}-${TRACT_PROJECT_UPPER}-Transform-Tract-Producer-Year"
TRACT_JOB_NAME="${4:-${TRACT_JOB_NAME:-$TRACT_JOB_NAME_DEFAULT}}"

export TRACT_ENV
export TRACT_REGION
export TRACT_PROJECT
export TRACT_JOB_NAME
export TRACT_MAX_RESULTS
export AWS_REGION="$TRACT_REGION"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
python3 "$SCRIPT_DIR/GetTractStatus.py"