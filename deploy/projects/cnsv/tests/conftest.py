"""
conftest.py — pytest configuration for CNSV deployer tests.

Adds the deploy/ directory to sys.path so that
  'from common.aws_common import ...'
resolves correctly regardless of where pytest is invoked from.
"""
from __future__ import annotations
import sys
from pathlib import Path

# deploy/projects/cnsv/tests/ → deploy/projects/cnsv/ → deploy/projects/ → deploy/
_DEPLOY_ROOT = Path(__file__).resolve().parents[3]   # .../deploy/
if str(_DEPLOY_ROOT) not in sys.path:
    sys.path.insert(0, str(_DEPLOY_ROOT))
