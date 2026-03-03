"""
AWS Glue Job wrapper: Transform-Tract-Producer-Year-Incremental

Purpose:
  Force incremental mode (full_load=false) and delegate execution to the
  shared Transform-Tract-Producer-Year implementation.
"""

import runpy
import sys
from pathlib import Path


def _set_or_add_flag(flag: str, value: str) -> None:
    try:
        idx = sys.argv.index(flag)
        if idx + 1 < len(sys.argv) and not sys.argv[idx + 1].startswith("--"):
            sys.argv[idx + 1] = value
        else:
            sys.argv.insert(idx + 1, value)
    except ValueError:
        sys.argv.extend([flag, value])


_set_or_add_flag("--full_load", "false")

base_script = Path(__file__).with_name("Transform-Tract-Producer-Year.py")
runpy.run_path(str(base_script), run_name="__main__")
