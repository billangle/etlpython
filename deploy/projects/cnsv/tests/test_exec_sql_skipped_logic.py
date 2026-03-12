"""
Regression tests for skipped/missing-config resilience in CNSV EXEC-SQL flow.

Covers:
- edv-check-results classification of missing-config errors into skipped tables
- edv-finalize-pipeline summary propagation of skipped counts/tables
- EXEC-SQL Glue script emitting SKIPPED_MISSING_CONFIG markers
"""
from __future__ import annotations

import importlib.util
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch


_CNSV_ROOT = Path(__file__).resolve().parents[1]  # .../deploy/projects/cnsv


def _load_module(module_name: str, file_path: Path):
    spec = importlib.util.spec_from_file_location(module_name, str(file_path))
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


class TestSkippedLogic(unittest.TestCase):
    def test_check_results_identifies_missing_config_errors_as_skipped(self):
        file_path = _CNSV_ROOT / "lambda" / "edv-check-results" / "lambda_function.py"

        with patch.dict(sys.modules, {"psycopg2": MagicMock()}):
            mod = _load_module("cnsv_edv_check_results_test", file_path)

        # Avoid touching DB logging in unit tests.
        mod.log_failures_to_db = MagicMock()

        event = {
            "layer": "STG",
            "results": [
                {"table_name": "t_ok", "status": "SUCCESS", "layer": "STG"},
                {
                    "table_name": "t_missing",
                    "status": "FAILED",
                    "layer": "STG",
                    "error": {
                        "Error": "States.TaskFailed",
                        "Cause": "FileNotFoundError: Could not resolve S3 casing for 'T_MISSING' under 'cnsv/_configs/STG/'",
                    },
                },
                {
                    "table_name": "t_hard_fail",
                    "status": "FAILED",
                    "layer": "STG",
                    "error": {
                        "Error": "States.TaskFailed",
                        "Cause": "syntax error at or near SELECT",
                    },
                },
            ],
        }

        out = mod.lambda_handler(event, None)

        self.assertEqual(out["successCount"], 1)
        self.assertEqual(out["skippedCount"], 1)
        self.assertEqual(out["skippedTables"], ["t_missing"])
        self.assertEqual(out["failedCount"], 1)
        self.assertEqual(out["failedTables"], ["t_hard_fail"])
        self.assertTrue(out["canContinue"])

    def test_finalize_summary_includes_skipped_fields(self):
        file_path = _CNSV_ROOT / "lambda" / "edv-finalize-pipeline" / "lambda_function.py"

        with patch.dict(sys.modules, {"psycopg2": MagicMock()}):
            mod = _load_module("cnsv_edv_finalize_test", file_path)

        # Avoid DB side effects in unit tests.
        mod.log_completion_to_db = MagicMock()

        event = {
            "data_src_nm": "cnsv",
            "run_type": "incremental",
            "start_date": "2026-01-27",
            "stageCheck": {
                "successCount": 10,
                "failedCount": 1,
                "failedTables": ["t_fail"],
                "skippedCount": 2,
                "skippedTables": ["t_skip_1", "t_skip_2"],
            },
            "edvCheck": {
                "successCount": 5,
                "failedCount": 0,
                "failedTables": [],
                "skippedCount": 1,
                "skippedTables": ["sat_skip"],
            },
            "counts": {"stg": 13, "dv": 6, "dvGroups": 1},
        }

        out = mod.lambda_handler(event, None)
        summary = out["summary"]

        self.assertEqual(summary["stage"]["skipped"], 2)
        self.assertEqual(summary["stage"]["skippedTables"], ["t_skip_1", "t_skip_2"])
        self.assertEqual(summary["edv"]["skipped"], 1)
        self.assertEqual(summary["edv"]["skippedTables"], ["sat_skip"])

    def test_exec_sql_glue_contains_skip_markers(self):
        """Static regression guard for SKIPPED_MISSING_CONFIG behavior in Glue script."""
        glue_path = _CNSV_ROOT / "glue" / "EXEC-SQL.py"
        text = glue_path.read_text(encoding="utf-8")

        self.assertIn("SKIPPED_MISSING_CONFIG: STG config missing", text)
        self.assertIn("SKIPPED_MISSING_CONFIG: Stage SQL not found", text)
        self.assertIn("SKIPPED_MISSING_CONFIG: {layer} config missing", text)


if __name__ == "__main__":
    unittest.main()
