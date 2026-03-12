"""
Regression tests for deploy/projects/cnsv/deploy.py (EXEC-SQL pipeline only).

These tests intentionally target recent production issues:
- handler name drift between deploy.py and lambda_function.py
- missing default EDV check path in the generated Step Functions definition
- Glue concurrency wiring from config into job deployment spec
"""
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

# deploy/projects/cnsv/tests/ -> deploy/projects/cnsv/ -> deploy/projects/ -> deploy/
_DEPLOY_ROOT = Path(__file__).resolve().parents[3]
if str(_DEPLOY_ROOT) not in sys.path:
    sys.path.insert(0, str(_DEPLOY_ROOT))

from projects.cnsv import deploy as deploy_exec_sql  # noqa: E402


def _base_cfg() -> dict:
    """Minimal config required by deploy/projects/cnsv/deploy.py."""
    return {
        "deployEnv": "TST",
        "project": "CNSV",
        "region": "us-east-1",
        "artifacts": {
            "artifactBucket": "unit-test-artifacts",
            "prefix": "cnsv/",
        },
        "strparams": {
            "glueJobRoleArnParam": "arn:aws:iam::123456789012:role/test-glue-role",
            "etlRoleArnParam": "arn:aws:iam::123456789012:role/test-lambda-role",
        },
        "stepFunctions": {
            "roleArn": "arn:aws:iam::123456789012:role/test-sfn-role",
        },
        "lambdas": {
            "layers": [
                "arn:aws:lambda:us-east-1:123456789012:layer:test-psycopg2:1",
                "arn:aws:lambda:us-east-1:123456789012:layer:test-polars:1",
            ],
            "runtime": "python3.11",
            "timeoutSeconds": 30,
            "memoryMb": 256,
        },
        "GlueConfig": [
            {
                "EXEC-SQL": {
                    "Connections": [{"ConnectionName": "TEST-CONN"}],
                    "MaxConcurrency": "3",
                    "MaxRetries": "0",
                    "TimeoutMinutes": "480",
                    "WorkerType": "G.2X",
                    "NumberOfWorkers": "2",
                    "GlueVersion": "4.0",
                    "JobParameters": {
                        "--data_src_nm": "cnsv",
                        "--run_type": "incremental",
                        "--start_date": "2026-01-27",
                    },
                }
            }
        ],
    }


class TestExecSqlDeployRegression(unittest.TestCase):
    def setUp(self):
        self.cfg = _base_cfg()
        self.region = "us-east-1"

    def _run_deploy(self):
        captured = {
            "glue_spec": None,
            "lambda_specs": [],
            "state_spec": None,
        }

        def _capture_glue(_client, _s3_client, spec):
            captured["glue_spec"] = spec
            return None

        def _capture_lambda(_client, spec):
            captured["lambda_specs"].append(spec)
            return f"arn:aws:lambda:us-east-1:123456789012:function:{spec.name}"

        def _capture_state_machine(_client, spec):
            captured["state_spec"] = spec
            return f"arn:aws:states:us-east-1:123456789012:stateMachine:{spec.name}"

        with (
            patch("projects.cnsv.deploy.boto3.Session") as mock_session,
            patch("projects.cnsv.deploy.ensure_bucket_exists"),
            patch("projects.cnsv.deploy.ensure_glue_job", side_effect=_capture_glue),
            patch("projects.cnsv.deploy.ensure_lambda", side_effect=_capture_lambda),
            patch("projects.cnsv.deploy.ensure_state_machine", side_effect=_capture_state_machine),
        ):
            mock_session.return_value = MagicMock()
            result = deploy_exec_sql.deploy(self.cfg, self.region)

        return result, captured

    def test_glue_max_concurrency_wired_from_config(self):
        _, captured = self._run_deploy()
        self.assertIsNotNone(captured["glue_spec"])
        self.assertEqual(captured["glue_spec"].max_concurrency, 3)

    def test_lambda_handler_auto_alignment_uses_lambda_handler(self):
        _, captured = self._run_deploy()
        self.assertEqual(len(captured["lambda_specs"]), 4)
        for spec in captured["lambda_specs"]:
            self.assertEqual(spec.handler, "lambda_function.lambda_handler")

    def test_lambda_layers_are_applied(self):
        _, captured = self._run_deploy()
        expected_layers = self.cfg["lambdas"]["layers"]
        for spec in captured["lambda_specs"]:
            self.assertEqual(spec.layers, expected_layers)

    def test_state_machine_has_default_edv_check_path(self):
        _, captured = self._run_deploy()
        definition = captured["state_spec"].definition
        states = definition["States"]

        self.assertIn("SetDefaultEDVCheck", states)
        self.assertEqual(states["ShouldProcessEDV"]["Default"], "SetDefaultEDVCheck")
        self.assertEqual(states["SetDefaultEDVCheck"]["ResultPath"], "$.edvCheck")
        self.assertEqual(
            states["FinalizePipeline"]["Parameters"]["Payload"]["edvCheck.$"],
            "$.edvCheck",
        )

    def test_state_machine_substitutions_from_job_parameters(self):
        _, captured = self._run_deploy()
        definition = captured["state_spec"].definition

        payload = definition["States"]["BuildProcessingPlan"]["Parameters"]["Payload"]
        self.assertEqual(payload["data_src_nm"], "cnsv")
        self.assertEqual(payload["run_type"], "incremental")
        self.assertEqual(payload["start_date"], "2026-01-27")
        self.assertEqual(payload["env"], "TST")

    def test_detect_lambda_handler_symbol_prefers_lambda_handler(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "lambda_function.py"
            p.write_text(
                "def handler(event, context):\n    return {}\n\n"
                "def lambda_handler(event, context):\n    return {}\n",
                encoding="utf-8",
            )
            sym = deploy_exec_sql._detect_lambda_handler_symbol(p)
            self.assertEqual(sym, "lambda_handler")

    def test_detect_lambda_handler_symbol_falls_back_to_handler(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "lambda_function.py"
            p.write_text("def handler(event, context):\n    return {}\n", encoding="utf-8")
            sym = deploy_exec_sql._detect_lambda_handler_symbol(p)
            self.assertEqual(sym, "handler")

    def test_detect_lambda_handler_symbol_raises_when_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "lambda_function.py"
            p.write_text("def not_a_handler():\n    return 1\n", encoding="utf-8")
            with self.assertRaises(RuntimeError):
                deploy_exec_sql._detect_lambda_handler_symbol(p)

    def test_exec_sql_glue_max_concurrency_aligned_across_env_configs(self):
        """Regression guard for ConcurrentRunsExceededException class of failures."""
        env_files = [
            Path("deploy/config/cnsv/dev.json"),
            Path("deploy/config/cnsv/steamdev.json"),
            Path("deploy/config/cnsv/prod.json"),
        ]

        for cfg_path in env_files:
            with self.subTest(config=str(cfg_path)):
                cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
                glue_entries = cfg.get("GlueConfig", [])

                exec_sql = None
                for entry in glue_entries:
                    if isinstance(entry, dict) and "EXEC-SQL" in entry:
                        exec_sql = entry["EXEC-SQL"]
                        break

                self.assertIsNotNone(exec_sql, f"EXEC-SQL section missing in {cfg_path}")
                self.assertEqual(
                    str(exec_sql.get("MaxConcurrency")),
                    "3",
                    f"EXEC-SQL MaxConcurrency must be 3 in {cfg_path}",
                )

    def test_exec_sql_state_map_concurrency_expected_values(self):
        """Regression guard for Step Functions fan-out limits."""
        asl_path = Path("deploy/projects/cnsv/states/EXEC-SQL.asl.json")
        asl = json.loads(asl_path.read_text(encoding="utf-8"))
        states = asl["States"]

        self.assertEqual(states["ProcessStageTables"]["MaxConcurrency"], 3)
        self.assertEqual(states["ProcessEDVGroups"]["MaxConcurrency"], 1)
        inner = (
            states["ProcessEDVGroups"]["Iterator"]["States"]["ProcessEDVTablesInGroup"]
        )
        self.assertEqual(inner["MaxConcurrency"], 3)


if __name__ == "__main__":
    unittest.main()
