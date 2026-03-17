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
            "state_specs": [],
        }

        def _capture_glue(_client, _s3_client, spec):
            captured["glue_spec"] = spec
            return None

        def _capture_lambda(_client, spec):
            captured["lambda_specs"].append(spec)
            return f"arn:aws:lambda:us-east-1:123456789012:function:{spec.name}"

        def _capture_state_machine(_client, spec):
            captured["state_specs"].append(spec)
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
        self.assertEqual(len(captured["lambda_specs"]), 5)
        for spec in captured["lambda_specs"]:
            self.assertEqual(spec.handler, "lambda_function.lambda_handler")

    def test_lambda_layers_are_applied(self):
        _, captured = self._run_deploy()
        expected_layers = self.cfg["lambdas"]["layers"]
        for spec in captured["lambda_specs"]:
            self.assertEqual(spec.layers, expected_layers)

    def test_check_sql_lambda_uses_artifact_bucket_from_artifacts_config(self):
        _, captured = self._run_deploy()
        check_sql_spec = next(
            s for s in captured["lambda_specs"] if s.name.endswith("-check-sql")
        )
        self.assertEqual(
            check_sql_spec.env.get("ARTIFACT_BUCKET"),
            self.cfg["artifacts"]["artifactBucket"],
        )

    def test_state_machine_has_preflight_check_sql_task(self):
        _, captured = self._run_deploy()
        check_spec = next(
            s for s in captured["state_specs"] if s.name.endswith("-CHECK-EXEC-SQL")
        )
        definition = check_spec.definition
        states = definition["States"]

        self.assertIn("BuildProcessingPlan", states)
        self.assertIn("CheckSqlFilesExist", states)
        self.assertEqual(states["BuildProcessingPlan"]["Next"], "CheckSqlFilesExist")
        self.assertEqual(states["CheckSqlFilesExist"]["ResultPath"], "$.checkSqlResult")
        self.assertEqual(states["CheckSqlFilesExist"]["Next"], "PreflightCheckComplete")

    def test_state_machine_runtime_parameters_with_defaults(self):
        _, captured = self._run_deploy()
        check_spec = next(
            s for s in captured["state_specs"] if s.name.endswith("-CHECK-EXEC-SQL")
        )
        definition = check_spec.definition

        payload = definition["States"]["BuildProcessingPlan"]["Parameters"]["Payload"]
        self.assertEqual(payload["data_src_nm.$"], "$.data_src_nm")
        self.assertEqual(payload["run_type.$"], "$.run_type")
        self.assertEqual(payload["start_date.$"], "$.start_date")
        self.assertEqual(payload["env.$"], "$.env")

        states = definition["States"]
        self.assertEqual(states["SetDefaultDataSrcNm"]["Result"], "cnsv")
        self.assertEqual(states["SetDefaultRunType"]["Result"], "incremental")
        self.assertEqual(states["SetDefaultStartDate"]["Result"], "2026-01-27")
        self.assertEqual(states["SetDefaultEnv"]["Result"], "TST")

    def test_deploys_both_exec_sql_and_check_exec_sql_state_machines(self):
        _, captured = self._run_deploy()
        names = {s.name for s in captured["state_specs"]}
        self.assertIn("FSA-TST-CNSV-EXEC-SQL", names)
        self.assertIn("FSA-TST-CNSV-CHECK-EXEC-SQL", names)

    def test_exec_sql_state_machine_uses_runtime_parameters(self):
        _, captured = self._run_deploy()
        exec_spec = next(
            s for s in captured["state_specs"] if s.name.endswith("-EXEC-SQL") and not s.name.endswith("-CHECK-EXEC-SQL")
        )
        states = exec_spec.definition["States"]
        payload = states["BuildProcessingPlan"]["Parameters"]["Payload"]

        self.assertEqual(payload["data_src_nm.$"], "$.data_src_nm")
        self.assertEqual(payload["run_type.$"], "$.run_type")
        self.assertEqual(payload["start_date.$"], "$.start_date")
        self.assertEqual(payload["env.$"], "$.env")
        self.assertEqual(states["SetDefaultDataSrcNm"]["Result"], "cnsv")

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

    def test_exec_sql_glue_tasks_retry_concurrent_runs_exceeded(self):
        """Regression guard for transient Glue concurrency throttling."""
        asl_path = Path("deploy/projects/cnsv/states/EXEC-SQL.asl.json")
        asl = json.loads(asl_path.read_text(encoding="utf-8"))
        states = asl["States"]

        stage_task = states["ProcessStageTables"]["Iterator"]["States"]["ExecuteStageSQL"]
        edv_task = (
            states["ProcessEDVGroups"]["Iterator"]["States"]["ProcessEDVTablesInGroup"]
            ["Iterator"]["States"]["ExecuteEDVSQL"]
        )

        for task in (stage_task, edv_task):
            retries = task.get("Retry", [])
            self.assertTrue(retries, "Expected Retry policy on Glue task")
            first = retries[0]
            self.assertIn("Glue.ConcurrentRunsExceededException", first.get("ErrorEquals", []))
            self.assertEqual(first.get("IntervalSeconds"), 20)
            self.assertEqual(first.get("BackoffRate"), 2)
            self.assertEqual(first.get("MaxAttempts"), 6)

    def test_exec_sql_glue_tasks_retry_states_task_failed_for_transient_startup(self):
        """Guardrail for transient Glue startup failures surfaced as States.TaskFailed."""
        asl_path = Path("deploy/projects/cnsv/states/EXEC-SQL.asl.json")
        asl = json.loads(asl_path.read_text(encoding="utf-8"))
        states = asl["States"]

        stage_task = states["ProcessStageTables"]["Iterator"]["States"]["ExecuteStageSQL"]
        edv_task = (
            states["ProcessEDVGroups"]["Iterator"]["States"]["ProcessEDVTablesInGroup"]
            ["Iterator"]["States"]["ExecuteEDVSQL"]
        )

        for task in (stage_task, edv_task):
            retries = task.get("Retry", [])
            tf_retry = next(
                (r for r in retries if "States.TaskFailed" in r.get("ErrorEquals", [])),
                None,
            )
            self.assertIsNotNone(tf_retry, "Expected States.TaskFailed retry policy")
            self.assertEqual(tf_retry.get("IntervalSeconds"), 90)
            self.assertEqual(tf_retry.get("BackoffRate"), 2)
            self.assertEqual(tf_retry.get("MaxAttempts"), 3)

    def test_all_cnsv_glue_state_files_have_concurrency_retry_marker(self):
        """CNSV guardrail: every Glue startJobRun.sync flow handles concurrency throttling."""
        state_files = sorted(Path("deploy/projects/cnsv/states").glob("*.asl.json"))

        checked = 0
        for state_file in state_files:
            text = state_file.read_text(encoding="utf-8")
            if "startJobRun.sync" not in text:
                continue

            checked += 1
            self.assertIn(
                "Glue.ConcurrentRunsExceededException",
                text,
                f"Missing Glue concurrent-runs retry marker in {state_file}",
            )

        self.assertGreater(checked, 0, "Expected at least one Glue state file to be checked")


if __name__ == "__main__":
    unittest.main()
