"""
Tests for deploy/projects/cnsv/deploy_base.py — CNSV base pipeline deployer.

All AWS calls are mocked so no live credentials are needed.
Run from the workspace root:

    pytest deploy/projects/cnsv/tests/test_deploy_base.py -v

Or run all CNSV tests via:

    deploy/projects/cnsv/tests/run_tests.sh
"""
from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Path bootstrap (also done by conftest.py, but duplicated here for safety
# when the file is executed directly).
# ---------------------------------------------------------------------------
_DEPLOY_ROOT = Path(__file__).resolve().parents[3]   # .../deploy/
if str(_DEPLOY_ROOT) not in sys.path:
    sys.path.insert(0, str(_DEPLOY_ROOT))

from projects.cnsv import deploy_base  # noqa: E402 (after path fix)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
_DATA_DIR = Path(__file__).resolve().parent / "data"


def _load_cfg(filename: str) -> dict:
    return json.loads((_DATA_DIR / filename).read_text(encoding="utf-8"))


def _fake_lambda_arn(name: str) -> str:
    return f"arn:aws:lambda:us-east-1:123456789012:function:{name}"


def _fake_sm_arn(name: str) -> str:
    return f"arn:aws:states:us-east-1:123456789012:stateMachine:{name}"


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
class TestDeployBaseNaming(unittest.TestCase):
    """Quick unit tests for the pure naming functions — no mocks needed."""

    def test_build_names_prefix(self):
        names = deploy_base.build_names("TST", "CNSV")
        self.assertEqual(names.prefix, "FSA-TST-CNSV")

    def test_build_names_glue_jobs(self):
        names = deploy_base.build_names("TST", "CNSV")
        self.assertEqual(names.landing_glue_job, "FSA-TST-CNSV-LandingFiles")
        self.assertEqual(names.raw_dm_glue_job, "FSA-TST-CNSV-Raw-DM")

    def test_build_names_state_machines(self):
        names = deploy_base.build_names("TST", "CNSV")
        self.assertEqual(names.sm_incremental_to_s3landing, "FSA-TST-CNSV-Incremental-to-S3Landing")
        self.assertEqual(names.sm_s3landing_to_s3final_raw_dm, "FSA-TST-CNSV-S3Landing-to-S3Final-Raw-DM")
        self.assertEqual(names.sm_process_control_update, "FSA-TST-CNSV-Process-Control-Update")
        self.assertEqual(names.sm_main, "FSA-TST-CNSV-Main")

    def test_build_names_lambdas(self):
        names = deploy_base.build_names("TST", "CNSV")
        self.assertEqual(names.fn_get_incremental_tables, "FSA-TST-CNSV-get-incremental-tables")
        self.assertEqual(names.fn_raw_dm_etl_workflow_update, "FSA-TST-CNSV-RAW-DM-etl-update-data-ppln-job")
        self.assertEqual(names.fn_raw_dm_sns_publish_errors, "FSA-TST-CNSV-RAW-DM-sns-step-function-errors")
        self.assertEqual(names.fn_job_logging_end, "FSA-TST-CNSV-Job-Logging-End")
        self.assertEqual(names.fn_validation_check, "FSA-TST-CNSV-validation-check")
        self.assertEqual(names.fn_sns_publish_validations_report, "FSA-TST-CNSV-sns-validations-report")

    def test_build_names_missing_env_raises(self):
        with self.assertRaises(RuntimeError):
            deploy_base.build_names("", "CNSV")

    def test_build_names_missing_project_raises(self):
        with self.assertRaises(RuntimeError):
            deploy_base.build_names("TST", "")


class TestDeployBaseConfig(unittest.TestCase):
    """Validate that the test JSON config is well-formed."""

    def setUp(self):
        self.cfg = _load_cfg("test_base.json")

    def test_required_top_level_keys(self):
        for key in ("deployEnv", "project", "region", "strparams", "artifacts", "stepFunctions", "GlueConfig"):
            with self.subTest(key=key):
                self.assertIn(key, self.cfg)

    def test_required_strparams(self):
        sp = self.cfg["strparams"]
        for key in (
            "landingBucketNameParam",
            "cleanBucketNameParam",
            "finalBucketNameParam",
            "jobIdKeyParam",
            "glueJobRoleArnParam",
            "etlRoleArnParam",
            "snsArnParam",
        ):
            with self.subTest(key=key):
                self.assertIn(key, sp)

    def test_glue_config_has_landing_and_raw_dm(self):
        keys = set()
        for entry in self.cfg["GlueConfig"]:
            keys.update(entry.keys())
        self.assertIn("LandingFiles", keys)
        self.assertIn("Raw-DM", keys)


class TestDeployBaseDeploy(unittest.TestCase):
    """Integration-style tests: mock all AWS helpers and verify the returned summary."""

    def setUp(self):
        self.cfg = _load_cfg("test_base.json")
        self.region = "us-east-1"

    def _run_deploy(self):
        """Call deploy() with all AWS surfaces mocked."""
        names = deploy_base.build_names(
            self.cfg["deployEnv"], self.cfg["project"]
        )

        lambda_side_effect = MagicMock(side_effect=lambda _client, spec: _fake_lambda_arn(spec.name))
        sm_side_effect = MagicMock(side_effect=lambda _client, spec: _fake_sm_arn(spec.name))

        with (
            patch("projects.cnsv.deploy_base.boto3.Session") as mock_session,
            patch("projects.cnsv.deploy_base.ensure_bucket_exists") as mock_bucket,
            patch("projects.cnsv.deploy_base.ensure_glue_job") as mock_glue_job,
            patch("projects.cnsv.deploy_base.ensure_lambda", side_effect=lambda _c, spec: _fake_lambda_arn(spec.name)),
            patch("projects.cnsv.deploy_base.ensure_state_machine", side_effect=lambda _c, spec: _fake_sm_arn(spec.name)),
        ):
            mock_session.return_value = MagicMock()
            result = deploy_base.deploy(self.cfg, self.region)

        return result, names

    def test_deploy_returns_dict(self):
        result, _ = self._run_deploy()
        self.assertIsInstance(result, dict)

    def test_deploy_env_and_project_in_result(self):
        result, _ = self._run_deploy()
        self.assertEqual(result["deploy_env"], "TST")
        self.assertEqual(result["project"], "CNSV")

    def test_glue_job_names_in_result(self):
        result, names = self._run_deploy()
        self.assertEqual(result["glue_job_landing_name"], names.landing_glue_job)
        self.assertEqual(result["glue_job_raw_dm_name"], names.raw_dm_glue_job)

    def test_lambda_names_in_result(self):
        result, names = self._run_deploy()
        self.assertEqual(result["lambda_get_incremental_tables_name"], names.fn_get_incremental_tables)
        self.assertEqual(result["lambda_job_logging_end_name"], names.fn_job_logging_end)
        self.assertEqual(result["lambda_validation_check_name"], names.fn_validation_check)

    def test_state_machine_names_in_result(self):
        result, names = self._run_deploy()
        self.assertEqual(result["state_machine_incremental_to_s3landing_name"], names.sm_incremental_to_s3landing)
        self.assertEqual(result["state_machine_s3landing_to_s3final_raw_dm_name"], names.sm_s3landing_to_s3final_raw_dm)
        self.assertEqual(result["state_machine_process_control_update_name"], names.sm_process_control_update)
        self.assertEqual(result["state_machine_main_name"], names.sm_main)

    def test_lambda_arns_have_expected_prefix(self):
        result, _ = self._run_deploy()
        for key in (
            "lambda_get_incremental_tables_arn",
            "lambda_raw_dm_etl_workflow_update_arn",
            "lambda_raw_dm_sns_publish_errors_arn",
            "lambda_job_logging_end_arn",
            "lambda_validation_check_arn",
            "lambda_sns_publish_validations_report_arn",
        ):
            with self.subTest(key=key):
                self.assertTrue(
                    result[key].startswith("arn:aws:lambda:"),
                    f"{key} ARN should start with 'arn:aws:lambda:' — got: {result[key]}",
                )

    def test_state_machine_arns_have_expected_prefix(self):
        result, _ = self._run_deploy()
        for key in (
            "state_machine_incremental_to_s3landing_arn",
            "state_machine_s3landing_to_s3final_raw_dm_arn",
            "state_machine_process_control_update_arn",
            "state_machine_main_arn",
        ):
            with self.subTest(key=key):
                self.assertTrue(
                    result[key].startswith("arn:aws:states:"),
                    f"{key} ARN should start with 'arn:aws:states:' — got: {result[key]}",
                )

    def test_missing_landing_bucket_raises(self):
        cfg = dict(self.cfg)
        sp = dict(cfg["strparams"])
        sp.pop("landingBucketNameParam", None)
        cfg["strparams"] = sp
        with self.assertRaises(RuntimeError, msg="Should raise when landingBucketNameParam is missing"):
            with (
                patch("projects.cnsv.deploy_base.boto3.Session"),
                patch("projects.cnsv.deploy_base.ensure_bucket_exists"),
                patch("projects.cnsv.deploy_base.ensure_glue_job"),
                patch("projects.cnsv.deploy_base.ensure_lambda", return_value="arn:test"),
                patch("projects.cnsv.deploy_base.ensure_state_machine", return_value="arn:test"),
            ):
                deploy_base.deploy(cfg, self.region)

    def test_missing_sfn_role_raises(self):
        cfg = dict(self.cfg)
        cfg["stepFunctions"] = {}
        with self.assertRaises(RuntimeError, msg="Should raise when stepFunctions.roleArn is missing"):
            with (
                patch("projects.cnsv.deploy_base.boto3.Session"),
                patch("projects.cnsv.deploy_base.ensure_bucket_exists"),
                patch("projects.cnsv.deploy_base.ensure_glue_job"),
                patch("projects.cnsv.deploy_base.ensure_lambda", return_value="arn:test"),
                patch("projects.cnsv.deploy_base.ensure_state_machine", return_value="arn:test"),
            ):
                deploy_base.deploy(cfg, self.region)


if __name__ == "__main__":
    unittest.main()
