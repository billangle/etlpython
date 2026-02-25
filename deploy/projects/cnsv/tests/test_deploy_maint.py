"""
Tests for deploy/projects/cnsv/deploy_maint.py — Contract Maintenance pipeline deployer.

All AWS calls are mocked so no live credentials are needed.
Run from the workspace root:

    pytest deploy/projects/cnsv/tests/test_deploy_maint.py -v

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

from projects.cnsv import deploy_maint  # noqa: E402 (after path fix)

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
class TestDeployMaintNaming(unittest.TestCase):
    """Quick unit tests for the pure naming functions — no mocks needed."""

    def test_build_names_prefix(self):
        names = deploy_maint.build_names("TST", "CNSV")
        self.assertEqual(names.prefix, "FSA-TST-CNSV")

    def test_build_names_glue_jobs(self):
        names = deploy_maint.build_names("TST", "CNSV")
        self.assertEqual(names.landing_glue_job, "FSA-TST-CNSV-Cntr-Maint-LandingFiles")
        self.assertEqual(names.raw_dm_glue_job, "FSA-TST-CNSV-Cntr-Maint-Raw-DM")

    def test_build_names_state_machines(self):
        names = deploy_maint.build_names("TST", "CNSV")
        self.assertEqual(names.sm_incremental_to_s3landing, "FSA-TST-CNSV-Cntr-Maint-Incremental-to-S3Landing")
        self.assertEqual(names.sm_s3landing_to_s3final_raw_dm, "FSA-TST-CNSV-Cntr-Maint-S3Landing-to-S3Final-Raw-DM")
        self.assertEqual(names.sm_process_control_update, "FSA-TST-CNSV-Cntr-Maint-Process-Control-Update")
        self.assertEqual(names.sm_main, "FSA-TST-CNSV-Cntr-Maint-Main")

    def test_build_names_lambdas(self):
        names = deploy_maint.build_names("TST", "CNSV")
        self.assertEqual(names.fn_get_incremental_tables, "FSA-TST-CNSV-Cntr-Maint-get-incremental-tables")
        self.assertEqual(names.fn_job_logging_end, "FSA-TST-CNSV-Cntr-Maint-Job-Logging-End")
        self.assertEqual(names.fn_validation_check, "FSA-TST-CNSV-Cntr-Maint-validation-check")
        self.assertEqual(names.fn_sns_publish_validations_report, "FSA-TST-CNSV-Cntr-Maint-sns-validations-report")

    def test_build_names_crawlers(self):
        names = deploy_maint.build_names("TST", "CNSV")
        self.assertIn("Cntr-Maint", names.crawler_final_zone)
        self.assertIn("Cntr-Maint", names.crawler_cdc)

    def test_build_names_missing_env_raises(self):
        with self.assertRaises(RuntimeError):
            deploy_maint.build_names("", "CNSV")

    def test_build_names_missing_project_raises(self):
        with self.assertRaises(RuntimeError):
            deploy_maint.build_names("TST", "")


class TestDeployMaintConfig(unittest.TestCase):
    """Validate that the test JSON config is well-formed for the maint deployer."""

    def setUp(self):
        self.cfg = _load_cfg("test_maint.json")

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
            "glueJobRoleArnParam",
            "etlRoleArnParam",
            "snsArnParam",
        ):
            with self.subTest(key=key):
                self.assertIn(key, sp)

    def test_glue_config_has_maint_sections(self):
        keys = set()
        for entry in self.cfg["GlueConfig"]:
            keys.update(entry.keys())
        self.assertIn("Cntr-Maint-LandingFiles", keys)
        self.assertIn("Cntr-Maint-Raw-DM", keys)

    def test_maint_specific_strparams_present(self):
        sp = self.cfg["strparams"]
        self.assertIn("cntrMaintJobIdKeyParam", sp)
        self.assertIn("cntrMaintFinalZoneCrawlerNameParam", sp)
        self.assertIn("cntrMaintCdcCrawlerNameParam", sp)


class TestDeployMaintDeploy(unittest.TestCase):
    """Integration-style tests: mock all AWS helpers and verify the returned summary."""

    def setUp(self):
        self.cfg = _load_cfg("test_maint.json")
        self.region = "us-east-1"

    def _run_deploy(self):
        """Call deploy() with all AWS surfaces mocked."""
        names = deploy_maint.build_names(
            self.cfg["deployEnv"], self.cfg["project"]
        )

        with (
            patch("projects.cnsv.deploy_maint.boto3.Session") as mock_session,
            patch("projects.cnsv.deploy_maint.ensure_bucket_exists"),
            patch("projects.cnsv.deploy_maint.ensure_glue_job"),
            patch("projects.cnsv.deploy_maint.ensure_lambda", side_effect=lambda _c, spec: _fake_lambda_arn(spec.name)),
            patch("projects.cnsv.deploy_maint.ensure_state_machine", side_effect=lambda _c, spec: _fake_sm_arn(spec.name)),
        ):
            mock_session.return_value = MagicMock()
            result = deploy_maint.deploy(self.cfg, self.region)

        return result, names

    def test_deploy_returns_dict(self):
        result, _ = self._run_deploy()
        self.assertIsInstance(result, dict)

    def test_deploy_env_and_project_in_result(self):
        result, _ = self._run_deploy()
        self.assertEqual(result["deploy_env"], "TST")
        self.assertEqual(result["project"], "CNSV")

    def test_glue_job_names_are_cntr_maint_prefixed(self):
        result, names = self._run_deploy()
        self.assertEqual(result["glue_job_landing_name"], names.landing_glue_job)
        self.assertIn("Cntr-Maint", result["glue_job_landing_name"])
        self.assertEqual(result["glue_job_raw_dm_name"], names.raw_dm_glue_job)
        self.assertIn("Cntr-Maint", result["glue_job_raw_dm_name"])

    def test_state_machine_names_are_cntr_maint_prefixed(self):
        result, names = self._run_deploy()
        for key in (
            "state_machine_incremental_to_s3landing_name",
            "state_machine_s3landing_to_s3final_raw_dm_name",
            "state_machine_process_control_update_name",
            "state_machine_main_name",
        ):
            with self.subTest(key=key):
                self.assertIn("Cntr-Maint", result[key])

    def test_lambda_names_are_cntr_maint_prefixed(self):
        result, _ = self._run_deploy()
        for key in (
            "lambda_get_incremental_tables_name",
            "lambda_raw_dm_etl_workflow_update_name",
            "lambda_raw_dm_sns_publish_errors_name",
            "lambda_job_logging_end_name",
            "lambda_validation_check_name",
            "lambda_sns_publish_validations_report_name",
        ):
            with self.subTest(key=key):
                self.assertIn("Cntr-Maint", result[key])

    def test_crawler_names_in_result(self):
        result, names = self._run_deploy()
        self.assertIn("crawler_final_zone_name", result)
        self.assertIn("crawler_cdc_name", result)
        self.assertIn("Cntr-Maint", result["crawler_final_zone_name"])
        self.assertIn("Cntr-Maint", result["crawler_cdc_name"])

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
        with self.assertRaises(RuntimeError):
            with (
                patch("projects.cnsv.deploy_maint.boto3.Session"),
                patch("projects.cnsv.deploy_maint.ensure_bucket_exists"),
                patch("projects.cnsv.deploy_maint.ensure_glue_job"),
                patch("projects.cnsv.deploy_maint.ensure_lambda", return_value="arn:test"),
                patch("projects.cnsv.deploy_maint.ensure_state_machine", return_value="arn:test"),
            ):
                deploy_maint.deploy(cfg, self.region)

    def test_missing_sfn_role_raises(self):
        cfg = dict(self.cfg)
        cfg["stepFunctions"] = {}
        with self.assertRaises(RuntimeError):
            with (
                patch("projects.cnsv.deploy_maint.boto3.Session"),
                patch("projects.cnsv.deploy_maint.ensure_bucket_exists"),
                patch("projects.cnsv.deploy_maint.ensure_glue_job"),
                patch("projects.cnsv.deploy_maint.ensure_lambda", return_value="arn:test"),
                patch("projects.cnsv.deploy_maint.ensure_state_machine", return_value="arn:test"),
            ):
                deploy_maint.deploy(cfg, self.region)


if __name__ == "__main__":
    unittest.main()
