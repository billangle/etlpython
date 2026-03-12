"""Contract tests for Cars deploy/config/state templates.

Modeled after athenafarm checks: enforce config + template invariants so
pipeline deployment behavior does not drift.
"""

from __future__ import annotations

import json
import re
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[4]
CARS_ROOT = REPO_ROOT / "deploy" / "projects" / "cars"
CONFIG_ROOT = REPO_ROOT / "deploy" / "config" / "cars"


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


class TestCarsConfigContract(unittest.TestCase):
    def setUp(self) -> None:
        self.config_files = [
            CONFIG_ROOT / "dev.json",
            CONFIG_ROOT / "steamdev.json",
            CONFIG_ROOT / "prod.json",
        ]

    def test_required_top_level_keys_all_configs(self):
        required = {
            "deployEnv",
            "project",
            "region",
            "artifacts",
            "strparams",
            "stepFunctions",
        }
        for cfg_path in self.config_files:
            with self.subTest(config=str(cfg_path)):
                cfg = _load_json(cfg_path)
                self.assertTrue(required.issubset(cfg.keys()))

    def test_required_strparams_keys_all_configs(self):
        required = {
            "etlRoleArnParam",
            "glueJobRoleArnParam",
            "thirdPartyLayerArnParam",
            "customLayerArnParam",
        }
        for cfg_path in self.config_files:
            with self.subTest(config=str(cfg_path)):
                cfg = _load_json(cfg_path)
                sp = cfg.get("strparams", {})
                self.assertTrue(required.issubset(sp.keys()))

    def test_stepfunction_role_arn_present_all_configs(self):
        for cfg_path in self.config_files:
            with self.subTest(config=str(cfg_path)):
                cfg = _load_json(cfg_path)
                role_arn = cfg.get("stepFunctions", {}).get("roleArn", "")
                self.assertIsInstance(role_arn, str)
                self.assertTrue(role_arn.startswith("arn:aws:iam::"))

    def test_project_name_is_cars_all_configs(self):
        for cfg_path in self.config_files:
            with self.subTest(config=str(cfg_path)):
                cfg = _load_json(cfg_path)
                self.assertEqual(cfg.get("project"), "cars")


class TestCarsStateTemplateContract(unittest.TestCase):
    def setUp(self) -> None:
        self.states = {
            "edv": _load_json(CARS_ROOT / "states" / "edv-pipeline.asl.json"),
            "dm": _load_json(CARS_ROOT / "states" / "dm-etl-pipeline.asl.json"),
            "master": _load_json(CARS_ROOT / "states" / "master-pipeline.asl.json"),
        }

    def test_all_state_templates_valid_json_objects(self):
        for name, doc in self.states.items():
            with self.subTest(template=name):
                self.assertIsInstance(doc, dict)
                self.assertIn("States", doc)

    def test_no_hardcoded_prod_resource_arns(self):
        bad_markers = [
            "arn:aws:lambda:us-east-1:253490756794:function:",
            "stateMachine:FSA-PROD-",
            "FSA-PROD-DATAMART-EXEC-DB-SQL",
            "FSA-PROD-DART-PG-TO-REDSHIFT",
        ]
        for name, doc in self.states.items():
            text = json.dumps(doc)
            with self.subTest(template=name):
                for marker in bad_markers:
                    self.assertNotIn(marker, text)

    def test_deploy_placeholder_contract_static_resources(self):
        edv_text = json.dumps(self.states["edv"])
        dm_text = json.dumps(self.states["dm"])
        master_text = json.dumps(self.states["master"])

        self.assertIn("__BUILD_PROCESSING_PLAN_FN_ARN__", edv_text)
        self.assertIn("__CHECK_RESULTS_FN_ARN__", edv_text)
        self.assertIn("__FINALIZE_PIPELINE_FN_ARN__", edv_text)
        self.assertIn("__HANDLE_FAILURE_FN_ARN__", edv_text)
        self.assertIn("__EXEC_SQL_GLUE_JOB_NAME__", edv_text)

        self.assertIn("__EXEC_SQL_GLUE_JOB_NAME__", dm_text)
        self.assertIn("__PG_TO_REDSHIFT_GLUE_JOB_NAME__", dm_text)

        self.assertIn("__EDV_PIPELINE_STATE_MACHINE_ARN__", master_text)
        self.assertIn("__DM_ETL_PIPELINE_STATE_MACHINE_ARN__", master_text)

    def test_env_and_data_src_are_deploy_placeholders(self):
        for name, doc in self.states.items():
            text = json.dumps(doc)
            with self.subTest(template=name):
                self.assertIn("__ENV__", text)
                self.assertIn("__DATA_SRC_NM__", text)
                self.assertNotIn('"env.$": "$.env"', text)
                self.assertNotIn('"data_src_nm.$": "$.data_src_nm"', text)

    def test_run_type_and_start_date_remain_runtime_overridable(self):
        for name, doc in self.states.items():
            text = json.dumps(doc)
            with self.subTest(template=name):
                self.assertIn("run_type.$", text)
                self.assertIn("start_date.$", text)
                self.assertNotIn("__RUN_TYPE__", text)
                self.assertNotIn("__START_DATE__", text)


class TestCarsDeployContract(unittest.TestCase):
    def test_build_names_convention(self):
        deploy_py = CARS_ROOT / "deploy.py"
        text = deploy_py.read_text(encoding="utf-8")
        self.assertIn('cars_master_state_machine=f"{prefix}-CARS-Master-Pipeline"', text)
        self.assertIn('edv_state_machine=f"{prefix}-CARS-EDV-Pipeline"', text)
        self.assertIn('cars_dm_etl_state_machine=f"{prefix}-CARS-DM-ETL-Pipeline"', text)

    def test_deploy_substitution_uses_env_and_data_src_not_run_type_start_date(self):
        deploy_py = CARS_ROOT / "deploy.py"
        text = deploy_py.read_text(encoding="utf-8")

        # Runtime override contract: these should NOT be deploy-time substitutions.
        self.assertNotRegex(text, r'"__RUN_TYPE__"\s*:')
        self.assertNotRegex(text, r'"__START_DATE__"\s*:')

        # Deploy-time substitutions expected.
        self.assertRegex(text, r'"__ENV__"\s*:')
        self.assertRegex(text, r'"__DATA_SRC_NM__"\s*:')


if __name__ == "__main__":
    unittest.main()
