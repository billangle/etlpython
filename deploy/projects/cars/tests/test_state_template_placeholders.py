"""Regression checks for Cars ASL template placeholder strategy."""

from __future__ import annotations

import json
import unittest
from pathlib import Path


class TestCarsStateTemplatePlaceholders(unittest.TestCase):
    def setUp(self) -> None:
        self.states_dir = Path("deploy/projects/cars/states")

    def _load_json(self, name: str) -> dict:
        path = self.states_dir / name
        return json.loads(path.read_text(encoding="utf-8"))

    def test_edv_template_uses_placeholders_not_hardcoded_arns(self):
        text = json.dumps(self._load_json("edv-pipeline.asl.json"))
        self.assertIn("__BUILD_PROCESSING_PLAN_FN_ARN__", text)
        self.assertIn("__CHECK_RESULTS_FN_ARN__", text)
        self.assertIn("__FINALIZE_PIPELINE_FN_ARN__", text)
        self.assertIn("__HANDLE_FAILURE_FN_ARN__", text)
        self.assertIn("__EXEC_SQL_GLUE_JOB_NAME__", text)
        self.assertNotIn("FSA-PROD", text)
        self.assertNotIn("arn:aws:lambda:us-east-1:253490756794", text)

    def test_dm_etl_template_uses_glue_job_placeholders(self):
        text = json.dumps(self._load_json("dm-etl-pipeline.asl.json"))
        self.assertIn("__EXEC_SQL_GLUE_JOB_NAME__", text)
        self.assertNotIn("FSA-PROD-DATAMART-EXEC-DB-SQL", text)
        self.assertNotIn("FSA-PROD-DART-PG-TO-REDSHIFT", text)

    def test_master_template_references_child_arns_via_placeholders(self):
        text = json.dumps(self._load_json("master-pipeline.asl.json"))
        self.assertIn("__EDV_PIPELINE_STATE_MACHINE_ARN__", text)
        self.assertIn("__DM_ETL_PIPELINE_STATE_MACHINE_ARN__", text)
        self.assertNotIn("stateMachine:FSA-PROD-CARS-EDV-Pipeline", text)
        self.assertNotIn("stateMachine:FSA-PROD-CARS-DM-ETL-Pipeline", text)


if __name__ == "__main__":
    unittest.main()
