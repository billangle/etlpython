"""Regression tests for deploy/projects/cars/deploy.py state machine substitutions."""

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

# deploy/projects/cars/tests/ -> deploy/projects/cars/ -> deploy/projects/ -> deploy/
_DEPLOY_ROOT = Path(__file__).resolve().parents[3]
if str(_DEPLOY_ROOT) not in sys.path:
    sys.path.insert(0, str(_DEPLOY_ROOT))

from projects.cars import deploy as cars_deploy  # noqa: E402


def _base_cfg() -> dict:
    return {
        "deployEnv": "TST",
        "artifacts": {
            "artifactBucket": "unit-test-artifacts",
            "prefix": "cars/",
        },
        "strparams": {
            "etlRoleArnParam": "arn:aws:iam::123456789012:role/test-lambda-role",
            "glueJobRoleArnParam": "arn:aws:iam::123456789012:role/test-glue-role",
            "lambdaRuntime": "python3.11",
            "thirdPartyLayerArnParam": "arn:aws:lambda:us-east-1:123456789012:layer:test-a:1",
            "customLayerArnParam": "arn:aws:lambda:us-east-1:123456789012:layer:test-b:1",
        },
        "stepFunctions": {
            "roleArn": "arn:aws:iam::123456789012:role/test-sfn-role",
        },
        "GlueJobParameters": {
            "Connections": [{"ConnectionName": "TEST-CONN"}],
            "MaxConcurrency": "3",
            "NumberOfWorkers": "2",
            "TimeoutMinutes": "60",
            "GlueVersion": "4.0",
        },
    }


class TestCarsDeployStateMachineSubstitutions(unittest.TestCase):
    def setUp(self) -> None:
        self.cfg = _base_cfg()
        self.region = "us-east-1"

    def _run_deploy(self):
        captured = {
            "state_specs": {},
            "state_order": [],
        }

        def _capture_lambda(_client, spec):
            return f"arn:aws:lambda:us-east-1:123456789012:function:{spec.name}"

        def _capture_state_machine(_client, spec):
            arn = (
                f"arn:aws:states:us-east-1:123456789012:stateMachine:{spec.name}"
            )
            captured["state_specs"][spec.name] = spec
            captured["state_order"].append(spec.name)
            return arn

        with (
            patch("projects.cars.deploy.boto3.Session") as mock_session,
            patch("projects.cars.deploy.ensure_bucket_exists"),
            patch("projects.cars.deploy.ensure_lambda", side_effect=_capture_lambda),
            patch("projects.cars.deploy.ensure_glue_job"),
            patch(
                "projects.cars.deploy.ensure_state_machine",
                side_effect=_capture_state_machine,
            ),
        ):
            mock_session.return_value = MagicMock()
            result = cars_deploy.deploy(self.cfg, self.region)

        return result, captured

    def test_master_pipeline_uses_deployed_child_state_machine_arns(self):
        result, captured = self._run_deploy()

        names = cars_deploy.build_names(self.cfg["deployEnv"])
        master_spec = captured["state_specs"][names.cars_master_state_machine]
        definition = master_spec.definition

        self.assertEqual(
            definition["States"]["BuildEDVInput"]["Parameters"]["StateMachineArn"],
            result["state_machine_edv_arn"],
        )
        self.assertEqual(
            definition["States"]["BuildDMInput"]["Parameters"]["StateMachineArn"],
            result["state_machine_cars_dm_etl_arn"],
        )

    def test_all_three_state_machines_are_deployed(self):
        result, captured = self._run_deploy()

        names = cars_deploy.build_names(self.cfg["deployEnv"])
        expected = {
            names.edv_state_machine,
            names.cars_dm_etl_state_machine,
            names.cars_master_state_machine,
        }

        self.assertEqual(set(captured["state_specs"].keys()), expected)
        self.assertIn("state_machine_cars_master_arn", result)

    def test_no_unresolved_placeholders_in_rendered_definitions(self):
        _, captured = self._run_deploy()
        for spec in captured["state_specs"].values():
            text = json.dumps(spec.definition)
            self.assertNotIn("__", text, f"Unresolved token in {spec.name}")


if __name__ == "__main__":
    unittest.main()
