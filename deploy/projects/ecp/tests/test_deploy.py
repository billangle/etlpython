"""
Regression tests for deploy/projects/ecp/deploy.py.

Adapted from relevant CNSV deploy tests:
- deploy-time wiring for Glue, Lambda, and Step Functions
- handler/env consistency checks
- state machine substitution checks, including shortened lambda name matching
"""
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

# deploy/projects/ecp/tests -> deploy/projects/ecp -> deploy/projects -> deploy
_DEPLOY_ROOT = Path(__file__).resolve().parents[3]
if str(_DEPLOY_ROOT) not in sys.path:
    sys.path.insert(0, str(_DEPLOY_ROOT))

from projects.ecp import deploy as cps_deploy  # noqa: E402


def _base_cfg() -> dict:
    return {
        "deployEnv": "TST",
        "project": "ECP",
        "region": "us-east-1",
        "artifacts": {
            "artifactBucket": "unit-test-artifacts",
            "prefix": "ecp/",
        },
        "strparams": {
            "landingBucketNameParam": "c108-tst-fpacfsa-landing-zone",
            "finalBucketNameParam": "c108-tst-fpacfsa-final-zone",
            "jobIdKeyParam": "ecp/etl-jobs/job_id.json",
            "glueJobRoleArnParam": "arn:aws:iam::123456789012:role/test-glue-role",
            "etlRoleArnParam": "arn:aws:iam::123456789012:role/test-lambda-role",
            "snsArnParam": "arn:aws:sns:us-east-1:123456789012:FSA-TST-ECP",
        },
        "secretId": "FSA-TST-secrets",
        "stepFunctions": {
            "roleArn": "arn:aws:iam::123456789012:role/test-sfn-role",
        },
        "glueDatabases": [
            {
                "name": "fsa-{deployEnvLower}-{projectLower}",
                "description": "primary {projectLower} database",
            },
            {
                "name": "fsa-{deployEnvLower}-{projectLower}-cdc",
                "description": "cdc {projectLower} database",
            },
        ],
        "LambdaConfig": [
            {
                "Job-Logging-End": {
                    "runtime": "python3.11",
                    "timeoutSeconds": 30,
                    "memoryMb": 256,
                    "layers": ["arn:aws:lambda:us-east-1:123456789012:layer:test:1"],
                    "environmentVariables": {
                        "SecretId": "FSA-TST-secrets",
                        "CRAWLER_NAME": "FSA-TST-ECP",
                    },
                }
            },
            {
                "RAW-DM-etl-workflow-update": {
                    "runtime": "python3.11",
                    "timeoutSeconds": 30,
                    "memoryMb": 256,
                    "layers": ["arn:aws:lambda:us-east-1:123456789012:layer:test:1"],
                }
            },
            {
                "RAW-DM-sns-pub-step-func-errs": {
                    "runtime": "python3.11",
                    "timeoutSeconds": 30,
                    "memoryMb": 256,
                    "layers": ["arn:aws:lambda:us-east-1:123456789012:layer:test:1"],
                    "environmentVariables": {
                        "SNS_ARN": "arn:aws:sns:us-east-1:123456789012:FSA-TST-ECP",
                    },
                }
            },
            {
                "get-incremental-tables": {
                    "runtime": "python3.11",
                    "timeoutSeconds": 30,
                    "memoryMb": 256,
                    "layers": ["arn:aws:lambda:us-east-1:123456789012:layer:test:1"],
                    "environmentVariables": {
                        "source_folder": "ecp",
                    },
                }
            },
            {
                "sns-publish-validations-report": {
                    "runtime": "python3.11",
                    "timeoutSeconds": 30,
                    "memoryMb": 256,
                    "layers": ["arn:aws:lambda:us-east-1:123456789012:layer:test:1"],
                    "environmentVariables": {
                        "SNS_ARN": "arn:aws:sns:us-east-1:123456789012:FSA-TST-ECP",
                    },
                }
            },
            {
                "validation-check": {
                    "runtime": "python3.11",
                    "timeoutSeconds": 30,
                    "memoryMb": 256,
                    "layers": ["arn:aws:lambda:us-east-1:123456789012:layer:test:1"],
                    "environmentVariables": {
                        "SecretId": "FSA-TST-secrets",
                        "CRAWLER_NAME": "FSA-TST-ECP",
                    },
                }
            },
        ],
        "GlueConfig": [
            {
                "LandingFiles": {
                    "Connections": [{"ConnectionName": "TEST-CONN"}],
                    "MaxConcurrency": "1",
                    "MaxRetries": "0",
                    "TimeoutMinutes": "2880",
                    "WorkerType": "G.2X",
                    "NumberOfWorkers": "8",
                    "GlueVersion": "4.0",
                    "ReferencePath": "s3://bucket/ref-files/config.json",
                    "AdditionalPythonModulesPath": "s3://bucket/wheels/psycopg2.whl",
                    "JobParameters": {
                        "--DestinationBucket": "c108-tst-fpacfsa-landing-zone",
                        "--DestinationPrefix": "ecp/etl-jobs",
                        "--PipelineName": "ecp",
                        "--SecretId": "FSA-TST-secrets",
                        "--SourcePrefix": "ecp/dbo",
                    },
                }
            },
            {
                "Raw-DM": {
                    "Connections": [{"ConnectionName": "TEST-CONN"}],
                    "MaxConcurrency": "20",
                    "MaxRetries": "0",
                    "TimeoutMinutes": "600",
                    "WorkerType": "G.2X",
                    "NumberOfWorkers": "2",
                    "GlueVersion": "4.0",
                    "JobParameters": {
                        "--env": "tst",
                        "--postgres_prcs_ctrl_dbname": "metadata_edw",
                        "--region_name": "us-east-1",
                        "--secret_name": "FSA-TST-secrets",
                        "--target_bucket": "c108-tst-fpacfsa-final-zone",
                        "--target_prefix": "ecp",
                    },
                }
            },
        ],
        "crawlers": [
            {
                "crawlerName": "FSA-{deployEnv}-{projectName}",
                "databaseName": "fsa-{deployEnvLower}-{projectLower}",
                "recrawlBehavior": "CRAWL_EVERYTHING",
                "excludePatterns": ["_cdc/**", "_configs/**"],
                "s3TargetsByBucket": [
                    {
                        "bucket": "c108-tst-fpacfsa-final-zone",
                        "prefixes": ["ecp/"],
                    }
                ],
            },
            {
                "crawlerName": "FSA-{deployEnv}-{projectName}-cdc",
                "databaseName": "fsa-{deployEnvLower}-{projectLower}-cdc",
                "recrawlBehavior": "CRAWL_EVERYTHING",
                "excludePatterns": ["_cdc/**", "_configs/**"],
                "s3TargetsByBucket": [
                    {
                        "bucket": "c108-tst-fpacfsa-final-zone",
                        "prefixes": ["ecp/_cdc/"],
                    }
                ],
            }
        ],
    }


class TestCpsDeployRegression(unittest.TestCase):
    def setUp(self):
        self.cfg = _base_cfg()
        self.region = "us-east-1"

    def _run_deploy(self):
        captured = {
            "glue_specs": [],
            "glue_dbs": [],
            "lambda_specs": [],
            "crawler_specs": [],
            "state_specs": [],
        }

        def _capture_glue(_client, _s3_client, spec):
            captured["glue_specs"].append(spec)
            return None

        def _capture_glue_db(_client, name, description=""):
            captured["glue_dbs"].append((name, description))
            return name

        def _capture_lambda(_client, spec):
            captured["lambda_specs"].append(spec)
            return f"arn:aws:lambda:us-east-1:123456789012:function:{spec.name}"

        def _capture_state(_client, spec):
            captured["state_specs"].append(spec)
            return f"arn:aws:states:us-east-1:123456789012:stateMachine:{spec.name}"

        def _capture_crawler(_client, spec):
            captured["crawler_specs"].append(spec)
            return spec.name

        with (
            patch("projects.ecp.deploy.boto3.Session") as mock_session,
            patch("projects.ecp.deploy.ensure_bucket_exists"),
            patch("projects.ecp.deploy.ensure_glue_job", side_effect=_capture_glue),
            patch("projects.ecp.deploy.ensure_glue_database", side_effect=_capture_glue_db),
            patch("projects.ecp.deploy.ensure_lambda", side_effect=_capture_lambda),
            patch("projects.ecp.deploy.ensure_glue_crawler", side_effect=_capture_crawler),
            patch("projects.ecp.deploy.ensure_state_machine", side_effect=_capture_state),
        ):
            mock_session.return_value = MagicMock()
            result = cps_deploy.deploy(self.cfg, self.region)

        return result, captured

    def test_crawlers_deploy_with_env_names_recrawl_and_exclusions(self):
        _, captured = self._run_deploy()
        self.assertEqual(len(captured["crawler_specs"]), 2)
        by_name = {s.name: s for s in captured["crawler_specs"]}

        self.assertIn("FSA-TST-ECP", by_name)
        self.assertIn("FSA-TST-ECP-cdc", by_name)

        main = by_name["FSA-TST-ECP"]
        self.assertEqual(main.recrawl_behavior, "CRAWL_EVERYTHING")
        self.assertEqual(main.exclude_patterns, ["_cdc/**", "_configs/**"])
        self.assertEqual(main.s3_targets[0]["Path"], "s3://c108-tst-fpacfsa-final-zone/ecp/")

    def test_glue_databases_ensured_before_crawlers(self):
        _, captured = self._run_deploy()
        db_names = {name for name, _desc in captured["glue_dbs"]}
        self.assertEqual(db_names, {"fsa-tst-ecp", "fsa-tst-ecp-cdc"})

    def test_crawler_name_override_is_used_when_provided(self):
        _, captured = self._run_deploy()
        deployed_names = {s.name for s in captured["crawler_specs"]}
        self.assertIn("FSA-TST-ECP", deployed_names)
        self.assertIn("FSA-TST-ECP-cdc", deployed_names)

    def test_glue_jobs_wired_from_config(self):
        _, captured = self._run_deploy()
        self.assertEqual(len(captured["glue_specs"]), 2)

        by_name = {s.name: s for s in captured["glue_specs"]}
        self.assertIn("FSA-TST-ECP-LandingFiles", by_name)
        self.assertIn("FSA-TST-ECP-Raw-DM", by_name)

        self.assertEqual(by_name["FSA-TST-ECP-LandingFiles"].max_concurrency, 1)
        self.assertEqual(by_name["FSA-TST-ECP-Raw-DM"].max_concurrency, 20)

    def test_glue_reference_and_python_module_paths_map_to_distinct_args(self):
        _, captured = self._run_deploy()
        by_name = {s.name: s for s in captured["glue_specs"]}
        landing_args = by_name["FSA-TST-ECP-LandingFiles"].default_args

        self.assertEqual(
            landing_args.get("--extra-files"),
            "s3://bucket/ref-files/config.json",
        )
        self.assertEqual(
            landing_args.get("--additional-python-modules"),
            "s3://bucket/wheels/psycopg2.whl",
        )
        self.assertNotIn("--extra-py-files", landing_args)

    def test_lambda_handlers_and_count(self):
        _, captured = self._run_deploy()
        self.assertEqual(len(captured["lambda_specs"]), 6)
        for spec in captured["lambda_specs"]:
            self.assertEqual(spec.handler, "lambda_function.lambda_handler")
            self.assertEqual(spec.runtime, "python3.11")
            self.assertEqual(spec.timeout, 30)
            self.assertEqual(spec.memory, 256)
            self.assertEqual(spec.layers, ["arn:aws:lambda:us-east-1:123456789012:layer:test:1"])

    def test_prepare_lambda_source_normalizes_hyphen_filename(self):
        with tempfile.TemporaryDirectory() as tmp:
            src = Path(tmp) / "fn"
            src.mkdir(parents=True, exist_ok=True)
            (src / "lambda-function.py").write_text(
                "def lambda_handler(event, context):\n    return {'ok': True}\n",
                encoding="utf-8",
            )

            staged_path, tmp_ctx = cps_deploy._prepare_lambda_source(src)
            try:
                staged = Path(staged_path)
                self.assertTrue((staged / "lambda_function.py").exists())
                self.assertFalse((staged / "lambda-function.py").exists())
                sym = cps_deploy._detect_handler_symbol(staged / "lambda_function.py")
                self.assertEqual(sym, "lambda_handler")
            finally:
                if tmp_ctx is not None:
                    tmp_ctx.cleanup()

    def test_detect_handler_symbol_raises_when_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "lambda_function.py"
            p.write_text("def not_a_handler():\n    return 1\n", encoding="utf-8")
            with self.assertRaises(RuntimeError):
                cps_deploy._detect_handler_symbol(p)

    def test_all_cps_lambdas_resolve_to_lambda_handler(self):
        lambda_root = _DEPLOY_ROOT / "projects" / "ecp" / "lambda"
        dirs = sorted([p for p in lambda_root.iterdir() if p.is_dir()])
        self.assertGreater(len(dirs), 0)

        for d in dirs:
            with self.subTest(lambda_dir=d.name):
                staged_path, tmp_ctx = cps_deploy._prepare_lambda_source(d)
                try:
                    sym = cps_deploy._detect_handler_symbol(Path(staged_path) / "lambda_function.py")
                    self.assertEqual(sym, "lambda_handler")
                finally:
                    if tmp_ctx is not None:
                        tmp_ctx.cleanup()

    def test_lambda_env_injections(self):
        _, captured = self._run_deploy()
        by_name = {s.name: s for s in captured["lambda_specs"]}

        get_inc = by_name["FSA-TST-ECP-get-incremental-tables"]
        self.assertEqual(get_inc.env.get("source_folder"), "ecp")

        sns_err = by_name["FSA-TST-ECP-RAW-DM-sns-pub-step-func-errs"]
        self.assertEqual(sns_err.env.get("SNS_ARN"), self.cfg["strparams"]["snsArnParam"])

        notify = by_name["FSA-TST-ECP-sns-publish-validations-report"]
        self.assertEqual(notify.env.get("SNS_ARN"), self.cfg["strparams"]["snsArnParam"])

        val = by_name["FSA-TST-ECP-validation-check"]
        self.assertEqual(val.env.get("SecretId"), self.cfg["secretId"])
        self.assertEqual(val.env.get("CRAWLER_NAME"), "FSA-TST-ECP")

    def test_state_machine_count_and_names(self):
        _, captured = self._run_deploy()
        self.assertEqual(len(captured["state_specs"]), 4)
        names = {s.name for s in captured["state_specs"]}
        self.assertEqual(
            names,
            {
                "FSA-TST-ECP-Incremental-to-S3Landing",
                "FSA-TST-ECP-S3Landing-to-S3Final-Raw-DM",
                "FSA-TST-ECP-Process-Control-Update",
                "FSA-TST-ECP-MAIN",
            },
        )

    def test_main_state_machine_child_arn_substitution(self):
        _, captured = self._run_deploy()
        main_spec = next(s for s in captured["state_specs"] if s.name.endswith("-MAIN"))
        states = main_spec.definition["States"]
        start_state = "FSA-TST-ECP-Incremental-to-S3Landing"

        self.assertEqual(
            main_spec.definition["StartAt"],
            start_state,
        )
        self.assertEqual(
            states[start_state]["Parameters"]["StateMachineArn"],
            "arn:aws:states:us-east-1:123456789012:stateMachine:FSA-TST-ECP-Incremental-to-S3Landing",
        )
        self.assertEqual(
            states["Update ECP RAW-DM"]["Parameters"]["StateMachineArn"],
            "arn:aws:states:us-east-1:123456789012:stateMachine:FSA-TST-ECP-S3Landing-to-S3Final-Raw-DM",
        )
        self.assertEqual(
            states["Update Process Control"]["Parameters"]["StateMachineArn"],
            "arn:aws:states:us-east-1:123456789012:stateMachine:FSA-TST-ECP-Process-Control-Update",
        )

    def test_shortened_lambda_name_matching_in_raw_dm_state_machine(self):
        _, captured = self._run_deploy()
        raw_dm_spec = next(
            s for s in captured["state_specs"] if s.name.endswith("-S3Landing-to-S3Final-Raw-DM")
        )
        states = raw_dm_spec.definition["States"]

        sns_fn = (
            states["Map"]["ItemProcessor"]["States"]["SNS Lambda RAW DM"]["Parameters"]["FunctionName"]
        )
        self.assertEqual(
            sns_fn,
            "arn:aws:lambda:us-east-1:123456789012:function:FSA-TST-ECP-RAW-DM-sns-pub-step-func-errs",
        )

        upd_fn = states["data_ppl_job_update RAW DM"]["Parameters"]["FunctionName"]
        self.assertEqual(
            upd_fn,
            "arn:aws:lambda:us-east-1:123456789012:function:FSA-TST-ECP-RAW-DM-etl-workflow-update",
        )


if __name__ == "__main__":
    unittest.main()
