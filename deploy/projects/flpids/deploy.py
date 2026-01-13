from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import boto3

from common.aws_common import (
    ensure_bucket_exists,
    ensure_lambda,
    LambdaSpec,
)


@dataclass(frozen=True)
class FpacNames:
    project_name: str
    checkfile_fn: str
    filechecker2_fn: str
    testfileloader_fn: str 


def build_names(deploy_env: str, project: str) -> FpacNames:
    """
    Centralized FPAC naming to match CDK exactly
    """
    project_name = f"Fpac{project.upper()}"
    base = f"FSA-{deploy_env}-{project_name}"

    return FpacNames(
        project_name=project_name,
        checkfile_fn=f"{base}-CheckFile",
        filechecker2_fn=f"{base}-FileChecker2",
        testfileloader_fn=f"{base}-TestFileLoader",
    )


def parse_networking(cfg: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    """
    Returns (subnet_ids, security_group_ids).

    - If networking is missing/empty/invalid/incomplete -> returns ([], [])
      which means "force detach from VPC".
    - If both subnetIds and securityGroupIds are present -> returns populated lists.
    """
    net = cfg.get("networking") or {}
    if not isinstance(net, dict) or not net:
        return ([], [])

    subnet_ids = net.get("subnetIds") or []
    sg_ids = net.get("securityGroupIds") or []

    if not isinstance(subnet_ids, list):
        subnet_ids = []
    if not isinstance(sg_ids, list):
        sg_ids = []

    subnet_ids = [s.strip() for s in subnet_ids if isinstance(s, str) and s.strip()]
    sg_ids = [s.strip() for s in sg_ids if isinstance(s, str) and s.strip()]

    # If one is missing, treat as "detach" to be safe and deterministic
    if not (subnet_ids and sg_ids):
        return ([], [])

    return (subnet_ids, sg_ids)


def deploy(cfg: Dict[str, Any], region: str) -> Dict[str, str]:
    deploy_env = cfg["deployEnv"]
    project = cfg["project"]
    config_data = cfg["configData"]
    bucket_region = cfg.get("bucketRegion", region)

    landing_bucket_name = cfg["strparams"]["landingBucketNameParam"]
    # kept for parity / future use
    _clean_bucket_name = cfg["strparams"]["cleanBucketNameParam"]
    _final_bucket_name = cfg["strparams"]["finalBucketNameParam"]

    _glue_job_role_arn = cfg["strparams"]["glueJobRoleArnParam"]
    etl_lambda_role_arn = cfg["strparams"]["etlRoleArnParam"]

    layers = [
        cfg["strparams"]["thirdPartyLayerArnParam"],
        cfg["strparams"]["customLayerArnParam"],
    ]

    artifact_bucket = cfg["artifacts"]["artifactBucket"]
    _prefix = cfg["artifacts"]["prefix"].rstrip("/") + "/"

    names = build_names(deploy_env, project)

    project_dir = Path(__file__).resolve().parent  # .../projects/fpac_pipeline
    lambda_root = project_dir / "lambda"

    # ✅ Deterministic networking: either in-VPC with both lists, or detach with []
    subnet_ids, security_group_ids = parse_networking(cfg)

    session = boto3.Session(region_name=region)
    s3 = session.client("s3")
    lam = session.client("lambda")

    ensure_bucket_exists(s3, artifact_bucket, region)

    env_vars = {
        "PROJECT": project,
        "LANDING_BUCKET": landing_bucket_name,
        "TABLE_NAME": config_data["dynamoTableName"],
        "BUCKET_REGION": bucket_region,
    }

    checkfile_arn = ensure_lambda(
        lam,
        LambdaSpec(
            name=names.checkfile_fn,
            role_arn=etl_lambda_role_arn,
            handler="lambda_function.lambda_handler",
            runtime="python3.12",
            source_dir=str(lambda_root / "CheckFile"),
            env=env_vars,
            layers=layers,
            subnet_ids=subnet_ids,
            security_group_ids=security_group_ids,
        ),
    )

    filechecker2_arn = ensure_lambda(
        lam,
        LambdaSpec(
            name=names.filechecker2_fn,
            role_arn=etl_lambda_role_arn,
            handler="lambda_function.lambda_handler",
            runtime="python3.12",
            source_dir=str(lambda_root / "FileChecker2"),
            env=env_vars,
            layers=layers,
            subnet_ids=subnet_ids,
            security_group_ids=security_group_ids,
        ),
    )

    testfileloader_arn = ensure_lambda(
        lam,
        LambdaSpec(
            name=names.testfileloader_fn,
            role_arn=etl_lambda_role_arn,
            handler="lambda_function.lambda_handler",
            runtime="python3.12",
            source_dir=str(lambda_root / "TestFileLoader"),
            env=env_vars,
            layers=layers,
            subnet_ids=subnet_ids,
            security_group_ids=security_group_ids,
        ),
    )

    return {
        "checkfile_lambda_arn": checkfile_arn,
        "filechecker2_lambda_arn": filechecker2_arn,
        "testfileloader_lambda_arn": testfileloader_arn,
    }
