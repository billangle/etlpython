from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

from .farm_rec_stepfunctions import FpacStateMachineBuilder, FpacStateMachineInputs


import boto3

from common.aws_common import (
    ensure_bucket_exists,
    ensure_lambda,
    ensure_glue_job,
    ensure_glue_crawler,
    ensure_state_machine,
    LambdaSpec,
    GlueJobSpec,
    GlueCrawlerSpec,
    StateMachineSpec,
)

from dataclasses import dataclass

@dataclass(frozen=True)
class FpacNames:
    project_name: str
    validate_fn: str
    create_id_fn: str
    log_results_fn: str
    glue_step1: str
    glue_step2: str
    glue_step3: str
    crawler: str
    sm_step1: str
    sm_step2: str
    sm_parent: str


def build_names(deploy_env: str, project: str) -> FpacNames:
    """
    Centralized FPAC naming to match CDK exactly
    """
    project_name = f"Fpac{project.upper()}"
    base = f"FSA-{deploy_env}-{project_name}"

    return FpacNames(
        project_name=project_name,
        validate_fn=f"{base}-ValidateInput",
        create_id_fn=f"{base}-CreateNewId",
        log_results_fn=f"{base}-LogResults",
        glue_step1=f"{base}-Step1-LandingFiles",
        glue_step2=f"{base}-Step2-CleansedFiles",
        glue_step3=f"{base}-Step3-FinalFiles",
        crawler=f"{base}-CRAWLER",
        sm_step1=f"{base}-PipelineStep1",
        sm_step2=f"{base}-PipelineStep2",
        sm_parent=f"{base}-Pipeline",
    )

def build_glue_args(
    *,
    deploy_env: str,
    project: str,
    landing_bucket: str,
    clean_bucket: str,
    final_bucket: str,
    bucket_region: str,
    step: str,
) -> dict[str, str]:
    """
    Standard Glue job arguments shared across FPAC pipelines.
    Mirrors CDK defaults.
    """
    return {
        "--env": deploy_env,
        "--project": project,
        "--landing_bucket": landing_bucket,
        "--clean_bucket": clean_bucket,
        "--final_bucket": final_bucket,
        "--bucket_region": bucket_region,
        "--step": step,

        # Glue logging / metrics
        "--enable-metrics": "true",
        "--enable-continuous-cloudwatch-log": "true",
    }

from typing import Any, Dict



def deploy(cfg: Dict[str, Any], region: str) -> Dict[str, str]:
    deploy_env = cfg["deployEnv"]
    project = cfg["project"]
    config_data = cfg["configData"]
    bucket_region = cfg.get("bucketRegion", region)

    landing_bucket_name = cfg["strparams"]["landingBucketNameParam"]
    clean_bucket_name = cfg["strparams"]["cleanBucketNameParam"]
    final_bucket_name = cfg["strparams"]["finalBucketNameParam"]

    glue_job_role_arn = cfg["strparams"]["glueJobRoleArnParam"]
    etl_lambda_role_arn = cfg["strparams"]["etlRoleArnParam"]

    layers = [
        cfg["strparams"]["thirdPartyLayerArnParam"],
        cfg["strparams"]["customLayerArnParam"],
    ]

    artifact_bucket = cfg["artifacts"]["artifactBucket"]
    prefix = cfg["artifacts"]["prefix"].rstrip("/") + "/"

    names = build_names(deploy_env, project)

    # ✅ NEW: assets are located under this project directory
    project_dir = Path(__file__).resolve().parent          # .../projects/fpac_pipeline
    lambda_root = project_dir / "lambda"
    glue_root = project_dir / "glue"

    session = boto3.Session(region_name=region)
    s3 = session.client("s3")
    lam = session.client("lambda")
    glue = session.client("glue")
    sfn = session.client("stepfunctions")

    ensure_bucket_exists(s3, artifact_bucket, region)

    env_vars = {
        "PROJECT": project,
        "LANDING_BUCKET": landing_bucket_name,
        "TABLE_NAME": config_data["dynamoTableName"],
        "BUCKET_REGION": bucket_region,
    }

    # ---- Lambdas (paths now under projects/fpac_pipeline/lambda/...) ----
    validate_arn = ensure_lambda(
        lam,
        LambdaSpec(
            name=names.validate_fn,
            role_arn=etl_lambda_role_arn,
            handler="index.handler",
            runtime="nodejs20.x",
            source_dir=str(lambda_root / "Validate"),
            env=env_vars,
            layers=layers,
        ),
    )

    create_id_arn = ensure_lambda(
        lam,
        LambdaSpec(
            name=names.create_id_fn,
            role_arn=etl_lambda_role_arn,
            handler="index.handler",
            runtime="nodejs20.x",
            source_dir=str(lambda_root / "CreateNewId"),
            env=env_vars,
            layers=layers,
        ),
    )

    log_results_arn = ensure_lambda(
        lam,
        LambdaSpec(
            name=names.log_results_fn,
            role_arn=etl_lambda_role_arn,
            handler="index.handler",
            runtime="nodejs20.x",
            source_dir=str(lambda_root / "LogResults"),
            env=env_vars,
            layers=layers,
        ),
    )

    # ---- Glue scripts (paths now under projects/fpac_pipeline/glue/...) ----
    step1_script_local = str(glue_root / "landingFiles" / "landing_job.py")
    step2_script_local = str(glue_root / "cleaningFiles" / "cleaning_job.py")
    step3_script_local = str(glue_root / "finalFiles" / "final_job.py")

    ensure_glue_job(
        glue,
        s3,
        GlueJobSpec(
            name=names.glue_step1,
            role_arn=glue_job_role_arn,
            script_local_path=step1_script_local,
            script_s3_bucket=artifact_bucket,
            script_s3_key=f"{prefix}glue/landing_job.py",
            default_args=build_glue_args(
                deploy_env=deploy_env,
                project=project,
                landing_bucket=landing_bucket_name,
                clean_bucket=clean_bucket_name,
                final_bucket=final_bucket_name,
                bucket_region=bucket_region,
                step="Step1",
            ),
        ),
    )

    ensure_glue_job(
        glue,
        s3,
        GlueJobSpec(
            name=names.glue_step2,
            role_arn=glue_job_role_arn,
            script_local_path=step2_script_local,
            script_s3_bucket=artifact_bucket,
            script_s3_key=f"{prefix}glue/cleaning_job.py",
            default_args=build_glue_args(
                deploy_env=deploy_env,
                project=project,
                landing_bucket=landing_bucket_name,
                clean_bucket=clean_bucket_name,
                final_bucket=final_bucket_name,
                bucket_region=bucket_region,
                step="Step2",
            ),
        ),
    )

    ensure_glue_job(
        glue,
        s3,
        GlueJobSpec(
            name=names.glue_step3,
            role_arn=glue_job_role_arn,
            script_local_path=step3_script_local,
            script_s3_bucket=artifact_bucket,
            script_s3_key=f"{prefix}glue/final_job.py",
            default_args=build_glue_args(
                deploy_env=deploy_env,
                project=project,
                landing_bucket=landing_bucket_name,
                clean_bucket=clean_bucket_name,
                final_bucket=final_bucket_name,
                bucket_region=bucket_region,
                step="Step3",
            ),
        ),
    )

    ensure_glue_crawler(
        glue,
        GlueCrawlerSpec(
            name=names.crawler,
            role_arn=glue_job_role_arn,
            database_name=config_data["databaseName"],
            target_s3_path=f"s3://{final_bucket_name}/",
        ),
    )

       # ---- Step Functions definitions are now built in fpac_stepfunctions.py ----
    sfn_role_arn = (cfg.get("stepFunctions", {}) or {}).get("roleArn") or ""
    if not sfn_role_arn:
        raise RuntimeError("stepFunctions.roleArn is empty. (Role creation not implemented here.)")

    sm_inputs = FpacStateMachineInputs(
        validate_lambda_arn=validate_arn,
        create_id_lambda_arn=create_id_arn,
        log_results_lambda_arn=log_results_arn,
        glue_step1_job_name=names.glue_step1,
        glue_step2_job_name=names.glue_step2,
        glue_step3_job_name=names.glue_step3,
        crawler_name=names.crawler,
    )

    step1_def = FpacStateMachineBuilder.step1_asl(sm_inputs)
    step1_arn = ensure_state_machine(
        sfn,
        StateMachineSpec(name=names.sm_step1, role_arn=sfn_role_arn, definition=step1_def),
    )

    step2_def = FpacStateMachineBuilder.step2_asl(sm_inputs)
    step2_arn = ensure_state_machine(
        sfn,
        StateMachineSpec(name=names.sm_step2, role_arn=sfn_role_arn, definition=step2_def),
    )

    parent_def = FpacStateMachineBuilder.parent_asl(step1_arn, step2_arn)
    parent_arn = ensure_state_machine(
        sfn,
        StateMachineSpec(name=names.sm_parent, role_arn=sfn_role_arn, definition=parent_def),
    )


    return {
        "landing_bucket": landing_bucket_name,
        "clean_bucket": clean_bucket_name,
        "final_bucket": final_bucket_name,
        "validate_lambda_arn": validate_arn,
        "create_id_lambda_arn": create_id_arn,
        "log_results_lambda_arn": log_results_arn,
        "glue_job_step1": names.glue_step1,
        "glue_job_step2": names.glue_step2,
        "glue_job_step3": names.glue_step3,
        "glue_crawler": names.crawler,
        "sfn_step1_arn": step1_arn,
        "sfn_step2_arn": step2_arn,
        "sfn_parent_arn": parent_arn,
    }

