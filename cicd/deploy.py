#!/usr/bin/env python3
import argparse
import json
import time
import zipfile
from pathlib import Path
from typing import Dict, Any, Optional, List

import boto3
from botocore.exceptions import ClientError


# ---------------- Helpers ----------------

def read_json(path: str) -> Dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))

def ssm_get(ssm, name: str) -> str:
    return ssm.get_parameter(Name=name)["Parameter"]["Value"]

def s3_upload_file(s3, local_path: str, bucket: str, key: str) -> str:
    s3.upload_file(local_path, bucket, key)
    return f"s3://{bucket}/{key}"

from botocore.exceptions import ClientError


def is_conflict(err: Exception) -> bool:
    return isinstance(err, ClientError) and err.response["Error"]["Code"] in (
        "ResourceConflictException",
        "TooManyRequestsException",
    )

def call_with_conflict_retry(fn, *, fn_name: str, lambda_client, max_wait_s: int = 180, base_sleep_s: float = 1.5, **kwargs):
    """
    Calls a boto3 lambda operation, retrying if Lambda says 'update in progress' or throttles.
    """
    start = time.time()
    attempt = 0
    while True:
        try:
            return fn(**kwargs)
        except ClientError as e:
            if not is_conflict(e):
                raise
            # wait until current update completes, then retry
            wait_for_lambda_update(lambda_client, fn_name, max_wait_s=max_wait_s)
            attempt += 1
            sleep_s = min(base_sleep_s * (2 ** min(attempt, 6)), 10.0)
            if time.time() - start > max_wait_s:
                raise TimeoutError(f"Timed out retrying Lambda operation due to conflicts: {fn_name}")
            time.sleep(sleep_s)


def ensure_bucket_exists(s3, bucket: str, region: str) -> None:
    """
    Ensures an S3 bucket exists. Creates it if missing.
    Note: S3 buckets are global names; if someone else owns the name, create will fail.
    """
    try:
        s3.head_bucket(Bucket=bucket)
        return
    except ClientError as e:
        code = str(e.response.get("Error", {}).get("Code", ""))
        # 404 / NotFound => bucket doesn't exist OR you have no access
        if code not in ("404", "NotFound", "NoSuchBucket"):
            raise

    # Try create
    params = {"Bucket": bucket}
    if region != "us-east-1":
        params["CreateBucketConfiguration"] = {"LocationConstraint": region}

    try:
        s3.create_bucket(**params)
        # optional: wait until exists
        waiter = s3.get_waiter("bucket_exists")
        waiter.wait(Bucket=bucket)
        print(f"[s3] created artifact bucket {bucket} in {region}")
    except ClientError as e:
        # If name is taken globally or you lack perms, this will fail with a clearer error
        raise RuntimeError(
            f"Artifact bucket '{bucket}' could not be created. "
            f"Either the name is already taken globally, or you don't have s3:CreateBucket permission. "
            f"Original error: {e}"
        )


def zip_dir(source_dir: str, out_zip: str) -> str:
    src = Path(source_dir).resolve()
    out = Path(out_zip).resolve()
    out.parent.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for p in src.rglob("*"):
            if p.is_dir():
                continue
            rel = p.relative_to(src)

            # skip junk
            if "__pycache__" in rel.parts:
                continue
            if rel.name.endswith((".pyc", ".pyo")):
                continue
            if rel.parts and rel.parts[0].startswith(".venv"):
                continue
            if rel.parts and rel.parts[0] in ("node_modules", ".git"):
                continue

            z.write(p, arcname=str(rel))
    return str(out)

def wait_for_lambda_update(lambda_client, fn_name: str, max_wait_s: int = 180) -> None:
    start = time.time()
    while True:
        resp = lambda_client.get_function_configuration(FunctionName=fn_name)
        status = resp.get("LastUpdateStatus")
        if status in (None, "Successful"):
            return
        if status == "Failed":
            raise RuntimeError(f"Lambda update failed: {resp.get('LastUpdateStatusReason')}")
        if time.time() - start > max_wait_s:
            raise TimeoutError(f"Timed out waiting for Lambda update: {fn_name}")
        time.sleep(3)


# ---------------- Lambda ----------------

def ensure_lambda(
    lambda_client,
    fn_name: str,
    role_arn: str,
    handler: str,
    runtime: str,
    source_dir: str,
    env: Optional[Dict[str, str]] = None,
    layers: Optional[List[str]] = None,
    timeout: int = 30,
    memory: int = 256,
    architecture: str = "x86_64",  # or "arm64"
    publish: bool = True,
) -> str:
    """
    Creates or updates a Lambda function in-place (idempotent).
    Works for Python, Node.js, etc., as long as runtime + handler are valid.
    For Node.js ESM (.mjs), handler should be like "validate.handler" (file validate.mjs exporting handler).
    """
    env = env or {}
    layers = layers or []

    build_zip = zip_dir(source_dir, f"build/lambda/{fn_name}.zip")
    code_bytes = Path(build_zip).read_bytes()

    def update_config():
        lambda_client.update_function_configuration(
            FunctionName=fn_name,
            Role=role_arn,
            Runtime=runtime,
            Handler=handler,
            Timeout=timeout,
            MemorySize=memory,
            Environment={"Variables": env} if env else {"Variables": {}},
            Layers=layers
        )

    try:
            resp = lambda_client.get_function(FunctionName=fn_name)
            arn = resp["Configuration"]["FunctionArn"]

            call_with_conflict_retry(
                lambda_client.update_function_code,
                fn_name=fn_name,
                lambda_client=lambda_client,
                FunctionName=fn_name,
                ZipFile=code_bytes,
                Publish=publish,
            )

            call_with_conflict_retry(
                lambda_client.update_function_configuration,
                fn_name=fn_name,
                lambda_client=lambda_client,
                FunctionName=fn_name,
                Role=role_arn,
                Runtime=runtime,
                Handler=handler,
                Timeout=timeout,
                MemorySize=memory,
                Environment={"Variables": env} if env else {"Variables": {}},
                Layers=layers,
            )

            wait_for_lambda_update(lambda_client, fn_name)
            print(f"[lambda] updated {fn_name} -> {arn} (runtime={runtime}, handler={handler})")
            return arn


    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise

        resp = lambda_client.create_function(
            FunctionName=fn_name,
            Role=role_arn,
            Runtime=runtime,
            Handler=handler,
            Code={"ZipFile": code_bytes},
            Timeout=timeout,
            MemorySize=memory,
            Publish=publish,
            Environment={"Variables": env} if env else {"Variables": {}},
            Layers=layers
        )
        arn = resp["FunctionArn"]
        wait_for_lambda_update(lambda_client, fn_name)
        print(f"[lambda] created {fn_name} -> {arn} (runtime={runtime}, handler={handler})")
        return arn


# ---------------- Glue ----------------

def ensure_glue_job(
    glue,
    s3,
    job_name: str,
    role_arn: str,
    script_local_path: str,
    script_s3_bucket: str,
    script_s3_key: str,
    default_args: Dict[str, str],
    glue_version: str = "4.0",
    worker_type: str = "G.1X",
    number_of_workers: int = 2,
    timeout_minutes: int = 60,
    max_retries: int = 0,
) -> str:
    script_s3_path = s3_upload_file(s3, script_local_path, script_s3_bucket, script_s3_key)

    job_update = {
        "Role": role_arn,
        "Command": {"Name": "glueetl", "ScriptLocation": script_s3_path, "PythonVersion": "3"},
        "DefaultArguments": default_args,
        "GlueVersion": glue_version,
        "WorkerType": worker_type,
        "NumberOfWorkers": number_of_workers,
        "Timeout": timeout_minutes,
        "MaxRetries": max_retries,
        "ExecutionProperty": {"MaxConcurrentRuns": 1},
    }

    try:
        glue.get_job(JobName=job_name)
        glue.update_job(JobName=job_name, JobUpdate=job_update)
        print(f"[glue] updated job {job_name} (script={script_s3_path})")
        return job_name
    except ClientError as e:
        if e.response["Error"]["Code"] != "EntityNotFoundException":
            raise
        glue.create_job(Name=job_name, **job_update)
        print(f"[glue] created job {job_name} (script={script_s3_path})")
        return job_name


def ensure_glue_crawler(
    glue,
    crawler_name: str,
    role_arn: str,
    database_name: str,
    target_s3_path: str,
) -> str:
    crawler_def = {
        "Name": crawler_name,
        "Role": role_arn,
        "DatabaseName": database_name,
        "Targets": {"S3Targets": [{"Path": target_s3_path}]},
        "SchemaChangePolicy": {
            "UpdateBehavior": "UPDATE_IN_DATABASE",
            "DeleteBehavior": "DEPRECATE_IN_DATABASE",
        },
    }

    try:
        glue.get_crawler(Name=crawler_name)
        glue.update_crawler(**crawler_def)
        print(f"[crawler] updated {crawler_name} -> {target_s3_path}")
        return crawler_name
    except ClientError as e:
        if e.response["Error"]["Code"] != "EntityNotFoundException":
            raise
        glue.create_crawler(**crawler_def)
        print(f"[crawler] created {crawler_name} -> {target_s3_path}")
        return crawler_name


# ---------------- Logs ----------------

def ensure_log_group(logs_client, name: str, retention_days: Optional[int] = None) -> None:
    try:
        logs_client.create_log_group(logGroupName=name)
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceAlreadyExistsException":
            raise
    if retention_days:
        logs_client.put_retention_policy(logGroupName=name, retentionInDays=retention_days)


# ---------------- IAM for SFN (optional) ----------------


# ---------------- Step Functions ----------------

def find_state_machine_arn(sfn, name: str) -> Optional[str]:
    paginator = sfn.get_paginator("list_state_machines")
    for page in paginator.paginate():
        for sm in page.get("stateMachines", []):
            if sm["name"] == name:
                return sm["stateMachineArn"]
    return None


from typing import Optional, Dict, Any
import json

def ensure_state_machine(
    sfn,
    name: str,
    role_arn: str,
    definition: Dict[str, Any],
    log_group_arn: Optional[str],
    enable_logging: bool = True,
) -> str:
    arn = find_state_machine_arn(sfn, name)

    common = {
        "definition": json.dumps(definition),
        "roleArn": role_arn,
        "tracingConfiguration": {"enabled": True},
    }

    # IMPORTANT: only include loggingConfiguration if explicitly enabled
    if enable_logging and log_group_arn:
        common["loggingConfiguration"] = {
            "level": "ALL",
            "includeExecutionData": True,
            "destinations": [{"cloudWatchLogsLogGroup": {"logGroupArn": log_group_arn}}],
        }

    if arn:
        sfn.update_state_machine(stateMachineArn=arn, **common)
        print(f"[sfn] updated {name} -> {arn}")
        return arn

    resp = sfn.create_state_machine(name=name, type="STANDARD", **common)
    arn = resp["stateMachineArn"]
    print(f"[sfn] created {name} -> {arn}")
    return arn





def asl_step1(validate_arn: str, create_id_arn: str, glue_step1_job: str) -> Dict[str, Any]:
    return {
        "StartAt": "ValidateInput",
        "States": {
            "ValidateInput": {
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {"FunctionName": validate_arn, "Payload.$": "$"},
                "OutputPath": "$.Payload",
                "Next": "CreateNewId"
            },
            "CreateNewId": {
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {"FunctionName": create_id_arn, "Payload.$": "$"},
                "OutputPath": "$.Payload",
                "Next": "Step1GlueJob"
            },
            "Step1GlueJob": {
                "Type": "Task",
                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                "Parameters": {"JobName": glue_step1_job},
                "ResultPath": "$.glueResult",
                "End": True
            }
        }
    }


def asl_step2(glue_step2_job: str, glue_step3_job: str, log_results_arn: str, crawler_name: str) -> Dict[str, Any]:
    return {
        "StartAt": "Step2GlueJob",
        "States": {
            "Step2GlueJob": {
                "Type": "Task",
                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                "Parameters": {"JobName": glue_step2_job},
                "ResultPath": "$.glueResult",
                "Next": "Step3GlueJob"
            },
            "Step3GlueJob": {
                "Type": "Task",
                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                "Parameters": {"JobName": glue_step3_job},
                "ResultPath": "$.glueResult",
                "Next": "LogGlueResults"
            },
            "LogGlueResults": {
                "Type": "Pass",
                "Parameters": {
                    "jobDetails.$": "$.glueResult",
                    "timestamp.$": "$$.State.EnteredTime"
                },
                "ResultPath": "$.logged",
                "Next": "FinalLogResults"
            },
            "FinalLogResults": {
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {"FunctionName": log_results_arn, "Payload.$": "$"},
                "OutputPath": "$.Payload",
                "Next": "StartCrawler"
            },
            "StartCrawler": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:glue:startCrawler",
                "Parameters": {"Name": crawler_name},
                "ResultPath": "$.crawlerResult",
                "Next": "WasGlueSuccessful"
            },
            "WasGlueSuccessful": {
                "Type": "Choice",
                "Choices": [
                    {
                        "Variable": "$.logged.jobDetails.JobRunState",
                        "StringEquals": "SUCCEEDED",
                        "Next": "Success"
                    }
                ],
                "Default": "Fail"
            },
            "Success": {"Type": "Succeed"},
            "Fail": {"Type": "Fail"}
        }
    }


def asl_parent(step1_sm_arn: str, step2_sm_arn: str) -> Dict[str, Any]:
    return {
            "StartAt": "Run Step1",
            "States": {
                "Run Step1": {
                "Next": "Run Step2",
                "Type": "Task",
                "ResultPath": "$.step1Result",
                "Resource": "arn:aws:states:::states:startExecution.sync:2",
                "Parameters": {
                    "Input.$": "$",
                    "StateMachineArn": step1_sm_arn
                }
                },
                "Run Step2": {
                "End": True,
                "Type": "Task",
                "ResultPath": "$.step2Result",
                "Resource": "arn:aws:states:::states:startExecution.sync:2",
                "Parameters": {
                    "Input.$": "$",
                    "StateMachineArn": step2_sm_arn
                }
                }
            }
        }



# ---------------- Main ----------------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--region", required=True)
    args = ap.parse_args()

    cfg = read_json(args.config)
    region = args.region

    deploy_env = cfg["deployEnv"]
    project = cfg["project"]
    project_name = f"Fpac{project.upper()}"  # matches CDK
    config_data = cfg["configData"]

    session = boto3.Session(region_name=region)
    ssm = session.client("ssm")
    s3 = session.client("s3")
    iam = session.client("iam")
    lam = session.client("lambda")
    glue = session.client("glue")
    sfn = session.client("stepfunctions")
    logs_client = session.client("logs")

    # --- SSM lookups (same as CDK) ---
    landing_bucket_name = ssm_get(ssm, cfg["ssm"]["landingBucketNameParam"])
    clean_bucket_name = ssm_get(ssm, cfg["ssm"]["cleanBucketNameParam"])
    final_bucket_name = ssm_get(ssm, cfg["ssm"]["finalBucketNameParam"])

    glue_job_role_arn = ssm_get(ssm, cfg["ssm"]["glueJobRoleArnParam"])
    etl_lambda_role_arn = ssm_get(ssm, cfg["ssm"]["etlRoleArnParam"])

    third_party_layer_arn = ssm_get(ssm, cfg["ssm"]["thirdPartyLayerArnParam"])
    custom_layer_arn = ssm_get(ssm, cfg["ssm"]["customLayerArnParam"])
    layers = [third_party_layer_arn, custom_layer_arn]

    # --- Artifact bucket for pushing scripts/zips ---
    artifact_bucket = cfg["artifacts"]["artifactBucket"]
    prefix = cfg["artifacts"]["prefix"].rstrip("/") + "/"
    ensure_bucket_exists(s3, artifact_bucket, region)


    # --- Lambda functions (names match CDK) ---
    lambda_root = cfg["paths"]["lambdaRootPath"].rstrip("/") + "/"

    validate_fn_name = f"FSA-{deploy_env}-{project_name}-ValidateInput"
    create_id_fn_name = f"FSA-{deploy_env}-{project_name}-CreateNewId"
    log_results_fn_name = f"FSA-{deploy_env}-{project_name}-LogResults"

    env_vars = {"PROJECT": project, "TABLE_NAME": config_data["dynamoTableName"]}

    validate_arn = ensure_lambda(
        lam,
        validate_fn_name,
        etl_lambda_role_arn,
        handler="index.handler",      
        runtime="nodejs20.x",
        source_dir=f"{lambda_root}Validate",
        env=env_vars,
        layers=layers
    )

    create_id_arn = ensure_lambda(
        lam, create_id_fn_name, etl_lambda_role_arn,
        handler="index.handler", runtime="nodejs20.x",
        source_dir=f"{lambda_root}CreateNewId",
        env=env_vars, layers=layers
    )
    log_results_arn = ensure_lambda(
        lam, log_results_fn_name, etl_lambda_role_arn,
        handler="index.handler", runtime="nodejs20.x",
        source_dir=f"{lambda_root}LogResults",
        env=env_vars, layers=layers
    )

    # --- Glue jobs (names modeled after your jobType/stepName patterns) ---
    glue_root = cfg["paths"]["glueRootPath"].rstrip("/") + "/"

    # You can change these job names to match your FpacGlueJob naming if you know the exact output.
    # This keeps them stable and environment-scoped.
    glue_job_step1 = f"FSA-{deploy_env}-{project_name}-Step1-LandingFiles"
    glue_job_step2 = f"FSA-{deploy_env}-{project_name}-Step2-CleansedFiles"
    glue_job_step3 = f"FSA-{deploy_env}-{project_name}-Step3-FinalFiles"

    def glue_args(step: str) -> Dict[str, str]:
        return {
            "--env": deploy_env,
            "--project": project,
            "--landing_bucket": landing_bucket_name,
            "--clean_bucket": clean_bucket_name,
            "--final_bucket": final_bucket_name,
            "--step": step,

            # Glue logging/metrics toggles (these keys are standard)
            "--enable-metrics": "true",
            "--enable-continuous-cloudwatch-log": "true",
        }


    step1_script_local = f"{glue_root}landingFiles/landing_job.py"
    step2_script_local = f"{glue_root}cleaningFiles/cleaning_job.py"
    step3_script_local = f"{glue_root}finalFiles/final_job.py"

    ensure_glue_job(
        glue, s3,
        job_name=glue_job_step1,
        role_arn=glue_job_role_arn,
        script_local_path=step1_script_local,
        script_s3_bucket=artifact_bucket,
        script_s3_key=f"{prefix}glue/landing_job.py",
        default_args=glue_args("Step1")
    )
    ensure_glue_job(
        glue, s3,
        job_name=glue_job_step2,
        role_arn=glue_job_role_arn,
        script_local_path=step2_script_local,
        script_s3_bucket=artifact_bucket,
        script_s3_key=f"{prefix}glue/cleaning_job.py",
        default_args=glue_args("Step2")
    )
    ensure_glue_job(
        glue, s3,
        job_name=glue_job_step3,
        role_arn=glue_job_role_arn,
        script_local_path=step3_script_local,
        script_s3_bucket=artifact_bucket,
        script_s3_key=f"{prefix}glue/final_job.py",
        default_args=glue_args("Step3")
    )

    # --- Glue crawler (same naming as CDK) ---
    crawler_name = f"FSA-{deploy_env}-{project_name}-CRAWLER"
    ensure_glue_crawler(
        glue,
        crawler_name=crawler_name,
        role_arn=glue_job_role_arn,  # CDK used same ARN for crawlerRole
        database_name=config_data["databaseName"],
        target_s3_path=f"s3://{final_bucket_name}/",
    )

    # --- CloudWatch log groups for SFN (match the 3 machines) ---
    lg_step1  = f"/aws/states/FSA-{deploy_env}-{project_name}-PipelineStep1"
    lg_step2  = f"/aws/states/FSA-{deploy_env}-{project_name}-PipelineStep2"
    lg_parent = f"/aws/states/FSA-{deploy_env}-{project_name}-Pipeline"

    ensure_log_group(logs_client, lg_step1, retention_days=30)
    ensure_log_group(logs_client, lg_step2, retention_days=30)
    ensure_log_group(logs_client, lg_parent, retention_days=30)

    # Need log group ARNs (DescribeLogGroups)
    def log_group_arn(name: str) -> str:
        resp = logs_client.describe_log_groups(logGroupNamePrefix=name)
        for g in resp.get("logGroups", []):
            if g.get("logGroupName") == name:
                return g["arn"]
        raise RuntimeError(f"Could not resolve log group ARN for {name}")

    lg_step1_arn = log_group_arn(lg_step1)
    lg_step2_arn = log_group_arn(lg_step2)
    lg_parent_arn = log_group_arn(lg_parent)

    # --- SFN role handling ---
    sfn_role_arn = (cfg.get("stepFunctions", {}) or {}).get("roleArn") or ""
    if not sfn_role_arn:
        if not cfg["stepFunctions"].get("createRoleIfMissing", False):
            raise RuntimeError("stepFunctions.roleArn is empty and createRoleIfMissing=false")
        sfn_role_arn = ensure_sfn_role(
            iam_client=iam,
            role_name=cfg["stepFunctions"]["roleName"],
            lambda_arns=[validate_arn, create_id_arn, log_results_arn],
            glue_job_names=[glue_job_step1, glue_job_step2, glue_job_step3],
            crawler_name=crawler_name,
            log_group_arns=[lg_step1_arn + ":*", lg_step2_arn + ":*", lg_parent_arn + ":*"],
        )

    # --- Step Functions definitions (exact structure from your CDK) ---
    sm_step1_name = f"FSA-{deploy_env}-{project_name}-PipelineStep1"
    sm_step2_name = f"FSA-{deploy_env}-{project_name}-PipelineStep2"
    sm_parent_name = f"FSA-{deploy_env}-{project_name}-Pipeline"

    step1_def = asl_step1(validate_arn, create_id_arn, glue_job_step1)
    step1_arn = ensure_state_machine(sfn, sm_step1_name, sfn_role_arn, step1_def, lg_step1_arn)

    step2_def = asl_step2(glue_job_step2, glue_job_step3, log_results_arn, crawler_name)
    step2_arn = ensure_state_machine(sfn, sm_step2_name, sfn_role_arn, step2_def, lg_step2_arn)

    parent_def = asl_parent(step1_arn, step2_arn)
    parent_arn = ensure_state_machine(
        sfn,
        sm_parent_name,
        sfn_role_arn,
        parent_def,
        lg_parent_arn,
        enable_logging=False,   # <-- this avoids "create managed-rule"
    )


    print("\nDEPLOY SUMMARY")
    print(f"  Landing bucket (SSM): {landing_bucket_name}")
    print(f"  Clean bucket   (SSM): {clean_bucket_name}")
    print(f"  Final bucket   (SSM): {final_bucket_name}")
    print(f"  Validate Lambda ARN : {validate_arn}")
    print(f"  CreateNewId ARN     : {create_id_arn}")
    print(f"  LogResults ARN      : {log_results_arn}")
    print(f"  Glue jobs           : {glue_job_step1}, {glue_job_step2}, {glue_job_step3}")
    print(f"  Glue crawler         : {crawler_name}")
    print(f"  SFN Step1 ARN        : {step1_arn}")
    print(f"  SFN Step2 ARN        : {step2_arn}")
    print(f"  SFN Parent ARN       : {parent_arn}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
