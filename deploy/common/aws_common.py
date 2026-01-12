#!/usr/bin/env python3
from __future__ import annotations

import json
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Callable

from botocore.exceptions import ClientError


# ---------------- Generic file helpers ----------------

def read_json(path: str) -> Dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


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


# ---------------- S3 helpers ----------------

def s3_upload_file(s3, local_path: str, bucket: str, key: str) -> str:
    s3.upload_file(local_path, bucket, key)
    return f"s3://{bucket}/{key}"


def ensure_bucket_exists(s3, bucket: str, region: str) -> None:
    """
    Ensures an S3 bucket exists. Creates it if missing.
    """
    try:
        s3.head_bucket(Bucket=bucket)
        return
    except ClientError as e:
        code = str(e.response.get("Error", {}).get("Code", ""))
        if code not in ("404", "NotFound", "NoSuchBucket"):
            raise

    params: Dict[str, Any] = {"Bucket": bucket}
    if region != "us-east-1":
        params["CreateBucketConfiguration"] = {"LocationConstraint": region}

    try:
        s3.create_bucket(**params)
        s3.get_waiter("bucket_exists").wait(Bucket=bucket)
        print(f"[s3] created artifact bucket {bucket} in {region}")
    except ClientError as e:
        raise RuntimeError(
            f"Artifact bucket '{bucket}' could not be created. "
            f"Either the name is already taken globally, or you don't have s3:CreateBucket permission. "
            f"Original error: {e}"
        )


# ---------------- CloudWatch Logs ----------------

def ensure_log_group(logs_client, name: str, retention_days: Optional[int] = None) -> None:
    try:
        logs_client.create_log_group(logGroupName=name)
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceAlreadyExistsException":
            raise
    if retention_days:
        logs_client.put_retention_policy(logGroupName=name, retentionInDays=retention_days)


def resolve_log_group_arn(logs_client, name: str) -> str:
    resp = logs_client.describe_log_groups(logGroupNamePrefix=name)
    for g in resp.get("logGroups", []):
        if g.get("logGroupName") == name:
            return g["arn"]
    raise RuntimeError(f"Could not resolve log group ARN for {name}")


# ---------------- Lambda conflict retry ----------------

def is_conflict(err: Exception) -> bool:
    return isinstance(err, ClientError) and err.response["Error"]["Code"] in (
        "ResourceConflictException",
        "TooManyRequestsException",
    )


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


def call_with_conflict_retry(
    fn: Callable[..., Any],
    *,
    fn_name: str,
    lambda_client,
    max_wait_s: int = 180,
    base_sleep_s: float = 1.5,
    **kwargs: Any
) -> Any:
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

            wait_for_lambda_update(lambda_client, fn_name, max_wait_s=max_wait_s)
            attempt += 1
            sleep_s = min(base_sleep_s * (2 ** min(attempt, 6)), 10.0)

            if time.time() - start > max_wait_s:
                raise TimeoutError(f"Timed out retrying Lambda operation due to conflicts: {fn_name}")

            time.sleep(sleep_s)


# ---------------- Ensure: Lambda ----------------

@dataclass(frozen=True)
class LambdaSpec:
    name: str
    role_arn: str
    handler: str
    runtime: str
    source_dir: str
    env: Dict[str, str]
    layers: List[str]
    timeout: int = 30
    memory: int = 256
    architecture: str = "x86_64"
    publish: bool = True

    # ✅ NEW: optional-ish VPC config
    # For safe deterministic behavior:
    #   - pass real lists to attach
    #   - pass [] / [] to detach
    subnet_ids: Optional[List[str]] = None
    security_group_ids: Optional[List[str]] = None


def _normalize_str_list(vals: Optional[List[str]]) -> List[str]:
    if vals is None:
        return []
    if not isinstance(vals, list):
        return []
    return [v.strip() for v in vals if isinstance(v, str) and v.strip()]


def _vpc_config_for_spec(spec: LambdaSpec) -> Dict[str, Any]:
    """
    Returns a VpcConfig dict ALWAYS, to ensure deterministic behavior.

    - If both lists have values -> attach to VPC
    - Otherwise -> detach from VPC (empty lists)
    """
    subnet_ids = _normalize_str_list(spec.subnet_ids)
    sg_ids = _normalize_str_list(spec.security_group_ids)

    if subnet_ids and sg_ids:
        return {"SubnetIds": subnet_ids, "SecurityGroupIds": sg_ids}

    # Force detach
    return {"SubnetIds": [], "SecurityGroupIds": []}


def ensure_lambda(lambda_client, spec: LambdaSpec) -> str:
    """
    Creates or updates a Lambda function in-place (idempotent).

    Deterministic networking:
      - If subnet_ids & security_group_ids are both non-empty -> VPC-attached
      - Else -> forced OUT of VPC by setting VpcConfig to empty lists
    """
    build_zip = zip_dir(spec.source_dir, f"build/lambda/{spec.name}.zip")
    code_bytes = Path(build_zip).read_bytes()

    vpc_cfg = _vpc_config_for_spec(spec)

    try:
        resp = lambda_client.get_function(FunctionName=spec.name)
        arn = resp["Configuration"]["FunctionArn"]

        call_with_conflict_retry(
            lambda_client.update_function_code,
            fn_name=spec.name,
            lambda_client=lambda_client,
            FunctionName=spec.name,
            ZipFile=code_bytes,
            Publish=spec.publish,
        )

        # Always send VpcConfig (either attach or detach) for deterministic behavior
        call_with_conflict_retry(
            lambda_client.update_function_configuration,
            fn_name=spec.name,
            lambda_client=lambda_client,
            FunctionName=spec.name,
            Role=spec.role_arn,
            Runtime=spec.runtime,
            Handler=spec.handler,
            Timeout=spec.timeout,
            MemorySize=spec.memory,
            Environment={"Variables": spec.env or {}},
            Layers=spec.layers or [],
            VpcConfig=vpc_cfg,
        )

        wait_for_lambda_update(lambda_client, spec.name)
        print(f"[lambda] updated {spec.name} -> {arn} (runtime={spec.runtime}, handler={spec.handler})")
        return arn

    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise

        # For create, sending empty VpcConfig is unnecessary; omit it when detaching.
        create_kwargs: Dict[str, Any] = dict(
            FunctionName=spec.name,
            Role=spec.role_arn,
            Runtime=spec.runtime,
            Handler=spec.handler,
            Code={"ZipFile": code_bytes},
            Timeout=spec.timeout,
            MemorySize=spec.memory,
            Publish=spec.publish,
            Environment={"Variables": spec.env or {}},
            Layers=spec.layers or [],
            Architectures=[spec.architecture],
        )

        if vpc_cfg["SubnetIds"] and vpc_cfg["SecurityGroupIds"]:
            create_kwargs["VpcConfig"] = vpc_cfg

        resp = lambda_client.create_function(**create_kwargs)
        arn = resp["FunctionArn"]
        wait_for_lambda_update(lambda_client, spec.name)
        print(f"[lambda] created {spec.name} -> {arn} (runtime={spec.runtime}, handler={spec.handler})")
        return arn


# ---------------- Ensure: Glue ----------------

@dataclass(frozen=True)
class GlueJobSpec:
    name: str
    role_arn: str
    script_local_path: str
    script_s3_bucket: str
    script_s3_key: str
    default_args: Dict[str, str]
    glue_version: str = "4.0"
    worker_type: str = "G.1X"
    number_of_workers: int = 2
    timeout_minutes: int = 60
    max_retries: int = 0


def ensure_glue_job(glue, s3, spec: GlueJobSpec) -> str:
    script_s3_path = s3_upload_file(s3, spec.script_local_path, spec.script_s3_bucket, spec.script_s3_key)

    job_update = {
        "Role": spec.role_arn,
        "Command": {"Name": "glueetl", "ScriptLocation": script_s3_path, "PythonVersion": "3"},
        "DefaultArguments": spec.default_args,
        "GlueVersion": spec.glue_version,
        "WorkerType": spec.worker_type,
        "NumberOfWorkers": spec.number_of_workers,
        "Timeout": spec.timeout_minutes,
        "MaxRetries": spec.max_retries,
        "ExecutionProperty": {"MaxConcurrentRuns": 1},
    }

    try:
        glue.get_job(JobName=spec.name)
        glue.update_job(JobName=spec.name, JobUpdate=job_update)
        print(f"[glue] updated job {spec.name} (script={script_s3_path})")
        return spec.name
    except ClientError as e:
        if e.response["Error"]["Code"] != "EntityNotFoundException":
            raise
        glue.create_job(Name=spec.name, **job_update)
        print(f"[glue] created job {spec.name} (script={script_s3_path})")
        return spec.name


@dataclass(frozen=True)
class GlueCrawlerSpec:
    name: str
    role_arn: str
    database_name: str
    target_s3_path: str


def ensure_glue_crawler(glue, spec: GlueCrawlerSpec) -> str:
    crawler_def = {
        "Name": spec.name,
        "Role": spec.role_arn,
        "DatabaseName": spec.database_name,
        "Targets": {"S3Targets": [{"Path": spec.target_s3_path}]},
        "SchemaChangePolicy": {"UpdateBehavior": "UPDATE_IN_DATABASE", "DeleteBehavior": "DEPRECATE_IN_DATABASE"},
    }

    try:
        glue.get_crawler(Name=spec.name)
        glue.update_crawler(**crawler_def)
        print(f"[crawler] updated {spec.name} -> {spec.target_s3_path}")
        return spec.name
    except ClientError as e:
        if e.response["Error"]["Code"] != "EntityNotFoundException":
            raise
        glue.create_crawler(**crawler_def)
        print(f"[crawler] created {spec.name} -> {spec.target_s3_path}")
        return spec.name


# ---------------- Ensure: Step Functions ----------------

def find_state_machine_arn(sfn, name: str) -> Optional[str]:
    paginator = sfn.get_paginator("list_state_machines")
    for page in paginator.paginate():
        for sm in page.get("stateMachines", []):
            if sm["name"] == name:
                return sm["stateMachineArn"]
    return None


@dataclass(frozen=True)
class StateMachineSpec:
    name: str
    role_arn: str
    definition: Dict[str, Any]
    enable_logging: bool = False
    log_group_arn: Optional[str] = None


def ensure_state_machine(sfn, spec: StateMachineSpec) -> str:
    arn = find_state_machine_arn(sfn, spec.name)

    common: Dict[str, Any] = {
        "definition": json.dumps(spec.definition),
        "roleArn": spec.role_arn,
        "tracingConfiguration": {"enabled": True},
    }

    if spec.enable_logging and spec.log_group_arn:
        common["loggingConfiguration"] = {
            "level": "ALL",
            "includeExecutionData": True,
            "destinations": [{"cloudWatchLogsLogGroup": {"logGroupArn": spec.log_group_arn}}],
        }

    if arn:
        sfn.update_state_machine(stateMachineArn=arn, **common)
        print(f"[sfn] updated {spec.name} -> {arn}")
        return arn

    resp = sfn.create_state_machine(name=spec.name, type="STANDARD", **common)
    arn = resp["stateMachineArn"]
    print(f"[sfn] created {spec.name} -> {arn}")
    return arn
