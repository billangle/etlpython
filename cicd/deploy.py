#!/usr/bin/env python3
import argparse
import json
import os
import sys
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any, Optional, List

import boto3
from botocore.exceptions import ClientError


def read_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def zip_dir(source_dir: str, out_zip: str) -> str:
    src = Path(source_dir).resolve()
    out = Path(out_zip).resolve()
    out.parent.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for p in src.rglob("*"):
            if p.is_dir():
                continue
            # skip venv/pycache
            rel = p.relative_to(src)
            if "__pycache__" in rel.parts or rel.parts[0].startswith(".venv"):
                continue
            z.write(p, arcname=str(rel))
    return str(out)


def s3_upload_file(s3, local_path: str, bucket: str, key: str) -> str:
    s3.upload_file(local_path, bucket, key)
    return f"s3://{bucket}/{key}"


def ensure_lambda(lambda_client, cfg: Dict[str, Any]) -> str:
    """
    Creates or updates lambda. Returns FunctionArn.
    Expects code zip built from cfg['source_dir'].
    """
    fn = cfg["function_name"]
    runtime = cfg["runtime"]
    handler = cfg["handler"]
    role_arn = cfg["role_arn"]
    timeout = int(cfg.get("timeout", 30))
    memory = int(cfg.get("memory_size", 256))
    env_vars = cfg.get("env", {})

    build_zip = zip_dir(cfg["source_dir"], f"build/{fn}.zip")
    with open(build_zip, "rb") as f:
        code_bytes = f.read()

    try:
        resp = lambda_client.get_function(FunctionName=fn)
        arn = resp["Configuration"]["FunctionArn"]

        # Update code
        lambda_client.update_function_code(
            FunctionName=fn,
            ZipFile=code_bytes,
            Publish=True
        )

        # Update config
        lambda_client.update_function_configuration(
            FunctionName=fn,
            Runtime=runtime,
            Role=role_arn,
            Handler=handler,
            Timeout=timeout,
            MemorySize=memory,
            Environment={"Variables": env_vars} if env_vars else {"Variables": {}},
        )

        wait_for_lambda_update(lambda_client, fn)
        print(f"[lambda] updated {fn} -> {arn}")
        return arn

    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise

        # Create
        resp = lambda_client.create_function(
            FunctionName=fn,
            Runtime=runtime,
            Role=role_arn,
            Handler=handler,
            Code={"ZipFile": code_bytes},
            Timeout=timeout,
            MemorySize=memory,
            Publish=True,
            Environment={"Variables": env_vars} if env_vars else {"Variables": {}},
        )
        arn = resp["FunctionArn"]
        wait_for_lambda_update(lambda_client, fn)
        print(f"[lambda] created {fn} -> {arn}")
        return arn


def wait_for_lambda_update(lambda_client, fn_name: str, max_wait_s: int = 120) -> None:
    start = time.time()
    while True:
        resp = lambda_client.get_function_configuration(FunctionName=fn_name)
        status = resp.get("LastUpdateStatus", "Successful")
        if status == "Successful":
            return
        if status == "Failed":
            raise RuntimeError(f"Lambda update failed: {resp.get('LastUpdateStatusReason')}")
        if time.time() - start > max_wait_s:
            raise TimeoutError(f"Timed out waiting for Lambda update: {fn_name}")
        time.sleep(3)


def ensure_glue_job(glue_client, s3_client, cfg: Dict[str, Any]) -> str:
    """
    Uploads script (and optional extra py files) to S3 and creates/updates Glue Job.
    Returns job name.
    """
    job_name = cfg["job_name"]
    role_arn = cfg["role_arn"]
    script_local = cfg["script_local_path"]
    bucket = cfg["script_s3_bucket"]
    script_key = cfg["script_s3_prefix"].rstrip("/") + "/" + Path(script_local).name
    script_s3_path = s3_upload_file(s3_client, script_local, bucket, script_key)

    extra_py_files_local: List[str] = cfg.get("extra_py_files_local", [])
    extra_py_uris: List[str] = []
    for lp in extra_py_files_local:
        k = cfg["extra_py_files_s3_prefix"].rstrip("/") + "/" + Path(lp).name
        uri = s3_upload_file(s3_client, lp, bucket, k)
        extra_py_uris.append(uri)

    default_args = {
        "--job-language": "python",
        "--enable-metrics": "true",
        "--enable-continuous-cloudwatch-log": "true",
        "--enable-spark-ui": "true",
    }
    if extra_py_uris:
        default_args["--extra-py-files"] = ",".join(extra_py_uris)

    job_update = {
        "Role": role_arn,
        "ExecutionProperty": {"MaxConcurrentRuns": 1},
        "Command": {
            "Name": "glueetl",
            "ScriptLocation": script_s3_path,
            "PythonVersion": "3"
        },
        "DefaultArguments": default_args,
        "GlueVersion": cfg.get("glue_version", "4.0"),
        "WorkerType": cfg.get("worker_type", "G.1X"),
        "NumberOfWorkers": int(cfg.get("number_of_workers", 2)),
        "MaxRetries": int(cfg.get("max_retries", 0)),
        "Timeout": int(cfg.get("timeout_minutes", 60)),
    }

    try:
        glue_client.get_job(JobName=job_name)
        glue_client.update_job(JobName=job_name, JobUpdate=job_update)
        print(f"[glue] updated job {job_name} (script={script_s3_path})")
        return job_name
    except ClientError as e:
        if e.response["Error"]["Code"] != "EntityNotFoundException":
            raise

        glue_client.create_job(Name=job_name, **job_update)
        print(f"[glue] created job {job_name} (script={script_s3_path})")
        return job_name


def render_asl(template_path: str, replacements: Dict[str, str]) -> str:
    raw = Path(template_path).read_text(encoding="utf-8")
    for k, v in replacements.items():
        raw = raw.replace(k, v)
    # Validate JSON
    json.loads(raw)
    return raw


def ensure_state_machine(sfn_client, cfg: Dict[str, Any], definition_str: str) -> str:
    """
    Create/update SFN state machine. Returns state machine ARN.
    """
    name = cfg["state_machine_name"]
    role_arn = cfg["role_arn"]

    # Find existing by listing (simple approach; ok for moderate counts)
    existing_arn = None
    paginator = sfn_client.get_paginator("list_state_machines")
    for page in paginator.paginate():
        for sm in page.get("stateMachines", []):
            if sm["name"] == name:
                existing_arn = sm["stateMachineArn"]
                break
        if existing_arn:
            break

    if existing_arn:
        sfn_client.update_state_machine(
            stateMachineArn=existing_arn,
            definition=definition_str,
            roleArn=role_arn,
        )
        print(f"[sfn] updated {name} -> {existing_arn}")
        return existing_arn

    resp = sfn_client.create_state_machine(
        name=name,
        definition=definition_str,
        roleArn=role_arn,
        type="STANDARD"
    )
    arn = resp["stateMachineArn"]
    print(f"[sfn] created {name} -> {arn}")
    return arn


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--region", required=True)
    ap.add_argument("--env", required=True)
    ap.add_argument("--project", required=True)
    args = ap.parse_args()

    config = read_json(args.config)

    session = boto3.Session(region_name=args.region)
    lambda_client = session.client("lambda")
    sfn_client = session.client("stepfunctions")
    glue_client = session.client("glue")
    s3_client = session.client("s3")

    # 1) Lambda
    lambda_arn = ensure_lambda(lambda_client, config["lambda"])

    # 2) Glue
    glue_job_name = ensure_glue_job(glue_client, s3_client, config["glue"])

    # 3) Step Functions (render template tokens)
    definition_file = config["stepfunctions"]["definition_file"]
    definition_str = render_asl(definition_file, {
        "${LAMBDA_ARN}": lambda_arn,
        "${GLUE_JOB_NAME}": glue_job_name,
    })
    sfn_arn = ensure_state_machine(sfn_client, config["stepfunctions"], definition_str)

    print("\nDEPLOY SUMMARY")
    print(f"  Lambda ARN:         {lambda_arn}")
    print(f"  Glue Job:           {glue_job_name}")
    print(f"  State Machine ARN:  {sfn_arn}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise

