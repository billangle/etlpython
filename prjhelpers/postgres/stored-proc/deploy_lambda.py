#!/usr/bin/env python3
from __future__ import annotations

import argparse
import io
import json
import os
import time
import zipfile

import boto3


LAYER_ARN_DEFAULT = "arn:aws:lambda:us-east-1:253490756794:layer:FSA-DEV-psycopg2-python311:1"
GLUE_CONN_DEFAULT = "FSA-PROD-PG-DART115"
OUTPUT_BUCKET_DEFAULT = "fsa-prod-ops"
OUTPUT_PREFIX_DEFAULT = "postgres-analysis"

ROLE_NAME_DEFAULT = "FSA-PROD-PostgresAnalysis-LambdaRole"
FUNCTION_NAME_DEFAULT = "FSA-PROD-Postgres-Proc-Status"


def zip_lambda_code(lambda_py_path: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as z:
        with open(lambda_py_path, "rb") as f:
            z.writestr("lambda_function.py", f.read())
    return buf.getvalue()


def ensure_role(iam, role_name: str) -> str:
    assume = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action": "sts:AssumeRole"
        }]
    }

    try:
        role_arn = iam.get_role(RoleName=role_name)["Role"]["Arn"]
    except iam.exceptions.NoSuchEntityException:
        role_arn = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(assume),
            Description="Role for Lambda to query Postgres via Secrets and write results to S3",
        )["Role"]["Arn"]

    for pol in (
        "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
        "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole",
    ):
        try:
            iam.attach_role_policy(RoleName=role_name, PolicyArn=pol)
        except Exception:
            pass

    inline = {
        "Version": "2012-10-17",
        "Statement": [
            {"Sid": "GlueReadConnection", "Effect": "Allow", "Action": ["glue:GetConnection"], "Resource": "*"},
            {"Sid": "SecretsRead", "Effect": "Allow", "Action": ["secretsmanager:GetSecretValue"], "Resource": "*"},
            {"Sid": "S3Write", "Effect": "Allow", "Action": ["s3:PutObject"], "Resource": "*"},
        ],
    }
    iam.put_role_policy(
        RoleName=role_name,
        PolicyName="FSA-PROD-PostgresAnalysis-Access",
        PolicyDocument=json.dumps(inline),
    )

    time.sleep(8)
    return role_arn


def get_vpc_config_from_glue(glue, glue_conn_name: str) -> dict:
    conn = glue.get_connection(Name=glue_conn_name, HidePassword=True)["Connection"]
    phys = conn.get("PhysicalConnectionRequirements") or {}
    subnet_id = phys.get("SubnetId")
    sg_ids = phys.get("SecurityGroupIdList") or []
    if not subnet_id or not sg_ids:
        raise RuntimeError(f"Glue connection {glue_conn_name} missing SubnetId or SecurityGroupIdList")
    return {"SubnetIds": [subnet_id], "SecurityGroupIds": sg_ids}


def upsert_lambda(lam, function_name: str, role_arn: str, zip_bytes: bytes,
                  layer_arn: str, vpc_cfg: dict, env_vars: dict) -> None:
    try:
        lam.get_function(FunctionName=function_name)
        lam.update_function_configuration(
            FunctionName=function_name,
            Role=role_arn,
            Runtime="python3.11",
            Handler="lambda_function.lambda_handler",
            Timeout=60,
            MemorySize=256,
            Layers=[layer_arn],
            VpcConfig=vpc_cfg,
            Environment={"Variables": env_vars},
        )
        lam.update_function_code(FunctionName=function_name, ZipFile=zip_bytes, Publish=True)
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(
            FunctionName=function_name,
            Runtime="python3.11",
            Role=role_arn,
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": zip_bytes},
            Timeout=60,
            MemorySize=256,
            Publish=True,
            Layers=[layer_arn],
            VpcConfig=vpc_cfg,
            Environment={"Variables": env_vars},
        )

    lam.get_waiter("function_active").wait(FunctionName=function_name)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--region", default=os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-east-1")
    ap.add_argument("--function-name", default=FUNCTION_NAME_DEFAULT)
    ap.add_argument("--role-name", default=ROLE_NAME_DEFAULT)
    ap.add_argument("--layer-arn", default=LAYER_ARN_DEFAULT)
    ap.add_argument("--glue-connection-name", default=GLUE_CONN_DEFAULT)
    ap.add_argument("--output-bucket", default=OUTPUT_BUCKET_DEFAULT)
    ap.add_argument("--output-prefix", default=OUTPUT_PREFIX_DEFAULT)
    ap.add_argument("--lambda-py", default="lambda_function.py")
    args = ap.parse_args()

    sess = boto3.session.Session(region_name=args.region)
    iam = sess.client("iam")
    glue = sess.client("glue")
    lam = sess.client("lambda")

    role_arn = ensure_role(iam, args.role_name)
    zip_bytes = zip_lambda_code(args.lambda_py)
    vpc_cfg = get_vpc_config_from_glue(glue, args.glue_connection_name)

    env_vars = {
        "GLUE_CONNECTION_NAME": args.glue_connection_name,
        "OUTPUT_BUCKET": args.output_bucket,
        "OUTPUT_PREFIX": args.output_prefix,
        # Optionally set defaults (can be overridden by event):
        # "SECRET_NAME": "FSA-PROD-secrets",
        # "SECRET_PREFIX": "edv",
    }

    upsert_lambda(lam, args.function_name, role_arn, zip_bytes, args.layer_arn, vpc_cfg, env_vars)

    print("[OK] Deployed:", args.function_name)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())