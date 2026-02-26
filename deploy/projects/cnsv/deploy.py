# deploy/projects/cnsv/deploy.py
# Deploys the CNSV EXEC-SQL Glue job, the 4 edv Lambda functions, and the
# EXEC-SQL Step Function state machine.
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import boto3

from common.aws_common import (
    ensure_bucket_exists,
    ensure_glue_job,
    ensure_state_machine,
    ensure_lambda,
    GlueJobSpec,
    StateMachineSpec,
    LambdaSpec,
)

# --------------------------------------------------------------------------------------
# Safe parsers (consistent with deploy_base.py)
# --------------------------------------------------------------------------------------

def _as_bool(v: Any, default: Optional[bool] = None) -> Optional[bool]:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("true", "1", "yes", "y", "on"):
            return True
        if s in ("false", "0", "no", "n", "off"):
            return False
    return default


def _as_int(v: Any, default: int) -> int:
    if v is None or v == "":
        return default
    try:
        return int(str(v).strip())
    except Exception:
        return default


def _as_str(v: Any, default: str = "") -> str:
    if v is None:
        return default
    s = str(v)
    return s if s.strip() else default


def _as_str_list(v: Any) -> List[str]:
    if not isinstance(v, list):
        return []
    return [item.strip() for item in v if isinstance(item, str) and item.strip()]


# --------------------------------------------------------------------------------------
# GlueConfig helpers (consistent with deploy_base.py)
# --------------------------------------------------------------------------------------

def _script_stem(p: Path) -> str:
    return p.name[:-3] if p.name.lower().endswith(".py") else p.name


def _parse_glue_config_array(cfg: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    root = cfg.get("GlueConfig")
    if not isinstance(root, list) or not root:
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for item in root:
        if not isinstance(item, dict):
            continue
        for k, v in item.items():
            if isinstance(k, str) and k.strip() and isinstance(v, dict):
                out[k.strip()] = v
    return out


def _glue_config_for_script(cfg: Dict[str, Any], script_stem_name: str) -> Dict[str, Any]:
    return _parse_glue_config_array(cfg).get(script_stem_name, {})


def _parse_connection_names(glue_job_params: Dict[str, Any]) -> List[str]:
    conns = glue_job_params.get("Connections") or []
    if not isinstance(conns, list):
        return []
    seen: set = set()
    out: List[str] = []
    for c in conns:
        if not isinstance(c, dict):
            continue
        n = c.get("ConnectionName")
        if isinstance(n, str) and n.strip() and n.strip() not in seen:
            seen.add(n.strip())
            out.append(n.strip())
    return out


def _merge_glue_default_args(base_args: Dict[str, Any], glue_job_params: Dict[str, Any]) -> Dict[str, str]:
    out: Dict[str, Any] = dict(base_args or {})

    spark_ui_path = glue_job_params.get("SparkUILogsPath")
    if spark_ui_path:
        out["--enable-spark-ui"] = "true"
        out["--spark-event-logs-path"] = str(spark_ui_path)

    if _as_bool(glue_job_params.get("GenerateMetrics"), default=False) is True:
        out["--enable-metrics"] = "true"

    bmk = _as_bool(glue_job_params.get("EnableJobBookmarks"), default=None)
    if bmk is True:
        out["--job-bookmark-option"] = "job-bookmark-enable"
    elif bmk is False:
        out["--job-bookmark-option"] = "job-bookmark-disable"

    if str(glue_job_params.get("JobObservabilityMetrics", "")).strip().upper() == "ENABLED":
        out["--enable-observability-metrics"] = "true"

    if str(glue_job_params.get("JobContinuousLogging", "")).strip().upper() == "ENABLED":
        out["--enable-continuous-cloudwatch-log"] = "true"

    temp_path = glue_job_params.get("TemporaryPath")
    if temp_path:
        out["--TempDir"] = str(temp_path)

    if _as_bool(glue_job_params.get("UseGlueDataCatalogAsTheHiveMetastore"), default=False) is True:
        out["--enable-glue-datacatalog"] = "true"

    extra_py = glue_job_params.get("AdditionalPythonModulesPath") or glue_job_params.get("AdditionalPythonModulesS3Path")
    if extra_py and str(extra_py).strip():
        out["--extra-py-files"] = str(extra_py).strip()

    python_lib = _as_str(glue_job_params.get("PythonLibraryPath"))
    if python_lib and "--extra-py-files" not in out:
        out["--extra-py-files"] = python_lib

    job_params = glue_job_params.get("JobParameters") or {}
    if isinstance(job_params, dict):
        for k, v in job_params.items():
            out[str(k)] = "" if v is None else str(v)

    return {str(k): str(v) for k, v in out.items()}


# --------------------------------------------------------------------------------------
# Script S3 key naming (consistent with deploy_base.py)
# --------------------------------------------------------------------------------------

def _strip_known_script_prefixes(filename: str) -> str:
    name = Path(filename).name
    if name.startswith("FSA-"):
        parts = name.split("-", 2)
        if len(parts) == 3:
            name = parts[2]
    return name


def _s3_script_key(prefix: str, deploy_env: str, project: str, local_filename: str) -> str:
    dep = (deploy_env or "").strip()
    proj = (project or "").strip()
    if not dep or not proj:
        raise RuntimeError("deploy_env and project are required for Glue script naming")
    suffix = _strip_known_script_prefixes(local_filename)
    final_name = f"FSA-{dep}-{proj}-{suffix}"
    return f"{prefix}glue/{final_name}"


# --------------------------------------------------------------------------------------
# Names
# --------------------------------------------------------------------------------------

@dataclass(frozen=True)
class Names:
    prefix: str
    exec_sql_glue_job: str
    sm_exec_sql: str
    fn_build_processing_plan: str
    fn_check_results: str
    fn_finalize_pipeline: str
    fn_handle_failure: str


def build_names(deploy_env: str, project: str) -> Names:
    dep = (deploy_env or "").strip()
    proj = (project or "").strip()
    if not dep:
        raise RuntimeError("Missing required cfg['deployEnv']")
    if not proj:
        raise RuntimeError("Missing required cfg['project']")
    prefix = f"FSA-{dep}-{proj}"
    return Names(
        prefix=prefix,
        exec_sql_glue_job=f"{prefix}-EXEC-SQL",
        sm_exec_sql=f"{prefix}-EXEC-SQL",
        fn_build_processing_plan=f"{prefix}-edv-build-processing-plan",
        fn_check_results=f"{prefix}-edv-check-results",
        fn_finalize_pipeline=f"{prefix}-edv-finalize-pipeline",
        fn_handle_failure=f"{prefix}-edv-handle-failure",
    )


# --------------------------------------------------------------------------------------
# ASL loader
# --------------------------------------------------------------------------------------

_PARAM_ASL_FILE = "EXEC-SQL.asl.json"


def _load_asl_file(project_dir: Path, filename: str) -> Dict[str, Any]:
    p = project_dir / "states" / filename
    if not p.exists():
        raise FileNotFoundError(f"Missing ASL file: {p}")
    obj = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(obj, dict) or not obj:
        raise RuntimeError(f"ASL file did not parse to a JSON object: {p}")
    return obj


# --------------------------------------------------------------------------------------
# Deploy
# --------------------------------------------------------------------------------------

def deploy(cfg: Dict[str, Any], region: str) -> Dict[str, str]:
    """
    CNSV EXEC-SQL deployer.

    Deploys:
      - Glue job:       FSA-{ENV}-CNSV-EXEC-SQL         (glue/EXEC-SQL.py)
      - Lambda x4:      FSA-{ENV}-CNSV-edv-*             (lambda/edv-*/lambda_function.py)
      - State machine:  FSA-{ENV}-CNSV-EXEC-SQL          (states/EXEC-SQL.asl.json)

    All names derived from cfg.deployEnv and cfg.project — no hardcoding.
    Lambda ARNs are obtained from ensure_lambda and substituted into the ASL at deploy time.
    """
    deploy_env = cfg["deployEnv"]
    project = cfg["project"]
    names = build_names(deploy_env, project)

    # Artifacts
    artifacts = cfg.get("artifacts") or {}
    artifact_bucket = _as_str(artifacts.get("artifactBucket"))
    prefix = _as_str(artifacts.get("prefix")).rstrip("/") + "/"
    if not artifact_bucket:
        raise RuntimeError("Missing required cfg.artifacts.artifactBucket")
    if prefix == "/":
        prefix = ""

    # strparams
    strparams = cfg.get("strparams") or {}
    glue_job_role_arn = _as_str(strparams.get("glueJobRoleArnParam"))
    etl_role_arn = _as_str(strparams.get("etlRoleArnParam"))
    sfn_role_arn = _as_str((cfg.get("stepFunctions") or {}).get("roleArn"))

    missing: List[str] = []
    if not glue_job_role_arn:
        missing.append("strparams.glueJobRoleArnParam")
    if not etl_role_arn:
        missing.append("strparams.etlRoleArnParam")
    if not sfn_role_arn:
        missing.append("stepFunctions.roleArn")
    if missing:
        raise RuntimeError("Missing required config keys: " + ", ".join(missing))

    # Lambda shared config (same pattern as deploy_base.py)
    lambdas_cfg = cfg.get("lambdas") or cfg.get("lambda") or {}
    if not isinstance(lambdas_cfg, dict):
        lambdas_cfg = {}
    shared_layers = _as_str_list(lambdas_cfg.get("layers"))
    shared_runtime = _as_str(lambdas_cfg.get("runtime")) or "python3.11"
    shared_timeout = _as_int(lambdas_cfg.get("timeoutSeconds"), 30)
    shared_memory = _as_int(lambdas_cfg.get("memoryMb"), 256)
    shared_env: Dict[str, str] = {}
    if isinstance(lambdas_cfg.get("environment"), dict):
        shared_env = {str(k): str(v) for k, v in lambdas_cfg["environment"].items()}

    vpc_cfg = lambdas_cfg.get("vpcConfig") or {}
    subnet_ids = _as_str_list(vpc_cfg.get("subnetIds")) or None
    security_group_ids = _as_str_list(vpc_cfg.get("securityGroupIds")) or None

    # Local paths
    project_dir = Path(__file__).resolve().parent
    glue_root = project_dir / "glue"
    lambda_root = project_dir / "lambda"

    # Glue script
    exec_sql_script_local = glue_root / "EXEC-SQL.py"
    if not exec_sql_script_local.exists():
        raise FileNotFoundError(f"Missing Glue script: {exec_sql_script_local}")

    exec_sql_params = _glue_config_for_script(cfg, _script_stem(exec_sql_script_local))

    glue_defaults = cfg.get("glueDefaultArgs") or {}
    exec_sql_default_args = _merge_glue_default_args(glue_defaults, exec_sql_params)

    exec_sql_conns = _parse_connection_names(exec_sql_params)

    exec_sql_d = {
        "GlueVersion":    _as_str(exec_sql_params.get("GlueVersion"), "4.0"),
        "WorkerType":     _as_str(exec_sql_params.get("WorkerType"), "G.2X"),
        "NumberOfWorkers": _as_int(exec_sql_params.get("NumberOfWorkers"), 2),
        "TimeoutMinutes": _as_int(exec_sql_params.get("TimeoutMinutes"), 480),
        "MaxRetries":     _as_int(exec_sql_params.get("MaxRetries"), 0),
        "MaxConcurrency": _as_int(exec_sql_params.get("MaxConcurrency"), 1),
    }

    # Clients
    session = boto3.Session(region_name=region)
    s3 = session.client("s3")
    glue = session.client("glue")
    sfn = session.client("stepfunctions")
    lam = session.client("lambda")

    ensure_bucket_exists(s3, artifact_bucket, region)

    # --- Glue job ---
    exec_sql_script_s3_key = _s3_script_key(prefix, deploy_env, project, exec_sql_script_local.name)

    print(f"[1/3] Deploying Glue job: {names.exec_sql_glue_job}")
    ensure_glue_job(
        glue,
        s3,
        GlueJobSpec(
            name=names.exec_sql_glue_job,
            role_arn=glue_job_role_arn,
            script_local_path=str(exec_sql_script_local),
            script_s3_bucket=artifact_bucket,
            script_s3_key=exec_sql_script_s3_key,
            default_args=exec_sql_default_args,
            glue_version=exec_sql_d["GlueVersion"],
            worker_type=exec_sql_d["WorkerType"],
            number_of_workers=exec_sql_d["NumberOfWorkers"],
            timeout_minutes=exec_sql_d["TimeoutMinutes"],
            max_retries=exec_sql_d["MaxRetries"],
            max_concurrency=exec_sql_d["MaxConcurrency"],
            connection_names=exec_sql_conns,
        ),
    )

    # --- Lambda functions ---
    print(f"[2/3] Deploying Lambda functions")
    required_lambdas: List[Tuple[str, str]] = [
        (names.fn_build_processing_plan, "edv-build-processing-plan"),
        (names.fn_check_results,          "edv-check-results"),
        (names.fn_finalize_pipeline,      "edv-finalize-pipeline"),
        (names.fn_handle_failure,         "edv-handle-failure"),
    ]

    lambda_arns: Dict[str, str] = {}
    for fn_name, dir_suffix in required_lambdas:
        src_dir = lambda_root / dir_suffix
        if not src_dir.exists():
            raise FileNotFoundError(f"Lambda source dir not found: {src_dir}")
        handler_file = src_dir / "lambda_function.py"
        if not handler_file.exists():
            raise FileNotFoundError(f"Missing lambda_function.py in: {src_dir}")
        spec = LambdaSpec(
            name=fn_name,
            role_arn=etl_role_arn,
            handler="lambda_function.handler",
            runtime=shared_runtime,
            source_dir=str(src_dir),
            env=dict(shared_env),
            layers=list(shared_layers),
            timeout=shared_timeout,
            memory=shared_memory,
            subnet_ids=subnet_ids,
            security_group_ids=security_group_ids,
        )
        arn = ensure_lambda(lam, spec)
        lambda_arns[fn_name] = arn
        print(f"  {fn_name}: {arn}")

    # --- State machine ---
    print(f"[3/3] Deploying State Machine: {names.sm_exec_sql}")
    raw_asl = _load_asl_file(project_dir, _PARAM_ASL_FILE)
    asl_text = json.dumps(raw_asl)

    # Read deploy-time payload values from EXEC-SQL.JobParameters in the config
    exec_sql_job_params = exec_sql_params.get("JobParameters") or {}
    data_src_nm = _as_str(exec_sql_job_params.get("--data_src_nm"))
    run_type     = _as_str(exec_sql_job_params.get("--run_type"))
    start_date   = _as_str(exec_sql_job_params.get("--start_date"))

    substitutions = {
        "__EXEC_SQL_GLUE_JOB_NAME__":       names.exec_sql_glue_job,
        "__BUILD_PROCESSING_PLAN_FN_ARN__": lambda_arns[names.fn_build_processing_plan],
        "__CHECK_RESULTS_FN_ARN__":          lambda_arns[names.fn_check_results],
        "__FINALIZE_PIPELINE_FN_ARN__":      lambda_arns[names.fn_finalize_pipeline],
        "__HANDLE_FAILURE_FN_ARN__":         lambda_arns[names.fn_handle_failure],
        "__DATA_SRC_NM__":                  data_src_nm,
        "__RUN_TYPE__":                     run_type,
        "__START_DATE__":                   start_date,
        "__ENV__":                          str(deploy_env),
    }
    for placeholder, value in substitutions.items():
        asl_text = asl_text.replace(placeholder, value)

    sm_arn = ensure_state_machine(
        sfn,
        StateMachineSpec(
            name=names.sm_exec_sql,
            role_arn=sfn_role_arn,
            definition=json.loads(asl_text),
        ),
    )

    print(f"\nDeploy complete.")
    return {
        "deploy_env":                       str(deploy_env),
        "project":                          str(project),
        "artifact_bucket":                  artifact_bucket,
        "artifact_prefix":                  prefix,
        "glue_job_name":                    names.exec_sql_glue_job,
        "glue_script_s3_key":               exec_sql_script_s3_key,
        "glue_config_applied":              "yes" if bool(exec_sql_params) else "no",
        "fn_build_processing_plan_name":    names.fn_build_processing_plan,
        "fn_build_processing_plan_arn":     lambda_arns[names.fn_build_processing_plan],
        "fn_check_results_name":            names.fn_check_results,
        "fn_check_results_arn":             lambda_arns[names.fn_check_results],
        "fn_finalize_pipeline_name":        names.fn_finalize_pipeline,
        "fn_finalize_pipeline_arn":         lambda_arns[names.fn_finalize_pipeline],
        "fn_handle_failure_name":           names.fn_handle_failure,
        "fn_handle_failure_arn":            lambda_arns[names.fn_handle_failure],
        "state_machine_name":               names.sm_exec_sql,
        "state_machine_arn":                sm_arn,
    }


# --------------------------------------------------------------------------------------
# CLI entry point
# --------------------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="Deploy CNSV EXEC-SQL Glue job and Step Function")
    parser.add_argument("--config", required=True, help="Path to env config JSON (e.g. config/cnsv/prod.json)")
    parser.add_argument("--region", default="us-east-1", help="AWS region (default: us-east-1)")
    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.exists():
        print(f"ERROR: Config file not found: {config_path}", file=sys.stderr)
        sys.exit(1)

    cfg = json.loads(config_path.read_text(encoding="utf-8"))
    result = deploy(cfg, args.region)

    print("\n--- Deployment Summary ---")
    for k, v in result.items():
        print(f"  {k}: {v}")
