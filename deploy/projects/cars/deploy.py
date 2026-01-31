# deploy/projects/cars/deploy.py
from __future__ import annotations

import importlib.util
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional
import sys

import boto3

from common.aws_common import (
    ensure_bucket_exists,
    ensure_lambda,
    ensure_glue_job,
    ensure_state_machine,
    LambdaSpec,
    GlueJobSpec,
    StateMachineSpec,
)


def _load_module_by_path(project_dir: Path, rel_path: str, module_name: str):
    """
    Load a python module by file path.
    IMPORTANT: register in sys.modules BEFORE exec_module so @dataclass works on Python 3.14.
    """
    mod_file = project_dir / rel_path
    if not mod_file.exists():
        raise FileNotFoundError(f"Missing module file: {mod_file}")

    spec = importlib.util.spec_from_file_location(module_name, str(mod_file))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not create module spec for: {mod_file}")

    mod = importlib.util.module_from_spec(spec)

    # ✅ Critical for Python 3.14 dataclasses: module must exist in sys.modules during execution
    sys.modules[module_name] = mod

    spec.loader.exec_module(mod)
    return mod


@dataclass(frozen=True)
class CarsNames:
    # Lambdas (EDV pipeline)
    build_processing_plan_fn: str
    check_results_fn: str
    finalize_pipeline_fn: str
    handle_failure_fn: str

    # Glue jobs
    exec_sql_glue_job: str
    pg_to_redshift_glue_job: str

    # Step Functions
    edv_state_machine: str
    cars_dm_etl_state_machine: str

    # Misc lambdas
    download_zip_fn: str
    upload_config_fn: str


def build_names(deploy_env: str) -> CarsNames:
    """
    Naming matches your convention:
      - FSA-<ENV>-edv-*
      - FSA-<ENV>-DATAMART-EXEC-DB-SQL
      - FSA-<ENV>-DART-PG-TO-REDSHIFT
      - FSA-<ENV>-DownloadZip
    """
    prefix = f"FSA-{deploy_env}"
    return CarsNames(
        # EDV lambdas
        build_processing_plan_fn=f"{prefix}-edv-build-processing-plan",
        check_results_fn=f"{prefix}-edv-check-results",
        finalize_pipeline_fn=f"{prefix}-edv-finalize-pipeline",
        handle_failure_fn=f"{prefix}-edv-handle-failure",
        # Glue jobs
        exec_sql_glue_job=f"{prefix}-DATAMART-EXEC-DB-SQL",
        pg_to_redshift_glue_job=f"{prefix}-DART-PG-TO-REDSHIFT",
        # State machines
        edv_state_machine=f"{prefix}-CARS-EDV-Pipeline",
        cars_dm_etl_state_machine=f"{prefix}-CARS-DM-ETL-Pipeline",
        # Utility lambdas
        download_zip_fn=f"{prefix}-DownloadZip",
        upload_config_fn=f"{prefix}-UploadConfig",
    )


def _layers(cfg: Dict[str, Any]) -> List[str]:
    """
    Keep existing deploy.py behavior (optional two layers).
    """
    sp = cfg.get("strparams") or {}
    layers: List[str] = []
    for k in ("thirdPartyLayerArnParam", "customLayerArnParam"):
        v = sp.get(k)
        if v:
            layers.append(v)
    return layers


# ---------------- GlueJobParameters helpers ----------------

def _as_bool(v: Any) -> Optional[bool]:
    if v is None:
        return None
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
    return None


def _as_int(v: Any, default: int) -> int:
    if v is None or v == "":
        return default
    try:
        return int(str(v).strip())
    except Exception:
        return default


def _parse_connection_names(glue_job_params: Dict[str, Any]) -> List[str]:
    """
    Glue job only references existing connection NAMES.
    """
    conns = glue_job_params.get("Connections") or []
    if not isinstance(conns, list):
        return []
    out: List[str] = []
    for c in conns:
        if not isinstance(c, dict):
            continue
        n = c.get("ConnectionName")
        if isinstance(n, str) and n.strip():
            out.append(n.strip())

    # de-dupe, preserve order
    seen = set()
    deduped: List[str] = []
    for n in out:
        if n in seen:
            continue
        seen.add(n)
        deduped.append(n)
    return deduped


def _merge_glue_default_args(
    base_args: Dict[str, Any],
    glue_job_params: Dict[str, Any],
) -> Dict[str, str]:
    """
    Produces DefaultArguments for Glue.

    Precedence:
      cfg.glueDefaultArgs < derived args from GlueJobParameters < GlueJobParameters.JobParameters
    """
    out: Dict[str, Any] = dict(base_args or {})

    # Spark UI logs
    spark_ui_path = glue_job_params.get("SparkUILogsPath")
    if spark_ui_path:
        out["--enable-spark-ui"] = "true"
        out["--spark-event-logs-path"] = str(spark_ui_path)

    # Metrics
    if _as_bool(glue_job_params.get("GenerateMetrics")) is True:
        out["--enable-metrics"] = "true"

    # Bookmarks
    bmk = _as_bool(glue_job_params.get("EnableJobBookmarks"))
    if bmk is True:
        out["--job-bookmark-option"] = "job-bookmark-enable"
    elif bmk is False:
        out["--job-bookmark-option"] = "job-bookmark-disable"

    # Observability metrics
    if str(glue_job_params.get("JobObservabilityMetrics", "")).strip().upper() == "ENABLED":
        out["--enable-observability-metrics"] = "true"

    # Continuous logging
    if str(glue_job_params.get("JobContinuousLogging", "")).strip().upper() == "ENABLED":
        out["--enable-continuous-cloudwatch-log"] = "true"

    # Temp dir
    temp_path = glue_job_params.get("TemporaryPath")
    if temp_path:
        out["--TempDir"] = str(temp_path)

    # Use Data Catalog as Hive metastore
    if _as_bool(glue_job_params.get("UseGlueDataCatalogAsTheHiveMetastore")) is True:
        out["--enable-glue-datacatalog"] = "true"

    # Extra files / py files (Glue expects comma-separated S3 URIs for these)
    ref_files = glue_job_params.get("ReferencedFilesS3Path")
    if ref_files:
        out["--extra-files"] = str(ref_files)

    extra_py = glue_job_params.get("AdditionalPythonModulesS3Path")
    if extra_py:
        out["--extra-py-files"] = str(extra_py)

    # Explicit job parameters override everything
    job_params = glue_job_params.get("JobParameters") or {}
    if isinstance(job_params, dict):
        for k, v in job_params.items():
            out[str(k)] = "" if v is None else str(v)

    # Glue expects string values
    return {str(k): str(v) for k, v in out.items()}


def _deploy_glue_job(
    *,
    glue_client,
    s3_client,
    cfg: Dict[str, Any],
    artifact_bucket: str,
    prefix: str,
    job_name: str,
    job_role_arn: str,
    script_local_path: Path,
    glue_job_params: Dict[str, Any],
) -> None:
    """
    Shared Glue job deployment logic for both jobs.
    """
    merged_default_args = _merge_glue_default_args(
        base_args=(cfg.get("glueDefaultArgs") or {}),
        glue_job_params=glue_job_params,
    )

    glue_version = str(glue_job_params.get("GlueVersion") or "4.0")
    worker_type = str(glue_job_params.get("WorkerType") or "G.1X")
    number_of_workers = _as_int(glue_job_params.get("NumberOfWorkers"), default=2)
    timeout_minutes = _as_int(glue_job_params.get("TimeoutMinutes"), default=60)
    max_retries = _as_int(glue_job_params.get("MaxRetries"), default=0)
    max_concurrency = _as_int(glue_job_params.get("MaxConcurrency"), default=1)

    connection_names = _parse_connection_names(glue_job_params)

    ensure_glue_job(
        glue_client,
        s3_client,
        GlueJobSpec(
            name=job_name,
            role_arn=job_role_arn,
            script_local_path=str(script_local_path),
            script_s3_bucket=artifact_bucket,
            script_s3_key=f"{prefix}glue/{script_local_path.name}",
            default_args=merged_default_args,
            glue_version=glue_version,
            worker_type=worker_type,
            number_of_workers=number_of_workers,
            timeout_minutes=timeout_minutes,
            max_retries=max_retries,
            max_concurrency=max_concurrency,
            connection_names=connection_names,
        ),
    )


def deploy(cfg: Dict[str, Any], region: str) -> Dict[str, str]:
    """
    Deploys:
      - Lambdas (EDV)
      - Glue jobs: DATAMART-EXEC-DB-SQL + DART-PG-TO-REDSHIFT
      - Step Functions: EDV pipeline + Cars DM ETL pipeline

    FIX:
      - Passes the actual Glue Job names into DatamartEtlStateMachineInputs so the
        DM ETL builder uses them (JobName / --JOB_NAME) as literals.
    """
    deploy_env = cfg["deployEnv"]
    names = build_names(deploy_env)

    artifact_bucket = cfg["artifacts"]["artifactBucket"]
    prefix = cfg["artifacts"]["prefix"].rstrip("/") + "/"

    strparams = cfg.get("strparams") or {}
    etl_lambda_role_arn = strparams["etlRoleArnParam"]
    glue_job_role_arn = strparams["glueJobRoleArnParam"]

    sfn_role_arn = (cfg.get("stepFunctions") or {}).get("roleArn") or ""
    if not sfn_role_arn:
        raise RuntimeError("Missing required cfg.stepFunctions.roleArn")

    project_dir = Path(__file__).resolve().parent  # .../deploy/projects/cars
    lambda_root = project_dir / "lambda"
    glue_root = project_dir / "glue"

    build_plan_dir = lambda_root / "edv-build-processing-plan"
    check_results_dir = lambda_root / "edv-check-results"
    finalize_dir = lambda_root / "edv-finalize-pipeline"
    handle_failure_dir = lambda_root / "edv-handle-failure"

    dz_cfg = cfg.get("downloadZip") or {}
    download_zip_dirname = dz_cfg.get("sourceDirName", "DownloadZip")
    download_zip_dir = lambda_root / download_zip_dirname
    download_zip_handler = dz_cfg.get("handler", "lambda_function.lambda_handler")

    exec_sql_script_local = glue_root / "FSA-CERT-DATAMART-EXEC-DB-SQL.py"
    pg_to_rs_script_local = glue_root / "FSA-CERT-DART-PG-TO-REDSHIFT.py"

    session = boto3.Session(region_name=region)
    s3 = session.client("s3")
    lam = session.client("lambda")
    glue = session.client("glue")
    sfn = session.client("stepfunctions")

    ensure_bucket_exists(s3, artifact_bucket, region)

    env_vars: Dict[str, str] = {}
    env_vars.update(cfg.get("lambdaEnv") or {})

    runtime = strparams.get("lambdaRuntime", "python3.11")
    layers = _layers(cfg)

    # --- Lambdas (EDV pipeline) ---
    build_plan_arn = ensure_lambda(
        lam,
        LambdaSpec(
            name=names.build_processing_plan_fn,
            role_arn=etl_lambda_role_arn,
            handler="lambda_function.handler",
            runtime=runtime,
            source_dir=str(build_plan_dir),
            env=env_vars,
            layers=layers,
        ),
    )

    check_results_arn = ensure_lambda(
        lam,
        LambdaSpec(
            name=names.check_results_fn,
            role_arn=etl_lambda_role_arn,
            handler="lambda_function.handler",
            runtime=runtime,
            source_dir=str(check_results_dir),
            env=env_vars,
            layers=layers,
        ),
    )

    finalize_arn = ensure_lambda(
        lam,
        LambdaSpec(
            name=names.finalize_pipeline_fn,
            role_arn=etl_lambda_role_arn,
            handler="lambda_function.handler",
            runtime=runtime,
            source_dir=str(finalize_dir),
            env=env_vars,
            layers=layers,
        ),
    )

    handle_failure_arn = ensure_lambda(
        lam,
        LambdaSpec(
            name=names.handle_failure_fn,
            role_arn=etl_lambda_role_arn,
            handler="lambda_function.handler",
            runtime=runtime,
            source_dir=str(handle_failure_dir),
            env=env_vars,
            layers=layers,
        ),
    )

    download_zip_arn = ensure_lambda(
        lam,
        LambdaSpec(
            name=names.download_zip_fn,
            role_arn=etl_lambda_role_arn,
            handler=download_zip_handler,
            runtime=runtime,
            source_dir=str(download_zip_dir),
            env=env_vars,
            layers=layers,
        ),
    )

    upload_config_arn = ensure_lambda(
        lam,
        LambdaSpec(
            name=names.upload_config_fn,
            role_arn=etl_lambda_role_arn,
            handler="lambda_function.handler",
            runtime=runtime,
            source_dir=str(lambda_root / "UploadConfig"),
            env=env_vars,
            layers=layers,
        ),
    )

    # --- Glue jobs ---
    glue_job_params_exec_sql = cfg.get("GlueJobParameters") or {}

    _deploy_glue_job(
        glue_client=glue,
        s3_client=s3,
        cfg=cfg,
        artifact_bucket=artifact_bucket,
        prefix=prefix,
        job_name=names.exec_sql_glue_job,
        job_role_arn=glue_job_role_arn,
        script_local_path=exec_sql_script_local,
        glue_job_params=glue_job_params_exec_sql,
    )

    glue_job_params_pg_to_rs = cfg.get("GlueJobParametersPgToRedshift")
    if glue_job_params_pg_to_rs is None:
        glue_job_params_pg_to_rs = glue_job_params_exec_sql

    _deploy_glue_job(
        glue_client=glue,
        s3_client=s3,
        cfg=cfg,
        artifact_bucket=artifact_bucket,
        prefix=prefix,
        job_name=names.pg_to_redshift_glue_job,
        job_role_arn=glue_job_role_arn,
        script_local_path=pg_to_rs_script_local,
        glue_job_params=glue_job_params_pg_to_rs or {},
    )

    # --- Step Function #1: EDV pipeline (states/cars_stepfunction.py) ---
    edv_mod = _load_module_by_path(
        project_dir=project_dir,
        rel_path="states/cars_stepfunction.py",
        module_name="projects.cars.states.cars_stepfunction",
    )
    EdvPipelineStateMachineInputs = edv_mod.EdvPipelineStateMachineInputs
    EdvPipelineStateMachineBuilder = edv_mod.EdvPipelineStateMachineBuilder

    edv_inputs = EdvPipelineStateMachineInputs(
        build_processing_plan_fn=build_plan_arn,
        check_results_fn=check_results_arn,
        finalize_pipeline_fn=finalize_arn,
        handle_failure_fn=handle_failure_arn,
        exec_sql_glue_job_name=names.exec_sql_glue_job,
    )
    edv_definition = EdvPipelineStateMachineBuilder.edv_pipeline_asl(edv_inputs)

    edv_sfn_arn = ensure_state_machine(
        sfn,
        StateMachineSpec(
            name=names.edv_state_machine,
            role_arn=sfn_role_arn,
            definition=edv_definition,
        ),
    )

    # --- Step Function #2: Cars DM ETL pipeline (states/cars_dm_etl_stepfunction.py) ---
    dm_mod = _load_module_by_path(
        project_dir=project_dir,
        rel_path="states/cars_dm_etl_stepfunction.py",
        module_name="projects.cars.states.cars_dm_etl_stepfunction",
    )
    DatamartEtlStateMachineInputs = dm_mod.DatamartEtlStateMachineInputs
    DatamartEtlStateMachineBuilder = dm_mod.DatamartEtlStateMachineBuilder

    # ✅ THIS is the “variables are passed” fix:
    # Pass the *actual deployed* Glue job names into the DM ETL builder inputs.
    dm_inputs = DatamartEtlStateMachineInputs(
        exec_db_sql_glue_job_name=names.exec_sql_glue_job,
        pg_to_redshift_glue_job_name=names.pg_to_redshift_glue_job,
    )
    dm_definition = DatamartEtlStateMachineBuilder.datamart_etl_asl(dm_inputs)

    dm_sfn_arn = ensure_state_machine(
        sfn,
        StateMachineSpec(
            name=names.cars_dm_etl_state_machine,
            role_arn=sfn_role_arn,
            definition=dm_definition,
        ),
    )

    return {
        "lambda_build_processing_plan_arn": build_plan_arn,
        "lambda_check_results_arn": check_results_arn,
        "lambda_finalize_pipeline_arn": finalize_arn,
        "lambda_handle_failure_arn": handle_failure_arn,
        "lambda_download_zip_arn": download_zip_arn,
        "lambda_upload_config_arn": upload_config_arn,
        "glue_exec_sql_job_name": names.exec_sql_glue_job,
        "glue_pg_to_redshift_job_name": names.pg_to_redshift_glue_job,
        "state_machine_edv_arn": edv_sfn_arn,
        "state_machine_cars_dm_etl_arn": dm_sfn_arn,
    }
