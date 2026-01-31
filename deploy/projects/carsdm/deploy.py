# deploy/projects/carsdm/deploy.py
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


def _load_stepfunction_module():
    """
    Load ./states/carsdm_stepfunction.py by file path.
    IMPORTANT: register in sys.modules BEFORE exec_module so @dataclass works on Python 3.14.
    """
    project_dir = Path(__file__).resolve().parent  # .../deploy/projects/carsdm
    step_fn_file = project_dir / "states" / "carsdm_stepfunction.py"

    if not step_fn_file.exists():
        raise FileNotFoundError(f"Missing step function file: {step_fn_file}")

    module_name = "projects.carsdm.states.carsdm_stepfunction"
    spec = importlib.util.spec_from_file_location(module_name, str(step_fn_file))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not create module spec for: {step_fn_file}")

    mod = importlib.util.module_from_spec(spec)

    # ✅ Critical for Python 3.14 dataclasses: module must exist in sys.modules during execution
    sys.modules[module_name] = mod

    spec.loader.exec_module(mod)
    return mod


@dataclass(frozen=True)
class CarsDmNames:
    # Lambdas
    get_incremental_tables_fn: str
    sns_publish_errors_fn: str
    etl_workflow_update_job_fn: str

    # Glue Jobs
    landing_files_glue_job: str
    raw_dm_glue_job: str

    # Step Function
    carsdm_state_machine: str


def build_names(deploy_env: str, project: str) -> CarsDmNames:
    """
    Prefix is: FSA-<deployEnv>-<project>

    Example:
      deployEnv="steam-dev", project="carsdm" -> FSA-steam-dev-carsdm-...
    """
    dep = (deploy_env or "").strip()
    proj = (project or "").strip()
    if not dep:
        raise RuntimeError("Missing required cfg['deployEnv']")
    if not proj:
        raise RuntimeError("Missing required cfg['project']")

    prefix = f"FSA-{dep}-{proj}"

    return CarsDmNames(
        get_incremental_tables_fn=f"{prefix}-get-incremental-tables",
        sns_publish_errors_fn=f"{prefix}-RAW-DM-sns-publish-step-function-errors",
        etl_workflow_update_job_fn=f"{prefix}-RAW-DM-etl-workflow-update-data-ppln-job",
        landing_files_glue_job=f"{prefix}-LandingFiles",
        raw_dm_glue_job=f"{prefix}-Cars-Raw-DM",
        carsdm_state_machine=f"{prefix}-Cars-S3Landing-to-S3Final-Raw-DM",
    )


def _layers(cfg: Dict[str, Any]) -> List[str]:
    """
    Optional two-layer behavior (existing pattern).
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

    # Extra files / py files
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

    return {str(k): str(v) for k, v in out.items()}


def _strip_known_script_prefixes(filename: str) -> str:
    """
    Remove environment/project-ish prefixes from existing script filenames.

    Examples:
      FSA-CERT-CARS-LandingFiles.py  -> LandingFiles.py
      FSA-CERT-Cars-Raw-DM.py        -> Raw-DM.py
      Cars-Raw-DM.py                 -> Raw-DM.py
      CARS-LandingFiles.py           -> LandingFiles.py

    Rule:
      - strip leading "FSA-<something>-" if present (first 2 segments)
      - then strip leading "CARS-" or "Cars-"
    """
    name = Path(filename).name

    # If it starts with FSA-XXX-..., drop the first two segments "FSA-XXX-"
    # (We specifically want to remove any old hardcoded env markers like CERT/PROD/etc.)
    if name.startswith("FSA-"):
        parts = name.split("-", 2)  # ["FSA", "<ENV>", "<rest>"]
        if len(parts) == 3:
            name = parts[2]

    # Now remove "CARS-" or "Cars-" prefix if present
    for pfx in ("CARS-", "Cars-"):
        if name.startswith(pfx):
            name = name[len(pfx):]
            break

    return name


def _s3_script_key(prefix: str, deploy_env: str, project: str, local_filename: str) -> str:
    """
    Upload Glue scripts to S3 with a name that ALWAYS matches:

      FSA-<deploy_env>-<project>-<something>.py

    Where <something> is derived from the original file name AFTER stripping old
    prefixes like FSA-CERT-CARS or Cars.

    Examples:
      local="FSA-CERT-CARS-LandingFiles.py" -> FSA-steam-dev-carsdm-LandingFiles.py
      local="FSA-CERT-Cars-Raw-DM.py"       -> FSA-steam-dev-carsdm-Raw-DM.py
    """
    dep = (deploy_env or "").strip()
    proj = (project or "").strip()
    if not dep or not proj:
        raise RuntimeError("deploy_env and project are required for Glue script naming")

    suffix = _strip_known_script_prefixes(local_filename)
    final_name = f"FSA-{dep}-{proj}-{suffix}"
    return f"{prefix}glue/{final_name}"


def _resolve_target_bucket(cfg: Dict[str, Any]) -> str:
    """
    Deterministic rule (no invented keys):

      - If cfg.carsdm.target_bucket exists, use it.
      - Else, use cfg.artifacts.artifactBucket.

    This guarantees NO MORE missing-bucket errors as long as artifacts.artifactBucket exists.
    """
    carsdm_cfg = cfg.get("carsdm") or {}
    override = carsdm_cfg.get("target_bucket")
    if isinstance(override, str) and override.strip():
        return override.strip()

    artifacts = cfg.get("artifacts") or {}
    bucket = artifacts.get("artifactBucket")
    if isinstance(bucket, str) and bucket.strip():
        return bucket.strip()

    raise RuntimeError("Missing required cfg.artifacts.artifactBucket")


def deploy(cfg: Dict[str, Any], region: str) -> Dict[str, str]:
    """
    Deploy CARS DM:
      - 3 lambdas
      - 2 glue jobs
      - 1 step function
    """
    deploy_env = cfg["deployEnv"]
    project = cfg["project"]
    names = build_names(deploy_env, project)

    artifact_bucket = cfg["artifacts"]["artifactBucket"]
    prefix = cfg["artifacts"]["prefix"].rstrip("/") + "/"

    strparams = cfg.get("strparams") or {}
    etl_lambda_role_arn = strparams["etlRoleArnParam"]
    glue_job_role_arn = strparams["glueJobRoleArnParam"]

    sfn_role_arn = (cfg.get("stepFunctions") or {}).get("roleArn") or ""
    if not sfn_role_arn:
        raise RuntimeError("Missing required cfg.stepFunctions.roleArn")

    # Project assets
    project_dir = Path(__file__).resolve().parent  # .../deploy/projects/carsdm
    lambda_root = project_dir / "lambda"
    glue_root = project_dir / "glue"

    # Lambda folders
    get_incremental_dir = lambda_root / "get-incremental-tables"
    sns_publish_dir = lambda_root / "sns-publish-step-function-errors"
    etl_update_dir = lambda_root / "etl-workflow-update-data-ppln-job"

    # Glue scripts (local) - repo filenames
    landing_glue_script_local = glue_root / "FSA-CERT-CARS-LandingFiles.py"
    raw_dm_glue_script_local = glue_root / "FSA-CERT-Cars-Raw-DM.py"

    if not landing_glue_script_local.exists():
        raise FileNotFoundError(f"Missing Glue script: {landing_glue_script_local}")
    if not raw_dm_glue_script_local.exists():
        raise FileNotFoundError(f"Missing Glue script: {raw_dm_glue_script_local}")

    # Clients
    session = boto3.Session(region_name=region)
    s3 = session.client("s3")
    lam = session.client("lambda")
    glue = session.client("glue")
    sfn = session.client("stepfunctions")

    ensure_bucket_exists(s3, artifact_bucket, region)

    # Optional env vars passed to all lambdas
    env_vars: Dict[str, str] = {}
    env_vars.update(cfg.get("lambdaEnv") or {})

    runtime = strparams.get("lambdaRuntime", "python3.11")
    layers = _layers(cfg)

    # Handlers
    handler = "lambda_function.handler"

    # --- Lambdas ---
    get_incremental_arn = ensure_lambda(
        lam,
        LambdaSpec(
            name=names.get_incremental_tables_fn,
            role_arn=etl_lambda_role_arn,
            handler=handler,
            runtime=runtime,
            source_dir=str(get_incremental_dir),
            env=env_vars,
            layers=layers,
        ),
    )

    sns_publish_arn = ensure_lambda(
        lam,
        LambdaSpec(
            name=names.sns_publish_errors_fn,
            role_arn=etl_lambda_role_arn,
            handler=handler,
            runtime=runtime,
            source_dir=str(sns_publish_dir),
            env=env_vars,
            layers=layers,
        ),
    )

    etl_update_arn = ensure_lambda(
        lam,
        LambdaSpec(
            name=names.etl_workflow_update_job_fn,
            role_arn=etl_lambda_role_arn,
            handler=handler,
            runtime=runtime,
            source_dir=str(etl_update_dir),
            env=env_vars,
            layers=layers,
        ),
    )

    # --- Glue jobs ---
    # NOTE: If you have cfg["GlueJobParameters"] it's used; otherwise empty dicts (no guessing).
    glue_job_params_root = cfg.get("GlueJobParameters")
    has_glue_job_parameters = isinstance(glue_job_params_root, dict) and bool(glue_job_params_root)

    landing_params: Dict[str, Any] = {}
    rawdm_params: Dict[str, Any] = {}
    if has_glue_job_parameters:
        lp = glue_job_params_root.get("landingFiles") or {}
        rp = glue_job_params_root.get("rawDm") or {}
        landing_params = lp if isinstance(lp, dict) else {}
        rawdm_params = rp if isinstance(rp, dict) else {}

    landing_default_args = _merge_glue_default_args(
        base_args=(cfg.get("glueDefaultArgs") or {}),
        glue_job_params=landing_params,
    )
    rawdm_default_args = _merge_glue_default_args(
        base_args=(cfg.get("glueDefaultArgs") or {}),
        glue_job_params=rawdm_params,
    )

    landing_script_s3_key = _s3_script_key(prefix, deploy_env, project, landing_glue_script_local.name)
    rawdm_script_s3_key = _s3_script_key(prefix, deploy_env, project, raw_dm_glue_script_local.name)

    ensure_glue_job(
        glue,
        s3,
        GlueJobSpec(
            name=names.landing_files_glue_job,
            role_arn=glue_job_role_arn,
            script_local_path=str(landing_glue_script_local),
            script_s3_bucket=artifact_bucket,
            script_s3_key=landing_script_s3_key,
            default_args=landing_default_args,
            glue_version=str(landing_params.get("GlueVersion") or "4.0"),
            worker_type=str(landing_params.get("WorkerType") or "G.1X"),
            number_of_workers=_as_int(landing_params.get("NumberOfWorkers"), default=2),
            timeout_minutes=_as_int(landing_params.get("TimeoutMinutes"), default=60),
            max_retries=_as_int(landing_params.get("MaxRetries"), default=0),
            max_concurrency=_as_int(landing_params.get("MaxConcurrency"), default=1),
            connection_names=_parse_connection_names(landing_params),
        ),
    )

    ensure_glue_job(
        glue,
        s3,
        GlueJobSpec(
            name=names.raw_dm_glue_job,
            role_arn=glue_job_role_arn,
            script_local_path=str(raw_dm_glue_script_local),
            script_s3_bucket=artifact_bucket,
            script_s3_key=rawdm_script_s3_key,
            default_args=rawdm_default_args,
            glue_version=str(rawdm_params.get("GlueVersion") or "4.0"),
            worker_type=str(rawdm_params.get("WorkerType") or "G.1X"),
            number_of_workers=_as_int(rawdm_params.get("NumberOfWorkers"), default=2),
            timeout_minutes=_as_int(rawdm_params.get("TimeoutMinutes"), default=60),
            max_retries=_as_int(rawdm_params.get("MaxRetries"), default=0),
            max_concurrency=_as_int(rawdm_params.get("MaxConcurrency"), default=1),
            connection_names=_parse_connection_names(rawdm_params),
        ),
    )

    # --- Step Function ---
    step_mod = _load_stepfunction_module()
    CarsDmStateMachineInputs = step_mod.CarsDmStateMachineInputs
    CarsDmStateMachineBuilder = step_mod.CarsDmStateMachineBuilder

    target_bucket = _resolve_target_bucket(cfg)

    carsdm_cfg = cfg.get("carsdm") or {}
    env_lower = str(carsdm_cfg.get("env") or deploy_env).strip().lower()

    sm_inputs = CarsDmStateMachineInputs(
        get_incremental_tables_fn_arn=get_incremental_arn,
        sns_publish_errors_fn_arn=sns_publish_arn,
        etl_workflow_update_job_fn_arn=etl_update_arn,
        raw_dm_glue_job_name=names.raw_dm_glue_job,
        env=env_lower,
        postgres_prcs_ctrl_dbname=str(carsdm_cfg.get("postgres_prcs_ctrl_dbname") or "metadata_edw"),
        region_name=str(carsdm_cfg.get("region_name") or region),
        secret_name=str(carsdm_cfg.get("secret_name") or f"FSA-{deploy_env}-{project}-secrets"),
        target_bucket=target_bucket,
    )

    definition = CarsDmStateMachineBuilder.carsdm_asl(sm_inputs)

    sfn_arn = ensure_state_machine(
        sfn,
        StateMachineSpec(
            name=names.carsdm_state_machine,
            role_arn=sfn_role_arn,
            definition=definition,
        ),
    )

    return {
        "lambda_get_incremental_tables_arn": get_incremental_arn,
        "lambda_sns_publish_errors_arn": sns_publish_arn,
        "lambda_etl_workflow_update_job_arn": etl_update_arn,
        "glue_job_landing_files_name": names.landing_files_glue_job,
        "glue_job_raw_dm_name": names.raw_dm_glue_job,
        "glue_landing_script_s3_key": landing_script_s3_key,
        "glue_rawdm_script_s3_key": rawdm_script_s3_key,
        "target_bucket_used": target_bucket,
        "state_machine_arn": sfn_arn,
    }
