# deploy/projects/cnsv/deploy_base.py
from __future__ import annotations

import json
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
# Safe parsers (match working deploy.py pattern)
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


def _as_dict(v: Any) -> Dict[str, Any]:
    return v if isinstance(v, dict) else {}


def _as_str_dict(v: Any) -> Dict[str, str]:
    if not isinstance(v, dict):
        return {}
    out: Dict[str, str] = {}
    for k, val in v.items():
        if k is None:
            continue
        ks = str(k).strip()
        if not ks:
            continue
        out[ks] = "" if val is None else str(val)
    return out


def _as_str_list(v: Any) -> List[str]:
    if not isinstance(v, list):
        return []
    out: List[str] = []
    for item in v:
        if isinstance(item, str) and item.strip():
            out.append(item.strip())
    return out


# --------------------------------------------------------------------------------------
# GlueConfig helpers (match working deploy.py pattern)
# --------------------------------------------------------------------------------------

def _script_stem(p: Path) -> str:
    return p.name[:-3] if p.name.lower().endswith(".py") else p.name


def _parse_glue_config_array(cfg: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    root = cfg.get("GlueConfig")
    if not isinstance(root, list) or not root:
        return {}

    out: Dict[str, Dict[str, Any]] = {}
    for item in root:
        if not isinstance(item, dict) or not item:
            continue
        for k, v in item.items():
            if isinstance(k, str) and k.strip() and isinstance(v, dict):
                out[k.strip()] = v
    return out


def _glue_config_for_script(cfg: Dict[str, Any], script_stem_name: str) -> Dict[str, Any]:
    m = _parse_glue_config_array(cfg)
    return m.get(script_stem_name, {}) if m else {}


def _parse_connection_names(glue_job_params: Dict[str, Any]) -> List[str]:
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

    # de-dupe while preserving order
    seen = set()
    deduped: List[str] = []
    for n in out:
        if n in seen:
            continue
        seen.add(n)
        deduped.append(n)
    return deduped


def _merge_glue_default_args(base_args: Dict[str, Any], glue_job_params: Dict[str, Any]) -> Dict[str, str]:
    """
    Keep behavior consistent with your working deploy.py.
    """
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

    ref_files = glue_job_params.get("ReferencedFilesS3Path")
    if ref_files:
        out["--extra-files"] = str(ref_files)

    extra_py = glue_job_params.get("AdditionalPythonModulesS3Path")
    if extra_py:
        out["--extra-py-files"] = str(extra_py)

    python_lib_path = _as_str(glue_job_params.get("PythonLibraryPath"), default="")
    if python_lib_path and "--extra-py-files" not in out:
        out["--extra-py-files"] = python_lib_path

    job_params = glue_job_params.get("JobParameters") or {}
    if isinstance(job_params, dict):
        for k, v in job_params.items():
            out[str(k)] = "" if v is None else str(v)

    return {str(k): str(v) for k, v in out.items()}


# --------------------------------------------------------------------------------------
# Script naming (match working deploy.py pattern)
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
# ASL loader (PARAM ASL ONLY for this deployer)
# --------------------------------------------------------------------------------------

_PARAM_ASL_FILES = [
    "Incremental-to-S3Landing.param.asl.json",
    "S3Landing-to-S3Final-Raw-DM.param.asl.json",
    "Process-Control-Update.param.asl.json",
    "Main.param.asl.json",
]


def _load_asl_file(project_dir: Path, filename: str) -> Dict[str, Any]:
    """
    For this deployer we ONLY load the parameterized ASL files: *.param.asl.json.
    (Your previous regen created these; do not look for non-param files.)
    """
    p = project_dir / "states" / filename
    if not p.exists():
        raise FileNotFoundError(f"Missing ASL file: {p}")
    obj = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(obj, dict) or not obj:
        raise RuntimeError(f"ASL file did not parse to a JSON object: {p}")
    return obj


# --------------------------------------------------------------------------------------
# Naming
# --------------------------------------------------------------------------------------

@dataclass(frozen=True)
class Names:
    prefix: str

    # Glue jobs (base only; NOT the dataset-specific jobs)
    landing_glue_job: str
    raw_dm_glue_job: str

    # State machines (base only)
    sm_incremental_to_s3landing: str
    sm_s3landing_to_s3final_raw_dm: str
    sm_process_control_update: str
    sm_main: str

    # Lambdas (base only)
    fn_get_incremental_tables: str
    fn_raw_dm_etl_workflow_update: str
    fn_raw_dm_sns_publish_errors: str
    fn_job_logging_end: str
    fn_validation_check: str
    fn_sns_publish_validations_report: str


def build_names(deploy_env: str, project: str) -> Names:
    dep = (deploy_env or "").strip()
    proj = (project or "").strip()
    if not dep:
        raise RuntimeError("Missing required cfg['deployEnv']")
    if not proj:
        raise RuntimeError("Missing required cfg['project']")

    prefix = f"FSA-{dep}-{proj}"

    # IMPORTANT: keep names short enough for Lambda (<=64 chars).
    # We only deploy BASE lambdas in this deployer, so this stays safe.
    return Names(
        prefix=prefix,
        landing_glue_job=f"{prefix}-LandingFiles",
        raw_dm_glue_job=f"{prefix}-Raw-DM",
        sm_incremental_to_s3landing=f"{prefix}-Incremental-to-S3Landing",
        sm_s3landing_to_s3final_raw_dm=f"{prefix}-S3Landing-to-S3Final-Raw-DM",
        sm_process_control_update=f"{prefix}-Process-Control-Update",
        sm_main=f"{prefix}-Main",
        fn_get_incremental_tables=f"{prefix}-get-incremental-tables",
        fn_raw_dm_etl_workflow_update=f"{prefix}-RAW-DM-etl-update-data-ppln-job",
        fn_raw_dm_sns_publish_errors=f"{prefix}-RAW-DM-sns-step-function-errors",
        fn_job_logging_end=f"{prefix}-Job-Logging-End",
        fn_validation_check=f"{prefix}-validation-check",
        fn_sns_publish_validations_report=f"{prefix}-sns-validations-report",
    )


# --------------------------------------------------------------------------------------
# Lambda directory resolution
# --------------------------------------------------------------------------------------

def _find_lambda_dir(lambda_root: Path, expected_suffix: str) -> Path:
    """
    Resolve a lambda source dir under ./lambda.

    We prefer:
      - exact dir name match
      - else case-insensitive match
      - else match by suffix (useful if dirs are named without the FSA-<env>-<proj> prefix)
    """
    if not lambda_root.exists():
        raise FileNotFoundError(f"Missing lambda root dir: {lambda_root}")

    dirs = [p for p in lambda_root.iterdir() if p.is_dir()]
    if not dirs:
        raise FileNotFoundError(f"No lambda directories found under: {lambda_root}")

    # exact match
    for d in dirs:
        if d.name == expected_suffix:
            return d

    # case-insensitive exact
    low = expected_suffix.lower()
    for d in dirs:
        if d.name.lower() == low:
            return d

    # suffix match (normalized)
    def norm(s: str) -> str:
        return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")

    target = norm(expected_suffix)
    for d in dirs:
        if norm(d.name) == target:
            return d

    # last resort: endswith token match
    for d in dirs:
        if d.name.lower().endswith(low):
            return d

    raise FileNotFoundError(
        f"Could not find lambda directory for '{expected_suffix}' under {lambda_root}. "
        f"Found: {[d.name for d in dirs]}"
    )


# --------------------------------------------------------------------------------------
# Deploy
# --------------------------------------------------------------------------------------

def deploy(cfg: Dict[str, Any], region: str) -> Dict[str, str]:
    """
    CNSV Base deployer (DEPLOYER #1):

    Deploy ONLY the resources used by the 4 base parameterized state machines:
      - Incremental-to-S3Landing.param.asl.json
      - S3Landing-to-S3Final-Raw-DM.param.asl.json
      - Process-Control-Update.param.asl.json
      - Main.param.asl.json

    And ONLY the base Glue + Lambda functions they reference.

    Absolutely no hardcoded env/project strings. All names are derived from:
      - cfg.deployEnv
      - cfg.project
    """
    deploy_env = cfg["deployEnv"]
    project = cfg["project"]
    names = build_names(deploy_env, project)

    # Artifacts (same as working deploy.py)
    artifacts = cfg.get("artifacts") or {}
    artifact_bucket = _as_str(artifacts.get("artifactBucket"))
    prefix = _as_str(artifacts.get("prefix")).rstrip("/") + "/"
    if not artifact_bucket:
        raise RuntimeError("Missing required cfg.artifacts.artifactBucket")
    if prefix == "/":
        prefix = ""

    # strparams required values (no hardcoding)
    strparams = cfg.get("strparams") or {}
    landing_bucket = _as_str(strparams.get("landingBucketNameParam"))
    clean_bucket = _as_str(strparams.get("cleanBucketNameParam"))
    final_bucket = _as_str(strparams.get("finalBucketNameParam"))
    sns_topic_arn = _as_str(strparams.get("snsArnParam"))

    # IMPORTANT: role selection consistent with what you've been using:
    # Prefer etlRoleArnParam; fall back to lambdaRoleArnParam if present.
    etl_role_arn = _as_str(strparams.get("etlRoleArnParam")) or _as_str(strparams.get("lambdaRoleArnParam"))
    glue_job_role_arn = _as_str(strparams.get("glueJobRoleArnParam"))

    # Optional folders
    landing_folder = _as_str(strparams.get("landingFolderNameParam"))
    stg_folder = _as_str(strparams.get("stgFolderNameParam"))
    ods_folder = _as_str(strparams.get("odsFolderNameParam"))

    # Main SM deploy-time substitution values
    job_id_bucket = _as_str(strparams.get("jobIdBucketParam") or strparams.get("landingBucketNameParam"))
    job_id_key = _as_str(strparams.get("jobIdKeyParam"))
    if not job_id_key:
        # Derive from LandingFiles.DestinationPrefix — the Glue job writes to {DestinationPrefix}/job_id.json
        _landing_dest_prefix = _as_str(
            (_glue_config_for_script(cfg, "LandingFiles").get("JobParameters") or {}).get("--DestinationPrefix")
        )
        if _landing_dest_prefix:
            job_id_key = f"{_landing_dest_prefix.rstrip('/')}/job_id.json"
    final_zone_crawler_name = _as_str(strparams.get("finalZoneCrawlerNameParam"))
    cdc_crawler_name = _as_str(strparams.get("cdcCrawlerNameParam"))
    postgres_prcs_ctrl_dbname = _as_str(
        (cfg.get("configData") or {}).get("databaseName") or strparams.get("postgresDbNameParam")
    )
    secret_name = _as_str(cfg.get("SecretId") or cfg.get("secretId") or strparams.get("secretNameParam"))
    target_bucket = _as_str(strparams.get("targetBucketParam") or strparams.get("finalBucketNameParam"))

    missing: List[str] = []
    if not landing_bucket:
        missing.append("strparams.landingBucketNameParam")
    if not clean_bucket:
        missing.append("strparams.cleanBucketNameParam")
    if not final_bucket:
        missing.append("strparams.finalBucketNameParam")
    if not sns_topic_arn:
        missing.append("strparams.snsArnParam")
    if not etl_role_arn:
        missing.append("strparams.etlRoleArnParam (preferred) or strparams.lambdaRoleArnParam")
    if not glue_job_role_arn:
        missing.append("strparams.glueJobRoleArnParam")
    if missing:
        raise RuntimeError("Missing required config keys: " + ", ".join(missing))

    # Step function role
    sfn_role_arn = _as_str((cfg.get("stepFunctions") or {}).get("roleArn"))
    if not sfn_role_arn:
        raise RuntimeError("Missing required cfg.stepFunctions.roleArn")

    # Local paths
    project_dir = Path(__file__).resolve().parent
    glue_root = project_dir / "glue"
    lambda_root = project_dir / "lambda"
    states_root = project_dir / "states"

    # --- ASL (param) ---
    raw_asls: Dict[str, Dict[str, Any]] = {}
    for fn in _PARAM_ASL_FILES:
        raw_asls[fn] = _load_asl_file(project_dir, fn)

    # --- Glue scripts (BASE ONLY) ---
    landing_script_local = glue_root / "LandingFiles.py"
    raw_dm_script_local = glue_root / "Raw-DM.py"

    if not landing_script_local.exists():
        raise FileNotFoundError(f"Missing Glue script: {landing_script_local}")
    if not raw_dm_script_local.exists():
        raise FileNotFoundError(f"Missing Glue script: {raw_dm_script_local}")

    landing_params = _glue_config_for_script(cfg, _script_stem(landing_script_local))
    raw_dm_params = _glue_config_for_script(cfg, _script_stem(raw_dm_script_local))

    def _defaults(p: Dict[str, Any], default_worker: str) -> Dict[str, Any]:
        return {
            "GlueVersion": _as_str(p.get("GlueVersion"), "4.0"),
            "WorkerType": _as_str(p.get("WorkerType"), default_worker),
            "NumberOfWorkers": _as_int(p.get("NumberOfWorkers"), 2),
            "TimeoutMinutes": _as_int(p.get("TimeoutMinutes"), 60),
            "MaxRetries": _as_int(p.get("MaxRetries"), 0),
            "MaxConcurrency": _as_int(p.get("MaxConcurrency"), 1),
        }

    landing_d = _defaults(landing_params, default_worker="G.1X")
    raw_dm_d = _defaults(raw_dm_params, default_worker="G.1X")

    glue_default_base = cfg.get("glueDefaultArgs") or {}
    landing_default_args = _merge_glue_default_args(glue_default_base, landing_params)
    raw_dm_default_args = _merge_glue_default_args(glue_default_base, raw_dm_params)

    def _set_if_missing(args: Dict[str, str], k: str, v: str) -> None:
        if k not in args and v:
            args[k] = v

    for args in (landing_default_args, raw_dm_default_args):
        _set_if_missing(args, "--env", str(deploy_env))
        _set_if_missing(args, "--landing_bucket", landing_bucket)
        _set_if_missing(args, "--clean_bucket", clean_bucket)
        _set_if_missing(args, "--final_bucket", final_bucket)
        _set_if_missing(args, "--sns_topic_arn", sns_topic_arn)
        _set_if_missing(args, "--landing_folder", landing_folder)
        _set_if_missing(args, "--stg_folder", stg_folder)
        _set_if_missing(args, "--ods_folder", ods_folder)

    landing_conns = _parse_connection_names(landing_params)
    raw_dm_conns = _parse_connection_names(raw_dm_params)

    # --- Lambda config (BASE ONLY) ---
    lambdas_cfg = _as_dict(cfg.get("lambdas") or cfg.get("lambda") or {})
    shared_layers = _as_str_list(lambdas_cfg.get("layers"))
    shared_env = _as_str_dict(lambdas_cfg.get("environment"))
    shared_runtime = _as_str(lambdas_cfg.get("runtime"), "python3.11")
    shared_timeout = _as_int(lambdas_cfg.get("timeoutSeconds"), 30)
    shared_memory = _as_int(lambdas_cfg.get("memoryMb"), 256)

    vpc_cfg = _as_dict(lambdas_cfg.get("vpcConfig"))
    subnet_ids = _as_str_list(vpc_cfg.get("subnetIds"))
    security_group_ids = _as_str_list(vpc_cfg.get("securityGroupIds"))

    # --- Clients ---
    session = boto3.Session(region_name=region)
    s3 = session.client("s3")
    glue = session.client("glue")
    sfn = session.client("stepfunctions")
    lam = session.client("lambda")

    ensure_bucket_exists(s3, artifact_bucket, region)

    # Script S3 keys (renamed with FSA-<env>-<proj>- prefix)
    landing_script_s3_key = _s3_script_key(prefix, deploy_env, project, landing_script_local.name)
    raw_dm_script_s3_key = _s3_script_key(prefix, deploy_env, project, raw_dm_script_local.name)

    # --- Glue jobs (BASE ONLY) ---
    ensure_glue_job(
        glue,
        s3,
        GlueJobSpec(
            name=names.landing_glue_job,
            role_arn=glue_job_role_arn,
            script_local_path=str(landing_script_local),
            script_s3_bucket=artifact_bucket,
            script_s3_key=landing_script_s3_key,
            default_args=landing_default_args,
            glue_version=str(landing_d["GlueVersion"]),
            worker_type=str(landing_d["WorkerType"]),
            number_of_workers=int(landing_d["NumberOfWorkers"]),
            timeout_minutes=int(landing_d["TimeoutMinutes"]),
            max_retries=int(landing_d["MaxRetries"]),
            max_concurrency=int(landing_d["MaxConcurrency"]),
            connection_names=landing_conns,
        ),
    )

    ensure_glue_job(
        glue,
        s3,
        GlueJobSpec(
            name=names.raw_dm_glue_job,
            role_arn=glue_job_role_arn,
            script_local_path=str(raw_dm_script_local),
            script_s3_bucket=artifact_bucket,
            script_s3_key=raw_dm_script_s3_key,
            default_args=raw_dm_default_args,
            glue_version=str(raw_dm_d["GlueVersion"]),
            worker_type=str(raw_dm_d["WorkerType"]),
            number_of_workers=int(raw_dm_d["NumberOfWorkers"]),
            timeout_minutes=int(raw_dm_d["TimeoutMinutes"]),
            max_retries=int(raw_dm_d["MaxRetries"]),
            max_concurrency=int(raw_dm_d["MaxConcurrency"]),
            connection_names=raw_dm_conns,
        ),
    )

    # --- Lambdas (BASE ONLY) ---
    required_lambdas: List[Tuple[str, str]] = [
        (names.fn_get_incremental_tables, "get-incremental-tables"),
        (names.fn_raw_dm_etl_workflow_update, "RAW-DM-etl-update-data-ppln-job"),
        (names.fn_raw_dm_sns_publish_errors, "RAW-DM-sns-step-function-errors"),
        (names.fn_job_logging_end, "Job-Logging-End"),
        (names.fn_validation_check, "validation-check"),
        (names.fn_sns_publish_validations_report, "sns-validations-report"),
    ]

    lambda_arns: Dict[str, str] = {}

    for fn_name, suffix in required_lambdas:
        src_dir = _find_lambda_dir(lambda_root, suffix)
        handler_file = src_dir / "lambda_function.py"
        if not handler_file.exists():
            raise FileNotFoundError(f"Lambda dir {src_dir} missing lambda_function.py")

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
            subnet_ids=subnet_ids if subnet_ids else None,
            security_group_ids=security_group_ids if security_group_ids else None,
        )
        arn = ensure_lambda(lam, spec)
        lambda_arns[fn_name] = arn

    # --- State machines (BASE ONLY) ---
    # IMPORTANT: names come from cfg-derived prefix ONLY.

    # Shared substitutions reused across all child SMs
    _shared_subs = {
        "__ENV__":                                    str(deploy_env),
        "__LANDING_GLUE_JOB_NAME__":                 names.landing_glue_job,
        "__JOB_ID_BUCKET__":                         job_id_bucket,
        "__JOB_ID_KEY__":                            job_id_key,
        "__RAW_DM_GLUE_JOB_NAME__":                  names.raw_dm_glue_job,
        "__GET_INCREMENTAL_TABLES_FN_ARN__":         lambda_arns.get(names.fn_get_incremental_tables, ""),
        "__RAW_DM_ETL_WORKFLOW_UPDATE_FN_ARN__":     lambda_arns.get(names.fn_raw_dm_etl_workflow_update, ""),
        "__RAW_DM_SNS_PUBLISH_ERRORS_FN_ARN__":      lambda_arns.get(names.fn_raw_dm_sns_publish_errors, ""),
        "__JOB_LOGGING_END_FN_ARN__":               lambda_arns.get(names.fn_job_logging_end, ""),
        "__VALIDATION_CHECK_FN_ARN__":              lambda_arns.get(names.fn_validation_check, ""),
        "__FINAL_ZONE_CRAWLER_NAME__":               final_zone_crawler_name,
        "__CDC_CRAWLER_NAME__":                      cdc_crawler_name,
        "__POSTGRES_PRCS_CTRL_DBNAME__":             postgres_prcs_ctrl_dbname,
        "__REGION_NAME__":                           region,
        "__SECRET_NAME__":                           secret_name,
        "__TARGET_BUCKET__":                         target_bucket,
    }

    def _substitute_asl(key: str) -> Dict[str, Any]:
        text = json.dumps(raw_asls[key])
        for placeholder, value in _shared_subs.items():
            text = text.replace(placeholder, value)
        return json.loads(text)

    sm_incremental_arn = ensure_state_machine(
        sfn,
        StateMachineSpec(
            name=names.sm_incremental_to_s3landing,
            role_arn=sfn_role_arn,
            definition=_substitute_asl("Incremental-to-S3Landing.param.asl.json"),
        ),
    )
    sm_s3landing_arn = ensure_state_machine(
        sfn,
        StateMachineSpec(
            name=names.sm_s3landing_to_s3final_raw_dm,
            role_arn=sfn_role_arn,
            definition=_substitute_asl("S3Landing-to-S3Final-Raw-DM.param.asl.json"),
        ),
    )
    sm_pc_arn = ensure_state_machine(
        sfn,
        StateMachineSpec(
            name=names.sm_process_control_update,
            role_arn=sfn_role_arn,
            definition=_substitute_asl("Process-Control-Update.param.asl.json"),
        ),
    )
    # Build Main SM definition with deploy-time substitutions
    _main_asl_text = json.dumps(raw_asls["Main.param.asl.json"])
    _main_substitutions = {
        "__ENV__":                                    str(deploy_env),
        "__INCREMENTAL_TO_S3LANDING_SM_ARN__":        sm_incremental_arn,
        "__LANDING_GLUE_JOB_NAME__":                 names.landing_glue_job,
        "__JOB_ID_BUCKET__":                         job_id_bucket,
        "__JOB_ID_KEY__":                            job_id_key,
        "__S3LANDING_TO_S3FINAL_RAW_DM_SM_ARN__":    sm_s3landing_arn,
        "__RAW_DM_GLUE_JOB_NAME__":                  names.raw_dm_glue_job,
        "__GET_INCREMENTAL_TABLES_FN_ARN__":         lambda_arns.get(names.fn_get_incremental_tables, ""),
        "__RAW_DM_ETL_WORKFLOW_UPDATE_FN_ARN__":     lambda_arns.get(names.fn_raw_dm_etl_workflow_update, ""),
        "__RAW_DM_SNS_PUBLISH_ERRORS_FN_ARN__":      lambda_arns.get(names.fn_raw_dm_sns_publish_errors, ""),
        "__FINAL_ZONE_CRAWLER_NAME__":               final_zone_crawler_name,
        "__CDC_CRAWLER_NAME__":                      cdc_crawler_name,
        "__POSTGRES_PRCS_CTRL_DBNAME__":             postgres_prcs_ctrl_dbname,
        "__REGION_NAME__":                           region,
        "__SECRET_NAME__":                           secret_name,
        "__TARGET_BUCKET__":                         target_bucket,
        "__PROCESS_CONTROL_UPDATE_SM_ARN__":         sm_pc_arn,
        "__JOB_LOGGING_END_FN_ARN__":               lambda_arns.get(names.fn_job_logging_end, ""),
        "__VALIDATION_CHECK_FN_ARN__":              lambda_arns.get(names.fn_validation_check, ""),
        "__SNS_PUBLISH_VALIDATIONS_REPORT_FN_ARN__": lambda_arns.get(names.fn_sns_publish_validations_report, ""),
    }
    for _placeholder, _value in _main_substitutions.items():
        _main_asl_text = _main_asl_text.replace(_placeholder, _value)
    _main_definition = json.loads(_main_asl_text)

    sm_main_arn = ensure_state_machine(
        sfn,
        StateMachineSpec(
            name=names.sm_main,
            role_arn=sfn_role_arn,
            definition=_main_definition,
        ),
    )

    # --- Summary (human-usable, like your working deploy.py) ---
    return {
        "deploy_env": str(deploy_env),
        "project": str(project),
        "artifact_bucket": artifact_bucket,
        "artifact_prefix": prefix,
        "landing_bucket_from_config": landing_bucket,
        "clean_bucket_from_config": clean_bucket,
        "final_bucket_from_config": final_bucket,
        "sns_topic_arn_from_config": sns_topic_arn,
        "etl_role_arn_used_for_lambdas": etl_role_arn,
        "glue_job_role_arn_used": glue_job_role_arn,
        "glue_job_landing_name": names.landing_glue_job,
        "glue_job_raw_dm_name": names.raw_dm_glue_job,
        "glue_landing_script_s3_key": landing_script_s3_key,
        "glue_raw_dm_script_s3_key": raw_dm_script_s3_key,
        "glue_config_used_for_landing": "yes" if bool(landing_params) else "no",
        "glue_config_used_for_raw_dm": "yes" if bool(raw_dm_params) else "no",
        "lambda_get_incremental_tables_name": names.fn_get_incremental_tables,
        "lambda_get_incremental_tables_arn": lambda_arns.get(names.fn_get_incremental_tables, ""),
        "lambda_raw_dm_etl_workflow_update_name": names.fn_raw_dm_etl_workflow_update,
        "lambda_raw_dm_etl_workflow_update_arn": lambda_arns.get(names.fn_raw_dm_etl_workflow_update, ""),
        "lambda_raw_dm_sns_publish_errors_name": names.fn_raw_dm_sns_publish_errors,
        "lambda_raw_dm_sns_publish_errors_arn": lambda_arns.get(names.fn_raw_dm_sns_publish_errors, ""),
        "lambda_job_logging_end_name": names.fn_job_logging_end,
        "lambda_job_logging_end_arn": lambda_arns.get(names.fn_job_logging_end, ""),
        "lambda_validation_check_name": names.fn_validation_check,
        "lambda_validation_check_arn": lambda_arns.get(names.fn_validation_check, ""),
        "lambda_sns_publish_validations_report_name": names.fn_sns_publish_validations_report,
        "lambda_sns_publish_validations_report_arn": lambda_arns.get(names.fn_sns_publish_validations_report, ""),
        "state_machine_incremental_to_s3landing_name": names.sm_incremental_to_s3landing,
        "state_machine_incremental_to_s3landing_arn": sm_incremental_arn,
        "state_machine_s3landing_to_s3final_raw_dm_name": names.sm_s3landing_to_s3final_raw_dm,
        "state_machine_s3landing_to_s3final_raw_dm_arn": sm_s3landing_arn,
        "state_machine_process_control_update_name": names.sm_process_control_update,
        "state_machine_process_control_update_arn": sm_pc_arn,
        "state_machine_main_name": names.sm_main,
        "state_machine_main_arn": sm_main_arn,
    }
