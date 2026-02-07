# deploy/projects/cnsv/deploy_pymts.py
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

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

    seen = set()
    deduped: List[str] = []
    for n in out:
        if n in seen:
            continue
        seen.add(n)
        deduped.append(n)
    return deduped


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
# ASL loader (PARAM ASL ONLY)
# --------------------------------------------------------------------------------------

_PARAM_ASL_FILES = [
    "Cons-Pymts-Incremental-to-S3Landing.param.asl.json",
    "Cons-Pymts-S3Landing-to-S3Final-Raw-DM.param.asl.json",
    "Cons-Pymts-Process-Control-Update.param.asl.json",
    "Cons-Pymts-Main.param.asl.json",
]


def _load_asl_file(project_dir: Path, filename: str) -> Dict[str, Any]:
    p = project_dir / "states" / filename
    if not p.exists():
        raise FileNotFoundError(f"Missing ASL file: {p}")
    obj = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(obj, dict) or not obj:
        raise RuntimeError(f"ASL file did not parse to a JSON object: {p}")
    return obj


# --------------------------------------------------------------------------------------
# Naming (uses the same convention as deploy_base/deploy_maint)
# --------------------------------------------------------------------------------------

@dataclass(frozen=True)
class Names:
    prefix: str

    # Glue jobs (dataset-specific)
    landing_glue_job: str
    raw_dm_glue_job: str

    # State machines (dataset-specific)
    sm_incremental_to_s3landing: str
    sm_s3landing_to_s3final_raw_dm: str
    sm_process_control_update: str
    sm_main: str

    # Lambdas (dataset-specific)
    fn_get_incremental_tables: str
    fn_raw_dm_etl_workflow_update: str
    fn_raw_dm_sns_publish_errors: str
    fn_job_logging_end: str
    fn_validation_check: str
    fn_sns_publish_validations_report: str

    # Crawler (optional)
    crawler_default: str


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

        landing_glue_job=f"{prefix}-Cons-Pymts-LandingFiles",
        raw_dm_glue_job=f"{prefix}-Cons-Pymts-Raw-DM",

        sm_incremental_to_s3landing=f"{prefix}-Cons-Pymts-Incremental-to-S3Landing",
        sm_s3landing_to_s3final_raw_dm=f"{prefix}-Cons-Pymts-S3Landing-to-S3Final-Raw-DM",
        sm_process_control_update=f"{prefix}-Cons-Pymts-Process-Control-Update",
        sm_main=f"{prefix}-Cons-Pymts-Main",

        fn_get_incremental_tables=f"{prefix}-Cons-Pymts-get-incremental-tables",
        fn_raw_dm_etl_workflow_update=f"{prefix}-Cons-Pymts-RAW-DM-etl-workflow-update-data-pplnjob",
        fn_raw_dm_sns_publish_errors=f"{prefix}-Cons-Pymts-RAW-DM-sns-publish-step-function-errors",
        fn_job_logging_end=f"{prefix}-Cons-Pymts-Job-Logging-End",
        fn_validation_check=f"{prefix}-Cons-Pymts-validation-check",
        fn_sns_publish_validations_report=f"{prefix}-Cons-Pymts-sns-publish-validations-report",

        crawler_default=f"{prefix}-Cons-Pymts-ODS",
    )


# --------------------------------------------------------------------------------------
# Lambda directory resolution
# --------------------------------------------------------------------------------------

def _find_lambda_dir(lambda_root: Path, expected_dir_name: str) -> Path:
    """
    CNSV convention: lambda source dirs are under ./lambda and match the suffix exactly:
      - Cons-Pymts-get-incremental-tables
      - Cons-Pymts-RAW-DM-etl-workflow-update-data-pplnjob
      - etc.

    We do NOT invent names; we look for what exists on disk.
    """
    if not lambda_root.exists():
        raise FileNotFoundError(f"Missing lambda root dir: {lambda_root}")

    dirs = [p for p in lambda_root.iterdir() if p.is_dir()]
    if not dirs:
        raise FileNotFoundError(f"No lambda directories found under: {lambda_root}")

    # exact
    for d in dirs:
        if d.name == expected_dir_name:
            return d

    # case-insensitive exact
    target = expected_dir_name.lower()
    for d in dirs:
        if d.name.lower() == target:
            return d

    raise RuntimeError(f"Lambda dir not found for: {expected_dir_name}. Found: {[d.name for d in dirs]}")


# --------------------------------------------------------------------------------------
# Crawler helpers (same role behavior as your working examples)
# --------------------------------------------------------------------------------------

def _build_crawler_s3_targets(crawler_cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    groups = crawler_cfg.get("s3TargetsByBucket")
    if not isinstance(groups, list):
        return out

    for g in groups:
        if not isinstance(g, dict):
            continue
        bucket = _as_str(g.get("bucket"))
        prefixes = g.get("prefixes")
        if not bucket or not isinstance(prefixes, list):
            continue
        for p in prefixes:
            pref = _as_str(p)
            if not pref:
                continue
            if pref.startswith("/"):
                pref = pref[1:]
            out.append({"Path": f"s3://{bucket}/{pref}"})
    return out


def ensure_glue_crawler(
    glue_client,
    *,
    name: str,
    role_arn: str,
    database_name: str,
    s3_targets: List[Dict[str, Any]],
    description: str = "",
    recrawl_behavior: str = "CRAWL_EVERYTHING",
) -> str:
    if not s3_targets:
        raise RuntimeError("Crawler requested but no S3 targets were provided.")

    try:
        glue_client.get_crawler(Name=name)
        exists = True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("EntityNotFoundException", "CrawlerNotFoundException"):
            exists = False
        else:
            raise

    params = {
        "Name": name,
        "Role": role_arn,
        "DatabaseName": database_name,
        "Description": description or "",
        "Targets": {"S3Targets": s3_targets},
        "RecrawlPolicy": {"RecrawlBehavior": recrawl_behavior},
        "SchemaChangePolicy": {
            "UpdateBehavior": "UPDATE_IN_DATABASE",
            "DeleteBehavior": "DEPRECATE_IN_DATABASE",
        },
    }

    if exists:
        glue_client.update_crawler(**params)
    else:
        glue_client.create_crawler(**params)

    return name


# --------------------------------------------------------------------------------------
# Definition binding (convert $.<var> placeholders to real ARNs/names in-memory)
# --------------------------------------------------------------------------------------

_JSONPATH_RE = re.compile(r"^\$\.(?P<key>[A-Za-z0-9_]+)$")


def _bind_placeholders(node: Any, bindings: Dict[str, str]) -> Any:
    """
    Walk any JSON (dict/list/scalar). For dict keys ending with '.$' and values like '$.foo',
    if 'foo' is in bindings, rewrite to key without '.$' and literal value bindings['foo'].
    """
    if isinstance(node, list):
        return [_bind_placeholders(x, bindings) for x in node]

    if isinstance(node, dict):
        out: Dict[str, Any] = {}
        for k, v in node.items():
            if isinstance(k, str) and k.endswith(".$") and isinstance(v, str):
                m = _JSONPATH_RE.match(v.strip())
                if m:
                    key = m.group("key")
                    if key in bindings and bindings[key]:
                        out[k[:-2]] = bindings[key]
                        continue
            out[k] = _bind_placeholders(v, bindings)
        return out

    return node


# --------------------------------------------------------------------------------------
# Deploy
# --------------------------------------------------------------------------------------

def deploy(cfg: Dict[str, Any], region: str) -> Dict[str, str]:
    """
    CNSV Consumer Payments deployer (DEPLOYER #3):

    - Glue: Cons-Pymts-LandingFiles, Cons-Pymts-Raw-DM
    - Lambdas: Cons-Pymts-* (exact dirs under ./lambda)
    - Step Functions: 4 param ASL files (*.param.asl.json), bound in-memory with ARNs/names
    - OPTIONAL: Glue Crawler if cfg['crawler'] exists (role defaults to Glue job role)
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

    # strparams required values (no hardcoding)
    strparams = cfg.get("strparams") or {}
    landing_bucket = _as_str(strparams.get("landingBucketNameParam"))
    clean_bucket = _as_str(strparams.get("cleanBucketNameParam"))
    final_bucket = _as_str(strparams.get("finalBucketNameParam"))
    sns_topic_arn = _as_str(strparams.get("snsArnParam"))

    # Roles (match your working approach)
    etl_role_arn = _as_str(strparams.get("etlRoleArnParam")) or _as_str(strparams.get("lambdaRoleArnParam"))
    glue_job_role_arn = _as_str(strparams.get("glueJobRoleArnParam"))

    # Optional folders
    landing_folder = _as_str(strparams.get("landingFolderNameParam"))
    stg_folder = _as_str(strparams.get("stgFolderNameParam"))
    ods_folder = _as_str(strparams.get("odsFolderNameParam"))

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

    # Local paths (must match repo layout)
    project_dir = Path(__file__).resolve().parent
    glue_root = project_dir / "glue"
    lambda_root = project_dir / "lambda"

    # --- Load parameterized ASLs (must exist on disk) ---
    raw_asls: Dict[str, Dict[str, Any]] = {}
    for fn in _PARAM_ASL_FILES:
        raw_asls[fn] = _load_asl_file(project_dir, fn)

    # --- Glue scripts (do not rename local files) ---
    landing_script_local = glue_root / "Cons-Pymts-LandingFiles.py"
    raw_dm_script_local = glue_root / "Cons-Pymts-Raw-DM.py"

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

    # --- Lambda shared config ---
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

    # --- Glue jobs ---
    landing_script_s3_key = _s3_script_key(prefix, deploy_env, project, landing_script_local.name)
    raw_dm_script_s3_key = _s3_script_key(prefix, deploy_env, project, raw_dm_script_local.name)

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

    # --- Lambdas (directory names MUST match exactly what exists) ---
    required_lambdas: List[Tuple[str, str]] = [
        (names.fn_get_incremental_tables, "Cons-Pymts-get-incremental-tables"),
        (names.fn_raw_dm_etl_workflow_update, "Cons-Pymts-RAW-DM-etl-workflow-update-data-pplnjob"),
        (names.fn_raw_dm_sns_publish_errors, "Cons-Pymts-RAW-DM-sns-publish-step-function-errors"),
        (names.fn_job_logging_end, "Cons-Pymts-Job-Logging-End"),
        (names.fn_validation_check, "Cons-Pymts-validation-check"),
        (names.fn_sns_publish_validations_report, "Cons-Pymts-sns-publish-validations-report"),
    ]

    lambda_arns: Dict[str, str] = {}
    for fn_name, dir_name in required_lambdas:
        src_dir = _find_lambda_dir(lambda_root, dir_name)
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
        lambda_arns[fn_name] = ensure_lambda(lam, spec)

    # --- OPTIONAL: Crawler ---
    crawler_result = "skipped"
    crawler_name_used = ""
    crawler_db_used = ""

    crawler_cfg = cfg.get("crawler")
    if isinstance(crawler_cfg, dict) and crawler_cfg:
        prefix_base = f"FSA-{(deploy_env or '').strip()}-{(project or '').strip()}"

        raw_name = _as_str(crawler_cfg.get("name"))
        if raw_name:
            crawler_name_used = raw_name if raw_name.startswith("FSA-") else f"{prefix_base}-{raw_name}"
        else:
            crawler_name_used = names.crawler_default

        crawler_desc = _as_str(crawler_cfg.get("description"))
        s3_targets = _build_crawler_s3_targets(crawler_cfg)

        crawler_db_used = _as_str(crawler_cfg.get("databaseName")) or _as_str((_as_dict(cfg.get("configData")).get("databaseName")))
        if not crawler_db_used:
            raise RuntimeError("crawler.databaseName (preferred) or cfg.configData.databaseName is required when crawler is enabled")

        # Role behavior: use the same permissions as Glue jobs by default
        crawler_role_arn = _as_str(crawler_cfg.get("roleArn")) or glue_job_role_arn

        ensure_glue_crawler(
            glue,
            name=crawler_name_used,
            role_arn=crawler_role_arn,
            database_name=crawler_db_used,
            s3_targets=s3_targets,
            description=crawler_desc,
            recrawl_behavior=_as_str(crawler_cfg.get("recrawlBehavior"), "CRAWL_EVERYTHING"),
        )
        crawler_result = "created_or_updated"

    # --- Bind definitions in memory ---
    # We intentionally do NOT keep any hardcoded names/ARNs in files.
    bindings: Dict[str, str] = {
        # lambdas
        "get_incremental_tables_fn_arn": lambda_arns[names.fn_get_incremental_tables],
        "raw_dm_etl_workflow_update_fn_arn": lambda_arns[names.fn_raw_dm_etl_workflow_update],
        "raw_dm_sns_publish_errors_fn_arn": lambda_arns[names.fn_raw_dm_sns_publish_errors],
        "job_logging_end_fn_arn": lambda_arns[names.fn_job_logging_end],
        "validation_check_fn_arn": lambda_arns[names.fn_validation_check],
        "sns_publish_validations_report_fn_arn": lambda_arns[names.fn_sns_publish_validations_report],
        # glue job names
        "landing_glue_job_name": names.landing_glue_job,
        "raw_dm_glue_job_name": names.raw_dm_glue_job,
        # sns
        "sns_topic_arn": sns_topic_arn,
        # crawler
        "crawler_name": crawler_name_used,
    }

    # Names sometimes passed to lambdas as metadata (no hardcoding in ASL files)
    bindings["incremental_to_s3landing_sm_name"] = names.sm_incremental_to_s3landing
    bindings["s3landing_to_s3final_raw_dm_sm_name"] = names.sm_s3landing_to_s3final_raw_dm
    bindings["process_control_update_sm_name"] = names.sm_process_control_update

    # Optional configData bindings (if present, they will be substituted into the ASL in-memory)
    config_data = _as_dict(cfg.get("configData"))
    secret_name = _as_str(config_data.get("secretName"))
    if secret_name:
        bindings["secret_name"] = secret_name

    prcs_ctrl_db = _as_str(config_data.get("postgres_prcs_ctrl_dbname"))
    if prcs_ctrl_db:
        bindings["postgres_prcs_ctrl_dbname"] = prcs_ctrl_db

    def _bound(filename: str) -> Dict[str, Any]:
        return _bind_placeholders(raw_asls[filename], bindings)

    # Create child SMs first (main references them)
    sm_incremental_arn = ensure_state_machine(
        sfn,
        StateMachineSpec(
            name=names.sm_incremental_to_s3landing,
            role_arn=sfn_role_arn,
            definition=_bound("Cons-Pymts-Incremental-to-S3Landing.param.asl.json"),
        ),
    )
    bindings["incremental_to_s3landing_sm_arn"] = sm_incremental_arn

    sm_s3landing_arn = ensure_state_machine(
        sfn,
        StateMachineSpec(
            name=names.sm_s3landing_to_s3final_raw_dm,
            role_arn=sfn_role_arn,
            definition=_bound("Cons-Pymts-S3Landing-to-S3Final-Raw-DM.param.asl.json"),
        ),
    )
    bindings["s3landing_to_s3final_raw_dm_sm_arn"] = sm_s3landing_arn

    sm_pc_arn = ensure_state_machine(
        sfn,
        StateMachineSpec(
            name=names.sm_process_control_update,
            role_arn=sfn_role_arn,
            definition=_bound("Cons-Pymts-Process-Control-Update.param.asl.json"),
        ),
    )
    bindings["process_control_update_sm_arn"] = sm_pc_arn

    sm_main_arn = ensure_state_machine(
        sfn,
        StateMachineSpec(
            name=names.sm_main,
            role_arn=sfn_role_arn,
            definition=_bind_placeholders(raw_asls["Cons-Pymts-Main.param.asl.json"], bindings),
        ),
    )

    # --- Summary (human-usable) ---
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
        "crawler": crawler_result,
        "crawler_name": crawler_name_used,
        "crawler_database": crawler_db_used,
        "state_machine_incremental_to_s3landing_name": names.sm_incremental_to_s3landing,
        "state_machine_incremental_to_s3landing_arn": sm_incremental_arn,
        "state_machine_s3landing_to_s3final_raw_dm_name": names.sm_s3landing_to_s3final_raw_dm,
        "state_machine_s3landing_to_s3final_raw_dm_arn": sm_s3landing_arn,
        "state_machine_process_control_update_name": names.sm_process_control_update,
        "state_machine_process_control_update_arn": sm_pc_arn,
        "state_machine_main_name": names.sm_main,
        "state_machine_main_arn": sm_main_arn,
    }
