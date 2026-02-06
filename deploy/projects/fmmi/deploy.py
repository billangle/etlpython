# deploy/projects/fmmi/deploy.py
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
    GlueJobSpec,
    StateMachineSpec,
)

# --------------------------------------------------------------------------------------
# Names
# --------------------------------------------------------------------------------------

@dataclass(frozen=True)
class FmmiNames:
    echo_landing_glue_job: str
    stg_ods_glue_job: str
    fmmi_state_machine: str


def build_names(deploy_env: str, project: str) -> FmmiNames:
    """
    Prefix is: FSA-<deployEnv>-<project>
    """
    dep = (deploy_env or "").strip()
    proj = (project or "").strip()
    if not dep:
        raise RuntimeError("Missing required cfg['deployEnv']")
    if not proj:
        raise RuntimeError("Missing required cfg['project']")

    prefix = f"FSA-{dep}-{proj}"

    return FmmiNames(
        echo_landing_glue_job=f"{prefix}-LandingFiles",
        stg_ods_glue_job=f"{prefix}-S3-STG-ODS-parquet",
        fmmi_state_machine=f"{prefix}-CSV-STG-ODS",
    )


# --------------------------------------------------------------------------------------
# Safe parsers
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


# --------------------------------------------------------------------------------------
# GlueConfig (NEW) helpers
# --------------------------------------------------------------------------------------

def _script_stem(p: Path) -> str:
    """GlueConfig key must match file name without .py (stem)."""
    return p.name[:-3] if p.name.lower().endswith(".py") else p.name


def _parse_glue_config_array(cfg: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    New structure:
      cfg["GlueConfig"] is a list
      each list item is {"<script_stem>": { ... glue params ... }}

    Returns a dict keyed by script_stem -> params dict
    """
    root = cfg.get("GlueConfig")
    if not isinstance(root, list) or not root:
        return {}

    out: Dict[str, Dict[str, Any]] = {}
    for item in root:
        if not isinstance(item, dict) or not item:
            continue
        # each item should have exactly one key: the script stem
        for k, v in item.items():
            if isinstance(k, str) and k.strip() and isinstance(v, dict):
                out[k.strip()] = v
    return out


def _glue_config_for_script(cfg: Dict[str, Any], script_stem_name: str) -> Dict[str, Any]:
    """
    Returns GlueConfig settings for the given script stem (no .py), else {}.
    """
    m = _parse_glue_config_array(cfg)
    return m.get(script_stem_name, {}) if m else {}


# --------------------------------------------------------------------------------------
# GlueJobParameters helpers (kept for backward compatibility / default args)
# --------------------------------------------------------------------------------------

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


def _merge_glue_default_args(
    base_args: Dict[str, Any],
    glue_job_params: Dict[str, Any],
) -> Dict[str, str]:
    """
    Produces DefaultArguments for Glue.

    Precedence:
      cfg.glueDefaultArgs < derived args from Glue params < Glue params.JobParameters
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

    # Explicit job parameters override everything
    job_params = glue_job_params.get("JobParameters") or {}
    if isinstance(job_params, dict):
        for k, v in job_params.items():
            out[str(k)] = "" if v is None else str(v)

    return {str(k): str(v) for k, v in out.items()}


# --------------------------------------------------------------------------------------
# Script naming
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
# ASL loader
# --------------------------------------------------------------------------------------

def _load_asl_definition(project_dir: Path) -> Dict[str, Any]:
    asl_file = project_dir / "states" / "CSV-STG-ODS.asl.json"
    if not asl_file.exists():
        raise FileNotFoundError(f"Missing ASL file: {asl_file}")

    obj = json.loads(asl_file.read_text(encoding="utf-8"))
    if not isinstance(obj, dict) or not obj:
        raise RuntimeError(f"ASL file did not parse to a JSON object: {asl_file}")
    return obj


# --------------------------------------------------------------------------------------
# Deploy
# --------------------------------------------------------------------------------------

def deploy(cfg: Dict[str, Any], region: str) -> Dict[str, str]:
    """
    Deploy FMMI:
      - 2 Glue jobs (Landing + STG/ODS Parquet)
      - 1 Step Function (definition from ASL file)

    NEW:
      - Reads Glue sizing/args from cfg["GlueConfig"] array keyed by script filename stem.
      - If GlueConfig missing or doesn't contain a matching entry, falls back to defaults
        (GlueVersion=4.0, WorkerType=G.1X, etc.) plus cfg.glueDefaultArgs.
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

    landing_script_local = glue_root / "FMMI-LandingFiles.py"
    stg_ods_script_local = glue_root / "S3-STG-ODS-parquet.py"

    if not landing_script_local.exists():
        raise FileNotFoundError(f"Missing Glue script: {landing_script_local}")
    if not stg_ods_script_local.exists():
        raise FileNotFoundError(f"Missing Glue script: {stg_ods_script_local}")

    landing_script_stem = _script_stem(landing_script_local)        # "FMMI-LandingFiles"
    stg_ods_script_stem = _script_stem(stg_ods_script_local)        # "S3-STG-ODS-parquet"

    # NEW GlueConfig lookup (optional)
    landing_params = _glue_config_for_script(cfg, landing_script_stem)
    stg_ods_params = _glue_config_for_script(cfg, stg_ods_script_stem)

    # Defaults if missing or empty
    def _defaults(p: Dict[str, Any], default_worker: str) -> Dict[str, Any]:
        return {
            "GlueVersion": _as_str(p.get("GlueVersion"), "4.0"),
            "WorkerType": _as_str(p.get("WorkerType"), default_worker),
            "NumberOfWorkers": _as_int(p.get("NumberOfWorkers"), 2),
            "TimeoutMinutes": _as_int(p.get("TimeoutMinutes"), 60),
            "MaxRetries": _as_int(p.get("MaxRetries"), 0),
            "MaxConcurrency": _as_int(p.get("MaxConcurrency"), 1),
        }

    # Reasonable defaults (can be overridden via GlueConfig)
    landing_d = _defaults(landing_params, default_worker="G.1X")
    stg_ods_d = _defaults(stg_ods_params, default_worker="G.1X")

    # DefaultArguments:
    # - If GlueConfig missing, these become mostly cfg.glueDefaultArgs plus injected values
    glue_default_base = cfg.get("glueDefaultArgs") or {}
    landing_default_args = _merge_glue_default_args(glue_default_base, landing_params)
    stg_ods_default_args = _merge_glue_default_args(glue_default_base, stg_ods_params)

    def _set_if_missing(args: Dict[str, str], k: str, v: str) -> None:
        if k not in args and v:
            args[k] = v

    # Helpful injected params if your scripts accept them (only if not already set)
    for args in (landing_default_args, stg_ods_default_args):
        _set_if_missing(args, "--env", str(deploy_env))
        _set_if_missing(args, "--landing_bucket", landing_bucket)
        _set_if_missing(args, "--clean_bucket", clean_bucket)
        _set_if_missing(args, "--final_bucket", final_bucket)
        _set_if_missing(args, "--sns_topic_arn", sns_topic_arn)
        _set_if_missing(args, "--landing_folder", landing_folder)
        _set_if_missing(args, "--stg_folder", stg_folder)
        _set_if_missing(args, "--ods_folder", ods_folder)

    # Connections (names only)
    landing_conns = _parse_connection_names(landing_params)
    stg_ods_conns = _parse_connection_names(stg_ods_params)

    # Clients
    session = boto3.Session(region_name=region)
    s3 = session.client("s3")
    glue = session.client("glue")
    sfn = session.client("stepfunctions")

    ensure_bucket_exists(s3, artifact_bucket, region)

    # Script S3 keys (renamed with FSA-<env>-<proj>- prefix)
    landing_script_s3_key = _s3_script_key(prefix, deploy_env, project, landing_script_local.name)
    stg_ods_script_s3_key = _s3_script_key(prefix, deploy_env, project, stg_ods_script_local.name)

    # --- Glue jobs ---
    ensure_glue_job(
        glue,
        s3,
        GlueJobSpec(
            name=names.echo_landing_glue_job,
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
            name=names.stg_ods_glue_job,
            role_arn=glue_job_role_arn,
            script_local_path=str(stg_ods_script_local),
            script_s3_bucket=artifact_bucket,
            script_s3_key=stg_ods_script_s3_key,
            default_args=stg_ods_default_args,
            glue_version=str(stg_ods_d["GlueVersion"]),
            worker_type=str(stg_ods_d["WorkerType"]),
            number_of_workers=int(stg_ods_d["NumberOfWorkers"]),
            timeout_minutes=int(stg_ods_d["TimeoutMinutes"]),
            max_retries=int(stg_ods_d["MaxRetries"]),
            max_concurrency=int(stg_ods_d["MaxConcurrency"]),
            connection_names=stg_ods_conns,
        ),
    )

    # --- Step Function ---
    definition = _load_asl_definition(project_dir)

    sfn_arn = ensure_state_machine(
        sfn,
        StateMachineSpec(
            name=names.fmmi_state_machine,
            role_arn=sfn_role_arn,
            definition=definition,
        ),
    )

    return {
        "deploy_env": str(deploy_env),
        "project": str(project),
        "artifact_bucket": artifact_bucket,
        "artifact_prefix": prefix,
        "landing_bucket_from_config": landing_bucket,
        "clean_bucket_from_config": clean_bucket,
        "final_bucket_from_config": final_bucket,
        "sns_topic_arn_from_config": sns_topic_arn,
        "glue_job_landing_name": names.echo_landing_glue_job,
        "glue_job_stg_ods_name": names.stg_ods_glue_job,
        "glue_landing_script_s3_key": landing_script_s3_key,
        "glue_stg_ods_script_s3_key": stg_ods_script_s3_key,
        "state_machine_name": names.fmmi_state_machine,
        "state_machine_arn": sfn_arn,
        "glue_config_used_for_landing": "yes" if bool(landing_params) else "no",
        "glue_config_used_for_stg_ods": "yes" if bool(stg_ods_params) else "no",
    }
