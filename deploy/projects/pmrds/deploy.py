# deploy/projects/pmrds/deploy.py
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

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
class PmrdsNames:
    stage1_glue_job: str
    stage2_glue_job: str
    pmrds_state_machine: str


def build_names(deploy_env: str, project: str) -> PmrdsNames:
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

    return PmrdsNames(
        stage1_glue_job=f"{prefix}-LOAD-STAGING",
        stage2_glue_job=f"{prefix}-LOADING-STG2FINAL",
        pmrds_state_machine=f"{prefix}-ODS-STG-FINAL",
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
# GlueJobParameters helpers
# --------------------------------------------------------------------------------------

def _parse_connection_names(glue_job_params: Dict[str, Any]) -> List[str]:
    """
    Glue job only references existing connection NAMES.

    Supports:
      "Connections": [{"ConnectionName":"..."}, ...]
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

    Handles "PythonLibraryPath" (comma-separated) by mapping to Glue's --extra-py-files
    if --extra-py-files is not already set.
    """
    out: Dict[str, Any] = dict(base_args or {})

    # Spark UI logs
    spark_ui_path = glue_job_params.get("SparkUILogsPath")
    if spark_ui_path:
        out["--enable-spark-ui"] = "true"
        out["--spark-event-logs-path"] = str(spark_ui_path)

    # Metrics
    if _as_bool(glue_job_params.get("GenerateMetrics"), default=False) is True:
        out["--enable-metrics"] = "true"

    # Bookmarks
    bmk = _as_bool(glue_job_params.get("EnableJobBookmarks"), default=None)
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
    if _as_bool(glue_job_params.get("UseGlueDataCatalogAsTheHiveMetastore"), default=False) is True:
        out["--enable-glue-datacatalog"] = "true"

    # Extra files / py files (preferred keys you used earlier)
    ref_files = glue_job_params.get("ReferencedFilesS3Path")
    if ref_files:
        out["--extra-files"] = str(ref_files)

    extra_py = glue_job_params.get("AdditionalPythonModulesS3Path")
    if extra_py:
        out["--extra-py-files"] = str(extra_py)

    # PythonLibraryPath (your new key): map -> --extra-py-files if not set
    python_lib_path = _as_str(glue_job_params.get("PythonLibraryPath"), default="")
    if python_lib_path and "--extra-py-files" not in out:
        out["--extra-py-files"] = python_lib_path

    # Explicit job parameters override everything
    job_params = glue_job_params.get("JobParameters") or {}
    if isinstance(job_params, dict):
        for k, v in job_params.items():
            out[str(k)] = "" if v is None else str(v)

    return {str(k): str(v) for k, v in out.items()}


def _glue_params_for_job(cfg: Dict[str, Any], job_key: str) -> Dict[str, Any]:
    """
    Supports either:
      A) cfg["GlueJobParameters"] is a shared blob (your example)
      B) cfg["GlueJobParameters"] has per-job sections: {"stage1": {...}, "stage2": {...}}
    """
    root = cfg.get("GlueJobParameters")
    if not isinstance(root, dict) or not root:
        return {}

    candidate = root.get(job_key)
    if isinstance(candidate, dict) and candidate:
        if "Connections" not in candidate and "Connections" in root:
            merged = dict(candidate)
            merged["Connections"] = root.get("Connections")
            return merged
        return candidate

    return root


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
    asl_file = project_dir / "states" / "PMRDS-ODS-STG-FINAL.asl.json"
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
    Deploy PMRDS:
      - 2 Glue jobs
      - 1 Step Function (definition from ASL file)

    No environment hardcoding: buckets + snsArnParam come from cfg["strparams"].
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

    # Scripts renamed (no PMRDS prefix)
    stage1_script_local = glue_root / "LOAD-STAGING.py"
    stage2_script_local = glue_root / "LOADING-STG2FINAL.py"
    if not stage1_script_local.exists():
        raise FileNotFoundError(f"Missing Glue script: {stage1_script_local}")
    if not stage2_script_local.exists():
        raise FileNotFoundError(f"Missing Glue script: {stage2_script_local}")

    # Clients
    session = boto3.Session(region_name=region)
    s3 = session.client("s3")
    glue = session.client("glue")
    sfn = session.client("stepfunctions")

    ensure_bucket_exists(s3, artifact_bucket, region)

    # Glue params (shared blob by default, or stage1/stage2 override)
    stage1_params = _glue_params_for_job(cfg, "stage1")
    stage2_params = _glue_params_for_job(cfg, "stage2")

    # Defaults if empty/missing
    def _defaults(p: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "GlueVersion": _as_str(p.get("GlueVersion"), "4.0"),
            "WorkerType": _as_str(p.get("WorkerType"), "G.1X"),
            "NumberOfWorkers": _as_int(p.get("NumberOfWorkers"), 2),
            "TimeoutMinutes": _as_int(p.get("TimeoutMinutes"), 60),
            "MaxRetries": _as_int(p.get("MaxRetries"), 0),
            "MaxConcurrency": _as_int(p.get("MaxConcurrency"), 1),
        }

    stage1_d = _defaults(stage1_params)
    stage2_d = _defaults(stage2_params)

    stage1_default_args = _merge_glue_default_args(cfg.get("glueDefaultArgs") or {}, stage1_params)
    stage2_default_args = _merge_glue_default_args(cfg.get("glueDefaultArgs") or {}, stage2_params)

    # Optional injection of runtime resources into job args (ONLY if not already provided)
    def _set_if_missing(args: Dict[str, str], k: str, v: str) -> None:
        if k not in args and v:
            args[k] = v

    for args in (stage1_default_args, stage2_default_args):
        _set_if_missing(args, "--landing_bucket", landing_bucket)
        _set_if_missing(args, "--clean_bucket", clean_bucket)
        _set_if_missing(args, "--final_bucket", final_bucket)
        _set_if_missing(args, "--sns_topic_arn", sns_topic_arn)

    # S3 keys
    stage1_script_s3_key = _s3_script_key(prefix, deploy_env, project, stage1_script_local.name)
    stage2_script_s3_key = _s3_script_key(prefix, deploy_env, project, stage2_script_local.name)

    # Connection names
    stage1_conns = _parse_connection_names(stage1_params)
    stage2_conns = _parse_connection_names(stage2_params)

    # --- Glue jobs ---
    ensure_glue_job(
        glue,
        s3,
        GlueJobSpec(
            name=names.stage1_glue_job,
            role_arn=glue_job_role_arn,
            script_local_path=str(stage1_script_local),
            script_s3_bucket=artifact_bucket,
            script_s3_key=stage1_script_s3_key,
            default_args=stage1_default_args,
            glue_version=str(stage1_d["GlueVersion"]),
            worker_type=str(stage1_d["WorkerType"]),
            number_of_workers=int(stage1_d["NumberOfWorkers"]),
            timeout_minutes=int(stage1_d["TimeoutMinutes"]),
            max_retries=int(stage1_d["MaxRetries"]),
            max_concurrency=int(stage1_d["MaxConcurrency"]),
            connection_names=stage1_conns,
        ),
    )

    ensure_glue_job(
        glue,
        s3,
        GlueJobSpec(
            name=names.stage2_glue_job,
            role_arn=glue_job_role_arn,
            script_local_path=str(stage2_script_local),
            script_s3_bucket=artifact_bucket,
            script_s3_key=stage2_script_s3_key,
            default_args=stage2_default_args,
            glue_version=str(stage2_d["GlueVersion"]),
            worker_type=str(stage2_d["WorkerType"]),
            number_of_workers=int(stage2_d["NumberOfWorkers"]),
            timeout_minutes=int(stage2_d["TimeoutMinutes"]),
            max_retries=int(stage2_d["MaxRetries"]),
            max_concurrency=int(stage2_d["MaxConcurrency"]),
            connection_names=stage2_conns,
        ),
    )

    # --- Step Function ---
    definition = _load_asl_definition(project_dir)

    sfn_arn = ensure_state_machine(
        sfn,
        StateMachineSpec(
            name=names.pmrds_state_machine,
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
        "glue_job_stage1_name": names.stage1_glue_job,
        "glue_job_stage2_name": names.stage2_glue_job,
        "glue_stage1_script_s3_key": stage1_script_s3_key,
        "glue_stage2_script_s3_key": stage2_script_s3_key,
        "state_machine_name": names.pmrds_state_machine,
        "state_machine_arn": sfn_arn,
    }
