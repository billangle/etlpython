# deploy/projects/nps/deploy.py
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
class NpsNames:
    state_machine: str


def build_names(deploy_env: str, project: str) -> NpsNames:
    dep = (deploy_env or "").strip()
    proj = (project or "").strip()
    if not dep:
        raise RuntimeError("Missing required cfg['deployEnv']")
    if not proj:
        raise RuntimeError("Missing required cfg['project']")

    # Keep the step function tied to the actual ASL filename (NPS_build_archive_tables)
    prefix = f"FSA-{dep}-{proj}"
    return NpsNames(state_machine=f"{prefix}-NPS_build_archive_tables")


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
# GlueConfig helpers
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


# --------------------------------------------------------------------------------------
# Glue job helpers
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
    asl_file = project_dir / "states" / "NPS_build_archive_tables.asl.json"
    if not asl_file.exists():
        raise FileNotFoundError(f"Missing ASL file: {asl_file}")

    obj = json.loads(asl_file.read_text(encoding="utf-8"))
    if not isinstance(obj, dict) or not obj:
        raise RuntimeError(f"ASL file did not parse to a JSON object: {asl_file}")
    return obj


def _inject_utils_path(definition: Any, utils_path: str) -> Any:
    """Recursively set --utils to a fixed path, removing any dynamic placeholders."""
    if isinstance(definition, dict):
        # If this dict represents Arguments, enforce a static --utils
        if definition.get("Arguments") and isinstance(definition.get("Arguments"), dict):
            args = definition["Arguments"]
            args.pop("--utils.$", None)
            args["--utils"] = utils_path

        # Also replace any stray --utils.$ at this level
        if "--utils.$" in definition:
            definition.pop("--utils.$", None)
            definition["--utils"] = utils_path

        for k, v in list(definition.items()):
            definition[k] = _inject_utils_path(v, utils_path)
        return definition
    if isinstance(definition, list):
        return [_inject_utils_path(v, utils_path) for v in definition]
    return definition


def _inject_runtime_values(
    definition: Any,
    *,
    glue_job_name: str,
    sns_topic_arn: str,
    environ: str,
    utils_path: str,
) -> Any:
    """
    Replace all dynamic Step Functions JSONPath variables with concrete values.

    - JobName.$ -> JobName
    - TopicArn.$ -> TopicArn
    - --environ.$ -> --environ
    - --utils.$ -> --utils
    """

    if isinstance(definition, dict):
        if "JobName.$" in definition:
            definition.pop("JobName.$", None)
            definition["JobName"] = glue_job_name

        if "TopicArn.$" in definition:
            definition.pop("TopicArn.$", None)
            definition["TopicArn"] = sns_topic_arn

        if "--environ.$" in definition:
            definition.pop("--environ.$", None)
            definition["--environ"] = environ

        if "--utils.$" in definition:
            definition.pop("--utils.$", None)
            definition["--utils"] = utils_path

        # If this dict holds Arguments, do the same within
        if definition.get("Arguments") and isinstance(definition.get("Arguments"), dict):
            args = definition["Arguments"]
            if "--environ.$" in args:
                args.pop("--environ.$", None)
                args["--environ"] = environ
            if "--utils.$" in args:
                args.pop("--utils.$", None)
                args["--utils"] = utils_path

        for k, v in list(definition.items()):
            definition[k] = _inject_runtime_values(
                v,
                glue_job_name=glue_job_name,
                sns_topic_arn=sns_topic_arn,
                environ=environ,
                utils_path=utils_path,
            )
        return definition

    if isinstance(definition, list):
        return [
            _inject_runtime_values(
                v,
                glue_job_name=glue_job_name,
                sns_topic_arn=sns_topic_arn,
                environ=environ,
                utils_path=utils_path,
            )
            for v in definition
        ]

    return definition


# --------------------------------------------------------------------------------------
# Deploy
# --------------------------------------------------------------------------------------


def _defaults(p: Dict[str, Any], default_worker: str) -> Dict[str, Any]:
    return {
        "GlueVersion": _as_str(p.get("GlueVersion"), "4.0"),
        "WorkerType": _as_str(p.get("WorkerType"), default_worker),
        "NumberOfWorkers": _as_int(p.get("NumberOfWorkers"), 2),
        "TimeoutMinutes": _as_int(p.get("TimeoutMinutes"), 60),
        "MaxRetries": _as_int(p.get("MaxRetries"), 0),
        "MaxConcurrency": _as_int(p.get("MaxConcurrency"), 1),
    }


def deploy(cfg: Dict[str, Any], region: Optional[str] = None) -> Dict[str, Any]:
    """
    Deploy NPS:
      - 7 Glue jobs (from projects/nps/glue)
      - 1 Step Function

    Defaults:
      - Region falls back to cfg.region or us-east-1
      - Artifact bucket defaults to f"fsa-{deployEnv.lower()}-ops"
      - Artifact prefix defaults to "nps/"
    """

    deploy_env = cfg["deployEnv"]
    project = cfg["project"]
    names = build_names(deploy_env, project)

    region_used = _as_str(region) or _as_str(cfg.get("region"), "us-east-1")
    bucket_region = _as_str(cfg.get("bucketRegion"), region_used)

    # Resolve roles
    strparams = cfg.get("strparams") or {}
    glue_job_role_arn = _as_str(strparams.get("glueJobRoleArnParam")) or _as_str(
        (cfg.get("stepFunctions") or {}).get("roleArn")
    )
    sfn_role_arn = _as_str((cfg.get("stepFunctions") or {}).get("roleArn"))
    if not glue_job_role_arn:
        raise RuntimeError("Missing glue job role (strparams.glueJobRoleArnParam)")
    if not sfn_role_arn:
        raise RuntimeError("Missing step function role (stepFunctions.roleArn)")

    # Buckets / artifacts
    artifacts = cfg.get("artifacts") or {}
    artifact_bucket = _as_str(artifacts.get("artifactBucket")) or f"fsa-{deploy_env.lower()}-ops"
    prefix = (_as_str(artifacts.get("prefix")) or "nps/").rstrip("/") + "/"

    project_dir = Path(__file__).resolve().parent
    glue_root = project_dir / "glue"
    glue_scripts = sorted(glue_root.glob("*.py"))
    if not glue_scripts:
        raise RuntimeError(f"No Glue scripts found under {glue_root}")

    # GlueConfig lookup
    glue_params_by_script = _parse_glue_config_array(cfg)
    glue_default_base = cfg.get("glueDefaultArgs") or {}

    job_prefix = f"FSA-{deploy_env}-{project}"

    # Clients
    session = boto3.Session(region_name=region_used)
    s3 = session.client("s3")
    glue = session.client("glue")
    sfn = session.client("stepfunctions")

    ensure_bucket_exists(s3, artifact_bucket, bucket_region)

    deployed_jobs: Dict[str, str] = {}

    for script in glue_scripts:
        stem = _script_stem(script)
        job_params = glue_params_by_script.get(stem, {})

        d = _defaults(job_params, default_worker=_as_str(job_params.get("WorkerType"), "G.1X"))
        default_args = _merge_glue_default_args(glue_default_base, job_params)

        def _set_if_missing(args: Dict[str, str], k: str, v: str) -> None:
            if k not in args and v:
                args[k] = v

        _set_if_missing(default_args, "--environ", str(deploy_env))
        _set_if_missing(default_args, "--env", str(deploy_env))
        _set_if_missing(default_args, "--landing_bucket", _as_str((cfg.get("configData") or {}).get("landingBucket")))

        conns = _parse_connection_names(job_params)

        script_s3_key = _s3_script_key(prefix, deploy_env, project, script.name)
        job_name = f"{job_prefix}-{stem}"

        ensure_glue_job(
            glue,
            s3,
            GlueJobSpec(
            name=job_name,
                role_arn=glue_job_role_arn,
                script_local_path=str(script),
                script_s3_bucket=artifact_bucket,
                script_s3_key=script_s3_key,
                default_args=default_args,
                glue_version=str(d["GlueVersion"]),
                worker_type=str(d["WorkerType"]),
                number_of_workers=int(d["NumberOfWorkers"]),
                timeout_minutes=int(d["TimeoutMinutes"]),
                max_retries=int(d["MaxRetries"]),
                max_concurrency=int(d["MaxConcurrency"]),
                connection_names=conns,
            ),
        )

        deployed_jobs[stem] = job_name

    glue_job_names: List[str] = [deployed_jobs[s] for s in sorted(deployed_jobs)]

    # --- Step Function ---
    definition = _load_asl_definition(project_dir)
    landing_bucket = _as_str((cfg.get("configData") or {}).get("landingBucket"))
    utils_rel = _as_str((cfg.get("configData") or {}).get("utilsPath"))
    utils_full = f"s3://{landing_bucket}/{utils_rel}" if landing_bucket and utils_rel else utils_rel
    definition = _inject_utils_path(definition, utils_full)

    sns_topic_arn = _as_str(((cfg.get("sns") or {}).get("topics") or {}).get("FWADM_NPS"))
    if not sns_topic_arn:
        raise RuntimeError("Missing SNS topic ARN (sns.topics.FWADM_NPS) in config")
    glue_job_name = f"{job_prefix}-NPS_build_archive_table"
    definition = _inject_runtime_values(
        definition,
        glue_job_name=glue_job_name,
        sns_topic_arn=sns_topic_arn,
        environ=str(deploy_env),
        utils_path=utils_full,
    )
    sfn_arn = ensure_state_machine(
        sfn,
        StateMachineSpec(
            name=names.state_machine,
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
        "sns_topic_arn_from_config": sns_topic_arn,
        "glue_jobs": glue_job_names,
        "state_machine_name": names.state_machine,
        "state_machine_arn": sfn_arn,
    }


# --------------------------------------------------------------------------------------
# Entry Point
# --------------------------------------------------------------------------------------


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("Usage: python deploy.py <config_file>")
        print("Example: python deploy.py ../../config/nps/dev.json")
        sys.exit(1)

    cfg_path = Path(sys.argv[1])
    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    result = deploy(cfg)

    print("DEPLOY SUMMARY")

    ordered_keys = [
        "deploy_env",
        "project",
        "artifact_bucket",
        "artifact_prefix",
        "landing_bucket_from_config",
        "sns_topic_arn_from_config",
        "state_machine_name",
        "state_machine_arn",
    ]

    for k in ordered_keys:
        if k in result:
            print(f"  {k}: {result[k]}")

    # Glue jobs - one line each, no JSON
    gj = result.get("glue_jobs")
    if isinstance(gj, list):
        for name in gj:
            print(f"  [glue]: {name}")