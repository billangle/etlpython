# deploy/projects/fmmi/deploy.py
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

from common.aws_common import (
    ensure_bucket_exists,
    ensure_glue_job,
    ensure_lambda,
    ensure_state_machine,
    GlueJobSpec,
    LambdaSpec,
    StateMachineSpec,
)

# --------------------------------------------------------------------------------------
# Names
# --------------------------------------------------------------------------------------

@dataclass(frozen=True)
class FmmiNames:
    echo_landing_glue_job: str
    stg_ods_glue_job: str
    ora_dms_ods_glue_job: str
    fmmi_state_machine: str
    fmmi_crawler: str
    check_ods_nofiles_fn: str
    ods_crawler_fn: str


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
        ora_dms_ods_glue_job=f"{prefix}-Ora-DMS-ODS",
        fmmi_state_machine=f"{prefix}-CSV-STG-ODS",
        fmmi_crawler=f"{prefix}-ODS",
        check_ods_nofiles_fn=f"{prefix}-Check-ODS-NoFiles",
        ods_crawler_fn=f"{prefix}-ODS-Crawler",
    )


# --------------------------------------------------------------------------------------
# Networking helpers
# --------------------------------------------------------------------------------------

def parse_networking(cfg: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    net = cfg.get("networking") or {}
    if not isinstance(net, dict) or not net:
        return ([], [])

    subnet_ids = net.get("subnetIds") or []
    sg_ids = net.get("securityGroupIds") or []

    if not isinstance(subnet_ids, list):
        subnet_ids = []
    if not isinstance(sg_ids, list):
        sg_ids = []

    subnet_ids = [s.strip() for s in subnet_ids if isinstance(s, str) and s.strip()]
    sg_ids = [s.strip() for s in sg_ids if isinstance(s, str) and s.strip()]

    if not (subnet_ids and sg_ids):
        return ([], [])

    return (subnet_ids, sg_ids)


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


def _merge_glue_default_args(
    base_args: Dict[str, Any],
    glue_job_params: Dict[str, Any],
) -> Dict[str, str]:
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
    asl_file = project_dir / "states" / "CSV-STG-ODS.asl.json"
    if not asl_file.exists():
        raise FileNotFoundError(f"Missing ASL file: {asl_file}")

    obj = json.loads(asl_file.read_text(encoding="utf-8"))
    if not isinstance(obj, dict) or not obj:
        raise RuntimeError(f"ASL file did not parse to a JSON object: {asl_file}")
    return obj


# --------------------------------------------------------------------------------------
# Crawler helpers
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
# Deploy
# --------------------------------------------------------------------------------------

def deploy(cfg: Dict[str, Any], region: str) -> Dict[str, str]:
    """
    Deploy FMMI:
      - 2 Glue jobs
      - 1 Step Function
      - OPTIONAL: 1 Glue Crawler if cfg["crawler"] is present

    Crawler naming:
      - Uses the same FSA-<deployEnv>-<project> prefix via build_names()
      - If cfg.crawler.name is provided, we append it as a suffix:
            <prefix>-<cfg.crawler.name>
        unless cfg.crawler.name already starts with "FSA-"
      - If cfg.crawler.name is NOT provided, default: names.fmmi_crawler
    Database name preference:
      - Prefer cfg.crawler.databaseName if present
      - Else cfg.configData.databaseName
    """
    deploy_env = cfg["deployEnv"]
    project = cfg["project"]
    bucket_region = cfg.get("bucketRegion") or region
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

    # Echo / pipeline identifiers (baked into both Glue default args and the ASL at deploy time)
    echo_secret_id = _as_str(strparams.get("echoSecretIdParam"))
    pipeline_name = _as_str(strparams.get("pipelineNameParam"), default=project.lower())

    # Lambda role and layers
    etl_lambda_role_arn = _as_str(strparams.get("etlRoleArnParam"))
    lambda_runtime = _as_str(strparams.get("lambdaRuntime"), default="python3.12")
    layers: List[str] = []
    third_party_layer = _as_str(strparams.get("thirdPartyLayerArnParam"))
    custom_layer = _as_str(strparams.get("customLayerArnParam"))
    if third_party_layer:
        layers.append(third_party_layer)
    if custom_layer:
        layers.append(custom_layer)

    # Optional folders — derive defaults from project name so callers don't have to set them
    _proj_lower = project.lower()
    landing_folder = _as_str(strparams.get("landingFolderNameParam"),
                             default=f"{_proj_lower}/{_proj_lower}_ocfo_files")
    stg_folder = _as_str(strparams.get("stgFolderNameParam"),
                         default=f"{_proj_lower}_stg")
    ods_folder = _as_str(strparams.get("odsFolderNameParam"),
                         default=f"{_proj_lower}_ods")

    # Landing prefix used by the Check-ODS-NoFiles Lambda (mirrors landing_folder by default)
    landing_prefix = _as_str(strparams.get("landingPrefixParam"), default=landing_folder)

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
    if not etl_lambda_role_arn:
        missing.append("strparams.etlRoleArnParam")
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
    ora_dms_ods_script_local = glue_root / "Ora-DMS-ODS.py"

    if not landing_script_local.exists():
        raise FileNotFoundError(f"Missing Glue script: {landing_script_local}")
    if not stg_ods_script_local.exists():
        raise FileNotFoundError(f"Missing Glue script: {stg_ods_script_local}")
    if not ora_dms_ods_script_local.exists():
        raise FileNotFoundError(f"Missing Glue script: {ora_dms_ods_script_local}")

    landing_script_stem = _script_stem(landing_script_local)
    stg_ods_script_stem = _script_stem(stg_ods_script_local)
    ora_dms_ods_script_stem = _script_stem(ora_dms_ods_script_local)

    # GlueConfig lookup (optional)
    landing_params = _glue_config_for_script(cfg, landing_script_stem)
    stg_ods_params = _glue_config_for_script(cfg, stg_ods_script_stem)
    ora_dms_ods_params = _glue_config_for_script(cfg, ora_dms_ods_script_stem)

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
    stg_ods_d = _defaults(stg_ods_params, default_worker="G.1X")
    ora_dms_ods_d = _defaults(ora_dms_ods_params, default_worker="G.1X")

    glue_default_base = cfg.get("glueDefaultArgs") or {}
    landing_default_args = _merge_glue_default_args(glue_default_base, landing_params)
    stg_ods_default_args = _merge_glue_default_args(glue_default_base, stg_ods_params)
    ora_dms_ods_default_args = _merge_glue_default_args(glue_default_base, ora_dms_ods_params)

    def _set_if_missing(args: Dict[str, str], k: str, v: str) -> None:
        if k not in args and v:
            args[k] = v

    # Landing job default args — match FMMI-LandingFiles.py getResolvedOptions
    _set_if_missing(landing_default_args, "--SecretId", echo_secret_id)
    _set_if_missing(landing_default_args, "--DestinationBucket", landing_bucket)
    _set_if_missing(landing_default_args, "--DestinationPrefix", landing_folder)
    _set_if_missing(landing_default_args, "--EchoFolder", "fmmi")
    _set_if_missing(landing_default_args, "--FileNames", "FMMI.FSA*")
    _set_if_missing(landing_default_args, "--PipelineName", pipeline_name)

    # STG/ODS job default args — match S3-STG-ODS-parquet.py getResolvedOptions
    _set_if_missing(stg_ods_default_args, "--SecretId", echo_secret_id)
    _set_if_missing(stg_ods_default_args, "--env", str(deploy_env))
    _set_if_missing(stg_ods_default_args, "--bucket_name", landing_bucket)
    _set_if_missing(stg_ods_default_args, "--folder_name", landing_folder)
    _set_if_missing(stg_ods_default_args, "--stg_bucket_name", clean_bucket)
    _set_if_missing(stg_ods_default_args, "--stg_folder_name", stg_folder)
    _set_if_missing(stg_ods_default_args, "--ods_bucket_name", final_bucket)
    _set_if_missing(stg_ods_default_args, "--ods_folder_name", ods_folder)
    _set_if_missing(stg_ods_default_args, "--PipelineName", pipeline_name)

    landing_conns = _parse_connection_names(landing_params)
    stg_ods_conns = _parse_connection_names(stg_ods_params)
    ora_dms_ods_conns = _parse_connection_names(ora_dms_ods_params)

    # Networking (optional VPC config for lambdas)
    subnet_ids, security_group_ids = parse_networking(cfg)

    # Clients
    session = boto3.Session(region_name=region)
    s3 = session.client("s3")
    lam = session.client("lambda")
    glue = session.client("glue")
    sfn = session.client("stepfunctions")

    ensure_bucket_exists(s3, artifact_bucket, region)

    # Script S3 keys (renamed with FSA-<env>-<proj>- prefix)
    landing_script_s3_key = _s3_script_key(prefix, deploy_env, project, landing_script_local.name)
    stg_ods_script_s3_key = _s3_script_key(prefix, deploy_env, project, stg_ods_script_local.name)
    ora_dms_ods_script_s3_key = _s3_script_key(prefix, deploy_env, project, ora_dms_ods_script_local.name)

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

    ensure_glue_job(
        glue,
        s3,
        GlueJobSpec(
            name=names.ora_dms_ods_glue_job,
            role_arn=glue_job_role_arn,
            script_local_path=str(ora_dms_ods_script_local),
            script_s3_bucket=artifact_bucket,
            script_s3_key=ora_dms_ods_script_s3_key,
            default_args=ora_dms_ods_default_args,
            glue_version=str(ora_dms_ods_d["GlueVersion"]),
            worker_type=str(ora_dms_ods_d["WorkerType"]),
            number_of_workers=int(ora_dms_ods_d["NumberOfWorkers"]),
            timeout_minutes=int(ora_dms_ods_d["TimeoutMinutes"]),
            max_retries=int(ora_dms_ods_d["MaxRetries"]),
            max_concurrency=int(ora_dms_ods_d["MaxConcurrency"]),
            connection_names=ora_dms_ods_conns,
        ),
    )

    # --- Lambda functions ---
    base_lambda_env: Dict[str, str] = {
        "PROJECT": project,
        "LANDING_BUCKET": landing_bucket,
        "BUCKET_REGION": bucket_region,
    }
    # Extra env vars from cfg.lambdaEnv override base values
    base_lambda_env.update(cfg.get("lambdaEnv") or {})

    check_ods_nofiles_env = {
        **base_lambda_env,
        "LANDING_PREFIX": landing_prefix,
    }
    ods_crawler_env = {
        **base_lambda_env,
        "CRAWLER_NAME": names.fmmi_crawler,
    }

    lambda_root = project_dir / "lambda"

    check_ods_nofiles_arn = ensure_lambda(
        lam,
        LambdaSpec(
            name=names.check_ods_nofiles_fn,
            role_arn=etl_lambda_role_arn,
            handler="lambda_function.handler",
            runtime=lambda_runtime,
            source_dir=str(lambda_root / "Check-FMMI_ODS-NoFiles"),
            env=check_ods_nofiles_env,
            layers=layers,
            subnet_ids=subnet_ids if subnet_ids else None,
            security_group_ids=security_group_ids if security_group_ids else None,
        ),
    )

    ods_crawler_arn = ensure_lambda(
        lam,
        LambdaSpec(
            name=names.ods_crawler_fn,
            role_arn=etl_lambda_role_arn,
            handler="lambda_function.handler",
            runtime=lambda_runtime,
            source_dir=str(lambda_root / "FMMI_ODS-Crawler"),
            env=ods_crawler_env,
            layers=layers,
            subnet_ids=subnet_ids if subnet_ids else None,
            security_group_ids=security_group_ids if security_group_ids else None,
        ),
    )

    # --- Step Function ---
    # All infrastructure values are substituted into the ASL at deploy time.
    # Step Functions does not support Resource.$ dynamic references, and baking
    # values in avoids requiring any infrastructure parameters at execution time.
    # Only runtime input when starting an execution: use_existing_landing (optional bool).
    _asl_text = json.dumps(_load_asl_definition(project_dir))
    _substitutions = {
        "__ECHO_LANDING_JOB_NAME__": names.echo_landing_glue_job,
        "__STG_ODS_JOB_NAME__":      names.stg_ods_glue_job,
        "__CHECK_NOFILES_LAMBDA_ARN__": check_ods_nofiles_arn,
        "__CRAWLER_LAMBDA_ARN__":    ods_crawler_arn,
        "__SNS_TOPIC_ARN__":         sns_topic_arn,
        "__ENV__":                   str(deploy_env),
        "__LANDING_BUCKET__":        landing_bucket,
        "__LANDING_FOLDER__":        landing_folder,
        "__STG_BUCKET__":            clean_bucket,
        "__STG_FOLDER__":            stg_folder,
        "__ODS_BUCKET__":            final_bucket,
        "__ODS_FOLDER__":            ods_folder,
        "__ECHO_SECRET_ID__":        echo_secret_id,
        "__PIPELINE_NAME__":         pipeline_name,
    }
    for placeholder, value in _substitutions.items():
        _asl_text = _asl_text.replace(placeholder, value)
    definition = json.loads(_asl_text)

    sfn_arn = ensure_state_machine(
        sfn,
        StateMachineSpec(
            name=names.fmmi_state_machine,
            role_arn=sfn_role_arn,
            definition=definition,
        ),
    )

    # --- OPTIONAL: Crawler ---
    crawler_result = "skipped"
    crawler_name_used = ""
    crawler_db_used = ""

    crawler_cfg = cfg.get("crawler")
    if isinstance(crawler_cfg, dict) and crawler_cfg:
        # base prefix aligns with glue + step function: FSA-<dep>-<proj>
        prefix_base = f"FSA-{(deploy_env or '').strip()}-{(project or '').strip()}"

        raw_name = _as_str(crawler_cfg.get("name"))
        if raw_name:
            # if user provided a full FSA-* name, accept it; otherwise suffix it
            crawler_name_used = raw_name if raw_name.startswith("FSA-") else f"{prefix_base}-{raw_name}"
        else:
            crawler_name_used = names.fmmi_crawler

        crawler_desc = _as_str(crawler_cfg.get("description"))
        s3_targets = _build_crawler_s3_targets(crawler_cfg)

        # Prefer crawler.databaseName if present, else configData.databaseName
        crawler_db_used = _as_str(crawler_cfg.get("databaseName")) or _as_str((cfg.get("configData") or {}).get("databaseName"))
        if not crawler_db_used:
            raise RuntimeError("crawler.databaseName (preferred) or cfg.configData.databaseName is required when crawler is enabled")

        # Determine crawler role:
        # 1) crawler.roleArn
        # 2) strparams.glueCrawlerRoleArnParam
        # 3) fallback to strparams.glueJobRoleArnParam
        crawler_role_arn = (
            _as_str(crawler_cfg.get("roleArn"))
            or _as_str((cfg.get("strparams") or {}).get("glueCrawlerRoleArnParam"))
            or glue_job_role_arn
        )

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

    return {
        # ---- deploy metadata ----
        "deploy_env": str(deploy_env),
        "project": str(project),
        "artifact_bucket": artifact_bucket,
        "artifact_prefix": prefix,

        # ---- Glue jobs ----
        "glue_job_landing_name": names.echo_landing_glue_job,
        "glue_job_stg_ods_name": names.stg_ods_glue_job,
        "glue_job_ora_dms_ods_name": names.ora_dms_ods_glue_job,
        "glue_landing_script_s3_key": landing_script_s3_key,
        "glue_stg_ods_script_s3_key": stg_ods_script_s3_key,
        "glue_ora_dms_ods_script_s3_key": ora_dms_ods_script_s3_key,
        "glue_config_used_for_landing": "yes" if bool(landing_params) else "no",
        "glue_config_used_for_stg_ods": "yes" if bool(stg_ods_params) else "no",
        "glue_config_used_for_ora_dms_ods": "yes" if bool(ora_dms_ods_params) else "no",

        # ---- Lambda ARNs ----
        "check_ods_nofiles_lambda_arn": check_ods_nofiles_arn,
        "ods_crawler_lambda_arn": ods_crawler_arn,

        # ---- Step Function ----
        "state_machine_name": names.fmmi_state_machine,
        "state_machine_arn": sfn_arn,

        # ---- Glue Crawler ----
        "crawler": crawler_result,
        "crawler_name": crawler_name_used,
        "crawler_database": crawler_db_used,

        # ---- State machine execution input template (all $.xxx keys the ASL expects) ----
        # Callers start the state machine with this as the base input, adding:
        #   use_existing_landing (optional)
        "sm_input_echo_landing_job_name": names.echo_landing_glue_job,
        "sm_input_stg_ods_job_name": names.stg_ods_glue_job,
        "sm_input_check_nofiles_lambda_arn": check_ods_nofiles_arn,
        "sm_input_crawler_lambda_arn": ods_crawler_arn,
        "sm_input_sns_topic_arn": sns_topic_arn,
        "sm_input_env": str(deploy_env),
        "sm_input_landing_bucket": landing_bucket,
        "sm_input_landing_folder": landing_folder,
        "sm_input_stg_bucket": clean_bucket,
        "sm_input_stg_folder": stg_folder,
        "sm_input_ods_bucket": final_bucket,
        "sm_input_ods_folder": ods_folder,
    }
