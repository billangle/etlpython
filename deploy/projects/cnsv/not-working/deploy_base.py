# deploy/projects/cnsv/deploy_base.py
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import boto3

from common.aws_common import (
    ensure_glue_job,
    ensure_glue_crawler,
    ensure_state_machine,
    ensure_lambda,
    GlueJobSpec,
    GlueCrawlerSpec,
    StateMachineSpec,
    LambdaSpec,
)

# --------------------------------------------------------------------------------------
# CNSV "base" deployer
#
# Responsibilities:
#   - Create/Update Glue jobs (LandingFiles.py, Raw-DM.py)
#   - Create/Update Lambdas (folders under ./lambda)
#   - Create/Update Glue crawlers (from cfg["crawlers"])
#   - Materialize (bind) the 4 parameterized ASL definitions *at deploy time* (Option A)
#     and deploy them as Step Functions using cfg.stepFunctions.roleArn.
#
# Hard rule:
#   - No hardcoded environment values inside ASL. All service identifiers are bound here.
# --------------------------------------------------------------------------------------

# --------------------------------------------------------------------------------------
# Safe parsers (match working deploy_* style)
# --------------------------------------------------------------------------------------

def _as_str(v: Any, default: str = "") -> str:
    if v is None:
        return default
    if isinstance(v, str):
        return v.strip()
    return str(v)


def _as_int(v: Any, default: int) -> int:
    if v is None or v == "":
        return default
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, (int, float)):
        return int(v)
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return default
        try:
            return int(float(s))
        except Exception:
            return default
    return default


def _as_dict(v: Any) -> Dict[str, Any]:
    return v if isinstance(v, dict) else {}


def _as_list(v: Any) -> List[Any]:
    return v if isinstance(v, list) else []


# --------------------------------------------------------------------------------------
# Names
# --------------------------------------------------------------------------------------

@dataclass(frozen=True)
class CnsvNames:
    prefix: str

    # Glue jobs
    landing_glue_job: str
    raw_dm_glue_job: str

    # Lambdas
    fn_get_incremental_tables: str
    fn_job_logging_end: str
    fn_raw_dm_etl_workflow_update: str
    fn_raw_dm_sns_publish_errors: str
    fn_sns_publish_validations_report: str

    # Step Functions
    sm_incremental_to_s3landing: str
    sm_s3landing_to_s3final_raw_dm: str
    sm_process_control_update: str
    sm_main: str


def build_names(deploy_env: str, project: str) -> CnsvNames:
    """
    Prefix convention:
      FSA-<deployEnv>-<project>-<suffix>
    """
    prefix = f"FSA-{deploy_env}-{project}"
    return CnsvNames(
        prefix=prefix,
        landing_glue_job=f"{prefix}-LandingFiles",
        raw_dm_glue_job=f"{prefix}-Raw-DM",
        fn_get_incremental_tables=f"{prefix}-get-incremental-tables",
        fn_job_logging_end=f"{prefix}-Job-Logging-End",
        fn_raw_dm_etl_workflow_update=f"{prefix}-RAW-DM-etl-workflow-update-data-ppln-job",
        fn_raw_dm_sns_publish_errors=f"{prefix}-RAW-DM-sns-publish-step-function-errors",
        fn_sns_publish_validations_report=f"{prefix}-sns-publish-validations-report",
        sm_incremental_to_s3landing=f"{prefix}-Incremental-to-S3Landing",
        sm_s3landing_to_s3final_raw_dm=f"{prefix}-S3Landing-to-S3Final-Raw-DM",
        sm_process_control_update=f"{prefix}-Process-Control-Update",
        sm_main=f"{prefix}-Main",
    )


# --------------------------------------------------------------------------------------
# ASL binding (Option A)
#   - For any dict key ending in '.$' and value '$.foo'
#     if 'foo' exists in bindings, replace with literal and drop '.$'
#   - Otherwise leave as runtime JSONPath.
# --------------------------------------------------------------------------------------

_JSONPATH_RE = re.compile(r"^\$\.(?P<key>[A-Za-z0-9_\-]+)$")


def _bind_placeholders(node: Any, bindings: Dict[str, Any]) -> Any:
    if isinstance(node, list):
        return [_bind_placeholders(x, bindings) for x in node]

    if isinstance(node, dict):
        out: Dict[str, Any] = {}
        for k, v in node.items():
            if isinstance(k, str) and k.endswith(".$") and isinstance(v, str):
                m = _JSONPATH_RE.match(v.strip())
                if m:
                    key = m.group("key")
                    if key in bindings:
                        out[k[:-2]] = bindings[key]
                        continue
            out[k] = _bind_placeholders(v, bindings)
        return out

    return node


_PARAM_ASL_FILES = [
    "Incremental-to-S3Landing.param.asl.json",
    "S3Landing-to-S3Final-Raw-DM.param.asl.json",
    "Process-Control-Update.param.asl.json",
    "Main.param.asl.json",
]


def _load_asl_file(project_dir: Path, filename: str) -> Dict[str, Any]:
    path = project_dir / "states" / filename
    if not path.exists():
        raise FileNotFoundError(f"Missing ASL file: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


# --------------------------------------------------------------------------------------
# Glue config lookup (your dev.json shape)
#   "GlueConfig": [ {"LandingFiles": {...}}, {"Raw-DM": {...}} ]
# --------------------------------------------------------------------------------------

def _glue_config(cfg: Dict[str, Any], section: str) -> Dict[str, Any]:
    root = cfg.get("GlueConfig")
    if isinstance(root, list):
        for item in root:
            if isinstance(item, dict) and section in item and isinstance(item[section], dict):
                return item[section]
        return {}
    if isinstance(root, dict) and isinstance(root.get(section), dict):
        return root[section]
    return {}


def _defaults_glue(params: Dict[str, Any], default_worker: str = "G.1X", default_workers: int = 2) -> Dict[str, Any]:
    gp = _as_dict(params.get("GlueJobParameters"))
    d = {
        "GlueVersion": _as_str(gp.get("GlueVersion"), "4.0"),
        "WorkerType": _as_str(gp.get("WorkerType"), default_worker),
        "NumberOfWorkers": _as_int(gp.get("NumberOfWorkers"), default_workers),
        "TimeoutMinutes": _as_int(gp.get("TimeoutMinutes"), 60),
        "MaxRetries": _as_int(gp.get("MaxRetries"), 0),
        "MaxConcurrency": _as_int(gp.get("MaxConcurrency"), 1),
        # Optional: Connections (Glue job connections)
        "ConnectionNames": [],
    }
    conns = gp.get("Connections")
    # allow either {"Connections": [{"ConnectionName": "X"}, ...]} or list of names or list of dicts
    if isinstance(conns, list):
        names: List[str] = []
        for c in conns:
            if isinstance(c, str) and c.strip():
                names.append(c.strip())
            elif isinstance(c, dict) and isinstance(c.get("ConnectionName"), str) and c["ConnectionName"].strip():
                names.append(c["ConnectionName"].strip())
        d["ConnectionNames"] = names
    return d


def _default_args(params: Dict[str, Any]) -> Dict[str, str]:
    da = _as_dict(params.get("DefaultArguments"))
    out: Dict[str, str] = {}
    for k, v in da.items():
        if k is None:
            continue
        ks = str(k)
        if v is None:
            continue
        out[ks] = str(v)
    return out


def _s3_script_key(artifact_prefix: str, deploy_env: str, project: str, filename: str) -> str:
    # Keep consistent with working deployers: <prefix>/glue/<FSA-ENV-PROJ-Name>.py
    pfx = artifact_prefix.rstrip("/") + "/" if artifact_prefix else ""
    return f"{pfx}glue/FSA-{deploy_env}-{project}-{Path(filename).stem}.py"


# --------------------------------------------------------------------------------------
# Crawlers parsing:
# You have shapes like:
#   "crawlers": [ {"crawler1": {...}, "crawler2": {...}, ... } ]
# or:
#   "crawlers": { "crawler1": {...}, ... }
# or:
#   "crawlers": [ {...}, {...} ]
# --------------------------------------------------------------------------------------

def _iter_crawlers(root: Any) -> Iterable[Dict[str, Any]]:
    if isinstance(root, dict):
        for v in root.values():
            if isinstance(v, dict):
                yield v
        return
    if isinstance(root, list):
        for item in root:
            if not isinstance(item, dict):
                continue
            # direct crawler object
            if "name" in item and "databaseName" in item:
                yield item
                continue
            # dict of crawler keys
            for v in item.values():
                if isinstance(v, dict):
                    yield v
        return


# --------------------------------------------------------------------------------------
# Deploy
# --------------------------------------------------------------------------------------

def deploy(cfg: Dict[str, Any], region: str) -> Dict[str, Any]:
    deploy_env = _as_str(cfg.get("deployEnv"))
    project = _as_str(cfg.get("project"))
    if not deploy_env:
        raise RuntimeError("Missing required cfg.deployEnv")
    if not project:
        raise RuntimeError("Missing required cfg.project")

    names = build_names(deploy_env, project)

    artifacts = _as_dict(cfg.get("artifacts"))
    artifact_bucket = _as_str(artifacts.get("artifactBucket"))
    artifact_prefix = _as_str(artifacts.get("prefix"))  # may be "" or "cnsv/"
    if not artifact_bucket:
        raise RuntimeError("Missing required cfg.artifacts.artifactBucket")

    strparams = _as_dict(cfg.get("strparams"))
    landing_bucket = _as_str(strparams.get("landingBucketNameParam"))
    clean_bucket = _as_str(strparams.get("cleanBucketNameParam"))
    final_bucket = _as_str(strparams.get("finalBucketNameParam"))
    glue_job_role_arn = _as_str(strparams.get("glueJobRoleArnParam"))
    etl_role_arn = _as_str(strparams.get("etlRoleArnParam"))
    sns_topic_arn = _as_str(strparams.get("snsArnParam"))

    if not glue_job_role_arn:
        raise RuntimeError("Missing required cfg.strparams.glueJobRoleArnParam")
    if not etl_role_arn:
        raise RuntimeError("Missing required cfg.strparams.etlRoleArnParam")

    sfn_role_arn = _as_str(_as_dict(cfg.get("stepFunctions")).get("roleArn"))
    if not sfn_role_arn:
        raise RuntimeError("Missing required cfg.stepFunctions.roleArn")

    # Inputs used by ASL templates (pulled from cfg)
    pipeline_name = _as_str(cfg.get("pipeline_name") or cfg.get("pipelineName") or project)
    secret_id = _as_str(cfg.get("secretId") or cfg.get("secret_id"))
    source_prefix = _as_str(cfg.get("source_prefix") or cfg.get("sourcePrefix") or artifacts.get("prefix") or "")

    job_id_bucket = _as_str(cfg.get("job_id_bucket") or cfg.get("jobIdBucket") or landing_bucket)
    job_id_prefix = _as_str(cfg.get("job_id_prefix") or cfg.get("jobIdPrefix") or artifacts.get("prefix") or "")
    job_id_key = _as_str(cfg.get("job_id_key") or cfg.get("jobIdKey") or "")

    target_bucket = _as_str(cfg.get("target_bucket") or cfg.get("targetBucket") or final_bucket)
    target_prefix = _as_str(cfg.get("target_prefix") or cfg.get("targetPrefix") or artifacts.get("prefix") or "")

    config_data = _as_dict(cfg.get("configData"))
    postgres_prcs_ctrl_dbname = _as_str(
        cfg.get("postgres_prcs_ctrl_dbname")
        or cfg.get("postgresPrcsCtrlDbname")
        or config_data.get("postgres_prcs_ctrl_dbname")
        or ""
    )
    step_seq_nbr = _as_str(cfg.get("step_seq_nbr") or cfg.get("stepSeqNbr") or "")

    enable_dq_metrics = _as_str(cfg.get("enable_dq_metrics") or cfg.get("enableDqMetrics") or "false")
    skip_count_tables = _as_str(cfg.get("skip_count_tables") or cfg.get("skipCountTables") or "")
    skip_dq_tables = _as_str(cfg.get("skip_dq_tables") or cfg.get("skipDqTables") or "")

    # Local paths
    project_dir = Path(__file__).resolve().parent
    glue_root = project_dir / "glue"
    lambda_root = project_dir / "lambda"

    landing_script_local = glue_root / "LandingFiles.py"
    raw_dm_script_local = glue_root / "Raw-DM.py"

    if not landing_script_local.exists():
        raise FileNotFoundError(f"Missing Glue script: {landing_script_local}")
    if not raw_dm_script_local.exists():
        raise FileNotFoundError(f"Missing Glue script: {raw_dm_script_local}")

    # Load ASLs
    raw_asls: Dict[str, Dict[str, Any]] = {}
    for fn in _PARAM_ASL_FILES:
        raw_asls[fn] = _load_asl_file(project_dir, fn)

    # Clients
    session = boto3.Session(region_name=region)
    s3 = session.client("s3")
    glue = session.client("glue")
    sfn = session.client("stepfunctions")
    lam = session.client("lambda")

    # --- Glue jobs ---
    landing_params = _glue_config(cfg, "LandingFiles")
    raw_dm_params = _glue_config(cfg, "Raw-DM")

    landing_d = _defaults_glue(landing_params, default_worker="G.1X", default_workers=2)
    raw_dm_d = _defaults_glue(raw_dm_params, default_worker="G.1X", default_workers=2)

    landing_default_args = _default_args(landing_params)
    raw_dm_default_args = _default_args(raw_dm_params)

    landing_script_s3_key = f"{artifact_prefix.rstrip('/') + '/' if artifact_prefix else ''}glue/{names.landing_glue_job}.py"
    raw_dm_script_s3_key = f"{artifact_prefix.rstrip('/') + '/' if artifact_prefix else ''}glue/{names.raw_dm_glue_job}.py"

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
            connection_names=list(landing_d.get("ConnectionNames") or []),
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
            connection_names=list(raw_dm_d.get("ConnectionNames") or []),
        ),
    )

    # --- Lambdas ---
    # Config-driven common lambda settings
    lambda_env = _as_dict(cfg.get("lambdaEnv"))
    lambda_layers = [x for x in _as_list(cfg.get("lambdaLayers")) if isinstance(x, str) and x.strip()]

    # optional VPC settings (if present in cfg)
    vpc_cfg = _as_dict(cfg.get("lambdaVpc") or cfg.get("vpc") or {})
    subnet_ids = vpc_cfg.get("subnetIds") if isinstance(vpc_cfg.get("subnetIds"), list) else None
    sg_ids = vpc_cfg.get("securityGroupIds") if isinstance(vpc_cfg.get("securityGroupIds"), list) else None

    def _deploy_lambda(fn_name: str, folder: str) -> str:
        src_dir = lambda_root / folder
        if not src_dir.exists():
            raise FileNotFoundError(f"Missing Lambda source dir: {src_dir}")
        spec = LambdaSpec(
            name=fn_name,
            role_arn=etl_role_arn,
            handler="lambda_function.handler",
            runtime="python3.11",
            source_dir=str(src_dir),
            env={k: str(v) for k, v in lambda_env.items()},
            layers=lambda_layers,
            subnet_ids=subnet_ids,
            security_group_ids=sg_ids,
        )
        return ensure_lambda(lam, spec)

    lambda_arns: Dict[str, str] = {}
    lambda_arns[names.fn_get_incremental_tables] = _deploy_lambda(names.fn_get_incremental_tables, "get-incremental-tables")
    lambda_arns[names.fn_job_logging_end] = _deploy_lambda(names.fn_job_logging_end, "Job-Logging-End")
    lambda_arns[names.fn_raw_dm_etl_workflow_update] = _deploy_lambda(names.fn_raw_dm_etl_workflow_update, "RAW-DM-etl-workflow-update-data-ppln-job")
    lambda_arns[names.fn_raw_dm_sns_publish_errors] = _deploy_lambda(names.fn_raw_dm_sns_publish_errors, "RAW-DM-sns-publish-step-function-errors")
    lambda_arns[names.fn_sns_publish_validations_report] = _deploy_lambda(names.fn_sns_publish_validations_report, "sns-publish-validations-report")

    # --- Crawlers ---
    # We create/update all crawlers defined in cfg, but we also pick one "final" and one "cdc"
    final_crawler_name = ""
    cdc_crawler_name = ""

    for crawler in _iter_crawlers(cfg.get("crawlers")):
        crawler_name = _as_str(crawler.get("name"))
        db_name = _as_str(crawler.get("databaseName"))
        if not crawler_name or not db_name:
            continue

        # Prefer databaseName from crawler JSON (per your request)
        s3_targets_by_bucket = crawler.get("s3TargetsByBucket")
        if not isinstance(s3_targets_by_bucket, list):
            s3_targets_by_bucket = []

        # Create at least one spec per target path (aws_common spec supports a single target_s3_path)
        for group in s3_targets_by_bucket:
            if not isinstance(group, dict):
                continue
            bucket = _as_str(group.get("bucket"))
            prefixes = group.get("prefixes")
            if not isinstance(prefixes, list):
                prefixes = []
            for pfx in prefixes:
                p = _as_str(pfx)
                if not bucket or p == "":
                    continue
                s3_path = f"s3://{bucket}/{p}"
                ensure_glue_crawler(
                    glue,
                    GlueCrawlerSpec(
                        name=crawler_name,
                        role_arn=glue_job_role_arn,
                        database_name=db_name,
                        target_s3_path=s3_path,
                    ),
                )

        if crawler_name.endswith("-cdc"):
            cdc_crawler_name = crawler_name
        elif not final_crawler_name:
            final_crawler_name = crawler_name

    # --- Bind ASLs ---
    bindings: Dict[str, Any] = {
        "env": deploy_env,
        "region": region,
        "secret_id": secret_id,
        "pipeline_name": pipeline_name,

        "source_prefix": source_prefix,

        "job_id_bucket": job_id_bucket,
        "job_id_prefix": job_id_prefix,
        "job_id_key": job_id_key,

        "target_bucket": target_bucket,
        "target_prefix": target_prefix,

        "landing_glue_job_name": names.landing_glue_job,
        "raw_dm_glue_job_name": names.raw_dm_glue_job,

        # Map.MaxConcurrency must be integer
        "map_max_concurrency": int(raw_dm_d["MaxConcurrency"]),

        "postgres_prcs_ctrl_dbname": postgres_prcs_ctrl_dbname,
        "step_seq_nbr": step_seq_nbr,

        "enable_dq_metrics": enable_dq_metrics,
        "skip_count_tables": skip_count_tables,
        "skip_dq_tables": skip_dq_tables,

        # lambdas (ASL expects these exact keys)
        "get_incremental_tables_lambda_arn": lambda_arns[names.fn_get_incremental_tables],
        "job_logging_end_lambda_arn": lambda_arns[names.fn_job_logging_end],
        "raw_dm_update_lambda_arn": lambda_arns[names.fn_raw_dm_etl_workflow_update],
        "raw_dm_error_lambda_arn": lambda_arns[names.fn_raw_dm_sns_publish_errors],
        "sns_publish_validations_report_lambda_arn": lambda_arns[names.fn_sns_publish_validations_report],
        "validation_check_lambda_arn": lambda_arns[names.fn_sns_publish_validations_report],

        # buckets sometimes referenced
        "final_bucket": final_bucket,

        # crawlers
        "final_crawler_name": final_crawler_name,
        "cdc_crawler_name": cdc_crawler_name,
    }

    def _bound(fn: str) -> Dict[str, Any]:
        return _bind_placeholders(raw_asls[fn], bindings)

    # Create child SMs first (main references them)
    sm_incremental_arn = ensure_state_machine(
        sfn,
        StateMachineSpec(
            name=names.sm_incremental_to_s3landing,
            role_arn=sfn_role_arn,
            definition=_bound("Incremental-to-S3Landing.param.asl.json"),
        ),
    )
    bindings["incremental_to_s3landing_sfn_arn"] = sm_incremental_arn

    sm_s3landing_arn = ensure_state_machine(
        sfn,
        StateMachineSpec(
            name=names.sm_s3landing_to_s3final_raw_dm,
            role_arn=sfn_role_arn,
            definition=_bound("S3Landing-to-S3Final-Raw-DM.param.asl.json"),
        ),
    )
    bindings["s3landing_to_s3final_raw_dm_sfn_arn"] = sm_s3landing_arn
    bindings["s3landing_to_s3final_raw_dm_sfn_name"] = names.sm_s3landing_to_s3final_raw_dm

    sm_pc_arn = ensure_state_machine(
        sfn,
        StateMachineSpec(
            name=names.sm_process_control_update,
            role_arn=sfn_role_arn,
            definition=_bound("Process-Control-Update.param.asl.json"),
        ),
    )
    bindings["process_control_update_sfn_arn"] = sm_pc_arn

    sm_main_arn = ensure_state_machine(
        sfn,
        StateMachineSpec(
            name=names.sm_main,
            role_arn=sfn_role_arn,
            definition=_bind_placeholders(raw_asls["Main.param.asl.json"], bindings),
        ),
    )

    return {
        "deploy_env": deploy_env,
        "project": project,
        "artifact_bucket": artifact_bucket,
        "artifact_prefix": artifact_prefix,
        "stepfunctions_role_arn": sfn_role_arn,
        "state_machine_incremental": sm_incremental_arn,
        "state_machine_raw_dm": sm_s3landing_arn,
        "state_machine_process_control": sm_pc_arn,
        "state_machine_main": sm_main_arn,
        "glue_job_landing": names.landing_glue_job,
        "glue_job_raw_dm": names.raw_dm_glue_job,
        "crawler_final": final_crawler_name,
        "crawler_cdc": cdc_crawler_name,
    }
