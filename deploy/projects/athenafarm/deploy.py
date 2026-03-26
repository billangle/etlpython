# deploy/projects/athenafarm/deploy.py
"""
Athenafarm deployer.

Version History:
    - 2026-03-25: Replaced direct tract transform with nested tract split pipeline
        state machine and TPY-01..TPY-10 Glue jobs.
    - 2026-03-26: Deployed TPY runtime hotfix that makes TPY-01 lightweight
        (ibsp-only) and shifts structure/farm/in_guid expansion into TPY-02.

Deploys the following AWS resources for the athenafarm ETL pipeline:
  - 7 Glue Spark jobs (Iceberg-enabled)
  - 2 Step Functions state machines (Main + Maintenance)
  - S3 script uploads

Usage (via master deployer — preferred):
    python ../../deploy.py --config ../../config/athenafarm/dev.json --region us-east-1 --project-type athenafarm
    python ../../deploy.py --config ../../config/athenafarm/prod.json --region us-east-1 --project-type athenafarm

Usage (standalone):
    python deploy.py --config ../../config/athenafarm/dev.json [--dry-run]

All names are derived from cfg.deployEnv + cfg.project — nothing is hardcoded.

Resource naming convention (mirrors cnsv deployer):
    FSA-{deployEnv}-{project}-{ScriptStem}   → Glue job
    FSA-{deployEnv}-{project}-Main            → Step Functions main pipeline
    FSA-{deployEnv}-{project}-Maintenance     → Step Functions maintenance pipeline
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import boto3

# Add the deploy/ root to the path so common imports work
_HERE = Path(__file__).resolve().parent
_DEPLOY_ROOT = _HERE.parent.parent
sys.path.insert(0, str(_DEPLOY_ROOT))

from common.aws_common import (
    ensure_bucket_exists,
    ensure_glue_job,
    ensure_lambda,
    ensure_state_machine,
    GlueJobSpec,
    LambdaSpec,
    StateMachineSpec,
    s3_upload_file,
)

# ---------------------------------------------------------------------------
# Safe parsers (consistent with cnsv deployers)
# ---------------------------------------------------------------------------

def _as_str(v: Any, default: str = "") -> str:
    if v is None:
        return default
    s = str(v)
    return s if s.strip() else default


def _as_int(v: Any, default: int) -> int:
    if v is None or v == "":
        return default
    try:
        return int(str(v).strip())
    except Exception:
        return default


def _as_bool(v: Any, default: bool = False) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.strip().lower() in ("true", "1", "yes", "y", "on")
    return bool(v)


def _as_dict(v: Any) -> Dict[str, Any]:
    return v if isinstance(v, dict) else {}


def _as_str_dict(v: Any) -> Dict[str, str]:
    if not isinstance(v, dict):
        return {}
    return {str(k): str(val) if val is not None else "" for k, val in v.items()}


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def _glue_config_for_script(cfg: Dict[str, Any], stem: str) -> Dict[str, Any]:
    """Extract per-script GlueConfig block from the array-of-objects pattern."""
    root = cfg.get("GlueConfig")
    if not isinstance(root, list):
        return {}
    for item in root:
        if isinstance(item, dict) and stem in item:
            return item[stem] if isinstance(item[stem], dict) else {}
    return {}


def _merge_default_args(defaults: Dict[str, Any], overrides: Dict[str, Any]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for k, v in defaults.items():
        out[str(k)] = "" if v is None else str(v)
    for k, v in overrides.items():
        out[str(k)] = "" if v is None else str(v)
    return out


# ---------------------------------------------------------------------------
# Names
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Names:
    prefix: str
    # Glue jobs
    ingest_sss_job: str
    ingest_pg_refs_job: str
    ingest_pg_cdc_job: str
    transform_fpy_job: str
    # Tract split pipeline jobs
    tpy_j1_spine_base_job: str
    tpy_j2_instance_guid_job: str
    tpy_j3_partner_map_job: str
    tpy_j4_zmi_map_job: str
    tpy_j5_coc_time_map_job: str
    tpy_j6_candidate_assemble_job: str
    tpy_j7_partner_customer_job: str
    tpy_j8_farm_tract_resolve_job: str
    tpy_j9_tract_year_resolve_job: str
    tpy_j10_publish_job: str
    sync_rds_job: str
    iceberg_maint_job: str
    # Lambda functions
    notify_fn: str
    # State machines
    sm_main: str
    sm_tract_pipeline: str
    sm_maintenance: str


def build_names(deploy_env: str, project: str) -> Names:
    dep = (deploy_env or "").strip()
    proj = (project or "").strip()
    if not dep:
        raise RuntimeError("Missing cfg['deployEnv']")
    if not proj:
        raise RuntimeError("Missing cfg['project']")
    pfx = f"FSA-{dep}-{proj}"
    return Names(
        prefix=pfx,
        ingest_sss_job=f"{pfx}-Ingest-SSS-Farmrecords",
        ingest_pg_refs_job=f"{pfx}-Ingest-PG-Reference-Tables",
        ingest_pg_cdc_job=f"{pfx}-Ingest-PG-CDC-Targets",
        transform_fpy_job=f"{pfx}-Transform-Farm-Producer-Year",
        tpy_j1_spine_base_job=f"{pfx}-TPY-01-SpineBase",
        tpy_j2_instance_guid_job=f"{pfx}-TPY-02-InstanceGuidMap",
        tpy_j3_partner_map_job=f"{pfx}-TPY-03-PartnerMap",
        tpy_j4_zmi_map_job=f"{pfx}-TPY-04-ZmiMap",
        tpy_j5_coc_time_map_job=f"{pfx}-TPY-05-CocTimeMap",
        tpy_j6_candidate_assemble_job=f"{pfx}-TPY-06-TractCandidateAssemble",
        tpy_j7_partner_customer_job=f"{pfx}-TPY-07-PartnerCustomerResolve",
        tpy_j8_farm_tract_resolve_job=f"{pfx}-TPY-08-FarmTractResolve",
        tpy_j9_tract_year_resolve_job=f"{pfx}-TPY-09-TractYearResolve",
        tpy_j10_publish_job=f"{pfx}-TPY-10-DedupAndPublish",
        sync_rds_job=f"{pfx}-Sync-Iceberg-To-RDS",
        iceberg_maint_job=f"{pfx}-Iceberg-Maintenance",
        notify_fn=f"{pfx}-NotifyPipeline",
        sm_main=f"{pfx}-Main",
        sm_tract_pipeline=f"{pfx}-TractProducerYear",
        sm_maintenance=f"{pfx}-Maintenance",
    )


# ---------------------------------------------------------------------------
# ASL loader + token substitution
# ---------------------------------------------------------------------------

def _load_asl(project_dir: Path, filename: str) -> Dict[str, Any]:
    p = project_dir / "states" / filename
    if not p.exists():
        raise FileNotFoundError(f"Missing ASL file: {p}")
    return json.loads(p.read_text(encoding="utf-8"))


def _substitute(obj: Any, tokens: Dict[str, str]) -> Any:
    if isinstance(obj, str):
        for k, v in tokens.items():
            obj = obj.replace(k, v)
        return obj
    if isinstance(obj, dict):
        return {k: _substitute(v, tokens) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_substitute(i, tokens) for i in obj]
    return obj


def _assert_main_state_machine_full_load_only(definition: Dict[str, Any], sm_name: str) -> None:
    """
    Defensive guard: the Main pipeline must never dispatch tract incremental.
    Fail deployment immediately if any incremental tract wiring is present.
    """
    text = json.dumps(definition, sort_keys=True)
    forbidden_markers = [
        "Transform-Tract-Producer-Year-Incremental",
        "__TRANSFORM_TRACT_PY_INCR_GLUE_JOB_NAME__",
        "ChooseTransformTractMode",
    ]
    for marker in forbidden_markers:
        if marker in text:
            raise RuntimeError(
                f"{sm_name}: invalid tract incremental reference detected in rendered state machine definition: {marker}"
            )


# ---------------------------------------------------------------------------
# Glue script uploader
# ---------------------------------------------------------------------------

def _upload_glue_script(s3, glue_dir: Path, script_name: str,
                         artifact_bucket: str, prefix: str,
                         deploy_env: str, project: str) -> Tuple[str, Path]:
    local = glue_dir / f"{script_name}.py"
    if not local.exists():
        raise FileNotFoundError(f"Glue script not found: {local}")
    s3_key = f"{prefix}glue/FSA-{deploy_env}-{project}-{script_name}.py"
    s3_upload_file(s3, str(local), artifact_bucket, s3_key)
    print(f"  Uploaded {local.name} → s3://{artifact_bucket}/{s3_key}")
    return s3_key, local


# ---------------------------------------------------------------------------
# Main deployer
# ---------------------------------------------------------------------------

def deploy(cfg: Dict[str, Any], region: Optional[str] = None, dry_run: bool = False) -> Dict[str, Any]:
    deploy_env = _as_str(cfg.get("deployEnv"))
    project    = _as_str(cfg.get("project"))
    region     = _as_str(region or cfg.get("region") or "us-east-1")
    names      = build_names(deploy_env, project)

    # Artifacts
    artifacts       = _as_dict(cfg.get("artifacts"))
    artifact_bucket = _as_str(artifacts.get("artifactBucket"))
    prefix          = _as_str(artifacts.get("prefix")).rstrip("/") + "/"

    # strparams
    strparams          = _as_dict(cfg.get("strparams"))
    final_bucket       = _as_str(strparams.get("finalBucketNameParam"))
    glue_role_arn      = _as_str(strparams.get("glueJobRoleArnParam"))
    etl_lambda_role_arn = _as_str(strparams.get("etlRoleArnParam"))
    # sfnRoleArn: primary source is stepFunctions.roleArn (cnsv schema);
    # fall back to strparams.sfnRoleArnParam for legacy configs.
    _sfn_cfg           = _as_dict(cfg.get("stepFunctions"))
    sfn_role_arn       = _as_str(_sfn_cfg.get("roleArn")) or _as_str(strparams.get("sfnRoleArnParam"))
    sns_notify_fn_arn  = _as_str(strparams.get("snsNotifyFnArnParam"))
    pg_connection_name = _as_str(strparams.get("pgConnectionNameParam"))
    secret_id          = _as_str(cfg.get("secretId"))
    iceberg_warehouse  = _as_str(cfg.get("icebergWarehouse"))
    sss_s3_path        = _as_str(cfg.get("sssFarmrecordsS3Path"))

    missing = []
    if not artifact_bucket: missing.append("artifacts.artifactBucket")
    if not final_bucket:    missing.append("strparams.finalBucketNameParam")
    if not glue_role_arn:   missing.append("strparams.glueJobRoleArnParam")
    if not sfn_role_arn:    missing.append("stepFunctions.roleArn (or strparams.sfnRoleArnParam)")
    if not iceberg_warehouse: missing.append("icebergWarehouse")
    if missing:
        raise RuntimeError(f"Config is missing required fields: {missing}")

    # Global Glue defaults
    glue_defaults_cfg = _as_dict(cfg.get("glueJobDefaults"))
    default_worker    = _as_str(glue_defaults_cfg.get("WorkerType"), "G.2X")
    default_workers   = _as_int(glue_defaults_cfg.get("NumberOfWorkers"), 10)
    default_autosc    = _as_bool(glue_defaults_cfg.get("EnableAutoScaling"), True)
    default_timeout   = _as_int(glue_defaults_cfg.get("Timeout"), 60)
    default_args      = _as_str_dict(_as_dict(glue_defaults_cfg.get("DefaultArguments")))
    debug_logging     = _as_bool(cfg.get("debugLogging"), False)

    # Always inject the Iceberg warehouse path as a Spark --conf so that
    # glue_catalog is recognised as an Iceberg SparkCatalog at session startup.
    # (Setting spark.conf.set() after SparkContext creation is insufficient.)
    warehouse_conf = f"spark.sql.catalog.glue_catalog.warehouse={iceberg_warehouse}"
    if "--conf" in default_args:
        default_args["--conf"] += f" --conf {warehouse_conf}"
    else:
        default_args["--conf"] = warehouse_conf

    # ── Boto3 clients ────────────────────────────────────────────────────────
    s3_client  = boto3.client("s3",             region_name=region)
    glue       = boto3.client("glue",           region_name=region)
    lam        = boto3.client("lambda",         region_name=region)
    sfn        = boto3.client("stepfunctions",  region_name=region)

    project_dir = Path(__file__).resolve().parent
    glue_dir    = project_dir / "glue"

    if dry_run:
        print("[DRY RUN] No AWS resources will be created/modified.\n")

    ensure_bucket_exists(s3_client, artifact_bucket, region)

    # ── Script definitions (stem, job_name, per-script overrides) ───────────
    SCRIPT_SPECS = [
        ("Ingest-SSS-Farmrecords",          names.ingest_sss_job),
        ("Ingest-PG-Reference-Tables",      names.ingest_pg_refs_job),
        ("Ingest-PG-CDC-Targets",           names.ingest_pg_cdc_job),
        ("Transform-Farm-Producer-Year",    names.transform_fpy_job),
        ("TPY-01-SpineBase",                names.tpy_j1_spine_base_job),
        ("TPY-02-InstanceGuidMap",          names.tpy_j2_instance_guid_job),
        ("TPY-03-PartnerMap",               names.tpy_j3_partner_map_job),
        ("TPY-04-ZmiMap",                   names.tpy_j4_zmi_map_job),
        ("TPY-05-CocTimeMap",               names.tpy_j5_coc_time_map_job),
        ("TPY-06-TractCandidateAssemble",   names.tpy_j6_candidate_assemble_job),
        ("TPY-07-PartnerCustomerResolve",   names.tpy_j7_partner_customer_job),
        ("TPY-08-FarmTractResolve",         names.tpy_j8_farm_tract_resolve_job),
        ("TPY-09-TractYearResolve",         names.tpy_j9_tract_year_resolve_job),
        ("TPY-10-DedupAndPublish",          names.tpy_j10_publish_job),
        ("Sync-Iceberg-To-RDS",             names.sync_rds_job),
        ("Iceberg-Maintenance",             names.iceberg_maint_job),
    ]

    LEGACY_JOB_NAMES = [
        f"{names.prefix}-Transform-Tract-Producer-Year-Incremental",
        f"{names.prefix}-Transform-Tract-Producer-Year-FullLoad",
        f"{names.prefix}-Transform-Tract-Producer-Year",
    ]

    print(f"\n{'='*60}")
    print(f"  athenafarm deployer — env={deploy_env}  project={project}")
    print(f"{'='*60}\n")

    # Remove deprecated jobs that must no longer be runnable.
    print("[legacy] Removing deprecated Glue jobs...")
    for legacy_job in LEGACY_JOB_NAMES:
        if dry_run:
            print(f"  [DRY] delete Glue job: {legacy_job}")
            continue
        try:
            glue.delete_job(JobName=legacy_job)
            print(f"  ✓ deleted {legacy_job}")
        except glue.exceptions.EntityNotFoundException:
            print(f"  ✓ {legacy_job} not present")

    # ── Ensure Glue catalog databases exist (Iceberg createOrReplace requires them) ──
    print("[0/4] Ensuring Glue catalog databases exist...")
    CATALOG_DATABASES = [
        "athenafarm_prod_raw",
        "athenafarm_prod_ref",
        "athenafarm_prod_cdc",
        "athenafarm_prod_gold",
    ]
    for db_name in CATALOG_DATABASES:
        if dry_run:
            print(f"  [DRY] Glue DB: {db_name}")
        else:
            try:
                glue.get_database(Name=db_name)
                print(f"  ✓ {db_name} (exists)")
            except glue.exceptions.EntityNotFoundException:
                glue.create_database(DatabaseInput={"Name": db_name})
                print(f"  ✓ {db_name} (created)")

    # ── Ensure sss-farmrecords Glue crawler exists and has been run ──────────
    # The SSS/IBase tables (ibib, ibsp, ibst, ibin, ibpart, crmd_partner, etc.)
    # live as Parquet files under sssFarmrecordsS3Path with UPPERCASE subfolder
    # names matching table names.  The crawler populates the sss-farmrecords
    # Glue catalog database so Ingest-SSS-Farmrecords can read them.
    if sss_s3_path:
        sss_crawler_name = f"FSA-{deploy_env}-SSS-Farmrecords-Crawler"
        sss_db_name      = "sss-farmrecords"
        if dry_run:
            print(f"  [DRY] Glue crawler: {sss_crawler_name} → {sss_s3_path}")
        else:
            # Ensure the sss-farmrecords catalog database exists
            try:
                glue.get_database(Name=sss_db_name)
            except glue.exceptions.EntityNotFoundException:
                glue.create_database(DatabaseInput={"Name": sss_db_name})
                print(f"  ✓ {sss_db_name} (created)")

            crawler_cfg = dict(
                Name=sss_crawler_name,
                Role=glue_role_arn,
                DatabaseName=sss_db_name,
                Targets={"S3Targets": [{"Path": sss_s3_path}]},
                SchemaChangePolicy={
                    "UpdateBehavior": "UPDATE_IN_DATABASE",
                    "DeleteBehavior": "LOG",
                },
                Configuration='{"Version":1.0,"CrawlerOutput":{"Partitions":{"AddOrUpdateBehavior":"InheritFromTable"}}}',
            )
            try:
                existing = glue.get_crawler(Name=sss_crawler_name)["Crawler"]
                # Update if S3 path has changed
                if existing.get("Targets", {}).get("S3Targets", [{}])[0].get("Path") != sss_s3_path:
                    glue.update_crawler(**crawler_cfg)
                    print(f"  ✓ {sss_crawler_name} (updated)")
                else:
                    print(f"  ✓ {sss_crawler_name} (exists)")
            except glue.exceptions.EntityNotFoundException:
                glue.create_crawler(**crawler_cfg)
                print(f"  ✓ {sss_crawler_name} (created)")

            # Crawler execution is a pipeline step — it runs inside the Step
            # Function (StartSSSCrawler → WaitForSSSCrawler → poll loop).
            # deploy.py only provisions the crawler definition here.
            print(f"  ✓ {sss_crawler_name} provisioned — will be executed by the Step Function")

    # ── Upload all Glue scripts ──────────────────────────────────────────────
    print("[1/4] Uploading Glue scripts...")
    script_info: Dict[str, Tuple[str, Path]] = {}
    for stem, _job_name in SCRIPT_SPECS:
        local = glue_dir / f"{stem}.py"
        s3_key = f"{prefix}glue/FSA-{deploy_env}-{project}-{stem}.py"
        if dry_run:
            print(f"  [DRY] {stem} → s3://{artifact_bucket}/{s3_key}")
        else:
            s3_key, local = _upload_glue_script(s3_client, glue_dir, stem, artifact_bucket, prefix, deploy_env, project)
        script_info[stem] = (s3_key, local)

    # ── Create / update Glue jobs ────────────────────────────────────────────
    print("\n[2/4] Creating/updating Glue jobs...")
    base_tpy_cfg = _glue_config_for_script(cfg, "Transform-Tract-Producer-Year")
    for stem, job_name in SCRIPT_SPECS:
        per = _glue_config_for_script(cfg, stem)
        if not per and stem.startswith("TPY-"):
            # Reuse legacy tract transform sizing/args unless explicitly overridden.
            per = base_tpy_cfg
        worker    = _as_str(per.get("WorkerType"), default_worker)
        n_workers = _as_int(per.get("NumberOfWorkers"), default_workers)
        timeout   = _as_int(per.get("TimeoutMinutes"), default_timeout)
        autosc    = _as_bool(per.get("AutomaticScalingEnabled"), default_autosc)
        job_params = _as_str_dict(_as_dict(per.get("JobParameters")))

        # Resolve connections from per-job GlueConfig Connections list first;
        # fall back to the global pgConnectionNameParam for PG/sync jobs.
        raw_connections: List[Any] = per.get("Connections") or []
        per_job_connections: List[str] = [
            c["ConnectionName"]
            for c in raw_connections
            if isinstance(c, dict) and c.get("ConnectionName")
        ]
        if not per_job_connections:
            # Legacy heuristic: inject pg connection for PG-touching jobs
            if "pg" in stem.lower() or "sync" in stem.lower():
                per_job_connections = [pg_connection_name] if pg_connection_name else []

        # Inject runtime args common to all jobs
        job_params.setdefault("--env", deploy_env)
        job_params.setdefault("--iceberg_warehouse", iceberg_warehouse)
        if per_job_connections:
            job_params.setdefault("--connection_name", per_job_connections[0])
        # Inject Secrets Manager secret ID for jobs that read/write PostgreSQL
        if ("pg" in stem.lower() or "sync" in stem.lower()) and secret_id:
            job_params.setdefault("--secret_id", secret_id)
        # Inject debug flag for all jobs
        job_params.setdefault("--debug", "true" if debug_logging else "false")

        # Enable native Iceberg support on Glue 4.0 (required for .using("iceberg"))
        job_params.setdefault("--datalake-formats", "iceberg")

        # Transform runtime profile defaults (AQE-first partition sizing).
        # Keep these as setdefault so config-level/per-job overrides still win.
        if stem == "Transform-Farm-Producer-Year":
            job_params.setdefault("--advisory_partition_size_mb", "128")
        if stem.startswith("TPY-"):
            job_params.setdefault("--advisory_partition_size_mb", "96")
            job_params.setdefault("--sss_database", _as_str(base_tpy_cfg.get("JobParameters", {}).get("--sss_database"), "athenafarm_prod_raw"))
            job_params.setdefault("--ref_database", _as_str(base_tpy_cfg.get("JobParameters", {}).get("--ref_database"), "athenafarm_prod_ref"))
            job_params.setdefault("--target_database", _as_str(base_tpy_cfg.get("JobParameters", {}).get("--target_database"), "athenafarm_prod_gold"))

        # Map AdditionalPythonModulesPath → --extra-py-files (WHL/egg/zip on S3)
        extra_py = _as_str(per.get("AdditionalPythonModulesPath"))
        if extra_py:
            job_params.setdefault("--extra-py-files", extra_py)

        # Enable continuous CloudWatch logging and name the log group after the
        # actual job so logs are trivially findable in the console.
        # --continuous-log-logStreamPrefix scopes each run's streams under a
        # unique prefix (job_name/) so runs don't overwrite each other's output.
        job_params.setdefault("--enable-continuous-cloudwatch-log", "true")
        job_params.setdefault("--continuous-log-logGroup", f"/aws-glue/jobs/{job_name}")
        job_params.setdefault("--continuous-log-logStreamPrefix", f"{job_name}/")

        merged_args = _merge_default_args(default_args, job_params)

        _s3_key, _local = script_info[stem]
        spec = GlueJobSpec(
            name=job_name,
            role_arn=glue_role_arn,
            script_local_path=str(_local),
            script_s3_bucket=artifact_bucket,
            script_s3_key=_s3_key,
            glue_version="4.0",
            worker_type=worker,
            number_of_workers=n_workers,
            max_retries=0,
            timeout_minutes=timeout,
            default_args=merged_args,
            max_concurrency=1,
            connection_names=per_job_connections or None,
        )

        if dry_run:
            print(f"  [DRY] Glue job: {job_name}  ({worker} x {n_workers})")
        else:
            ensure_glue_job(glue, s3_client, spec)
            print(f"  ✓ {job_name}")

    # ── Deploy Lambda functions ──────────────────────────────────────────────
    print("\n[3/4] Deploying Lambda functions...")
    lambda_dir = Path(__file__).resolve().parent / "lambda"
    notify_spec = LambdaSpec(
        name=names.notify_fn,
        role_arn=etl_lambda_role_arn,
        handler="lambda_function.handler",
        runtime="python3.12",
        source_dir=str(lambda_dir / "notify_pipeline"),
        env={"ENV": deploy_env, "PIPELINE": "athenafarm"},
        layers=[],
        timeout=30,
        memory=256,
    )
    if dry_run:
        print(f"  [DRY] Lambda: {names.notify_fn}")
    else:
        ensure_lambda(lam, notify_spec)
        print(f"  ✓ {names.notify_fn}")

    # ── Deploy Step Functions ────────────────────────────────────────────────
    print("\n[4/4] Deploying Step Functions state machines...")

    # Token map shared by both ASL files
    sss_crawler_name = f"FSA-{deploy_env}-SSS-Farmrecords-Crawler"

    sts = boto3.client("sts", region_name=region)
    account_id = sts.get_caller_identity()["Account"]
    tract_pipeline_sm_arn = f"arn:aws:states:{region}:{account_id}:stateMachine:{names.sm_tract_pipeline}"

    tokens = {
        "__ENV__":                           deploy_env,
        "__ICEBERG_WAREHOUSE__":             iceberg_warehouse,
        "__PG_CONNECTION_NAME__":            pg_connection_name,
        "__SSS_CRAWLER_NAME__":              sss_crawler_name,
        "__INGEST_SSS_GLUE_JOB_NAME__":      names.ingest_sss_job,
        "__INGEST_PG_REFS_GLUE_JOB_NAME__":  names.ingest_pg_refs_job,
        "__INGEST_PG_CDC_GLUE_JOB_NAME__":   names.ingest_pg_cdc_job,
        "__TRANSFORM_FARM_PY_GLUE_JOB_NAME__": names.transform_fpy_job,
        "__TPY_01_SPINE_BASE_GLUE_JOB_NAME__": names.tpy_j1_spine_base_job,
        "__TPY_02_INSTANCE_GUID_GLUE_JOB_NAME__": names.tpy_j2_instance_guid_job,
        "__TPY_03_PARTNER_MAP_GLUE_JOB_NAME__": names.tpy_j3_partner_map_job,
        "__TPY_04_ZMI_MAP_GLUE_JOB_NAME__": names.tpy_j4_zmi_map_job,
        "__TPY_05_COC_TIME_MAP_GLUE_JOB_NAME__": names.tpy_j5_coc_time_map_job,
        "__TPY_06_CANDIDATE_ASSEMBLE_GLUE_JOB_NAME__": names.tpy_j6_candidate_assemble_job,
        "__TPY_07_PARTNER_CUSTOMER_GLUE_JOB_NAME__": names.tpy_j7_partner_customer_job,
        "__TPY_08_FARM_TRACT_RESOLVE_GLUE_JOB_NAME__": names.tpy_j8_farm_tract_resolve_job,
        "__TPY_09_TRACT_YEAR_RESOLVE_GLUE_JOB_NAME__": names.tpy_j9_tract_year_resolve_job,
        "__TPY_10_PUBLISH_GLUE_JOB_NAME__": names.tpy_j10_publish_job,
        "__TRACT_TPY_PIPELINE_SM_ARN__":     tract_pipeline_sm_arn,
        "__SYNC_RDS_GLUE_JOB_NAME__":        names.sync_rds_job,
        "__ICEBERG_MAINT_GLUE_JOB_NAME__":   names.iceberg_maint_job,
        "__SNS_NOTIFY_FN_ARN__":             sns_notify_fn_arn,
    }

    SM_SPECS = [
        ("TractProducerYear.param.asl.json", names.sm_tract_pipeline),
        ("Main.param.asl.json",        names.sm_main),
        ("Maintenance.param.asl.json", names.sm_maintenance),
    ]

    for asl_file, sm_name in SM_SPECS:
        asl = _load_asl(project_dir, asl_file)
        asl = _substitute(asl, tokens)
        if asl_file == "Main.param.asl.json":
            _assert_main_state_machine_full_load_only(asl, sm_name)

        spec = StateMachineSpec(
            name=sm_name,
            role_arn=sfn_role_arn,
            definition=asl,
        )
        if dry_run:
            print(f"  [DRY] State machine: {sm_name}")
        else:
            ensure_state_machine(sfn, spec)
            print(f"  ✓ {sm_name}")

    print(f"\n{'='*60}")
    print(f"  athenafarm deploy {'(DRY RUN) ' if dry_run else ''}complete.")
    print(f"{'='*60}\n")

    return {  # type: ignore[return-value]
        "deploy_env": deploy_env,
        "project": project,
        "region": region,
        "artifact_bucket": artifact_bucket,
        "artifact_prefix": prefix,
        "iceberg_warehouse": iceberg_warehouse,
        # Glue jobs
        "glue_job_ingest_sss": names.ingest_sss_job,
        "glue_job_ingest_pg_refs": names.ingest_pg_refs_job,
        "glue_job_ingest_pg_cdc": names.ingest_pg_cdc_job,
        "glue_job_transform_fpy": names.transform_fpy_job,
        "glue_job_tpy_01_spine_base": names.tpy_j1_spine_base_job,
        "glue_job_tpy_02_instance_guid": names.tpy_j2_instance_guid_job,
        "glue_job_tpy_03_partner_map": names.tpy_j3_partner_map_job,
        "glue_job_tpy_04_zmi_map": names.tpy_j4_zmi_map_job,
        "glue_job_tpy_05_coc_time_map": names.tpy_j5_coc_time_map_job,
        "glue_job_tpy_06_candidate_assemble": names.tpy_j6_candidate_assemble_job,
        "glue_job_tpy_07_partner_customer": names.tpy_j7_partner_customer_job,
        "glue_job_tpy_08_farm_tract_resolve": names.tpy_j8_farm_tract_resolve_job,
        "glue_job_tpy_09_tract_year_resolve": names.tpy_j9_tract_year_resolve_job,
        "glue_job_tpy_10_publish": names.tpy_j10_publish_job,
        "glue_job_sync_rds": names.sync_rds_job,
        "glue_job_iceberg_maint": names.iceberg_maint_job,
        # Lambda
        "lambda_notify_fn": names.notify_fn,
        # Step Functions
        "state_machine_main": names.sm_main,
        "state_machine_tract_pipeline": names.sm_tract_pipeline,
        "state_machine_maintenance": names.sm_maintenance,
        "sfn_role_arn": sfn_role_arn,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Deploy the athenafarm ETL pipeline resources.")
    parser.add_argument("--config", required=True, help="Path to the JSON config file.")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be deployed without making changes.")
    args = parser.parse_args()

    cfg_path = Path(args.config).resolve()
    if not cfg_path.exists():
        print(f"ERROR: config file not found: {cfg_path}", file=sys.stderr)
        sys.exit(1)

    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    deploy(cfg, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
