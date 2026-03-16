from __future__ import annotations

import ast
import json
import re
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import boto3

from common.aws_common import (
    GlueJobSpec,
    LambdaSpec,
    StateMachineSpec,
    ensure_bucket_exists,
    ensure_glue_job,
    ensure_lambda,
    ensure_state_machine,
)


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


def _as_str_list(v: Any) -> List[str]:
    if not isinstance(v, list):
        return []
    return [x.strip() for x in v if isinstance(x, str) and x.strip()]


def _as_dict(v: Any) -> Dict[str, Any]:
    return v if isinstance(v, dict) else {}


def _parse_glue_config_array(cfg: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    root = cfg.get("GlueConfig")
    if not isinstance(root, list):
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for item in root:
        if not isinstance(item, dict):
            continue
        for k, v in item.items():
            if isinstance(k, str) and isinstance(v, dict):
                out[k] = v
    return out


def _parse_connection_names(glue_job_params: Dict[str, Any]) -> List[str]:
    conns = glue_job_params.get("Connections") or []
    if not isinstance(conns, list):
        return []
    out: List[str] = []
    seen = set()
    for c in conns:
        if not isinstance(c, dict):
            continue
        name = _as_str(c.get("ConnectionName"))
        if name and name not in seen:
            out.append(name)
            seen.add(name)
    return out


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

    if _as_str(glue_job_params.get("JobObservabilityMetrics")).upper() == "ENABLED":
        out["--enable-observability-metrics"] = "true"

    if _as_str(glue_job_params.get("JobContinuousLogging")).upper() == "ENABLED":
        out["--enable-continuous-cloudwatch-log"] = "true"

    temp_path = glue_job_params.get("TemporaryPath")
    if temp_path:
        out["--TempDir"] = str(temp_path)

    if _as_bool(glue_job_params.get("UseGlueDataCatalogAsTheHiveMetastore"), default=False) is True:
        out["--enable-glue-datacatalog"] = "true"

    ref_files = glue_job_params.get("ReferencePath")
    if ref_files and str(ref_files).strip():
        out["--extra-files"] = str(ref_files).strip()

    extra_py = glue_job_params.get("AdditionalPythonModulesPath")
    if extra_py and str(extra_py).strip():
        out["--additional-python-modules"] = str(extra_py).strip()

    python_lib_path = _as_str(glue_job_params.get("PythonLibraryPath"), default="")
    if python_lib_path:
        out["--extra-py-files"] = python_lib_path

    job_params = _as_dict(glue_job_params.get("JobParameters"))
    for k, v in job_params.items():
        out[str(k)] = "" if v is None else str(v)

    return {str(k): str(v) for k, v in out.items()}


def _detect_handler_symbol(handler_file: Path) -> str:
    text = handler_file.read_text(encoding="utf-8")
    module = ast.parse(text, filename=str(handler_file))
    names = {n.name for n in module.body if isinstance(n, ast.FunctionDef)}
    if "lambda_handler" in names:
        return "lambda_handler"
    if "handler" in names:
        return "handler"
    raise RuntimeError(f"No lambda entrypoint found in {handler_file}")


def _prepare_lambda_source(src_dir: Path) -> Tuple[str, Optional[tempfile.TemporaryDirectory]]:
    """Ensure lambda source has a valid module file name for python handler import."""
    standard = src_dir / "lambda_function.py"
    hyphen = src_dir / "lambda-function.py"
    if standard.exists():
        return str(src_dir), None
    if hyphen.exists():
        tmp = tempfile.TemporaryDirectory(prefix="cps-lambda-")
        tmp_dir = Path(tmp.name)
        shutil.copytree(src_dir, tmp_dir / src_dir.name, dirs_exist_ok=True)
        staged = tmp_dir / src_dir.name
        shutil.copy2(staged / "lambda-function.py", staged / "lambda_function.py")
        return str(staged), tmp
    raise FileNotFoundError(f"No lambda handler file found in {src_dir}")


def _strip_fsa_prefix(name: str) -> str:
    s = _as_str(name)
    if s.startswith("arn:") and ":function:" in s:
        s = s.split(":function:", 1)[1]
    if s.startswith("arn:") and ":stateMachine:" in s:
        s = s.split(":stateMachine:", 1)[1]
    if s.startswith("FSA-"):
        parts = s.split("-", 3)
        if len(parts) == 4:
            return parts[3]
    return s


def _norm(name: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", _strip_fsa_prefix(name).upper())


def _match_by_prefix(ref: str, candidates: List[str]) -> Optional[str]:
    if not candidates:
        return None
    ref_norm = _norm(ref)
    if not ref_norm:
        return None

    for c in candidates:
        if _norm(c) == ref_norm:
            return c

    for n in (6, 5):
        if len(ref_norm) < n:
            continue
        key = ref_norm[:n]
        matches = [c for c in candidates if _norm(c).startswith(key)]
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            matches.sort(key=lambda c: len(os_common_prefix(_norm(c), ref_norm)), reverse=True)
            return matches[0]

    return None


def os_common_prefix(a: str, b: str) -> str:
    i = 0
    max_i = min(len(a), len(b))
    while i < max_i and a[i] == b[i]:
        i += 1
    return a[:i]


@dataclass(frozen=True)
class Names:
    prefix: str
    glue_landing_files: str
    glue_raw_dm: str
    sm_incremental_to_landing: str
    sm_s3landing_to_rawdm: str
    sm_process_control_update: str
    sm_main: str
    crawler_main: str
    crawler_cdc: str


def build_names(deploy_env: str, project: str) -> Names:
    dep = _as_str(deploy_env)
    proj = _as_str(project)
    if not dep or not proj:
        raise RuntimeError("Missing required deployEnv/project")
    pfx = f"FSA-{dep}-{proj}"
    return Names(
        prefix=pfx,
        glue_landing_files=f"{pfx}-LandingFiles",
        glue_raw_dm=f"{pfx}-Raw-DM",
        sm_incremental_to_landing=f"{pfx}-Incremental-to-S3Landing",
        sm_s3landing_to_rawdm=f"{pfx}-S3Landing-to-S3Final-Raw-DM",
        sm_process_control_update=f"{pfx}-Process-Control-Update",
        sm_main=f"{pfx}-MAIN",
        crawler_main=f"{pfx}",
        crawler_cdc=f"{pfx}-cdc",
    )


def _load_asl(project_dir: Path, filename: str) -> Dict[str, Any]:
    p = project_dir / "states" / filename
    if not p.exists():
        raise FileNotFoundError(f"Missing ASL file: {p}")
    return json.loads(p.read_text(encoding="utf-8"))


def _rewrite_definition(
    obj: Any,
    *,
    lambda_arns: Dict[str, str],
    glue_names: Dict[str, str],
    state_machine_arns: Dict[str, str],
    crawler_names: List[str],
    landing_bucket: str,
    job_id_key: str,
    raw_dm_job_params: Dict[str, str],
) -> Any:
    lambda_candidates = list(lambda_arns.keys())
    glue_candidates = list(glue_names.values())
    sm_candidates = list(state_machine_arns.keys())

    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if k == "JobName" and isinstance(v, str) and v.startswith("FSA-"):
                m = _match_by_prefix(v, glue_candidates)
                out[k] = m or v
                continue

            if k == "FunctionName" and isinstance(v, str):
                ref = v
                if ":function:" in v:
                    ref = v.split(":function:", 1)[1]
                m = _match_by_prefix(ref, lambda_candidates)
                out[k] = lambda_arns[m] if m else v
                continue

            if k == "StateMachineArn" and isinstance(v, str):
                ref = v
                if ":stateMachine:" in v:
                    ref = v.split(":stateMachine:", 1)[1]
                m = _match_by_prefix(ref, sm_candidates)
                out[k] = state_machine_arns[m] if m else v
                continue

            if k == "Resource" and isinstance(v, str) and ":function:" in v:
                ref = v.split(":function:", 1)[1]
                m = _match_by_prefix(ref, lambda_candidates)
                out[k] = lambda_arns[m] if m else v
                continue

            if k == "Bucket" and isinstance(v, str) and "landing-zone" in v:
                out[k] = landing_bucket
                continue

            if k == "Key" and isinstance(v, str) and v.endswith("job_id.json"):
                out[k] = job_id_key
                continue

            if k == "Name" and isinstance(v, str) and v.startswith("FSA-"):
                m = _match_by_prefix(v, crawler_names)
                out[k] = m or v
                continue

            out[k] = _rewrite_definition(
                v,
                lambda_arns=lambda_arns,
                glue_names=glue_names,
                state_machine_arns=state_machine_arns,
                crawler_names=crawler_names,
                landing_bucket=landing_bucket,
                job_id_key=job_id_key,
                raw_dm_job_params=raw_dm_job_params,
            )

        # Patch raw-dm glue arguments from config where keys exist.
        if isinstance(out.get("Arguments"), dict):
            patched = dict(out["Arguments"])
            for arg_key, arg_val in raw_dm_job_params.items():
                if arg_key.endswith(".$"):
                    continue
                if arg_key in patched and isinstance(patched[arg_key], str) and not str(patched[arg_key]).endswith(".$"):
                    patched[arg_key] = arg_val
            out["Arguments"] = patched

        return out

    if isinstance(obj, list):
        return [
            _rewrite_definition(
                x,
                lambda_arns=lambda_arns,
                glue_names=glue_names,
                state_machine_arns=state_machine_arns,
                crawler_names=crawler_names,
                landing_bucket=landing_bucket,
                job_id_key=job_id_key,
                raw_dm_job_params=raw_dm_job_params,
            )
            for x in obj
        ]

    return obj


def deploy(cfg: Dict[str, Any], region: str) -> Dict[str, Any]:
    deploy_env = _as_str(cfg.get("deployEnv"))
    project = _as_str(cfg.get("project"))
    names = build_names(deploy_env, project)

    artifacts = _as_dict(cfg.get("artifacts"))
    artifact_bucket = _as_str(artifacts.get("artifactBucket"))
    prefix = _as_str(artifacts.get("prefix")).rstrip("/") + "/"
    if prefix == "/":
        prefix = ""

    strparams = _as_dict(cfg.get("strparams"))
    glue_role_arn = _as_str(strparams.get("glueJobRoleArnParam"))
    etl_role_arn = _as_str(strparams.get("etlRoleArnParam"))
    sfn_role_arn = _as_str(_as_dict(cfg.get("stepFunctions")).get("roleArn"))
    landing_bucket = _as_str(strparams.get("landingBucketNameParam"))
    final_bucket = _as_str(strparams.get("finalBucketNameParam"))
    job_id_key = _as_str(strparams.get("jobIdKeyParam"))
    sns_arn = _as_str(strparams.get("snsArnParam"))
    secret_id = _as_str(cfg.get("secretId"))

    missing = []
    if not artifact_bucket:
        missing.append("artifacts.artifactBucket")
    if not glue_role_arn:
        missing.append("strparams.glueJobRoleArnParam")
    if not etl_role_arn:
        missing.append("strparams.etlRoleArnParam")
    if not sfn_role_arn:
        missing.append("stepFunctions.roleArn")
    if not landing_bucket:
        missing.append("strparams.landingBucketNameParam")
    if not final_bucket:
        missing.append("strparams.finalBucketNameParam")
    if not job_id_key:
        missing.append("strparams.jobIdKeyParam")
    if missing:
        raise RuntimeError("Missing required config keys: " + ", ".join(missing))

    project_dir = Path(__file__).resolve().parent
    glue_root = project_dir / "glue"
    lambda_root = project_dir / "lambda"

    session = boto3.Session(region_name=region)
    s3 = session.client("s3")
    glue = session.client("glue")
    lam = session.client("lambda")
    sfn = session.client("stepfunctions")

    ensure_bucket_exists(s3, artifact_bucket, region)

    glue_cfg = _parse_glue_config_array(cfg)

    glue_specs = [
        ("LandingFiles", names.glue_landing_files),
        ("Raw-DM", names.glue_raw_dm),
    ]
    glue_results: Dict[str, str] = {}
    for script_stem, job_name in glue_specs:
        local = glue_root / f"{script_stem}.py"
        if not local.exists():
            raise FileNotFoundError(f"Missing Glue script: {local}")
        gcfg = glue_cfg.get(script_stem, {})
        defaults = _merge_glue_default_args(_as_dict(cfg.get("glueDefaultArgs")), gcfg)
        script_key = f"{prefix}glue/FSA-{deploy_env}-{project}-{script_stem}.py"
        ensure_glue_job(
            glue,
            s3,
            GlueJobSpec(
                name=job_name,
                role_arn=glue_role_arn,
                script_local_path=str(local),
                script_s3_bucket=artifact_bucket,
                script_s3_key=script_key,
                default_args=defaults,
                glue_version=_as_str(gcfg.get("GlueVersion"), "4.0"),
                worker_type=_as_str(gcfg.get("WorkerType"), "G.2X"),
                number_of_workers=_as_int(gcfg.get("NumberOfWorkers"), 2),
                timeout_minutes=_as_int(gcfg.get("TimeoutMinutes"), 480),
                max_retries=_as_int(gcfg.get("MaxRetries"), 0),
                max_concurrency=_as_int(gcfg.get("MaxConcurrency"), 1),
                connection_names=_parse_connection_names(gcfg),
            ),
        )
        glue_results[script_stem] = job_name

    lambda_arns: Dict[str, str] = {}
    lambda_dirs = sorted([p for p in lambda_root.iterdir() if p.is_dir()])

    lambda_shared = _as_dict(cfg.get("lambdas") or cfg.get("lambda"))
    shared_layers = _as_str_list(lambda_shared.get("layers"))
    shared_runtime = _as_str(lambda_shared.get("runtime"), "python3.11")
    shared_timeout = _as_int(lambda_shared.get("timeoutSeconds"), 30)
    shared_memory = _as_int(lambda_shared.get("memoryMb"), 256)

    for src_dir in lambda_dirs:
        fn_name = f"{names.prefix}-{src_dir.name}"
        source_path, tmp_ctx = _prepare_lambda_source(src_dir)
        try:
            handler_sym = _detect_handler_symbol(Path(source_path) / "lambda_function.py")
            fn_env: Dict[str, str] = {}
            if src_dir.name == "get-incremental-tables":
                fn_env["source_folder"] = project.lower()
            if src_dir.name in {"RAW-DM-sns-pub-step-func-errs", "sns-publish-validations-report"} and sns_arn:
                fn_env["SNS_ARN"] = sns_arn
            if src_dir.name in {"Job-Logging-End", "validation-check"}:
                if secret_id:
                    fn_env["SecretId"] = secret_id
                fn_env["CRAWLER_NAME"] = names.crawler_main

            arn = ensure_lambda(
                lam,
                LambdaSpec(
                    name=fn_name,
                    role_arn=etl_role_arn,
                    handler=f"lambda_function.{handler_sym}",
                    runtime=shared_runtime,
                    source_dir=source_path,
                    env=fn_env,
                    layers=shared_layers,
                    timeout=shared_timeout,
                    memory=shared_memory,
                ),
            )
            lambda_arns[fn_name] = arn
        finally:
            if tmp_ctx is not None:
                tmp_ctx.cleanup()

    crawler_names: List[str] = []
    for crawler_group in cfg.get("crawlers", []) if isinstance(cfg.get("crawlers"), list) else []:
        if not isinstance(crawler_group, dict):
            continue
        for item in crawler_group.values():
            if isinstance(item, dict):
                name = _as_str(item.get("name"))
                if name:
                    crawler_names.append(name)
    if not crawler_names:
        crawler_names = [names.crawler_main, names.crawler_cdc]

    glue_names = {
        "LandingFiles": names.glue_landing_files,
        "Raw-DM": names.glue_raw_dm,
    }

    raw_dm_job_params = {
        k: str(v)
        for k, v in _as_dict(glue_cfg.get("Raw-DM", {}).get("JobParameters")).items()
    }

    sm_files = [
        ("Incremental-to-S3Landing.asl.json", names.sm_incremental_to_landing),
        ("S3Landing-to-S3Final-Raw-DM.asl.json", names.sm_s3landing_to_rawdm),
        ("Process-Control-Update.asl.json", names.sm_process_control_update),
        ("MAIN.asl.json", names.sm_main),
    ]

    state_machine_arns: Dict[str, str] = {}
    for asl_file, sm_name in sm_files:
        definition = _load_asl(project_dir, asl_file)
        rewritten = _rewrite_definition(
            definition,
            lambda_arns=lambda_arns,
            glue_names=glue_names,
            state_machine_arns=state_machine_arns,
            crawler_names=crawler_names,
            landing_bucket=landing_bucket,
            job_id_key=job_id_key,
            raw_dm_job_params=raw_dm_job_params,
        )
        sm_arn = ensure_state_machine(
            sfn,
            StateMachineSpec(
                name=sm_name,
                role_arn=sfn_role_arn,
                definition=rewritten,
            ),
        )
        state_machine_arns[sm_name] = sm_arn

    return {
        "deploy_env": deploy_env,
        "project": project,
        "artifact_bucket": artifact_bucket,
        "artifact_prefix": prefix,
        "glue_jobs": glue_results,
        "lambda_count": len(lambda_arns),
        "state_machine_count": len(state_machine_arns),
        "state_machines": state_machine_arns,
    }


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="Deploy CPS pipeline resources")
    ap.add_argument("--config", required=True)
    ap.add_argument("--region", default="us-east-1")
    args = ap.parse_args()

    cfg = json.loads(Path(args.config).read_text(encoding="utf-8"))
    result = deploy(cfg, args.region)
    for k, v in result.items():
        print(f"{k}: {v}")
