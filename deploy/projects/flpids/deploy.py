from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

import boto3

from common.aws_common import (
    ensure_lambda,
    ensure_state_machine,
    LambdaSpec,
    StateMachineSpec,
)

from .stepfunctions.fpac_stepfunctions import (
    FsaFileChecksStateMachineBuilder,
    FsaFileChecksStateMachineInputs,
)


@dataclass(frozen=True)
class FpacNames:
    project_name: str

    # existing lambdas
    checkfile_fn: str
    testfileloader_fn: str
    dynacheckfile_fn: str
    checkfilenotsecure_fn: str
    realjobname_fn: str

    # stream trigger lambda
    streamstartfilechecks_fn: str

    # state-machine lambdas
    setrunning_fn: str
    transferfile_fn: str
    finalizejob_fn: str

    # state machine
    sm_filechecks: str


def build_names(deploy_env: str, project: str) -> FpacNames:
    project_name = f"Fpac{project.upper()}"
    base = f"FSA-{deploy_env}-{project_name}"

    return FpacNames(
        project_name=project_name,
        checkfile_fn=f"{base}-CheckFile",
        testfileloader_fn=f"{base}-TestFileLoader",
        dynacheckfile_fn=f"{base}-DynaCheckFile",
        checkfilenotsecure_fn=f"{base}-CheckFileNotSecure",
        realjobname_fn=f"{base}-RealJobName",
        streamstartfilechecks_fn=f"{base}-StreamStartFileChecks",
        setrunning_fn=f"{base}-SetRunning",
        transferfile_fn=f"{base}-TransferFile",
        finalizejob_fn=f"{base}-FinalizeJob",
        sm_filechecks=f"{base}-FileChecks",
    )


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


def parse_lambda_data_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Parse lambda-data configuration with defaults"""
    lambda_data = cfg.get("lambda-data") or {}
    
    # Parse networking for lambda-data (separate from main networking)
    subnet_ids, security_group_ids = parse_networking(lambda_data)
    
    # Parse environment variables
    env_config = lambda_data.get("env") or {}
    
    # Parse database connections
    db_connections = lambda_data.get("database-connections") or []
    if not isinstance(db_connections, list):
        db_connections = []
    
    # Extract security groups from database connections
    db_security_groups = set()
    db_env_vars = {}
    
    for i, db_conn in enumerate(db_connections):
        if isinstance(db_conn, dict):
            db_name = db_conn.get("name", f"db_{i}")
            lambda_sg = db_conn.get("lambda-security_group")
            db_sg = db_conn.get("db-security_group")
            
            # Add lambda security groups to the set
            if lambda_sg:
                db_security_groups.add(lambda_sg)
            
            # Add database connection info to environment variables
            db_env_vars[f"DB_CONNECTION_{i}_NAME"] = db_name
            if lambda_sg:
                db_env_vars[f"DB_CONNECTION_{i}_LAMBDA_SG"] = lambda_sg
            if db_sg:
                db_env_vars[f"DB_CONNECTION_{i}_DB_SG"] = db_sg
    
    # Merge security groups from networking and database connections
    all_security_groups = list(set(security_group_ids + list(db_security_groups)))
    
    # Parse other lambda configuration with defaults
    memory_size = lambda_data.get("memory-size")
    if memory_size is None or not isinstance(memory_size, int):
        memory_size = 256  # Default AWS memory size
    
    timeout = lambda_data.get("timeout")
    if timeout is None or not isinstance(timeout, int):
        timeout = 30  # Default AWS timeout
    
    layers = lambda_data.get("layers") or []
    if not isinstance(layers, list):
        layers = []
    
    return {
        "subnet_ids": subnet_ids,
        "security_group_ids": all_security_groups,
        "env_config": {**env_config, **db_env_vars},  # Merge env config with db connection vars
        "database_connections": db_connections,
        "memory_size": memory_size,
        "timeout": timeout,
        "layers": layers,
    }


def deploy(cfg: Dict[str, Any], region: str) -> Dict[str, str]:
    deploy_env = cfg["deployEnv"]
    project = cfg["project"]
    config_data = cfg["configData"]
    bucket_region = cfg.get("bucketRegion", region)

    # Canonical DynamoDB table name for all lambdas (including StreamStartFileChecks)
    dynamo_table_name = config_data["dynamoTableName"]

    # From config: this is the *actual* bucket name string in your json
    landing_bucket_name = cfg["strparams"]["landingBucketNameParam"]

    etl_lambda_role_arn = cfg["strparams"]["etlRoleArnParam"]

    layers = [
        cfg["strparams"]["thirdPartyLayerArnParam"],
        cfg["strparams"]["customLayerArnParam"],
    ]

    names = build_names(deploy_env, project)

    project_dir = Path(__file__).resolve().parent
    lambda_root = project_dir / "lambda"

    subnet_ids, security_group_ids = parse_networking(cfg)

    session = boto3.Session(region_name=region)
    lam = session.client("lambda")
    sfn = session.client("stepfunctions")

    # ✅ Shared env across ALL lambdas.
    # IMPORTANT:
    # - LANDING_BUCKET is the canonical bucket source per your config.
    # - TransferFile (and others) should look at LANDING_BUCKET as fallback.
    base_env = {
        "PROJECT": project,
        "LANDING_BUCKET": landing_bucket_name,
        "TABLE_NAME": dynamo_table_name,
        "BUCKET_REGION": bucket_region,
    }

    # ---- existing lambdas ----
    checkfile_arn = ensure_lambda(
        lam,
        LambdaSpec(
            name=names.checkfile_fn,
            role_arn=etl_lambda_role_arn,
            handler="lambda_function.lambda_handler",
            runtime="python3.12",
            source_dir=str(lambda_root / "CheckFile"),
            env=base_env,
            layers=layers,
            subnet_ids=subnet_ids,
            security_group_ids=security_group_ids,
        ),
    )

    testfileloader_arn = ensure_lambda(
        lam,
        LambdaSpec(
            name=names.testfileloader_fn,
            role_arn=etl_lambda_role_arn,
            handler="lambda_function.lambda_handler",
            runtime="python3.12",
            source_dir=str(lambda_root / "TestFileLoader"),
            env=base_env,
            layers=layers,
            subnet_ids=subnet_ids,
            security_group_ids=security_group_ids,
        ),
    )

    dynacheckfile_arn = ensure_lambda(
        lam,
        LambdaSpec(
            name=names.dynacheckfile_fn,
            role_arn=etl_lambda_role_arn,
            handler="lambda_function.lambda_handler",
            runtime="python3.12",
            source_dir=str(lambda_root / "DynaCheckFile"),
            env=base_env,
            layers=layers,
            subnet_ids=subnet_ids,
            security_group_ids=security_group_ids,
        ),
    )

    checkfilenotsecure_arn = ensure_lambda(
        lam,
        LambdaSpec(
            name=names.checkfilenotsecure_fn,
            role_arn=etl_lambda_role_arn,
            handler="lambda_function.lambda_handler",
            runtime="python3.12",
            source_dir=str(lambda_root / "CheckFileNotSecure"),
            env=base_env,
            layers=layers,
            subnet_ids=subnet_ids,
            security_group_ids=security_group_ids,
        ),
    )

    # ---- lambdas used by state machine ----
    setrunning_arn = ensure_lambda(
        lam,
        LambdaSpec(
            name=names.setrunning_fn,
            role_arn=etl_lambda_role_arn,
            handler="lambda_function.lambda_handler",
            runtime="python3.12",
            source_dir=str(lambda_root / "SetRunning"),
            env=base_env,
            layers=layers,
            subnet_ids=subnet_ids,
            security_group_ids=security_group_ids,
        ),
    )

    transferfile_arn = ensure_lambda(
        lam,
        LambdaSpec(
            name=names.transferfile_fn,
            role_arn=etl_lambda_role_arn,
            handler="lambda_function.lambda_handler",
            runtime="python3.12",
            source_dir=str(lambda_root / "TransferFile"),
            env=base_env,  # ✅ includes LANDING_BUCKET + TABLE_NAME
            layers=layers,
            subnet_ids=subnet_ids,
            security_group_ids=security_group_ids,
        ),
    )

    finalizejob_arn = ensure_lambda(
        lam,
        LambdaSpec(
            name=names.finalizejob_fn,
            role_arn=etl_lambda_role_arn,
            handler="lambda_function.lambda_handler",
            runtime="python3.12",
            source_dir=str(lambda_root / "FinalizeJob"),
            env=base_env,
            layers=layers,
            subnet_ids=subnet_ids,
            security_group_ids=security_group_ids,
        ),
    )

    realjobname_arn = ensure_lambda(
        lam,
        LambdaSpec(
            name=names.realjobname_fn,
            role_arn=etl_lambda_role_arn,
            handler="lambda_function.lambda_handler",
            runtime="python3.12",
            source_dir=str(lambda_root / "RealJobName"),
            env=base_env,
            layers=layers,
            subnet_ids=subnet_ids,
            security_group_ids=security_group_ids,
        ),
    )

    # ---- deploy state machine ----
    sfn_role_arn = (cfg.get("stepFunctions", {}) or {}).get("roleArn") or ""
    if not sfn_role_arn:
        raise RuntimeError("stepFunctions.roleArn is empty. (Role creation not implemented here.)")

    sm_inputs = FsaFileChecksStateMachineInputs(
        set_running_lambda_arn=setrunning_arn,
        transfer_file_lambda_arn=transferfile_arn,
        finalize_job_lambda_arn=finalizejob_arn,
    )
    sm_def = FsaFileChecksStateMachineBuilder.filechecks_asl(sm_inputs)

    filechecks_sm_arn = ensure_state_machine(
        sfn,
        StateMachineSpec(
            name=names.sm_filechecks,
            role_arn=sfn_role_arn,
            definition=sm_def,
        ),
    )

    # ---- deploy stream trigger lambda LAST (needs the SM ARN) ----
    stream_env = {
        **base_env,
        "STATE_MACHINE_ARN": filechecks_sm_arn,
        # Optional behavior knobs (enable if desired):
        # "ENABLE_LOCK": "true",
        # "LOCK_STATUS_VALUE": "QUEUED",
        # "DEFAULT_TIMEOUT_SECONDS": "120",
        # "DEFAULT_VERIFY_TLS": "false",
        # "DEFAULT_DEBUG": "false",
    }

    streamstartfilechecks_arn = ensure_lambda(
        lam,
        LambdaSpec(
            name=names.streamstartfilechecks_fn,
            role_arn=etl_lambda_role_arn,
            handler="lambda_function.lambda_handler",
            runtime="python3.11",
            source_dir=str(lambda_root / "StreamStartFileChecks"),
            env=stream_env,
            layers=layers,
            subnet_ids=subnet_ids,
            security_group_ids=security_group_ids,
        ),
    )

    # ---- deploy lambda-data functions ----
    lambda_data_config = parse_lambda_data_config(cfg)
    lambda_data_root = project_dir / "lambda-data"
    
    # Environment variables for lambda-data functions (includes database connection info)
    lambda_data_env = {
        **base_env,
        **lambda_data_config["env_config"],  # Includes env vars from lambda-data config + db connections
    }
    
    # Define the 6 lambda-data functions to deploy  
    lambda_data_functions = [
        ("EchoFetchFlpidsLoad", f"FSA-{deploy_env}-{names.project_name}-EchoFetchFlpidsLoad"),
        ("EchoFetchFlpidsNats", f"FSA-{deploy_env}-{names.project_name}-EchoFetchFlpidsNats"), 
        ("EchoFetchFlpidsRpt", f"FSA-{deploy_env}-{names.project_name}-EchoFetchFlpidsRpt"),
        ("EchoFetchFlpidsScims", f"FSA-{deploy_env}-{names.project_name}-EchoFetchFlpidsScims"),
        ("EchoFetchRC540Monthly", f"FSA-{deploy_env}-{names.project_name}-EchoFetchRC540Monthly"),
        ("EchoFetchRC540Weekly", f"FSA-{deploy_env}-{names.project_name}-EchoFetchRC540Weekly"),
    ]
    
    lambda_data_arns = {}
    for folder_name, function_name in lambda_data_functions:
        arn = ensure_lambda(
            lam,
            LambdaSpec(
                name=function_name,
                role_arn=etl_lambda_role_arn,
                handler="lambda_function.lambda_handler",
                runtime="python3.12",
                source_dir=str(lambda_data_root / folder_name),
                env=lambda_data_env,
                layers=lambda_data_config["layers"] or layers,  # Use lambda-data layers or fallback to default
                subnet_ids=lambda_data_config["subnet_ids"],
                security_group_ids=lambda_data_config["security_group_ids"],
                memory=lambda_data_config["memory_size"],
                timeout=lambda_data_config["timeout"],
            ),
        )
        # Store with consistent key naming
        key_name = folder_name.lower()
        lambda_data_arns[f"{key_name}_lambda_arn"] = arn

    return {
        "checkfile_lambda_arn": checkfile_arn,
        "testfileloader_lambda_arn": testfileloader_arn,
        "dynacheckfile_lambda_arn": dynacheckfile_arn,
        "checkfilenotsecure_lambda_arn": checkfilenotsecure_arn,

        "setrunning_lambda_arn": setrunning_arn,
        "transferfile_lambda_arn": transferfile_arn,
        "finalizejob_lambda_arn": finalizejob_arn,

        "filechecks_state_machine_arn": filechecks_sm_arn,
        "streamstartfilechecks_lambda_arn": streamstartfilechecks_arn,
        "realjobname_lambda_arn": realjobname_arn,
        
        # Lambda-data functions
        "echofetchflpidsload_lambda_arn": lambda_data_arns.get("echofetchflpidsload_lambda_arn"),
        "echofetchflpidsnats_lambda_arn": lambda_data_arns.get("echofetchflpidsnats_lambda_arn"),
        "echofetchflpidsrpt_lambda_arn": lambda_data_arns.get("echofetchflpidsrpt_lambda_arn"),
        "echofetchflpidsscims_lambda_arn": lambda_data_arns.get("echofetchflpidsscims_lambda_arn"),
        "echofetchrc540monthly_lambda_arn": lambda_data_arns.get("echofetchrc540monthly_lambda_arn"),
        "echofetchrc540weekly_lambda_arn": lambda_data_arns.get("echofetchrc540weekly_lambda_arn"),
    }
