import os
import json
import boto3
import smart_open
import polars as pl
import multiprocessing
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import hashlib

from dart.session import DARTSession
from dart.database import DARTDatabase
from ftps_client import iFTP_TLS, FTPSWriter


# --------------------------------------------------------------------------------------
# Polars formatting
# --------------------------------------------------------------------------------------
@pl.Config(
    tbl_formatting="ASCII_MARKDOWN",
    tbl_hide_column_data_types=True,
    tbl_hide_dataframe_shape=True,
    fmt_str_lengths=1000,
    tbl_width_chars=1000,
    tbl_rows=-1,
)
def frame_to_markdown(df: pl.DataFrame) -> str:
    return str(df)


# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------
def _bool_env(name: str, default: bool = False) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def _redact_value(key: str, value):
    lk = key.lower()
    if lk in ("password", "passwd", "pwd", "secret", "token", "access_key", "secret_key"):
        return "***REDACTED***"
    return value


def _safe_dict(d: dict) -> dict:
    return {k: _redact_value(str(k), v) for k, v in (d or {}).items()}


def _pw_fingerprint(pw: str) -> dict:
    """
    Returns a non-reversible fingerprint of the password so you can confirm
    whether the password changed / which source is being used.
    """
    if pw is None:
        return {"password_present": False, "password_length": 0, "password_sha256_12": None}
    pw_s = str(pw)
    if pw_s == "":
        return {"password_present": False, "password_length": 0, "password_sha256_12": None}
    h = hashlib.sha256(pw_s.encode("utf-8")).hexdigest()[:12]
    return {"password_present": True, "password_length": len(pw_s), "password_sha256_12": h}


def fetch_secret(secret_id: str, secret_key: str) -> dict:
    """
    NOTE: Your original version looked wrong: get_secret_value() returns SecretString/SecretBinary,
    not a dict keyed by secret_key. This version supports either:
      - secret_key is a key inside SecretString JSON, OR
      - secret_key == "SecretString" to return the entire JSON object in SecretString
    """
    client = boto3.client("secretsmanager")
    resp = client.get_secret_value(SecretId=secret_id)

    if "SecretString" in resp and resp["SecretString"]:
        payload = json.loads(resp["SecretString"])
    else:
        # fall back (rare)
        payload = json.loads(resp["SecretBinary"].decode("utf-8"))

    if secret_key == "SecretString":
        return payload

    if secret_key not in payload:
        raise KeyError(f"Secret key '{secret_key}' not found in secret JSON keys={list(payload.keys())}")

    return payload[secret_key]


def print_runtime_context():
    sts = boto3.client("sts")
    ident = sts.get_caller_identity()
    print("[RUNTIME] caller_identity:", ident)
    print("[RUNTIME] AWS_REGION:", os.environ.get("AWS_REGION"))
    print("[RUNTIME] env var 'env':", os.environ.get("env"))
    print("[RUNTIME] function name:", os.environ.get("AWS_LAMBDA_FUNCTION_NAME"))


def print_glue_connection_safely(glue_connection_name: str):
    """
    Best-effort: fetch Glue connection and print host/user/url etc (NO password).
    """
    try:
        glue = boto3.client("glue")
        resp = glue.get_connection(Name=glue_connection_name, HidePassword=False)
        c = resp.get("Connection", {})
        props = c.get("ConnectionProperties", {}) or {}

        # Glue often stores:
        #  - JDBC_CONNECTION_URL
        #  - USERNAME
        #  - PASSWORD
        safe_props = _safe_dict(props)

        # Add password fingerprint (without printing)
        pw = props.get("PASSWORD")
        safe_props.update(_pw_fingerprint(pw))

        print("[GLUE] Connection.Name:", c.get("Name"))
        print("[GLUE] Connection.Type:", c.get("ConnectionType"))
        print("[GLUE] Connection.Properties:", safe_props)
        print("[GLUE] Connection.PhysicalConnectionRequirements:", c.get("PhysicalConnectionRequirements", {}))
    except Exception as e:
        print("[GLUE] Failed to get_connection:", glue_connection_name, "error:", repr(e))


def log_meta_client_safely(meta_client):
    """
    Best-effort introspection of DART LambdaDB client.
    """
    try:
        jdbc = getattr(meta_client, "jdbc_conf", None)
        props = getattr(meta_client, "connection_properties", None)

        print("[DB] meta_client type:", type(meta_client).__name__)

        if jdbc is not None:
            j = dict(jdbc)
            # redact but keep fingerprint
            pw = j.get("password")
            safe_j = _safe_dict(j)
            safe_j.update(_pw_fingerprint(pw))
            print("[DB] jdbc_conf:", safe_j)

        if props is not None:
            p = dict(props)
            pw = p.get("password") or p.get("passwd") or p.get("pwd")
            safe_p = _safe_dict(p)
            safe_p.update(_pw_fingerprint(pw))
            print("[DB] connection_properties:", safe_p)
        else:
            print("[DB] No connection_properties attribute found on meta_client")
    except Exception as e:
        print("[DB] Failed to introspect meta_client:", repr(e))


# --------------------------------------------------------------------------------------
# Lambda Handler
# --------------------------------------------------------------------------------------
def lambda_handler(event, context):
    """
    Sample Event
    {
        "file_pattern": "^(FILE_NATS_\\w+)\\.(\\d{8})\\.csv$",
        "echo_folder": "nats",
        "pipeline": "FLPIDS_ODS",
        "step": "FLPIDS_ODS Landing Zone"
    }
    """
    debug = _bool_env("DEBUG", True)

    if debug:
        print_runtime_context()
        print("[INPUT] event:", event)

    env = os.environ["env"]

    glue_connection_name = os.environ.get("GLUE_CONNECTION_NAME", f"FSA-{env}-PG-DART114")
    meta_db_name = os.environ.get("META_DB_NAME", "metadata_edw")

    if debug:
        print("[CONFIG] GLUE_CONNECTION_NAME:", glue_connection_name)
        print("[CONFIG] META_DB_NAME:", meta_db_name)
        # show Glue connection details (no password, but fingerprint)
        print_glue_connection_safely(glue_connection_name)

    # Build metadata DB client (DART uses Glue connection under the hood)
    meta_client = DARTDatabase.client(
        service="lambda",
        glue_connection=glue_connection_name,
        database=meta_db_name,
    )

    if debug:
        log_meta_client_safely(meta_client)

    filters = {
        "file_pattern": event.get("file_pattern"),
        "targets": event.get("targets", []),
    }

    header = event.get("header", True)
    to_queue = event.get("to_queue", False)

    if debug:
        print("[CONFIG] header:", header, "to_queue:", to_queue, "filters:", filters)

    # Echo FTPS secret
    secret_id = os.environ["SecretId"]
    secret_key = os.environ["SecretKey"]
    secret = fetch_secret(secret_id=secret_id, secret_key=secret_key)

    echo = {
        "connection": {
            "host": secret["echo_ip"],
            "port": 990,
            "username": secret["echo_dart_username"],
            "password": secret["echo_dart_password"],  # used for FTPS only
            "log_level": 0,
        },
        "path": "/{root}/{folder}/in/{subfolder}".format(
            root=secret["echo_dart_path"],
            folder=event["echo_folder"],
            subfolder=event.get("echo_subfolder", "")
        ),
    }

    if debug:
        # Do NOT print FTPS password
        safe_echo = {"connection": _safe_dict(echo["connection"]), "path": echo["path"]}
        safe_echo["connection"].update(_pw_fingerprint(echo["connection"].get("password")))
        print("[ECHO] config:", safe_echo)

    def list_files():
        with iFTP_TLS(timeout=10) as ftps:
            ftps.make_connection(**echo["connection"])
            ftps.cwd(echo["path"])
            files = ftps.filter_entries(**filters)
            print(frame_to_markdown(files))
            return files.rows(named=True)

    def echo_file_transfer(file):
        s3_uri, echo_uri = None, None

        with iFTP_TLS(timeout=600) as ftps:
            ftps.make_connection(**echo["connection"])
            ftps.cwd(echo["path"])

            if re.match(r"^\d{14}$", file["name"].split(".")[-1]):
                data_obj_type_nm = file["name"].split(".")[-2].upper()
            elif "." in file["name"]:
                data_obj_type_nm = file["name"].split(".")[-1].upper()
            else:
                data_obj_type_nm = "Flat File"

            # This is where your DB auth error currently happens:
            with DARTSession(
                client=meta_client,
                job_id=context.log_stream_name,
                ppln_nm=event["pipeline"],
                step_nm=event["step"],
                tgt_nm=file["target"],
                data_eng_oper_nm="Lambda",
                data_obj_type_nm=data_obj_type_nm,
                data_obj_proc_rtn_nm=os.environ.get("FUNCTION_NAME", context.function_name),
                env=env,
                system_date=file["system_date"],
                to_queue=to_queue,
                override_prev_run=True if datetime.now().strftime("%Y%m%d") == "20250714" else False
            ) as session:

                file_date_format = "%Y%m%d%H%M%S" if event["echo_folder"] == "plas" and "MFO900" in event["file_pattern"] else "%Y%m%d"

                dataset = session.dataset.properties._asdict().copy()
                bucket = dataset["data_bkt_nm"]
                key = "{prefix}/{object}".format(
                    prefix=dataset["data_bkt_pfx_nm"],
                    object=dataset["data_bkt_obj_tmpl_txt"].format(
                        YYYYMMDD=file["system_date"].strftime(file_date_format)
                    )
                )

                s3_uri = f"s3://{bucket}/{key}"

                # If > 500 MB, use Glue job
                if ftps.size(file["name"]) > 500000000:
                    load_oper_tgt_id = session.dataset.properties.tgt_id
                    job_name = f"FSA-{env}-GLS-LandingFiles"
                    arguments = {
                        "--FileName": file["name"],
                        "--load_oper_tgt_id": str(load_oper_tgt_id),
                        "--job_id": context.log_stream_name
                    }
                    glue = boto3.client("glue")
                    glue.start_job_run(JobName=job_name, Arguments=arguments)
                    record_count = -1
                else:
                    with smart_open.open(s3_uri, "wb") as f:
                        writer = FTPSWriter(f)
                        if file["name"] in ["TNA_LOANSTATRPT", "TNA_COMMENT"]:
                            total_size = ftps.size(file["name"])
                            print(f"Size of {file['name']}: {total_size}")
                        else:
                            ftps.retrbinary("RETR " + file["name"], writer)
                        record_count = writer.count - 1 if header else writer.count

                ftps.delete(file["name"])
                echo_uri = "{}{}".format(echo["path"], file["name"])

                session.metadata.data_obj_nm = s3_uri
                session.metadata.rcd_ct = record_count
                session.complete = True

        return s3_uri, echo_uri

    files = list_files()

    if files:
        max_workers = multiprocessing.cpu_count() * 10
        print(f"Max Number of Workers: {max_workers}")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(echo_file_transfer, file) for file in files]
            for future in as_completed(futures):
                s3_uri, echo_uri = future.result()
                if s3_uri:
                    print(f"uploaded: {s3_uri}")
                if echo_uri:
                    print(f"deleted: {echo_uri}")

    return {"statusCode": 200, "body": json.dumps("success")}
