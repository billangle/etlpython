"""
Lambda: edv-build-processing-plan
Purpose: Get stage tables and EDV tables with dependency-based load order from S3 metadata files

Input:
{
    "data_src_nm": "CARS",
    "run_type": "initial" | "incremental",
    "start_date": "2025-01-12",
    "env": "cert"
}

Output:
{
    "stgTables": ["table1", "table2", ...],
    "dvGroups": [["hub_a", "hub_b"], ["sat_a", "lnk_ab"], ...],
    "counts": {"stg": N, "dv": M, "dvGroups": G},
    "data_src_nm": "CARS",
    "run_type": "initial",
    "start_date": "2025-01-12",
    "env": "cert"
}

S3 Metadata Location:
    s3://c108-{env}-fpacfsa-final-zone/{data_src_nm}/_configs/metadata/stg_tables.csv
    s3://c108-{env}-fpacfsa-final-zone/{data_src_nm}/_configs/metadata/dv_tables.csv
    s3://c108-{env}-fpacfsa-final-zone/{data_src_nm}/_configs/metadata/dv_tbl_dpnds.csv
"""

import os
import json
import boto3
import csv
import logging
from datetime import date
from io import StringIO

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variables
REGION = os.environ.get("REGION", "us-east-1")


def lambda_handler(event, context):
    logger.info(f"Build processing plan: {json.dumps(event)}")

    # Extract parameters
    data_src_nm = event.get("data_src_nm", "").upper()
    run_type = event.get("run_type", "incremental").lower()
    start_date = event.get("start_date")
    env = event.get("env", "cert").lower()

    # Validation
    if not data_src_nm:
        raise ValueError("Missing required 'data_src_nm'")

    if run_type not in ["initial", "incremental"]:
        raise ValueError(f"Invalid run_type: {run_type}. Must be 'initial' or 'incremental'")

    if not start_date:
        start_date = str(date.today())

    try:
        # S3 bucket and prefix
        bucket = f"c108-{env}-fpacfsa-final-zone"
        metadata_prefix = f"{data_src_nm.lower()}/_configs/metadata"

        logger.info(f"Reading metadata from s3://{bucket}/{metadata_prefix}/")

        # 1. Get Stage table names
        stg_tables = fetch_stage_tables_from_s3(bucket, metadata_prefix)
        logger.info(f"Found {len(stg_tables)} stage tables")

        # 2. Get EDV table names with dependency order
        dv_groups = fetch_edv_table_groups_from_s3(bucket, metadata_prefix)
        dv_count = sum(len(g) for g in dv_groups)
        logger.info(f"Found {dv_count} EDV tables in {len(dv_groups)} groups")

        result = {
            "stgTables": stg_tables,
            "dvGroups": dv_groups,
            "counts": {
                "stg": len(stg_tables),
                "dv": dv_count,
                "dvGroups": len(dv_groups)
            },
            "data_src_nm": data_src_nm,
            "run_type": run_type,
            "start_date": start_date,
            "env": env
        }

        logger.info(f"Plan built successfully: {result['counts']}")
        return result

    except Exception as e:
        logger.error(f"Error building processing plan: {e}")
        raise


def read_csv_from_s3(bucket, key):
    """Read CSV file from S3 and return list of dicts."""
    s3 = boto3.client("s3", region_name=REGION)

    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response["Body"].read().decode("utf-8")

        reader = csv.DictReader(StringIO(content), delimiter='|')
        return list(reader)
    except s3.exceptions.NoSuchKey:
        logger.warning(f"File not found: s3://{bucket}/{key}")
        return []
    except Exception as e:
        logger.error(f"Error reading s3://{bucket}/{key}: {e}")
        raise


def fetch_stage_tables_from_s3(bucket, metadata_prefix):
    """Get stage table names from S3 CSV file."""
    key = f"{metadata_prefix}/stg_tables.csv"
    rows = read_csv_from_s3(bucket, key)

    tables = [row["tbl"].lower() for row in rows if row.get("tbl")]
    tables.sort()

    if tables:
        logger.info(f"Stage tables: {tables[:5]}{'...' if len(tables) > 5 else ''}")

    return tables


def fetch_edv_table_groups_from_s3(bucket, metadata_prefix):
    """Get EDV tables with dependency-based load order from S3 CSV files."""

    # Read DV tables
    dv_key = f"{metadata_prefix}/dv_tables.csv"
    dv_rows = read_csv_from_s3(bucket, dv_key)

    if not dv_rows:
        return []

    id_to_name = {row["etl_dv_tbl_id"]: row["dv_tbl_nm"].lower() for row in dv_rows}
    all_tables = list(id_to_name.values())

    # Read dependencies
    deps_key = f"{metadata_prefix}/dv_tbl_dpnds.csv"
    deps_rows = read_csv_from_s3(bucket, deps_key)

    deps = {name: [] for name in all_tables}
    for row in deps_rows:
        child_name = id_to_name.get(row.get("etl_dv_tbl_id"))
        parent_name = id_to_name.get(row.get("rqr_etl_dv_tbl_id"))
        if child_name and parent_name and parent_name in deps:
            deps[child_name].append(parent_name)

    return topological_groups(all_tables, deps)


def topological_groups(tables, deps):
    """Group tables by dependency level using Kahn's algorithm."""

    table_set = set(tables)
    in_degree = {t: 0 for t in table_set}
    children = {t: [] for t in table_set}

    for table in table_set:
        for parent in deps.get(table, []):
            if parent in table_set:
                in_degree[table] += 1
                children[parent].append(table)

    groups = []
    remaining = set(table_set)

    while remaining:
        current = [t for t in remaining if in_degree[t] == 0]

        if not current:
            logger.warning(f"Circular dependency detected among: {sorted(remaining)}")
            groups.append(sorted(remaining))
            break

        current.sort()
        groups.append(current)

        for table in current:
            remaining.remove(table)
            for child in children[table]:
                if child in remaining:
                    in_degree[child] -= 1

    logger.info(f"Created {len(groups)} dependency groups")
    return groups