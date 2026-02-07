import boto3
import os

s3 = boto3.client("s3")

def handler(event, context):
    job_id = str(event["JobId"]).strip()
    sf_name = event["SfName"]

    parts = sf_name.split("-")
    if len(parts) < 3:
        raise ValueError("Invalid SfName format. Expected format like FSA-DEV-SBSD-*")
    env = parts[1].lower()
    bucket = f"c108-{env}-fpacfsa-landing-zone"
    print(f"Landing Bucket: {bucket}")

    source_folder = os.environ.get("source_folder")
    if not source_folder:
        raise ValueError("Missing required env var 'source_folder'")
    source_folder = source_folder.strip("/")

    base_prefix = f"{source_folder}/etl-jobs/"
    print(f"Scanning s3://{bucket}/{base_prefix} for job_id: {job_id}")

    paginator = s3.get_paginator("list_objects_v2")
    job_id_path = None
    for page in paginator.paginate(Bucket=bucket, Prefix=base_prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            candidate = f"{cp['Prefix']}{job_id}/"
            probe = s3.list_objects_v2(Bucket=bucket, Prefix=candidate, MaxKeys=1)
            if probe.get("KeyCount", 0) > 0:
                job_id_path = candidate
                print(f"Found job_id folder: {job_id_path}")
                break
        if job_id_path:
            break

    if not job_id_path:
        raise Exception(f"Could not find job_id {job_id} under any date folder in {base_prefix}")

    datasets_prefix = f"{job_id_path}Datasets/"
    print(f"Datasets prefix: s3://{bucket}/{datasets_prefix}")

    table_names = set()
    for page in paginator.paginate(Bucket=bucket, Prefix=datasets_prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            # cp["Prefix"] = .../Datasets/<TABLE or TABLE-LOAD>/
            sub = cp["Prefix"][len(datasets_prefix):].rstrip("/")
            if not sub or sub.startswith("_") or sub.startswith("."):
                continue
            # strip "-LOAD" suffix if present
            if sub.endswith("-LOAD"):
                sub = sub[:-5]
            table_names.add(sub.lower())
            print(f"Extracted table: {sub.lower()}")

        # ignore top-level files in Datasets/ by design

    result_obj = {"tables": [{"tablename": tbl} for tbl in sorted(table_names)]}
    print("Final extracted tables:", result_obj)
    return result_obj