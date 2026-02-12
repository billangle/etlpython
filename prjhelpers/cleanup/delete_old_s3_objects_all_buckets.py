#!/usr/bin/env python3
"""
Delete S3 objects older than N days across account buckets.

Features:
- Dry run default
- --execute deletes immediately
- Include bucket prefix filter
- Ignore bucket prefix filter (multiple allowed)


python delete_old_s3_objects_all_buckets.py \
  --days 60 \
  --ignore-bucket aws \
  --ignore-bucket shared \
  --execute

python delete_old_s3_objects_all_buckets.py \                    
  --days 20 \
  --ignore-bucket cdk \
  --ignore-bucket dart    

python delete_old_s3_objects_all_buckets.py \                    
  --days 90 \
  --ignore-bucket cdk \
  --ignore-bucket dms \
  --ignore-bucket cdo \   
  --ignore-bucket aws 
"""

import argparse
from datetime import datetime, timezone, timedelta
from typing import Dict, List

import boto3
from botocore.exceptions import ClientError


def human_size(num_bytes: int) -> str:
    b = float(num_bytes)
    for unit in ["B", "KB", "MB", "GB", "TB", "PB"]:
        if b < 1024:
            return f"{b:.2f} {unit}"
        b /= 1024
    return f"{b:.2f} EB"


def get_bucket_region(s3_global, bucket: str) -> str:
    resp = s3_global.get_bucket_location(Bucket=bucket)
    loc = resp.get("LocationConstraint")
    if loc in (None, ""):
        return "us-east-1"
    if loc == "EU":
        return "eu-west-1"
    return loc


def list_account_buckets(s3_global) -> List[str]:
    resp = s3_global.list_buckets()
    return [b["Name"] for b in resp.get("Buckets", [])]


def bucket_allowed(bucket, include_prefix, ignore_prefixes):
    if include_prefix and not bucket.startswith(include_prefix):
        return False
    for p in ignore_prefixes:
        if bucket.startswith(p):
            return False
    return True


def find_old_objects(s3, bucket, cutoff, obj_prefix):
    paginator = s3.get_paginator("list_objects_v2")

    keys = []
    total_size = 0

    params = {"Bucket": bucket}
    if obj_prefix:
        params["Prefix"] = obj_prefix

    for page in paginator.paginate(**params):
        for obj in page.get("Contents", []):
            if obj["LastModified"] < cutoff:
                keys.append(obj["Key"])
                total_size += obj.get("Size", 0)

    return keys, total_size


def delete_batch(s3, bucket, keys):
    deleted = 0
    errors = 0

    for i in range(0, len(keys), 1000):
        chunk = keys[i:i + 1000]
        try:
            resp = s3.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": k} for k in chunk]},
            )
            deleted += len(resp.get("Deleted", []))
            errors += len(resp.get("Errors", []))
            print(f"    Deleted batch {len(chunk)}")
        except ClientError as e:
            print(f"    Batch delete failed: {e}")
            errors += len(chunk)

    return deleted, errors


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, required=True)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--bucket-prefix", help="Only process buckets with this prefix")
    parser.add_argument(
        "--ignore-bucket",
        action="append",
        default=[],
        help="Skip buckets starting with this prefix (repeatable)",
    )
    parser.add_argument("--object-prefix", help="Only delete objects under this prefix")

    args = parser.parse_args()

    cutoff = datetime.now(timezone.utc) - timedelta(days=args.days)
    s3_global = boto3.client("s3")

    region_clients: Dict[str, any] = {}

    total_candidates = 0
    total_bytes = 0
    total_deleted = 0
    total_errors = 0

    buckets = list_account_buckets(s3_global)

    print(f"\nScanning buckets for objects older than {args.days} days...\n")

    for bucket in buckets:

        if not bucket_allowed(bucket, args.bucket_prefix, args.ignore_bucket):
            print(f"Skipping bucket: {bucket}")
            continue

        print(f"\nBucket: {bucket}")

        try:
            region = get_bucket_region(s3_global, bucket)

            if region not in region_clients:
                region_clients[region] = boto3.client("s3", region_name=region)

            s3 = region_clients[region]

            keys, size = find_old_objects(s3, bucket, cutoff, args.object_prefix)

        except ClientError as e:
            print(f"  Access/list error: {e}")
            total_errors += 1
            continue

        if not keys:
            print("  No old objects found.")
            continue

        total_candidates += len(keys)
        total_bytes += size

        print(f"  Found {len(keys)} old objects ({human_size(size)})")

        if args.execute:
            deleted, errors = delete_batch(s3, bucket, keys)
            total_deleted += deleted
            total_errors += errors

    print("\n===== SUMMARY =====")
    print(f"Candidates: {total_candidates}")
    print(f"Space: {human_size(total_bytes)}")
    print(f"Deleted: {total_deleted}")
    print(f"Errors: {total_errors}")
    print("===================")

    if not args.execute:
        print("\nDRY RUN — nothing deleted.")


if __name__ == "__main__":
    main()
