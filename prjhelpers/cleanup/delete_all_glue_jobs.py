#!/usr/bin/env python3
"""
Delete all AWS Glue Jobs in an account/region.

Features:
- Dry run preview (default)
- Optional name prefix filter
- Confirmation prompt (type DELETE)
- Pagination support
- Safe logging

Usage examples:

Dry run (default):
    python delete_all_glue_jobs.py

Delete everything:
    python delete_all_glue_jobs.py --execute

Delete only matching prefix:
    python delete_all_glue_jobs.py --prefix FSA-DEV --execute
"""

import argparse
import boto3
import sys
from botocore.exceptions import ClientError


def list_glue_jobs(client, prefix=None):
    jobs = []
    paginator = client.get_paginator("get_jobs")

    for page in paginator.paginate():
        for job in page.get("Jobs", []):
            name = job.get("Name")
            if not name:
                continue
            if prefix and not name.startswith(prefix):
                continue
            jobs.append(name)

    return jobs


def delete_glue_job(client, name):
    try:
        client.delete_job(JobName=name)
        print(f"✅ Deleted Glue Job: {name}")
    except ClientError as e:
        print(f"❌ Failed to delete Glue Job {name}: {e}")


def main():
    parser = argparse.ArgumentParser(description="Delete AWS Glue Jobs")
    parser.add_argument("--prefix", help="Only delete Glue jobs with this name prefix")
    parser.add_argument("--execute", action="store_true", help="Actually perform deletion")

    args = parser.parse_args()

    client = boto3.client("glue")

    print("\n🔍 Discovering Glue jobs...")
    jobs = list_glue_jobs(client, args.prefix)

    if not jobs:
        print("No matching Glue jobs found.")
        return

    print(f"\nFound {len(jobs)} Glue job(s):\n")
    for name in jobs:
        print(f" - {name}")

    if not args.execute:
        print("\n⚠ DRY RUN — nothing deleted.")
        print("Add --execute to perform deletion.")
        return

    confirm = input("\nType DELETE to confirm: ")
    if confirm != "DELETE":
        print("Cancelled.")
        sys.exit(0)

    print("\n🔥 Deleting Glue jobs...\n")
    for name in jobs:
        delete_glue_job(client, name)

    print("\n✅ Done.")


if __name__ == "__main__":
    main()
