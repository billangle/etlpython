#!/usr/bin/env python3
"""
Delete AWS Lambda functions with safety filters.

Features:
- Dry run preview (default)
- Optional include prefix filter
- Optional ignore prefix filter
- Pagination support
- Confirmation prompt
- Safe logging

Usage examples:

Dry run:
    python delete_all_lambdas.py

Delete only DEV lambdas:
    python delete_all_lambdas.py --prefix FSA-DEV --execute

Delete everything except PROD lambdas:
    python delete_all_lambdas.py --ignore-prefix FSA-steam-dev --execute

Combine filters:
    python delete_all_lambdas.py --prefix FSA --ignore-prefix FSA-PROD --execute 
"""

import argparse
import boto3
import sys
from botocore.exceptions import ClientError


def list_lambdas(client, prefix=None, ignore_prefix=None):
    functions = []
    paginator = client.get_paginator("list_functions")

    for page in paginator.paginate():
        for fn in page.get("Functions", []):
            name = fn.get("FunctionName")
            if not name:
                continue

            # include filter
            if prefix and not name.startswith(prefix):
                continue

            # ignore filter
            if ignore_prefix and name.startswith(ignore_prefix):
                continue

            functions.append(name)

    return functions


def delete_lambda(client, name):
    try:
        client.delete_function(FunctionName=name)
        print(f"✅ Deleted Lambda: {name}")
    except ClientError as e:
        print(f"❌ Failed to delete {name}: {e}")


def main():
    parser = argparse.ArgumentParser(description="Delete AWS Lambda functions safely")
    parser.add_argument("--prefix", help="Only delete functions with this prefix")
    parser.add_argument("--ignore-prefix", help="Skip functions with this prefix")
    parser.add_argument("--execute", action="store_true", help="Actually perform deletion")

    args = parser.parse_args()

    client = boto3.client("lambda")

    print("\n🔍 Discovering Lambda functions...")
    lambdas = list_lambdas(client, args.prefix, args.ignore_prefix)

    if not lambdas:
        print("No matching Lambda functions found.")
        return

    print(f"\nFound {len(lambdas)} Lambda function(s) to delete:\n")
    for name in lambdas:
        print(f" - {name}")

    if not args.execute:
        print("\n⚠ DRY RUN — nothing deleted.")
        print("Add --execute to perform deletion.")
        return

    confirm = input("\nType DELETE to confirm: ")
    if confirm != "DELETE":
        print("Cancelled.")
        sys.exit(0)

    print("\n🔥 Deleting Lambda functions...\n")

    for name in lambdas:
        delete_lambda(client, name)

    print("\n✅ Done.")


if __name__ == "__main__":
    main()
