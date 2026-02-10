#!/usr/bin/env python3
"""
Delete all AWS Step Functions state machines in an account/region.

Features:
- Dry run preview (default)
- Optional name prefix filter
- Confirmation prompt
- Pagination support
- Safe logging

Usage examples:

Dry run (default):
    python delete_all_stepfunctions.py

Delete everything:
    python delete_all_stepfunctions.py --execute

Delete only matching prefix:
    python delete_all_stepfunctions.py --prefix FSA-DEV --execute
"""

import argparse
import boto3
import sys
from botocore.exceptions import ClientError


def list_state_machines(client, prefix=None):
    machines = []

    paginator = client.get_paginator("list_state_machines")

    for page in paginator.paginate():
        for sm in page["stateMachines"]:
            name = sm["name"]

            if prefix and not name.startswith(prefix):
                continue

            machines.append(sm)

    return machines


def delete_state_machine(client, arn):
    try:
        client.delete_state_machine(stateMachineArn=arn)
        print(f"✅ Deleted: {arn}")
    except ClientError as e:
        print(f"❌ Failed to delete {arn}: {e}")


def main():
    parser = argparse.ArgumentParser(description="Delete AWS Step Functions state machines")
    parser.add_argument("--prefix", help="Only delete state machines with this name prefix")
    parser.add_argument("--execute", action="store_true", help="Actually perform deletion")

    args = parser.parse_args()

    client = boto3.client("stepfunctions")

    print("\n🔍 Discovering state machines...")
    machines = list_state_machines(client, args.prefix)

    if not machines:
        print("No matching state machines found.")
        return

    print(f"\nFound {len(machines)} state machine(s):\n")

    for sm in machines:
        print(f" - {sm['name']}")

    if not args.execute:
        print("\n⚠ DRY RUN — nothing deleted.")
        print("Add --execute to perform deletion.")
        return

    confirm = input("\nType DELETE to confirm: ")

    if confirm != "DELETE":
        print("Cancelled.")
        sys.exit(0)

    print("\n🔥 Deleting state machines...\n")

    for sm in machines:
        delete_state_machine(client, sm["stateMachineArn"])

    print("\n✅ Done.")


if __name__ == "__main__":
    main()
