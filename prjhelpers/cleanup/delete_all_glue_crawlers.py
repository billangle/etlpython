#!/usr/bin/env python3
"""
Delete all AWS Glue Crawlers in an account/region.

Features:
- Dry run preview (default)
- Optional name prefix filter
- Confirmation prompt (type DELETE)
- Pagination support
- Safe logging

Usage examples:

Dry run (default):
    python delete_all_glue_crawlers.py

Delete everything:
    python delete_all_glue_crawlers.py --execute

Delete only matching prefix:
    python delete_all_glue_crawlers.py --prefix FSA-DEV --execute
"""

import argparse
import boto3
import sys
from botocore.exceptions import ClientError


def list_crawlers(client, prefix=None):
    names = []
    paginator = client.get_paginator("get_crawlers")

    for page in paginator.paginate():
        for crawler in page.get("Crawlers", []):
            name = crawler.get("Name")
            if not name:
                continue
            if prefix and not name.startswith(prefix):
                continue
            names.append(name)

    return names


def delete_crawler(client, name):
    try:
        client.delete_crawler(Name=name)
        print(f"✅ Deleted crawler: {name}")
    except ClientError as e:
        print(f"❌ Failed to delete crawler {name}: {e}")


def main():
    parser = argparse.ArgumentParser(description="Delete AWS Glue Crawlers")
    parser.add_argument("--prefix", help="Only delete crawlers with this name prefix")
    parser.add_argument("--execute", action="store_true", help="Actually perform deletion")

    args = parser.parse_args()

    client = boto3.client("glue")

    print("\n🔍 Discovering Glue crawlers...")
    crawlers = list_crawlers(client, args.prefix)

    if not crawlers:
        print("No matching crawlers found.")
        return

    print(f"\nFound {len(crawlers)} crawler(s):\n")
    for name in crawlers:
        print(f" - {name}")

    if not args.execute:
        print("\n⚠ DRY RUN — nothing deleted.")
        print("Add --execute to perform deletion.")
        return

    confirm = input("\nType DELETE to confirm: ")
    if confirm != "DELETE":
        print("Cancelled.")
        sys.exit(0)

    print("\n🔥 Deleting crawlers...\n")
    for name in crawlers:
        delete_crawler(client, name)

    print("\n✅ Done.")


if __name__ == "__main__":
    main()
