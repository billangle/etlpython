#!/usr/bin/env python3
"""
Generate a Markdown README-style S3 folder size report
sorted by folder size (largest first).
"""

import argparse
import boto3
from collections import defaultdict
from datetime import datetime


def human_size(num_bytes):
    size = float(num_bytes)
    for unit in ["B", "KB", "MB", "GB", "TB", "PB"]:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} EB"


def get_top_folder(key):
    if "/" in key:
        return key.split("/", 1)[0]
    return "(root)"


def get_account_info():
    sts = boto3.client("sts")
    identity = sts.get_caller_identity()
    return identity["Account"], identity["Arn"]


def generate_markdown(bucket, counts, sizes, account, arn, region):
    total_files = sum(counts.values())
    total_bytes = sum(sizes.values())

    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    # 🔥 SORT BY SIZE (largest first)
    sorted_folders = sorted(
        counts.keys(),
        key=lambda f: sizes[f],
        reverse=True
    )

    md = []

    md.append(f"# S3 Bucket Size Report — `{bucket}`\n")

    md.append("## Environment\n")
    md.append(f"- AWS Account: **{account}**")
    md.append(f"- Caller ARN: `{arn}`")
    md.append(f"- Region: **{region}**")
    md.append(f"- Generated: **{timestamp}**\n")

    md.append("## Summary\n")
    md.append(f"- Total objects: **{total_files:,}**")
    md.append(f"- Total storage: **{human_size(total_bytes)}**\n")

    md.append("## Folder Breakdown (Largest First)\n")
    md.append("| Folder | Files | Size (Bytes) | Size |")
    md.append("|--------|------:|-------------:|------|")

    for folder in sorted_folders:
        md.append(
            f"| {folder} | {counts[folder]:,} | {sizes[folder]:,} | {human_size(sizes[folder])} |"
        )

    md.append("\n---\n")
    md.append("_Generated via boto3 S3 analyzer_\n")

    return "\n".join(md)


def summarize_bucket(bucket):
    session = boto3.session.Session()
    region = session.region_name or "default"

    s3 = session.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    counts = defaultdict(int)
    sizes = defaultdict(int)

    print(f"Scanning bucket: {bucket}")

    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            folder = get_top_folder(obj["Key"])
            counts[folder] += 1
            sizes[folder] += obj["Size"]

    account, arn = get_account_info()

    md_content = generate_markdown(
        bucket, counts, sizes, account, arn, region
    )

    filename = f"project-sizes-{bucket}.md"

    with open(filename, "w") as f:
        f.write(md_content)

    print(f"\nMarkdown report generated → {filename}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    args = parser.parse_args()

    summarize_bucket(args.bucket)


if __name__ == "__main__":
    main()
