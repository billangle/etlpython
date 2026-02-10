#!/usr/bin/env python3
"""
s3_bucket_file_age_report.py

Generates a Markdown report (default: s3bucketfileage.md) with per-bucket object counts
grouped into age ranges based on LastModified:

Columns (defaults):
- <=30 days
- 31–60 days
- 61–90 days
- >90 days

You can override the boundaries with --days (e.g. --days 7,30,90)

The report includes:
- AWS identity used by boto3 (sts:GetCallerIdentity)
- Run timestamp (UTC)
- Per-bucket age distribution table
- Account totals

Usage:
  python3 s3_bucket_file_age_report.py
  python3 s3_bucket_file_age_report.py --profile myprofile
  python3 s3_bucket_file_age_report.py --output s3bucketfileage.md
  python3 s3_bucket_file_age_report.py --max-workers 16
  python3 s3_bucket_file_age_report.py --prefix logs/
  python3 s3_bucket_file_age_report.py --days 30,60,90
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


# ------------------------- helpers -------------------------

def md_escape(text: str) -> str:
    return text.replace("|", "\\|")


def parse_days(s: str) -> List[int]:
    """
    Parse comma-separated day boundaries like '30,60,90' into sorted unique ints.
    """
    parts = [p.strip() for p in s.split(",") if p.strip()]
    days = sorted({int(p) for p in parts})
    if len(days) < 1:
        raise ValueError("Must provide at least one day boundary.")
    if any(d <= 0 for d in days):
        raise ValueError("Day boundaries must be positive integers.")
    return days


def normalize_bucket_region(location_constraint: Optional[str]) -> str:
    if not location_constraint:
        return "us-east-1"
    if location_constraint == "EU":
        return "eu-west-1"
    return location_constraint


def get_bucket_region(s3_client, bucket: str) -> str:
    resp = s3_client.get_bucket_location(Bucket=bucket)
    return normalize_bucket_region(resp.get("LocationConstraint"))


def bucket_age_headers(days: List[int]) -> List[str]:
    """
    For [30,60,90] -> ["<=30d","31-60d","61-90d",">90d"]
    """
    headers: List[str] = []
    if not days:
        return headers
    headers.append(f"<= {days[0]}d")
    for i in range(1, len(days)):
        headers.append(f"{days[i-1] + 1}-{days[i]}d")
    headers.append(f"> {days[-1]}d")
    return headers


def classify_age_days(age_days: int, boundaries: List[int]) -> int:
    """
    Return bucket index for given age_days.
    Example boundaries [30,60,90] => bins:
      0: <=30
      1: 31-60
      2: 61-90
      3: >90
    """
    if age_days <= boundaries[0]:
        return 0
    for i in range(1, len(boundaries)):
        if boundaries[i - 1] < age_days <= boundaries[i]:
            return i
    return len(boundaries)  # last bin (> last boundary)


# ------------------------- core scan -------------------------

@dataclass(frozen=True)
class BucketAgeResult:
    bucket: str
    region: str
    counts: List[int]  # one per age bin (len = len(boundaries)+1)
    total_objects: int
    error: Optional[str] = None


def compute_bucket_age_counts(
    s3_client,
    bucket: str,
    *,
    now: datetime,
    boundaries: List[int],
    prefix: Optional[str] = None,
) -> BucketAgeResult:
    """
    List objects and count them by age range based on LastModified.
    """
    try:
        region = get_bucket_region(s3_client, bucket)

        bins = [0] * (len(boundaries) + 1)
        total = 0

        paginator = s3_client.get_paginator("list_objects_v2")
        kwargs = {"Bucket": bucket}
        if prefix:
            kwargs["Prefix"] = prefix

        for page in paginator.paginate(**kwargs):
            for obj in page.get("Contents", []) or []:
                lm = obj.get("LastModified")
                if lm is None:
                    continue

                # LastModified is timezone-aware (UTC) datetime from botocore
                age_days = int((now - lm).total_seconds() // 86400)
                if age_days < 0:
                    age_days = 0

                idx = classify_age_days(age_days, boundaries)
                bins[idx] += 1
                total += 1

        return BucketAgeResult(bucket=bucket, region=region, counts=bins, total_objects=total)
    except ClientError as e:
        try:
            region = get_bucket_region(s3_client, bucket)
        except Exception:
            region = "unknown"
        return BucketAgeResult(bucket=bucket, region=region, counts=[0] * (len(boundaries) + 1), total_objects=0, error=str(e))
    except Exception as e:
        try:
            region = get_bucket_region(s3_client, bucket)
        except Exception:
            region = "unknown"
        return BucketAgeResult(bucket=bucket, region=region, counts=[0] * (len(boundaries) + 1), total_objects=0, error=str(e))


# ------------------------- report -------------------------

def build_markdown(
    *,
    generated_at_utc: str,
    identity: Dict[str, str],
    profile: Optional[str],
    prefix: Optional[str],
    boundaries: List[int],
    results: List[BucketAgeResult],
) -> str:
    headers = bucket_age_headers(boundaries)

    ok = [r for r in results if not r.error]
    errs = [r for r in results if r.error]

    # Account totals across OK buckets
    total_bins = [0] * (len(boundaries) + 1)
    total_objects = 0
    for r in ok:
        total_objects += r.total_objects
        for i, c in enumerate(r.counts):
            total_bins[i] += c

    results_sorted = sorted(results, key=lambda r: (r.error is not None, -r.total_objects, r.bucket.lower()))

    lines: List[str] = []
    lines.append("# S3 Bucket File Age Report\n")
    lines.append("This file is auto-generated by `s3_bucket_file_age_report.py`.\n")

    lines.append("## Run Context\n")
    lines.append(f"- **Generated (UTC):** {generated_at_utc}")
    lines.append(f"- **AWS Profile:** {profile or '(default credential chain)'}")
    lines.append(f"- **Prefix Filter:** {prefix or '(none)'}")
    lines.append(f"- **Age Buckets (days):** {', '.join(map(str, boundaries))} (plus >{boundaries[-1]}d)\n")

    lines.append("## AWS Identity (boto3 / STS)\n")
    lines.append(f"- **Account:** `{identity.get('Account', '')}`")
    lines.append(f"- **Arn:** `{identity.get('Arn', '')}`")
    lines.append(f"- **UserId:** `{identity.get('UserId', '')}`\n")

    lines.append("## Account Summary\n")
    lines.append(f"- **Buckets discovered:** {len(results)}")
    lines.append(f"- **Buckets scanned successfully:** {len(ok)}")
    lines.append(f"- **Buckets with errors:** {len(errs)}")
    lines.append(f"- **Total objects counted (successful buckets):** {total_objects:,d}\n")

    lines.append("## Per-Bucket Object Counts by Age\n")
    # Table header
    col_header = "| Bucket | Region | Total Objects | " + " | ".join(headers) + " | Status |"
    col_sep = "|---|---|---:|" + "|".join(["---:"] * len(headers)) + "|---|"
    lines.append(col_header)
    lines.append(col_sep)

    for r in results_sorted:
        status = "OK" if not r.error else f"ERROR: {md_escape(r.error)}"
        counts_str = " | ".join(f"{c:,d}" for c in r.counts)
        lines.append(
            f"| `{md_escape(r.bucket)}` "
            f"| `{md_escape(r.region)}` "
            f"| {r.total_objects:,d} "
            f"| {counts_str} "
            f"| {status} |"
        )

    lines.append("\n## Account Totals by Age (Successful Buckets)\n")
    lines.append("| Total Objects | " + " | ".join(headers) + " |")
    lines.append("|---:|" + "|".join(["---:"] * len(headers)) + "|")
    lines.append("| " + f"{total_objects:,d}" + " | " + " | ".join(f"{c:,d}" for c in total_bins) + " |")

    lines.append("\n## Notes\n")
    lines.append("- Ages are based on each object’s `LastModified` timestamp.")
    lines.append("- Counts are for objects, not prefixes/folders.")
    lines.append("- Very large buckets may take significant time to enumerate.\n")

    return "\n".join(lines)


# ------------------------- main -------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Generate per-bucket S3 object age counts and write a Markdown report.")
    ap.add_argument("--profile", default=None, help="AWS profile name (optional).")
    ap.add_argument("--prefix", default=None, help="Optional prefix filter (only count keys under this prefix).")
    ap.add_argument("--max-workers", type=int, default=8, help="Parallel workers across buckets (default: 8).")
    ap.add_argument("--no-parallel", action="store_true", help="Disable parallel scanning.")
    ap.add_argument("--output", default="s3bucketfileage.md", help="Output markdown filename (default: s3bucketfileage.md).")
    ap.add_argument("--days", default="30,60,90", help="Comma-separated day boundaries (default: 30,60,90).")
    args = ap.parse_args()

    boundaries = parse_days(args.days)

    session = boto3.Session(profile_name=args.profile) if args.profile else boto3.Session()
    cfg = Config(retries={"max_attempts": 10, "mode": "standard"})

    s3 = session.client("s3", config=cfg)
    sts = session.client("sts", config=cfg)

    identity = sts.get_caller_identity()  # Account, Arn, UserId

    # List buckets
    buckets_resp = s3.list_buckets()
    buckets = [b["Name"] for b in buckets_resp.get("Buckets", [])]

    now = datetime.now(timezone.utc)

    results: List[BucketAgeResult] = []
    if buckets:
        if args.no_parallel or args.max_workers <= 1:
            for name in buckets:
                results.append(
                    compute_bucket_age_counts(
                        s3, name, now=now, boundaries=boundaries, prefix=args.prefix
                    )
                )
        else:
            with cf.ThreadPoolExecutor(max_workers=args.max_workers) as ex:
                futs = [
                    ex.submit(
                        compute_bucket_age_counts,
                        s3,
                        name,
                        now=now,
                        boundaries=boundaries,
                        prefix=args.prefix,
                    )
                    for name in buckets
                ]
                for fut in cf.as_completed(futs):
                    results.append(fut.result())

    generated_at_utc = now.strftime("%Y-%m-%d %H:%M:%S %Z")

    md = build_markdown(
        generated_at_utc=generated_at_utc,
        identity={
            "Account": identity.get("Account", ""),
            "Arn": identity.get("Arn", ""),
            "UserId": identity.get("UserId", ""),
        },
        profile=args.profile,
        prefix=args.prefix,
        boundaries=boundaries,
        results=results,
    )

    out_path = Path(args.output).resolve()
    out_path.write_text(md, encoding="utf-8")

    print(f"Wrote Markdown report to: {out_path}")
    print(f"AWS Account: {identity.get('Account')}  Arn: {identity.get('Arn')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
