#!/usr/bin/env python3
"""
s3_aged_filesizes_report.py

Generates a Markdown report (default: s3-aged-filesizes.md) that shows, per bucket,
the TOTAL SIZE of objects grouped into age ranges based on LastModified.

Example columns (default):
- <=30 days (TB)
- 31–60 days (TB)
- 61–90 days (TB)
- >90 days (TB)

Also includes:
- AWS identity used by boto3 (sts:GetCallerIdentity)
- Run timestamp (UTC)
- Per-bucket totals by age (TB)
- Account totals by age (TB)
- Optional prefix filtering

IMPORTANT:
- This enumerates ALL objects (ListObjectsV2) to sum sizes by age; large buckets can take time.

Usage:
  python3 s3_aged_filesizes_report.py
  python3 s3_aged_filesizes_report.py --profile myprofile
  python3 s3_aged_filesizes_report.py --output s3-aged-filesizes.md
  python3 s3_aged_filesizes_report.py --max-workers 16
  python3 s3_aged_filesizes_report.py --prefix logs/
  python3 s3_aged_filesizes_report.py --days 30,60,90
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


# ------------------------- helpers -------------------------

def md_escape(text: str) -> str:
    return text.replace("|", "\\|")


def parse_days(s: str) -> List[int]:
    parts = [p.strip() for p in s.split(",") if p.strip()]
    days = sorted({int(p) for p in parts})
    if not days:
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


def age_headers(days: List[int]) -> List[str]:
    # For [30,60,90] -> ["<= 30d","31-60d","61-90d","> 90d"]
    headers: List[str] = []
    headers.append(f"<= {days[0]}d")
    for i in range(1, len(days)):
        headers.append(f"{days[i-1] + 1}-{days[i]}d")
    headers.append(f"> {days[-1]}d")
    return headers


def classify_age_days(age_days: int, boundaries: List[int]) -> int:
    """
    Return bin index for age_days.
    boundaries [30,60,90] => bins:
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
    return len(boundaries)  # > last boundary


def bytes_to_tb(num_bytes: int) -> float:
    return num_bytes / (1024 ** 4)


def fmt_tb(num_bytes: int) -> str:
    return f"{bytes_to_tb(num_bytes):,.6f}"


# ------------------------- core scan -------------------------

@dataclass(frozen=True)
class BucketAgedSizeResult:
    bucket: str
    region: str
    sizes_by_age_bytes: List[int]  # len = len(boundaries)+1
    total_bytes: int
    total_objects: int
    error: Optional[str] = None


def compute_bucket_aged_sizes(
    s3_client,
    bucket: str,
    *,
    now: datetime,
    boundaries: List[int],
    prefix: Optional[str] = None,
) -> BucketAgedSizeResult:
    """
    Sum object sizes into age bins by LastModified.
    """
    bins_bytes = [0] * (len(boundaries) + 1)
    total_bytes = 0
    total_objs = 0

    try:
        region = get_bucket_region(s3_client, bucket)

        paginator = s3_client.get_paginator("list_objects_v2")
        kwargs = {"Bucket": bucket}
        if prefix:
            kwargs["Prefix"] = prefix

        for page in paginator.paginate(**kwargs):
            for obj in page.get("Contents", []) or []:
                lm = obj.get("LastModified")
                if lm is None:
                    continue

                size = int(obj.get("Size", 0))
                age_days = int((now - lm).total_seconds() // 86400)
                if age_days < 0:
                    age_days = 0

                idx = classify_age_days(age_days, boundaries)
                bins_bytes[idx] += size

                total_bytes += size
                total_objs += 1

        return BucketAgedSizeResult(
            bucket=bucket,
            region=region,
            sizes_by_age_bytes=bins_bytes,
            total_bytes=total_bytes,
            total_objects=total_objs,
        )

    except ClientError as e:
        try:
            region = get_bucket_region(s3_client, bucket)
        except Exception:
            region = "unknown"
        return BucketAgedSizeResult(
            bucket=bucket,
            region=region,
            sizes_by_age_bytes=[0] * (len(boundaries) + 1),
            total_bytes=0,
            total_objects=0,
            error=str(e),
        )
    except Exception as e:
        try:
            region = get_bucket_region(s3_client, bucket)
        except Exception:
            region = "unknown"
        return BucketAgedSizeResult(
            bucket=bucket,
            region=region,
            sizes_by_age_bytes=[0] * (len(boundaries) + 1),
            total_bytes=0,
            total_objects=0,
            error=str(e),
        )


# ------------------------- report -------------------------

def build_markdown(
    *,
    generated_at_utc: str,
    identity: Dict[str, str],
    profile: Optional[str],
    prefix: Optional[str],
    boundaries: List[int],
    results: List[BucketAgedSizeResult],
) -> str:
    headers = age_headers(boundaries)

    ok = [r for r in results if not r.error]
    errs = [r for r in results if r.error]

    # Totals across successful buckets
    total_bins_bytes = [0] * (len(boundaries) + 1)
    total_bytes = 0
    total_objects = 0
    for r in ok:
        total_bytes += r.total_bytes
        total_objects += r.total_objects
        for i, b in enumerate(r.sizes_by_age_bytes):
            total_bins_bytes[i] += b

    results_sorted = sorted(results, key=lambda r: (r.error is not None, -r.total_bytes, r.bucket.lower()))

    lines: List[str] = []
    lines.append("# S3 Aged File Sizes Report\n")
    lines.append("This file is auto-generated by `s3_aged_filesizes_report.py`.\n")

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
    lines.append(f"- **Total objects scanned (successful buckets):** {total_objects:,d}")
    lines.append(f"- **Total size (successful buckets):** {bytes_to_tb(total_bytes):,.6f} TB\n")

    lines.append("## Per-Bucket Aged Sizes (TB)\n")
    lines.append("> Each cell is the **sum of object sizes** whose `LastModified` falls within that age range.\n")

    # Table
    col_header = "| Bucket | Region | Total Objects | Total (TB) | " + " | ".join(f"{h} (TB)" for h in headers) + " | Status |"
    col_sep = "|---|---|---:|---:|" + "|".join(["---:"] * len(headers)) + "|---|"
    lines.append(col_header)
    lines.append(col_sep)

    for r in results_sorted:
        status = "OK" if not r.error else f"ERROR: {md_escape(r.error)}"
        cells = " | ".join(fmt_tb(b) for b in r.sizes_by_age_bytes)
        lines.append(
            f"| `{md_escape(r.bucket)}` "
            f"| `{md_escape(r.region)}` "
            f"| {r.total_objects:,d} "
            f"| {bytes_to_tb(r.total_bytes):,.6f} "
            f"| {cells} "
            f"| {status} |"
        )

    lines.append("\n## Account Totals by Age (TB) — Successful Buckets\n")
    lines.append("| Total Objects | Total (TB) | " + " | ".join(f"{h} (TB)" for h in headers) + " |")
    lines.append("|---:|---:|" + "|".join(["---:"] * len(headers)) + "|")
    lines.append(
        "| "
        f"{total_objects:,d} | "
        f"{bytes_to_tb(total_bytes):,.6f} | "
        + " | ".join(fmt_tb(b) for b in total_bins_bytes)
        + " |"
    )

    lines.append("\n## Notes\n")
    lines.append("- Ages are computed from each object’s `LastModified` timestamp.")
    lines.append("- Sizes are computed by summing each object’s `Size` from `ListObjectsV2` (read-only).")
    lines.append("- Counts/sizes are for objects, not prefixes/folders.")
    lines.append("- Very large buckets may take significant time to enumerate.\n")

    return "\n".join(lines)


# ------------------------- main -------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Generate per-bucket S3 aged file sizes (TB) and write Markdown report.")
    ap.add_argument("--profile", default=None, help="AWS profile name (optional).")
    ap.add_argument("--prefix", default=None, help="Optional prefix filter (only scan keys under this prefix).")
    ap.add_argument("--max-workers", type=int, default=8, help="Parallel workers across buckets (default: 8).")
    ap.add_argument("--no-parallel", action="store_true", help="Disable parallel scanning.")
    ap.add_argument("--output", default="s3-aged-filesizes.md", help="Output markdown filename (default: s3-aged-filesizes.md).")
    ap.add_argument("--days", default="30,60,90", help="Comma-separated day boundaries (default: 30,60,90).")
    args = ap.parse_args()

    boundaries = parse_days(args.days)
    now = datetime.now(timezone.utc)

    session = boto3.Session(profile_name=args.profile) if args.profile else boto3.Session()
    cfg = Config(retries={"max_attempts": 10, "mode": "standard"})

    s3 = session.client("s3", config=cfg)
    sts = session.client("sts", config=cfg)

    identity = sts.get_caller_identity()  # Account, Arn, UserId

    # List buckets
    buckets_resp = s3.list_buckets()
    buckets = [b["Name"] for b in buckets_resp.get("Buckets", [])]

    results: List[BucketAgedSizeResult] = []
    if buckets:
        if args.no_parallel or args.max_workers <= 1:
            for name in buckets:
                results.append(
                    compute_bucket_aged_sizes(
                        s3, name, now=now, boundaries=boundaries, prefix=args.prefix
                    )
                )
        else:
            with cf.ThreadPoolExecutor(max_workers=args.max_workers) as ex:
                futs = [
                    ex.submit(
                        compute_bucket_aged_sizes,
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
