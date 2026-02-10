#!/usr/bin/env python3
"""
s3_bucket_sizes_to_md.py

Generates a Markdown report (default: s3bucket.md) that includes:
- AWS identity used by boto3 (sts:GetCallerIdentity)
- Per-bucket sizes (sum of object sizes)
- Per-bucket estimated monthly storage cost using S3 Standard pricing (via AWS Pricing API)
- Account totals for bytes + estimated monthly cost

Usage:
  python3 s3_bucket_sizes_to_md.py
  python3 s3_bucket_sizes_to_md.py --profile myprofile
  python3 s3_bucket_sizes_to_md.py --output s3bucket.md
  python3 s3_bucket_sizes_to_md.py --max-workers 16
  python3 s3_bucket_sizes_to_md.py --prefix logs/
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


# ------------------------- formatting helpers -------------------------

def bytes_to_units(num_bytes: int) -> Dict[str, float]:
    # 1024-based units but labeled as MB/GB/TB as requested.
    mb = num_bytes / (1024 ** 2)
    gb = num_bytes / (1024 ** 3)
    tb = num_bytes / (1024 ** 4)
    return {"MB": mb, "GB": gb, "TB": tb}


def bytes_to_gb(num_bytes: int) -> float:
    return num_bytes / (1024 ** 3)


def fmt_units(num_bytes: int) -> str:
    u = bytes_to_units(num_bytes)
    return f"{u['MB']:,.2f} MB | {u['GB']:,.4f} GB | {u['TB']:,.6f} TB"


def md_escape(text: str) -> str:
    return text.replace("|", "\\|")


def usd(x: float) -> str:
    return f"${x:,.2f}"


# ------------------------- S3 sizing -------------------------

@dataclass(frozen=True)
class BucketSizeResult:
    bucket: str
    region: str
    bytes_total: int
    objects_count: int
    error: Optional[str] = None


def normalize_bucket_region(location_constraint: Optional[str]) -> str:
    # S3 returns None/"" for us-east-1, and sometimes legacy "EU" for eu-west-1.
    if not location_constraint:
        return "us-east-1"
    if location_constraint == "EU":
        return "eu-west-1"
    return location_constraint


def get_bucket_region(s3_client, bucket: str) -> str:
    resp = s3_client.get_bucket_location(Bucket=bucket)
    loc = resp.get("LocationConstraint")
    return normalize_bucket_region(loc)


def iter_object_sizes(
    s3_client,
    bucket: str,
    prefix: Optional[str] = None,
) -> Iterable[Tuple[int, int]]:
    paginator = s3_client.get_paginator("list_objects_v2")
    kwargs = {"Bucket": bucket}
    if prefix:
        kwargs["Prefix"] = prefix

    for page in paginator.paginate(**kwargs):
        for obj in page.get("Contents", []) or []:
            yield int(obj.get("Size", 0)), 1


def compute_bucket_size(
    s3_client,
    bucket: str,
    prefix: Optional[str] = None,
) -> BucketSizeResult:
    try:
        region = get_bucket_region(s3_client, bucket)
        total = 0
        count = 0
        for size_bytes, inc in iter_object_sizes(s3_client, bucket, prefix):
            total += size_bytes
            count += inc
        return BucketSizeResult(bucket=bucket, region=region, bytes_total=total, objects_count=count)
    except ClientError as e:
        # Try to still resolve region if possible
        try:
            region = get_bucket_region(s3_client, bucket)
        except Exception:
            region = "unknown"
        return BucketSizeResult(bucket=bucket, region=region, bytes_total=0, objects_count=0, error=str(e))
    except Exception as e:
        try:
            region = get_bucket_region(s3_client, bucket)
        except Exception:
            region = "unknown"
        return BucketSizeResult(bucket=bucket, region=region, bytes_total=0, objects_count=0, error=str(e))


# ------------------------- Pricing (S3 Standard storage) -------------------------

@dataclass(frozen=True)
class PriceTier:
    begin_gb: float
    end_gb: Optional[float]  # None means infinity
    rate_usd_per_gb_month: float


def _location_for_pricing(region: str) -> str:
    """
    AWS Pricing API uses human-readable location names (e.g., 'US East (N. Virginia)'),
    not region codes. This map covers common commercial regions.
    """
    region_to_location = {
        "us-east-1": "US East (N. Virginia)",
        "us-east-2": "US East (Ohio)",
        "us-west-1": "US West (N. California)",
        "us-west-2": "US West (Oregon)",
        "ca-central-1": "Canada (Central)",
        "eu-west-1": "EU (Ireland)",
        "eu-west-2": "EU (London)",
        "eu-west-3": "EU (Paris)",
        "eu-central-1": "EU (Frankfurt)",
        "eu-central-2": "EU (Zurich)",
        "eu-north-1": "EU (Stockholm)",
        "eu-south-1": "EU (Milan)",
        "eu-south-2": "EU (Spain)",
        "ap-south-1": "Asia Pacific (Mumbai)",
        "ap-south-2": "Asia Pacific (Hyderabad)",
        "ap-southeast-1": "Asia Pacific (Singapore)",
        "ap-southeast-2": "Asia Pacific (Sydney)",
        "ap-southeast-3": "Asia Pacific (Jakarta)",
        "ap-southeast-4": "Asia Pacific (Melbourne)",
        "ap-northeast-1": "Asia Pacific (Tokyo)",
        "ap-northeast-2": "Asia Pacific (Seoul)",
        "ap-northeast-3": "Asia Pacific (Osaka)",
        "sa-east-1": "South America (São Paulo)",
        "af-south-1": "Africa (Cape Town)",
        "me-south-1": "Middle East (Bahrain)",
        "me-central-1": "Middle East (UAE)",
        "il-central-1": "Israel (Tel Aviv)",
    }
    return region_to_location.get(region, "US East (N. Virginia)")  # safe fallback


def _fallback_standard_storage_tiers_us_east_1() -> List[PriceTier]:
    # Common S3 Standard tier rates (USD/GB-month) used as a fallback if Pricing API isn't available.
    # Tier boundaries are in GB: 50 TB = 50 * 1024 = 51200 GB; 500 TB = 512000 GB.
    return [
        PriceTier(begin_gb=0.0, end_gb=51200.0, rate_usd_per_gb_month=0.023),
        PriceTier(begin_gb=51200.0, end_gb=512000.0, rate_usd_per_gb_month=0.022),
        PriceTier(begin_gb=512000.0, end_gb=None, rate_usd_per_gb_month=0.021),
    ]


def _apply_tiered_pricing(gb: float, tiers: List[PriceTier]) -> Tuple[float, str]:
    remaining = gb
    cost = 0.0
    parts: List[str] = []
    for t in tiers:
        if remaining <= 0:
            break

        tier_cap = (t.end_gb - t.begin_gb) if t.end_gb is not None else None
        use = remaining if tier_cap is None else min(remaining, max(0.0, tier_cap))

        if use > 0:
            tier_cost = use * t.rate_usd_per_gb_month
            cost += tier_cost
            end_label = f"{t.end_gb:,.0f}GB" if t.end_gb is not None else "∞"
            parts.append(f"{use:,.2f}GB @ {t.rate_usd_per_gb_month:.6f} (tier {t.begin_gb:,.0f}GB–{end_label})")
            remaining -= use

    breakdown = "; ".join(parts) if parts else "0GB"
    return cost, breakdown


@lru_cache(maxsize=128)
def _fetch_s3_standard_storage_tiers(pricing_client, region: str) -> Tuple[List[PriceTier], str]:
    """
    Fetch S3 Standard storage tiers for a given region using AWS Pricing API.
    Returns (tiers, source_note).

    This attempts to locate the OnDemand price dimensions that represent:
    - Amazon S3 Standard - Storage
    - Unit: GB-Mo
    - Tiered ranges

    If anything fails, returns fallback tiers for us-east-1 with a note.
    """
    location = _location_for_pricing(region)

    try:
        # Pricing API is only in a few regions; us-east-1 is typical.
        # ServiceCode must be "AmazonS3"
        resp = pricing_client.get_products(
            ServiceCode="AmazonS3",
            Filters=[
                {"Type": "TERM_MATCH", "Field": "location", "Value": location},
                {"Type": "TERM_MATCH", "Field": "productFamily", "Value": "Storage"},
                # Keep this broad and filter in code; AWS attributes vary.
            ],
            MaxResults=100,
        )

        tiers: List[PriceTier] = []
        matched_any = False

        for price_item_str in resp.get("PriceList", []) or []:
            data = json.loads(price_item_str)
            product = data.get("product", {})
            attrs = product.get("attributes", {})

            # We only want S3 Standard storage (not IA/Glacier/Intelligent-Tiering/etc.)
            # Heuristic: description in priceDimensions must include "S3 Standard" and "Storage".
            terms = data.get("terms", {}).get("OnDemand", {})
            for _term_code, term in terms.items():
                for _dim_code, dim in (term.get("priceDimensions", {}) or {}).items():
                    unit = dim.get("unit")
                    desc = (dim.get("description") or "").lower()
                    price_per_unit = dim.get("pricePerUnit", {}).get("USD")

                    if unit != "GB-Mo":
                        continue
                    if price_per_unit is None:
                        continue

                    # Filter for Standard storage specifically
                    if "standard" not in desc or "storage" not in desc:
                        continue
                    # Avoid "S3 Standard - Infrequent Access" etc.
                    if "infrequent" in desc or "intelligent" in desc or "glacier" in desc or "one zone" in desc:
                        continue

                    matched_any = True

                    begin = float(dim.get("beginRange", "0") or 0.0)
                    end_raw = dim.get("endRange")
                    end = None
                    if end_raw and str(end_raw).lower() != "inf":
                        end = float(end_raw)

                    rate = float(price_per_unit)
                    tiers.append(PriceTier(begin_gb=begin, end_gb=end, rate_usd_per_gb_month=rate))

        if matched_any and tiers:
            # Sort tiers by begin range
            tiers.sort(key=lambda t: t.begin_gb)
            return tiers, f"AWS Pricing API (location={location})"

        # If no match found, fallback
        return _fallback_standard_storage_tiers_us_east_1(), "Fallback rates (Pricing API did not return a clear match)"
    except Exception:
        return _fallback_standard_storage_tiers_us_east_1(), "Fallback rates (Pricing API unavailable or not permitted)"


# ------------------------- report generation -------------------------

def build_markdown_report(
    *,
    generated_at_utc: str,
    identity: Dict[str, str],
    profile: Optional[str],
    prefix: Optional[str],
    pricing_region: str,
    pricing_source_notes: Dict[str, str],
    results: List[BucketSizeResult],
    cost_rows: List[Dict[str, str]],
    account_total_cost: float,
) -> str:
    ok = [r for r in results if not r.error]
    errs = [r for r in results if r.error]

    total_bytes = sum(r.bytes_total for r in ok)
    total_units = fmt_units(total_bytes)

    results_sorted = sorted(results, key=lambda r: (r.error is not None, -r.bytes_total, r.bucket.lower()))

    lines: List[str] = []
    lines.append("# S3 Bucket Storage & Cost Report\n")
    lines.append("This file is auto-generated by `s3_bucket_sizes_to_md.py`.\n")

    lines.append("## Run Context\n")
    lines.append(f"- **Generated (UTC):** {generated_at_utc}")
    lines.append(f"- **AWS Profile:** {profile or '(default credential chain)'}")
    lines.append(f"- **Prefix Filter:** {prefix or '(none)'}")
    lines.append(f"- **Pricing API client region:** {pricing_region}\n")

    lines.append("## AWS Identity (boto3 / STS)\n")
    lines.append(f"- **Account:** `{identity.get('Account', '')}`")
    lines.append(f"- **Arn:** `{identity.get('Arn', '')}`")
    lines.append(f"- **UserId:** `{identity.get('UserId', '')}`\n")

    lines.append("## Account Summary\n")
    lines.append(f"- **Buckets discovered:** {len(results)}")
    lines.append(f"- **Buckets sized successfully:** {len(ok)}")
    lines.append(f"- **Buckets with errors:** {len(errs)}")
    lines.append(f"- **Total storage (successful buckets):** {total_units}")
    lines.append(f"- **Total bytes (successful buckets):** `{total_bytes:,d}`")
    lines.append(f"- **Estimated monthly S3 Standard storage cost (successful buckets):** **{usd(account_total_cost)}**\n")

    lines.append("## Per-Bucket Storage\n")
    lines.append("| Bucket | Region | Objects | Bytes | MB / GB / TB | Status |")
    lines.append("|---|---|---:|---:|---:|---|")
    for r in results_sorted:
        status = "OK" if not r.error else f"ERROR: {md_escape(r.error)}"
        lines.append(
            f"| `{md_escape(r.bucket)}` "
            f"| `{md_escape(r.region)}` "
            f"| {r.objects_count:,d} "
            f"| `{r.bytes_total:,d}` "
            f"| {fmt_units(r.bytes_total)} "
            f"| {status} |"
        )

    lines.append("\n## Per-Bucket Estimated Monthly Storage Cost (S3 Standard)\n")
    lines.append("> **Storage-only estimate.** Excludes requests, retrieval, replication, data transfer, lifecycle transitions, etc.\n")
    lines.append("| Bucket | Region | Storage (GB) | Est. Monthly Cost (USD) | Pricing Source | Tier Breakdown | Status |")
    lines.append("|---|---|---:|---:|---|---|---|")
    for row in cost_rows:
        lines.append(
            f"| `{md_escape(row['bucket'])}` "
            f"| `{md_escape(row['region'])}` "
            f"| {row['gb']} "
            f"| **{row['cost']}** "
            f"| {md_escape(row['pricing_source'])} "
            f"| {md_escape(row['breakdown'])} "
            f"| {md_escape(row['status'])} |"
        )

    lines.append("\n## Pricing Notes\n")
    lines.append("- The script attempts to pull **current** S3 Standard storage pricing via the **AWS Pricing API** per bucket region.")
    lines.append("- If pricing could not be retrieved for a region, it falls back to common S3 Standard tier rates.\n")

    # Optional: summarize which source used per region
    lines.append("### Pricing Source by Region\n")
    lines.append("| Region | Pricing Source |")
    lines.append("|---|---|")
    for region, note in sorted(pricing_source_notes.items(), key=lambda x: x[0]):
        lines.append(f"| `{md_escape(region)}` | {md_escape(note)} |")

    lines.append("\n## Notes\n")
    lines.append("- Sizes are computed by summing object sizes via `ListObjectsV2` pagination (read-only).")
    lines.append("- Very large buckets may take significant time to enumerate.")
    lines.append("- Cost totals exclude buckets that returned errors during sizing.\n")

    return "\n".join(lines)


# ------------------------- main -------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Compute S3 bucket sizes and write a Markdown report to s3bucket.md.")
    ap.add_argument("--profile", default=None, help="AWS profile name (optional).")
    ap.add_argument("--prefix", default=None, help="Optional prefix filter (sizes only keys under this prefix).")
    ap.add_argument("--max-workers", type=int, default=8, help="Parallel workers across buckets (default: 8).")
    ap.add_argument("--no-parallel", action="store_true", help="Disable parallel sizing.")
    ap.add_argument("--output", default="s3bucket.md", help="Output markdown filename (default: s3bucket.md).")
    ap.add_argument(
        "--pricing-region",
        default="us-east-1",
        help="Region to create the AWS Pricing API client in (default: us-east-1).",
    )
    args = ap.parse_args()

    session = boto3.Session(profile_name=args.profile) if args.profile else boto3.Session()
    cfg = Config(retries={"max_attempts": 10, "mode": "standard"})

    s3 = session.client("s3", config=cfg)
    sts = session.client("sts", config=cfg)

    # AWS identity used by boto3
    identity = sts.get_caller_identity()  # Account, Arn, UserId

    # Pricing client (often only supported/used in us-east-1)
    pricing = session.client("pricing", region_name=args.pricing_region, config=cfg)

    # List all buckets
    buckets_resp = s3.list_buckets()
    buckets = [b["Name"] for b in buckets_resp.get("Buckets", [])]

    # Size all buckets
    results: List[BucketSizeResult] = []
    if buckets:
        if args.no_parallel or args.max_workers <= 1:
            for name in buckets:
                results.append(compute_bucket_size(s3, name, prefix=args.prefix))
        else:
            with cf.ThreadPoolExecutor(max_workers=args.max_workers) as ex:
                futs = [ex.submit(compute_bucket_size, s3, name, args.prefix) for name in buckets]
                for fut in cf.as_completed(futs):
                    results.append(fut.result())

    # Build cost table
    pricing_source_notes: Dict[str, str] = {}
    cost_rows: List[Dict[str, str]] = []
    account_total_cost = 0.0

    for r in sorted(results, key=lambda x: x.bucket.lower()):
        if r.error:
            cost_rows.append(
                {
                    "bucket": r.bucket,
                    "region": r.region,
                    "gb": f"{bytes_to_gb(r.bytes_total):,.4f}",
                    "cost": usd(0.0),
                    "pricing_source": "N/A",
                    "breakdown": "N/A",
                    "status": "ERROR (size unavailable)",
                }
            )
            continue

        gb = bytes_to_gb(r.bytes_total)

        tiers, source_note = _fetch_s3_standard_storage_tiers(pricing, r.region)
        pricing_source_notes[r.region] = source_note

        bucket_cost, breakdown = _apply_tiered_pricing(gb, tiers)
        account_total_cost += bucket_cost

        cost_rows.append(
            {
                "bucket": r.bucket,
                "region": r.region,
                "gb": f"{gb:,.4f}",
                "cost": usd(bucket_cost),
                "pricing_source": source_note,
                "breakdown": breakdown,
                "status": "OK",
            }
        )

    generated_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")

    md = build_markdown_report(
        generated_at_utc=generated_at_utc,
        identity={
            "Account": identity.get("Account", ""),
            "Arn": identity.get("Arn", ""),
            "UserId": identity.get("UserId", ""),
        },
        profile=args.profile,
        prefix=args.prefix,
        pricing_region=args.pricing_region,
        pricing_source_notes=pricing_source_notes,
        results=results,
        cost_rows=cost_rows,
        account_total_cost=account_total_cost,
    )

    out_path = Path(args.output).resolve()
    out_path.write_text(md, encoding="utf-8")

    print(f"Wrote Markdown report to: {out_path}")
    print(f"AWS Account: {identity.get('Account')}  Arn: {identity.get('Arn')}")
    print(f"Estimated monthly S3 Standard storage cost (storage only): {usd(account_total_cost)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
