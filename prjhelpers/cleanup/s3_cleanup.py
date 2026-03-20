#!/usr/bin/env python3
"""
Delete S3 objects older than N days across account buckets, with optional
JSON-based override rules for ignore and retention.

Base features (same as delete_old_s3_objects_all_buckets.py):
- Dry run by default
- --execute deletes immediately
- Include bucket prefix filter
- Ignore bucket prefix filter (multiple allowed)
- Optional object prefix filter

New feature:
- --rules-json <path>: JSON array of rules to ignore folders/folder+file patterns
  or override retention for matching keys.

Rule schema (array of objects):
[
  {
    "Bucket Name": "c108-dev-fpacfsa-landing-zone",
    "Folder": "dmart/raw/fwadm/",
    "files": "*.py",
    "action": "IGNORE"
  },
  {
    "Bucket Name": "c108-dev-fpacfsa-landing-zone",
    "Folder": "fmmi/fmmi_ocfo_files/",
    "retention": 35
  }
]

Notes:
- "action": "IGNORE" skips matching keys.
- "retention": <days> overrides --days for matching keys.
- If retention is missing but action text includes "Keep files for <N> days",
  retention is inferred.
"""

import argparse
import fnmatch
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import boto3
from botocore.exceptions import ClientError


@dataclass(frozen=True)
class CleanupRule:
    bucket: Optional[str]
    folder: str
    files: Optional[str]
    ignore: bool
    retention_days: Optional[int]

    def matches(self, bucket: str, key: str) -> bool:
        if self.bucket and self.bucket != bucket:
            return False

        if self.folder and not key.startswith(self.folder):
            return False

        if not self.files:
            return True

        rel = key[len(self.folder):] if self.folder else key
        basename = key.rsplit("/", 1)[-1]

        if "/" in self.files:
            return fnmatch.fnmatch(rel, self.files) or fnmatch.fnmatch(key, self.files)

        return fnmatch.fnmatch(rel, self.files) or fnmatch.fnmatch(basename, self.files)

    def score(self) -> Tuple[int, int, int]:
        # Higher is more specific.
        return (
            1 if self.bucket else 0,
            1 if self.files else 0,
            len(self.folder),
        )

    def summary_target(self, runtime_bucket: str) -> str:
        # Always summarize by folder path for compact dry-run output.
        target = self.folder or "/"
        target = _normalize_slashes(target)
        return f"{runtime_bucket}:{target}"


def human_size(num_bytes: int) -> str:
    b = float(num_bytes)
    for unit in ["B", "KB", "MB", "GB", "TB", "PB"]:
        if b < 1024:
            return f"{b:.2f} {unit}"
        b /= 1024
    return f"{b:.2f} EB"


def size_tb(num_bytes: int) -> str:
    tb = float(num_bytes) / (1024 ** 4)
    return f"{tb:.2f} TB"


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


def bucket_allowed(bucket: str, include_prefix: Optional[str], ignore_prefixes: List[str]) -> bool:
    if include_prefix and not bucket.startswith(include_prefix):
        return False
    for p in ignore_prefixes:
        if bucket.startswith(p):
            return False
    return True


def _normalize_slashes(path: str) -> str:
    return re.sub(r"/+", "/", path)


def _parse_s3_like_folder(bucket: Optional[str], folder: str) -> Tuple[Optional[str], str]:
    raw = folder.strip()

    # Handle full URI style.
    if raw.startswith("s3://"):
        rest = raw[5:]
        if "/" in rest:
            uri_bucket, key_prefix = rest.split("/", 1)
        else:
            uri_bucket, key_prefix = rest, ""
        if not bucket:
            bucket = uri_bucket
        if bucket == uri_bucket:
            raw = key_prefix
        else:
            raw = rest
    elif raw.startswith("s3:"):
        # Handle malformed style seen in sheets, e.g. s3:bucket/path.
        rest = raw[3:]
        if rest.startswith("//"):
            return _parse_s3_like_folder(bucket, "s3:" + rest)
        if "/" in rest:
            maybe_bucket, key_prefix = rest.split("/", 1)
            if not bucket:
                bucket = maybe_bucket
            if bucket == maybe_bucket:
                raw = key_prefix
            else:
                raw = rest
        else:
            raw = ""

    raw = raw.lstrip("/")
    raw = _normalize_slashes(raw)
    if raw and not raw.endswith("/"):
        raw = raw + "/"
    return bucket, raw


def _parse_retention(action: Optional[str], retention: Optional[int]) -> Optional[int]:
    if retention is not None:
        return int(retention)

    if isinstance(action, str):
        m = re.search(r"keep\s+files\s+for\s+(\d+)\s+days", action, flags=re.IGNORECASE)
        if m:
            return int(m.group(1))

    return None


def load_rules(path: Optional[str]) -> List[CleanupRule]:
    if not path:
        return []

    p = Path(path)
    raw = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise ValueError(f"Rules file must contain a JSON array: {p}")

    rules: List[CleanupRule] = []

    for i, row in enumerate(raw):
        if not isinstance(row, dict):
            continue

        bucket = row.get("Bucket Name")
        folder = row.get("Folder") or ""
        files = row.get("files")
        action = row.get("action")
        retention = row.get("retention")

        if bucket is not None and not isinstance(bucket, str):
            bucket = str(bucket)
        if not isinstance(folder, str):
            folder = str(folder)
        if files is not None and not isinstance(files, str):
            files = str(files)

        bucket, folder = _parse_s3_like_folder(bucket, folder)

        ignore = isinstance(action, str) and action.strip().upper() == "IGNORE"
        retention_days = _parse_retention(action if isinstance(action, str) else None, retention)

        # Skip no-op entries.
        if not ignore and retention_days is None:
            continue

        rules.append(
            CleanupRule(
                bucket=bucket,
                folder=folder,
                files=files.strip() if isinstance(files, str) and files.strip() else None,
                ignore=ignore,
                retention_days=retention_days,
            )
        )

    print(f"Loaded {len(rules)} cleanup rules from {p}")
    return rules


def resolve_rule_effect(
    rules: List[CleanupRule], bucket: str, key: str
) -> Tuple[bool, Optional[int], bool, Optional[CleanupRule]]:
    matched = [r for r in rules if r.matches(bucket, key)]
    if not matched:
        return False, None, False, None

    matched.sort(key=lambda r: r.score(), reverse=True)

    # Ignore wins if any matching ignore rule exists.
    ignore_matches = [r for r in matched if r.ignore]
    if ignore_matches:
        return True, None, True, ignore_matches[0]

    for r in matched:
        if r.retention_days is not None:
            return False, r.retention_days, True, r

    return False, None, True, matched[0]


def _add_rule_summary(
    summary: Dict[str, Dict[str, Union[int, str]]],
    label: str,
    size: int,
    reason: str,
) -> None:
    if label not in summary:
        summary[label] = {"count": 0, "size": 0, "reason": reason}
    summary[label]["count"] += 1
    summary[label]["size"] += size


def find_old_objects(
    s3,
    bucket: str,
    now_utc: datetime,
    default_days: int,
    obj_prefix: Optional[str],
    rules: List[CleanupRule],
) -> Tuple[
    List[str],
    int,
    int,
    int,
    List[Tuple[str, int]],
    Dict[str, Dict[str, Union[int, str]]],
    int,
    int,
    int,
]:
    paginator = s3.get_paginator("list_objects_v2")

    keys: List[str] = []
    total_size = 0
    ignored_by_rule = 0
    retention_override_hits = 0
    migration_candidates: List[Tuple[str, int]] = []
    matched_rule_summary: Dict[str, Dict[str, Union[int, str]]] = {}
    ignored_by_rule_size = 0
    total_old_space_found = 0
    folder_marker_skipped = 0

    params = {"Bucket": bucket}
    if obj_prefix:
        params["Prefix"] = obj_prefix

    for page in paginator.paginate(**params):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            last_modified = obj["LastModified"]

            obj_size = obj.get("Size", 0)

            # Never delete S3 folder marker objects.
            if key.endswith("/"):
                folder_marker_skipped += 1
                continue

            default_cutoff = now_utc - timedelta(days=default_days)
            if last_modified < default_cutoff:
                total_old_space_found += obj_size

            ignore, retention_days, had_match, matched_rule = resolve_rule_effect(rules, bucket, key)
            if ignore:
                ignored_by_rule += 1
                ignored_by_rule_size += obj_size
                label = matched_rule.summary_target(bucket) if matched_rule else f"{bucket}:/"
                _add_rule_summary(matched_rule_summary, label, obj_size, "IGNORE")
                continue

            if retention_days is not None and had_match:
                retention_override_hits += 1
                label = matched_rule.summary_target(bucket) if matched_rule else f"{bucket}:/"
                _add_rule_summary(matched_rule_summary, label, obj_size, f"RETENTION={retention_days}")
                # Retention rule objects are not deletion candidates.
                # If beyond retention, classify for migration to cheaper storage.
                retention_cutoff = now_utc - timedelta(days=retention_days)
                if last_modified < retention_cutoff:
                    migration_candidates.append((key, obj_size))
                continue

            cutoff = now_utc - timedelta(days=default_days)
            if last_modified < cutoff:
                keys.append(key)
                total_size += obj.get("Size", 0)

    return (
        keys,
        total_size,
        ignored_by_rule,
        retention_override_hits,
        migration_candidates,
        matched_rule_summary,
        ignored_by_rule_size,
        total_old_space_found,
        folder_marker_skipped,
    )


def delete_batch(s3, bucket: str, keys: List[str]) -> Tuple[int, int]:
    deleted = 0
    errors = 0

    for i in range(0, len(keys), 1000):
        chunk = keys[i : i + 1000]
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
    parser.add_argument("--days", type=int, required=True, help="Default retention in days")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--bucket-prefix", help="Only process buckets with this prefix")
    parser.add_argument(
        "--ignore-bucket",
        action="append",
        default=[],
        help="Skip buckets starting with this prefix (repeatable)",
    )
    parser.add_argument("--object-prefix", help="Only scan objects under this prefix")
    parser.add_argument(
        "--rules-json",
        help="JSON file with folder/file ignore and retention override rules",
    )

    args = parser.parse_args()

    now_utc = datetime.now(timezone.utc)
    s3_global = boto3.client("s3")
    rules = load_rules(args.rules_json)

    region_clients: Dict[str, object] = {}

    total_candidates = 0
    total_bytes = 0
    total_deleted = 0
    total_errors = 0
    total_ignored_by_rule = 0
    total_retention_overrides = 0
    total_migration_candidates = 0
    total_migration_bytes = 0
    total_rule_ignored_size = 0
    total_old_space_found = 0
    total_folder_markers_skipped = 0

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

            (
                keys,
                size,
                ignored_by_rule,
                retention_hits,
                migration_candidates,
                matched_rule_summary,
                ignored_by_rule_size,
                old_space_found,
                folder_markers_skipped,
            ) = find_old_objects(
                s3=s3,
                bucket=bucket,
                now_utc=now_utc,
                default_days=args.days,
                obj_prefix=args.object_prefix,
                rules=rules,
            )
        except ClientError as e:
            print(f"  Access/list error: {e}")
            total_errors += 1
            continue

        total_ignored_by_rule += ignored_by_rule
        total_rule_ignored_size += ignored_by_rule_size
        total_old_space_found += old_space_found
        total_folder_markers_skipped += folder_markers_skipped
        total_retention_overrides += retention_hits
        total_migration_candidates += len(migration_candidates)
        total_migration_bytes += sum(sz for _, sz in migration_candidates)

        if ignored_by_rule:
            print(f"  Rule-ignored objects: {ignored_by_rule} ({human_size(ignored_by_rule_size)})")
        if retention_hits:
            print(f"  Objects evaluated with retention override: {retention_hits}")
        if migration_candidates:
            print(
                "  Migration candidates (retention reached): "
                f"{len(migration_candidates)} ({human_size(sum(sz for _, sz in migration_candidates))})"
            )

        if not args.execute and matched_rule_summary:
            print("  ***** Rule match summary *****")
            ranked = sorted(
                matched_rule_summary.items(),
                key=lambda kv: (int(kv[1]["size"]), int(kv[1]["count"])),
                reverse=True,
            )
            for label, item in ranked[:3]:
                print(
                    f"  ***** {item['reason']}: {label} -> "
                    f"{item['count']} objects ({human_size(item['size'])})"
                )
            remaining = len(ranked) - 3
            if remaining > 0:
                print(f"  ***** ... plus {remaining} more matched folders")
        if not args.execute and migration_candidates:
            print("  ***** Marked for cheaper storage migration (summary only) *****")

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
    print(f"Folder markers skipped: {total_folder_markers_skipped}")
    print(f"Rule-ignored: {total_ignored_by_rule}")
    print(f"Rule-ignored size: {human_size(total_rule_ignored_size)}")
    print(f"Retention overrides evaluated: {total_retention_overrides}")
    print(f"Migration candidates: {total_migration_candidates}")
    print(f"Migration candidate size: {human_size(total_migration_bytes)}")
    print(f"Total old object space found: {size_tb(total_old_space_found)}")
    print(f"Total deletable object space: {size_tb(total_bytes)}")
    print(f"New total space utilized: {size_tb(max(total_old_space_found - total_bytes, 0))}")
    print("===================")

    if not args.execute:
        print("\nDRY RUN - nothing deleted.")


if __name__ == "__main__":
    main()
