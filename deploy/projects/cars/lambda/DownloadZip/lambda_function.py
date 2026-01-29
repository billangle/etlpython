import os
import json
import shutil
import zipfile
from fnmatch import fnmatch
from typing import List, Dict, Any

import boto3

s3 = boto3.client("s3")


def _normalize_prefix(prefix: str) -> str:
    """
    Normalize S3 prefix: allow '' or 'folder' or 'folder/'.
    """
    prefix = (prefix or "").lstrip("/")
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    return prefix


def _safe_join(base_dir: str, rel_path: str) -> str:
    """
    Prevent path traversal and normalize rel_path -> local path.
    """
    rel_path = rel_path.lstrip("/")
    local_path = os.path.normpath(os.path.join(base_dir, rel_path))

    base_abs = os.path.abspath(base_dir)
    local_abs = os.path.abspath(local_path)

    if local_abs != base_abs and not local_abs.startswith(base_abs + os.sep):
        raise ValueError(f"Unsafe key path traversal detected: {rel_path}")

    return local_path


def _should_ignore(key: str, ignore_patterns: List[str]) -> bool:
    """
    Decide whether an S3 object key should be ignored based on glob patterns.

    We check patterns against:
      - the basename (filename only)
      - the full key (so patterns like '*/_SUCCESS' work)
    """
    filename = os.path.basename(key)
    for pat in ignore_patterns:
        if fnmatch(filename, pat) or fnmatch(key, pat):
            return True
    return False


def lambda_handler(event: Dict[str, Any], context):
    """
    Required event fields:
      - source_bucket: str
      - source_folder: str   (S3 prefix/folder; e.g. "my/data/" or "my/data")
      - destination_bucket: str

    Optional:
      - destination_key: str (default: "{source_bucket}-{source_folder_sanitized}.zip")
      - ignore_patterns: list[str] (default: ["*.zip"])
      - include_source_folder_in_zip: bool (default: True)
          If True, files in the zip start under the source_folder path.
          If False, the source_folder prefix is stripped in the zip.

    Returns:
      { ok, destination_bucket, destination_key, zipped_files, skipped_files, zip_size_bytes }
    """
    source_bucket = event.get("source_bucket")
    source_folder = event.get("source_folder")
    destination_bucket = event.get("destination_bucket")

    if not source_bucket:
        raise ValueError("event.source_bucket is required")
    if source_folder is None:
        raise ValueError("event.source_folder is required (can be '' but must be present)")
    if not destination_bucket:
        raise ValueError("event.destination_bucket is required")

    prefix = _normalize_prefix(source_folder)

    ignore_patterns = event.get("ignore_patterns") or ["*.zip"]
    if not isinstance(ignore_patterns, list) or not all(isinstance(x, str) for x in ignore_patterns):
        raise ValueError("event.ignore_patterns must be a list of strings (e.g., ['*.zip','*.tmp'])")

    include_source_folder_in_zip = bool(event.get("include_source_folder_in_zip", True))

    # Default destination key
    destination_key = event.get("destination_key")
    if not destination_key:
        safe_prefix = prefix.rstrip("/").replace("/", "_") or "root"
        destination_key = f"exports/{source_bucket}-{safe_prefix}.zip"

    # Workspace in /tmp
    work_dir = "/tmp/s3zip_work"
    download_root = os.path.join(work_dir, "download")
    zip_path = os.path.join(work_dir, "bundle.zip")

    # Clean workspace
    if os.path.exists(work_dir):
        shutil.rmtree(work_dir)
    os.makedirs(download_root, exist_ok=True)

    paginator = s3.get_paginator("list_objects_v2")
    page_iter = paginator.paginate(Bucket=source_bucket, Prefix=prefix)

    downloaded = 0
    skipped = 0

    for page in page_iter:
        for obj in page.get("Contents", []):
            key = obj["Key"]

            # Skip folder markers
            if key.endswith("/"):
                continue

            if _should_ignore(key, ignore_patterns):
                skipped += 1
                continue

            # Determine the path in the zip
            if include_source_folder_in_zip:
                # Keep full key path (but without leading slash)
                rel_key = key.lstrip("/")
            else:
                # Strip the prefix (folder) so zip starts "inside" that folder
                rel_key = key[len(prefix):] if prefix and key.startswith(prefix) else key
                rel_key = rel_key.lstrip("/")

            local_path = _safe_join(download_root, rel_key)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)

            s3.download_file(source_bucket, key, local_path)
            downloaded += 1

    # Create zip
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(download_root):
            for filename in files:
                full_path = os.path.join(root, filename)
                arcname = os.path.relpath(full_path, download_root)
                zf.write(full_path, arcname)

    # Upload zip
    s3.upload_file(zip_path, destination_bucket, destination_key)

    return {
        "ok": True,
        "source_bucket": source_bucket,
        "source_folder": prefix,
        "destination_bucket": destination_bucket,
        "destination_key": destination_key,
        "ignore_patterns": ignore_patterns,
        "include_source_folder_in_zip": include_source_folder_in_zip,
        "zipped_files": downloaded,
        "skipped_files": skipped,
        "zip_size_bytes": os.path.getsize(zip_path),
    }
