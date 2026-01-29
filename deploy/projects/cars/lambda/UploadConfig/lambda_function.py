from __future__ import annotations

import io
import os
import posixpath
import zipfile
from typing import Any, Dict

import boto3


def _is_safe_member(name: str) -> bool:
    # Prevent zip slip: no absolute paths and no traversal
    if not name or name.endswith("/"):
        return False
    name = name.replace("\\", "/")
    if name.startswith("/") or name.startswith("\\"):
        return False
    if ".." in name.split("/"):
        return False
    return True


def _normalize_prefix(prefix: str) -> str:
    prefix = (prefix or "").strip()
    prefix = prefix.lstrip("/")
    return prefix.rstrip("/")


def handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    """
    Expand a zip stored in S3 input and write extracted objects to S3 output.

    Event:
    {
      "input":  { "bucket": "...", "key": "path/to/file.zip" },
      "output": { "bucket": "...", "prefix": "cars/expanded" },
      "overwrite": false,     # optional
      "max_files": 20000,     # optional safety
      "debug": false          # optional
    }
    """
    input_cfg = event.get("input") or {}
    out_cfg = event.get("output") or {}

    in_bucket = input_cfg.get("bucket") or os.environ.get("INPUT_BUCKET")
    in_key = input_cfg.get("key") or os.environ.get("INPUT_KEY")

    out_bucket = out_cfg.get("bucket") or os.environ.get("OUTPUT_BUCKET")
    out_prefix = _normalize_prefix(out_cfg.get("prefix") or os.environ.get("OUTPUT_PREFIX") or "")

    _ = bool(event.get("overwrite", False))  # S3 PutObject overwrites; kept for parity
    max_files = int(event.get("max_files", 20000))
    debug = bool(event.get("debug", False))

    if not in_bucket or not in_key:
        raise ValueError("Missing input.bucket or input.key (or INPUT_BUCKET/INPUT_KEY env vars)")
    if not out_bucket:
        raise ValueError("Missing output.bucket (or OUTPUT_BUCKET env var)")

    s3 = boto3.client("s3")

    obj = s3.get_object(Bucket=in_bucket, Key=in_key)
    body = obj["Body"].read()

    zf = zipfile.ZipFile(io.BytesIO(body), "r")

    uploaded = 0
    skipped = 0
    bytes_uploaded = 0
    sample_keys = []

    for i, info in enumerate(zf.infolist(), start=1):
        if i > max_files:
            raise RuntimeError(f"Zip contains more than max_files={max_files}")

        name = info.filename.replace("\\", "/")
        if name.endswith("/"):
            continue
        if not _is_safe_member(name):
            skipped += 1
            continue

        out_key = f"{out_prefix}/{name}" if out_prefix else name
        out_key = posixpath.normpath(out_key)

        if out_prefix and not out_key.startswith(out_prefix):
            skipped += 1
            continue

        with zf.open(info, "r") as src:
            s3.upload_fileobj(src, out_bucket, out_key)

        uploaded += 1
        bytes_uploaded += int(info.file_size)

        if debug and len(sample_keys) < 200:
            sample_keys.append(out_key)

    result = {
        "input": {"bucket": in_bucket, "key": in_key},
        "output": {"bucket": out_bucket, "prefix": out_prefix},
        "uploaded": uploaded,
        "skipped": skipped,
        "bytes_uploaded": bytes_uploaded,
    }
    if debug:
        result["keys"] = sample_keys

    return result
