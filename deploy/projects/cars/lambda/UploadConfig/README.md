# UploadConfig Existing-Zip Runner

This folder contains a JSON-driven helper that invokes the UploadConfig Lambda to expand an existing zip object from one S3 location into another S3 bucket/folder.

## Files

- `lambda_function.py`: Lambda runtime that reads zip bytes from S3 and uploads extracted members to destination S3.
- `run_existing_zip.sh`: Shell wrapper for local use or AWS CloudShell.
- `parms.json`: Default configuration consumed by `run_existing_zip.sh`.

## parms.json

The script runs with no options when `parms.json` is present and valid.

Expected shape:

```json
{
  "function_name": "FSA-FPACDEV-UploadConfig",
  "region": "us-east-1",
  "source": {
    "bucket": "fsa-dev-ops",
    "key": "input/zips/config-20260218-164058-dae4d5562795.zip"
  },
  "target": {
    "bucket": "c108-dev-fpacfsa-final-zone",
    "key": "car"
  },
  "debug": true,
  "out": "lambda_response_existing_zip.json"
}
```

Field meaning:

- `source.bucket`: S3 bucket that already contains the zip object.
- `source.key`: S3 key for the zip object to expand.
- `target.bucket`: S3 bucket where extracted files will be uploaded.
- `target.key`: Destination folder prefix for extracted files.
- `out`: Response file path. Relative paths are written under this UploadConfig folder.

## Usage

Run with defaults from `parms.json`:

```bash
./run_existing_zip.sh
```

Run with overrides:

```bash
./run_existing_zip.sh \
  --source-bucket fsa-dev-ops \
  --source-key input/zips/another.zip \
  --target-bucket c108-dev-fpacfsa-final-zone \
  --target-key car \
  --region us-east-1
```

## AWS CloudShell Notes

This script is CloudShell-friendly:

- Uses `#!/usr/bin/env bash`.
- Requires only `aws` and `python3`.
- Disables pager with `AWS_PAGER=""`.
- Uses `--cli-read-timeout 0` and `--cli-connect-timeout 60`.
- Does not require `jq` (pretty-printing is optional fallback).

## Jenkins One-Liner

Run from the repository root in a pipeline shell step:

```bash
cd deploy/projects/cars/lambda/UploadConfig && ./run_existing_zip.sh
```
