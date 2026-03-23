# DownloadZip CloudShell Runner

This folder contains the DownloadZip Lambda and a CloudShell-friendly script to invoke it.

## Files

- lambda_function.py: Lambda handler that downloads objects from a source S3 folder, zips them, and uploads the zip to destination S3.
- run_downloadzip_cloudshell.sh: Shell helper to invoke the Lambda.
- parms.json: Default input values for the shell helper.

## Quick Start

Run with defaults from parms.json:

```bash
cd deploy/projects/cars/lambda/DownloadZip
./run_downloadzip_cloudshell.sh
```

Show help:

```bash
./run_downloadzip_cloudshell.sh --help
```

## parms.json

The script reads this file automatically when present.

Current shape:

```json
{
  "source_bucket": "c108-cert-fpacfsa-final-zone",
  "source_folder": "cnsv/_configs/STG",
  "destination_bucket": "fsa-dev-ops",
  "destination_key": "output/cnsv/configs-cert-stg.zip",
  "ignore_patterns": ["*.zip", "*.tmp", "*.bak", "*/_SUCCESS"],
  "include_source_folder_in_zip": false
}
```

Field meaning:

- source_bucket: S3 bucket containing files to include in the zip.
- source_folder: S3 prefix to scan and include.
- destination_bucket: S3 bucket that receives the zip file.
- destination_key: S3 key for the output zip.
- ignore_patterns: Optional glob list to skip files.
- include_source_folder_in_zip: Keep or strip source folder path inside the zip.

## Common Overrides

```bash
./run_downloadzip_cloudshell.sh \
  --region us-east-1 \
  --source-bucket c108-dev-fpacfsa-landing-zone \
  --source-folder cars/etl-jobs \
  --destination-bucket fsa-dev-ops \
  --destination-key output/cars/dev-etl-jobs.zip
```

## AWS CloudShell Notes

- Script is designed for bash and works in AWS CloudShell.
- Uses AWS CLI invoke with configurable read/connect timeouts.
- Can run without arguments when parms.json is populated.
