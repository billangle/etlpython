# S3 Cleanup Tooling

This folder contains S3 cleanup utilities for dry-run analysis, delete execution, and rule-driven behavior.

## Files

- `delete_old_s3_objects_all_buckets.py`: Original account-wide cleanup script.
- `s3_cleanup.py`: Enhanced cleanup script with JSON rule support.
- `s3-non-prod.sh`: CloudShell wrapper for `s3_cleanup.py` + `S3-NON-PROD.json`.
- `s3-prod.sh`: CloudShell wrapper for `s3_cleanup.py` + `S3-PROD.json`.
- `S3-DEV.json`, `S3-CERT.json`, `S3-NON-PROD.json`, `S3-PROD.json`: Rule sets.

## Safety Model

- Default behavior is **dry run**.
- Deletions occur only when `--execute` is provided.
- Shell wrappers print the mode at startup:
  - `Mode: DRY RUN (default, nothing deleted)`
  - `Mode: EXECUTE (deletions enabled)`

## Tool 1: delete_old_s3_objects_all_buckets.py (original)

### Purpose
Delete S3 objects older than a fixed age across buckets (no JSON rules).

### Options
- `--days DAYS` (required): Age threshold in days.
- `--execute`: Perform deletion. Omit for dry run.
- `--bucket-prefix BUCKET_PREFIX`: Process only buckets starting with this prefix.
- `--ignore-bucket IGNORE_BUCKET`: Skip buckets starting with this prefix. Repeatable.
- `--object-prefix OBJECT_PREFIX`: Limit scan/delete to keys under this prefix.

### Example
```bash
python3 delete_old_s3_objects_all_buckets.py \
  --days 90 \
  --ignore-bucket aws \
  --ignore-bucket cdk
```

## Tool 2: s3_cleanup.py (enhanced)

### Purpose
Same base behavior as original script, plus JSON rules for ignore and retention overrides.

### Options
- `--days DAYS` (required): Default retention threshold in days.
- `--execute`: Perform deletion. Omit for dry run.
- `--bucket-prefix BUCKET_PREFIX`: Process only buckets starting with this prefix.
- `--ignore-bucket IGNORE_BUCKET`: Skip buckets starting with this prefix. Repeatable.
- `--object-prefix OBJECT_PREFIX`: Limit scan to keys under this prefix.
- `--rules-json RULES_JSON`: Path to JSON rule file.

### Rule behavior
- `action: "IGNORE"`: Matching objects are skipped.
- `retention: <days>`: Matching objects use this retention instead of `--days`.
- If action text includes `Keep files for <N> days`, `N` is inferred as retention.
- In dry run, rule matches are highlighted with `*****`.
- Objects matching retention rules are not deletion candidates; if past retention they are reported as migration candidates (cheaper storage).

### JSON rule schema
Rules file must be a JSON array of objects, for example:

```json
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
```

### Rule fields
- `Bucket Name` (optional): Exact bucket name to match.
- `Folder`: Prefix to match in object key.
- `files` (optional): Filename/pattern filter (glob style, e.g. `*.csv`).
- `action` (optional): Use `IGNORE` to skip matching objects.
- `retention` (optional): Retention days override for matching objects.

## Tool 3: s3-non-prod.sh

### Purpose
CloudShell wrapper for non-prod rules (`S3-NON-PROD.json`).

### Behavior
- Uses `python3` by default.
- Uses `--days 60` by default unless `--days` is passed.
- Always adds these defaults:
  - `--ignore-bucket cdk`
  - `--ignore-bucket dms`
  - `--ignore-bucket cdo`
  - `--ignore-bucket aws`
- Injects `--rules-json ./S3-NON-PROD.json` automatically.
- Dry run by default; pass `--execute` to delete.

### Environment variables
- `PYTHON_BIN`: Python executable override (default: `python3`).
- `DAYS`: Default days override when `--days` is not provided (default: `60`).

### Example
```bash
./s3-non-prod.sh --days 45 --bucket-prefix c108-dev
```

Equivalent default filter behavior:
```bash
./s3-non-prod.sh \
  --days 60 \
  --ignore-bucket cdk \
  --ignore-bucket dms \
  --ignore-bucket cdo \
  --ignore-bucket aws
```

## Tool 4: s3-prod.sh

### Purpose
CloudShell wrapper for prod rules (`S3-PROD.json`).

### Behavior
- Uses `python3` by default.
- Uses `--days 60` by default unless `--days` is passed.
- Always adds these defaults:
  - `--ignore-bucket cdk`
  - `--ignore-bucket dms`
  - `--ignore-bucket cdo`
  - `--ignore-bucket aws`
- Injects `--rules-json ./S3-PROD.json` automatically.
- Dry run by default; pass `--execute` to delete.

### Environment variables
- `PYTHON_BIN`: Python executable override (default: `python3`).
- `DAYS`: Default days override when `--days` is not provided (default: `60`).

### Example
```bash
./s3-prod.sh --days 90 --bucket-prefix c108-prod
```

Equivalent default filter behavior:
```bash
./s3-prod.sh \
  --days 60 \
  --ignore-bucket cdk \
  --ignore-bucket dms \
  --ignore-bucket cdo \
  --ignore-bucket aws
```

## Recommended dry-run workflow

1. Run non-prod dry run.
2. Review `*****` rule matches and migration candidates.
3. Narrow scope with `--bucket-prefix` and/or `--object-prefix` if needed.
4. Execute only after confirming output.

Example:
```bash
./s3-non-prod.sh --days 60 --bucket-prefix c108-dev
./s3-prod.sh --days 90 --bucket-prefix c108-prod
```
