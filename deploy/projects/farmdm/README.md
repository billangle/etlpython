# farmdm

Config upload deployer for the FSA Farm Data Mart pipeline. Zips and uploads a library of SQL transformation scripts (EDV, STG, DM, PREP layers) to the S3 Final Zone where Glue ETL jobs consume them at runtime.

**AWS Region:** `us-east-1`  
**Project key:** `farmdm`  
**No Glue jobs, Lambdas, or Step Functions are deployed by this project.**

---

## Overview

This deployer does not create or update any AWS compute resources. Its sole job is:

1. Zip the local `configs/` directory (SQL transformation scripts)
2. Upload the zip to the artifact S3 bucket
3. Invoke the pre-existing `FSA-{ENV}-UploadConfig` Lambda to expand the zip and write all SQL/CSV files to `s3://<final-zone>/farm/`

The SQL files are consumed at runtime by Glue ETL jobs in the broader FPAC Farm Data Mart pipeline.

---

## Environments

| Config file | `deployEnv` | AWS Account | Final zone (upload target) | Artifact bucket |
|---|---|---|---|---|
| [config/farmdm/dev.json](../../config/farmdm/dev.json) | `FPACDEV` | `241533156429` | `c108-dev-fpacfsa-final-zone` | `fsa-dev-ops` |
| [config/farmdm/steamdev.json](../../config/farmdm/steamdev.json) | `steam-dev` | `335965711887` | `punkdev-fpacfsa-final-zone` | `dev-cars-artifacts` |
| [config/farmdm/prod.json](../../config/farmdm/prod.json) | `PROD` | `253490756794` | `c108-prod-fpacfsa-final-zone` | `fsa-prod-ops` |

---

## What Is Uploaded

The `configs/` directory contains SQL config files organised into five layers:

| Layer | Path | Content |
|---|---|---|
| **EDV** (Data Vault) | `_configs/edv/` | ~120 table folders — insert SQLs for history (`_rh`) and raw/snapshot (`_rs`) tables |
| **STG** (Staging) | `_configs/stg/` | ~31 table folders — staging-layer transform SQLs |
| **DM** (Data Mart) | `_configs/dm/` | ~31 table folders — upsert/delete/target SQLs producing FACTs and DIMs (e.g. `FARM_YR_FACT`, `TR_DIM`, `FLD_PRDR_FACT`) |
| **PREP** | `_configs/prep/` | 6 table folders — intermediate prep SQLs |
| **Metadata** | `_configs/metadata/` | `dv_tables.csv`, `dv_tbl_dpnds.csv`, `stg_tables.csv` — dependency graphs |

All files are written to `s3://<final-zone>/farm/` after expansion.

---

## Upload Lambda

The deployer invokes a pre-existing `FSA-{ENV}-UploadConfig` Lambda — not one deployed by this project.

**Invocation payload:**
```json
{
  "input":  { "bucket": "<artifactBucket>", "key": "<zip-s3-key>" },
  "output": { "bucket": "<finalZoneBucket>", "prefix": "farm" },
  "overwrite": false,
  "debug": true
}
```

---

## Deploying

```bash
# From deploy/projects/farmdm/

# Dev
./deploy_config.sh ../../config/farmdm/dev.json

# Steam-dev
./deploy_config.sh ../../config/farmdm/steamdev.json

# Production
./deploy_config.sh ../../config/farmdm/prod.json
```

**What deploy_config.sh does:**
1. Reads config values from the JSON
2. Zips `configs/` → uploads to `s3://<artifactBucket>/input/zips/<name>-<ts>-<sha>.zip`
3. Invokes `FSA-{ENV}-UploadConfig` Lambda with S3 zip location
4. Lambda expands zip → writes all SQL/CSV files to `s3://<final-zone>/farm/`
5. Verifies extracted object count via `s3api list-objects-v2`

## Project Structure

```
farmdm/
├── deploy_config.sh            # Entry point (not deploy.py)
├── scripts/
│   ├── make_and_upload_zip.sh  # Zips configs/ and uploads to S3
│   └── invoke_expand_lambda.sh # Invokes UploadConfig Lambda
├── configs/
│   └── _configs/
│       ├── edv/                # ~120 EDV tables (RH/RS/L/LS patterns)
│       ├── stg/                # ~31 staging tables
│       ├── dm/                 # ~31 data mart tables (FACT/DIM)
│       ├── prep/               # 6 prep/intermediate tables
│       └── metadata/           # dv_tables.csv, dv_tbl_dpnds.csv, stg_tables.csv
└── pipe/
    ├── dev/jenkins
    └── prod/jenkins
```
