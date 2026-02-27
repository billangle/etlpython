# Deploy Config Files

This directory contains one JSON configuration file per project per deployment
environment. Config files are passed to the deployer at runtime and drive every
resource name, IAM ARN, bucket reference, Glue job parameter, and Step Functions
timeout — nothing is hardcoded in the deployer scripts.

---

## Directory Layout

```
deploy/config/
├── depeast2.json             # Legacy dev (us-east-2) global deployer config
├── fpacprod.json             # Legacy prod (us-east-2) global deployer config
├── py2deployer.json          # Legacy py2 deployer config
├── pydemo.json               # Demo deployer config
│
├── athenafarm/
│   ├── dev.json
│   ├── prod.json
│   └── steamdev.json
├── cars/
│   ├── dev.json
│   ├── prod.json
│   ├── steamdev.json
│   └── carssteam.json
├── carsdm/
│   ├── dev.json
│   ├── prod.json
│   └── carssteam.json
├── cnsv/
│   ├── dev.json
│   ├── prod.json
│   ├── steamdev.json
│   └── crawlers.example.json
├── farmdm/
│   ├── dev.json
│   ├── prod.json
│   └── steamdev.json
├── flpids/
│   ├── dev.json
│   ├── prod.json
│   └── steamdev.json
├── fmmi/
│   ├── dev.json
│   ├── prod.json
│   └── steamdev.json
├── nps/
│   ├── dev.json
│   ├── prod.json
│   └── steamdev.json
├── pmrds/
│   ├── dev.json
│   ├── prod.json
│   ├── steamdev.json
│   └── cert.json
├── sbsd/
│   ├── dev.json
│   ├── prod.json
│   ├── steamdev.json
│   └── cert.json
└── tsthooks/
    ├── fpacdev.json
    └── steam.json
```

### Environments

| Filename | Environment | AWS Account |
|---|---|---|
| `dev.json` | Development | dev / punkdev |
| `steamdev.json` | Steam / Cert-Dev | steamdev |
| `cert.json` | Certification | cert (selected projects) |
| `prod.json` | Production | `253490756794` (c108-prod) |
| `carssteam.json` | CARS-specific Steam | steamdev |
| `fpacdev.json` | FPAC Dev (tsthooks) | dev |
| `steam.json` | Steam (tsthooks) | steamdev |

---

## Config File Structure

Every config file shares a common set of top-level keys. Some keys are optional
and only appear in projects that use that feature.

### Required Keys (all projects)

```json
{
  "deployEnv": "PROD",
  "project":   "CNSV",
  "region":    "us-east-1",
  "bucketRegion": "us-east-1"
}
```

| Key | Purpose |
|---|---|
| `deployEnv` | Token embedded in every AWS resource name: `FSA-{deployEnv}-{project}-*` |
| `project` | Project tag embedded in every resource name |
| `region` | AWS region for API calls (Glue, Lambda, SFN) |
| `bucketRegion` | Region where S3 buckets reside (may differ from `region`) |

---

### `strparams` — Parameterised ARNs and Bucket Names

Direct string values resolved by the deployer. Preferred over `ssm` in all
modern configs.

```json
"strparams": {
  "landingBucketNameParam":  "c108-prod-fpacfsa-landing-zone",
  "cleanBucketNameParam":    "c108-prod-fpacfsa-cleansed-zone",
  "finalBucketNameParam":    "c108-prod-fpacfsa-final-zone",
  "glueJobRoleArnParam":     "arn:aws:iam::253490756794:role/disc-fsa-prod-glue-servicerole",
  "etlRoleArnParam":         "arn:aws:iam::253490756794:role/disc-fsa-prod-lambda-servicerole",
  "sfnRoleArnParam":         "arn:aws:iam::253490756794:role/disc-fsa-prod-stepfunction-servicerole",
  "snsArnParam":             "arn:aws:sns:us-east-1:253490756794:FSA-PROD-CNSV",
  "pgConnectionNameParam":   "FSA-PROD-PG-DART115",
  "thirdPartyLayerArnParam": "arn:aws:lambda:us-east-1:253490756794:layer:FSA-PROD-MDART-layer:1",
  "customLayerArnParam":     "arn:aws:lambda:us-east-1:253490756794:layer:FSA-polars:1"
}
```

> **Legacy:** Older configs contain an `ssm` block with SSM Parameter Store key
> names instead of direct values. The deployer resolves those at runtime via
> `ssm.get_parameter()`. New projects use `strparams` exclusively.

---

### `artifacts` — S3 Upload Location for Scripts

```json
"artifacts": {
  "artifactBucket": "fsa-prod-ops",
  "prefix":         "cnsv/"
}
```

Glue scripts and Lambda ZIPs are uploaded to
`s3://{artifactBucket}/{prefix}glue/` and `s3://{artifactBucket}/{prefix}lambda/`
respectively before the Glue jobs and Lambda functions are created or updated.

---

### `stepFunctions` — Step Functions IAM Roles

```json
"stepFunctions": {
  "createRoleIfMissing":  false,
  "roleName":             "disc-fsa-prod-stepfunction-servicerole",
  "roleArn":              "arn:aws:iam::253490756794:role/disc-fsa-prod-stepfunction-servicerole",
  "nestedCallerRoleArn":  "arn:aws:iam::253490756794:role/disc-fsa-prod-stepfunction-nestedcaller-servicerole"
}
```

| Key | Purpose |
|---|---|
| `roleArn` | Execution role for all state machines in this project |
| `nestedCallerRoleArn` | Role used when one state machine starts another (nested execution) |
| `createRoleIfMissing` | If `true` the deployer creates the role; set `false` when the role is managed by CDK / CloudFormation |

---

### `GlueConfig` — Per-Job Overrides

An array of single-key objects. Each key matches the Glue script stem name (the
filename without `.py`). Settings here override the global Glue defaults for
that specific job.

```json
"GlueConfig": [
  {
    "Ingest-PG-Reference-Tables": {
      "Connections":             [{ "ConnectionName": "FSA-PROD-PG-DART115" }],
      "MaxConcurrency":          "1",
      "MaxRetries":              "0",
      "TimeoutMinutes":          "480",
      "WorkerType":              "G.2X",
      "NumberOfWorkers":         "4",
      "AutomaticScalingEnabled": "true",
      "GlueVersion":             "4.0",
      "JobLanguage":             "python",
      "GenerateMetrics":         "true",
      "EnableJobBookmarks":      "false",
      "JobObservabilityMetrics": "ENABLED",
      "JobContinuousLogging":    "ENABLED",
      "SparkUILogsPath":         "s3://fsa-prod-ops/glue-logs/spark-ui/athenafarm/",
      "TemporaryPath":           "s3://fsa-prod-ops/temp/glue/",
      "AdditionalPythonModulesPath": "s3://c108-prod-fpacfsa-landing-zone/dart/scripts/python-modules/dart/glue/psycopg2_binary-2.9.9-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl",
      "UseGlueDataCatalogAsTheHiveMetastore": "true",
      "JobParameters": {
        "--target_database": "athenafarm_prod_ref"
      }
    }
  }
]
```

#### `GlueConfig` Field Reference

| Field | Type | Description |
|---|---|---|
| `Connections` | array | Glue VPC connections; use `""` if not needed |
| `WorkerType` | string | `G.1X`, `G.2X`, `G.4X`, `G.025X` |
| `NumberOfWorkers` | string | Worker count; ignored when `AutomaticScalingEnabled=true` |
| `TimeoutMinutes` | string | Glue job timeout — **must be coordinated with the Step Functions `TimeoutSeconds` in the ASL** |
| `AutomaticScalingEnabled` | string | `"true"` enables Glue auto-scaling |
| `EnableJobBookmarks` | string | `"true"` enables incremental processing |
| `AdditionalPythonModulesPath` | string | S3 URI to a `.whl` file added as `--extra-py-files` |
| `JobParameters` | object | Extra `--key value` pairs merged into `DefaultArguments` |

> **Timeout coordination:** `TimeoutMinutes` in `GlueConfig` sets the Glue job
> hard limit. The corresponding Step Functions state must have
> `TimeoutSeconds >= TimeoutMinutes × 60` or Step Functions will terminate the
> wait before Glue finishes. All states in this project use `28800` seconds
> (480 minutes) to match.

---

### `glueJobDefaults` — Global Glue Defaults (optional)

When present, sets defaults for all jobs in the project. Per-job `GlueConfig`
entries override these.

```json
"glueJobDefaults": {
  "WorkerType":          "G.2X",
  "NumberOfWorkers":     10,
  "EnableAutoScaling":   true,
  "Timeout":             60,
  "DefaultArguments": {
    "--enable-metrics":            "true",
    "--enable-continuous-cloudwatch-log": "true"
  }
}
```

---

### `configUpload` — S3 Config Data Upload (cnsv only)

Present only in projects that push local configuration data to S3 as part of
deployment (currently **cnsv**).

```json
"configUpload": {
  "functionName":       "FSA-PROD-UploadConfig",
  "sourceDir":          "configs",
  "inputBucket":        "fsa-prod-ops",
  "inputPrefix":        "input/zips",
  "outputBucket":       "c108-prod-fpacfsa-final-zone",
  "outputPrefix":       "cnsv",
  "minExpectedObjects": 2,
  "debug":              true
}
```

The deployer zips `projects/cnsv/configs/_configs/`, uploads the ZIP to
`inputBucket/inputPrefix`, then invokes `functionName` to unzip and distribute
the contents to `outputBucket/outputPrefix/`. For **sbsd** and **farmdm** the
equivalent upload is handled directly by `deploy_config.sh` without a Lambda.

---

### `configData` — Project Metadata

Miscellaneous project-level metadata consumed by Lambdas or Glue jobs at runtime.

```json
"configData": {
  "databaseName":    "fsa-fpac-db",
  "dynamoTableName": "FsaFpacMetadata"
}
```

---

### Project-Specific Top-Level Keys

Some projects add extra top-level keys for values that don't fit the standard
blocks.

| Key | Projects | Description |
|---|---|---|
| `secretId` | athenafarm, cnsv, fmmi, nps, … | Secrets Manager secret ID for DB credentials |
| `icebergWarehouse` | athenafarm | S3 URI root for Iceberg table files |
| `sssFarmrecordsS3Path` | athenafarm | S3 path for SSS/IBase Parquet source files |
| `debugLogging` | athenafarm | Sets `--debug true` on all Glue jobs when `true` |
| `paths.glueRootPath` | legacy configs | Relative path to Glue scripts (overridden by modern deployers) |

---

## Full Example — `athenafarm/prod.json` (condensed)

```json
{
  "deployEnv":        "PROD",
  "project":          "ATHENAFARM",
  "region":           "us-east-1",
  "bucketRegion":     "us-east-1",
  "secretId":         "FSA-PROD-secrets",
  "debugLogging":     true,
  "icebergWarehouse": "s3://c108-prod-fpacfsa-final-zone/athenafarm/iceberg",
  "sssFarmrecordsS3Path": "s3://c108-prod-fpacfsa-landing-zone/farm_records/dbo/",

  "strparams": {
    "finalBucketNameParam":  "c108-prod-fpacfsa-final-zone",
    "glueJobRoleArnParam":   "arn:aws:iam::253490756794:role/disc-fsa-prod-glue-servicerole",
    "etlRoleArnParam":       "arn:aws:iam::253490756794:role/disc-fsa-prod-lambda-servicerole",
    "sfnRoleArnParam":       "arn:aws:iam::253490756794:role/disc-fsa-prod-stepfunction-servicerole",
    "pgConnectionNameParam": "FSA-PROD-PG-DART115"
  },

  "artifacts": {
    "artifactBucket": "fsa-prod-ops",
    "prefix":         "athenafarm/"
  },

  "stepFunctions": {
    "roleArn": "arn:aws:iam::253490756794:role/disc-fsa-prod-stepfunction-servicerole",
    "nestedCallerRoleArn": "arn:aws:iam::253490756794:role/disc-fsa-prod-stepfunction-nestedcaller-servicerole"
  },

  "GlueConfig": [
    {
      "Ingest-SSS-Farmrecords": {
        "TimeoutMinutes":  "120",
        "NumberOfWorkers": "10",
        "WorkerType":      "G.2X",
        "JobParameters": {
          "--source_database": "sss-farmrecords",
          "--target_database": "athenafarm_prod_raw"
        }
      }
    },
    {
      "Ingest-PG-Reference-Tables": {
        "Connections":     [{ "ConnectionName": "FSA-PROD-PG-DART115" }],
        "TimeoutMinutes":  "480",
        "NumberOfWorkers": "4",
        "AdditionalPythonModulesPath": "s3://...psycopg2_binary...whl",
        "JobParameters": {
          "--target_database": "athenafarm_prod_ref"
        }
      }
    }
  ]
}
```

---

## How the Deployer Uses the Config

```
deploy.py --config config/athenafarm/prod.json
    │
    ├─ deployEnv + project  →  resource naming prefix  FSA-PROD-ATHENAFARM-*
    ├─ strparams             →  IAM ARNs, bucket names, connection names
    ├─ artifacts             →  where Glue scripts and Lambda ZIPs are uploaded
    ├─ stepFunctions.roleArn →  execution role for all state machines
    ├─ GlueConfig[stem]      →  per-job worker type, timeout, connections, args
    └─ project-specific keys →  icebergWarehouse, secretId, sssFarmrecordsS3Path, …
```

Resource names follow the convention:

```
FSA-{deployEnv}-{project}-{ScriptStem}     →  Glue job
FSA-{deployEnv}-{project}-{FunctionName}   →  Lambda function
FSA-{deployEnv}-{project}-{MachineName}    →  Step Functions state machine
```
