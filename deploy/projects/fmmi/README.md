# fmmi

Ingests FSA financial system (FMMI/SAP) CSV data from an Echo FTPS server into S3, transforms 19 tables through Staging and ODS layers as Parquet, and makes them queryable via Glue Data Catalog/Athena.

**AWS Region:** `us-east-1`  
**Project key:** `fmmi`  
**Resource naming:** `FSA-{deployEnv}-FMMI-{ResourceName}`

---

## Overview

```
Echo FTPS (fmmi/in/) ──► LandingFiles Glue ──► S3 Landing Zone (fmmi/fmmi_ocfo_files/YYYYMMDD/)
                                                        │
                              S3-STG-ODS-parquet Glue ◄─┘
                                                │
                    ┌───────────────────────────┴────────────────────────┐
                    ▼                                                     ▼
          S3 Cleansed Zone (fmmi_stg/)                      S3 Final Zone (fmmi_ods/)
                                                                         │
                                              Ora-DMS-ODS Glue ◄─────── Oracle DMS CSVs
                                                                         │
                                                              Glue Crawler → Athena
```

Files matched: `FMMI.FSA*` — 19 distinct financial tables (GL, payroll, commitments, cost centers, fund hierarchy, POs, vendors, WBS, etc.). File mode is either `reload` (full-replace) or `append` per table.

---

## Environments

| Config file | `deployEnv` | AWS Account | Secrets | SNS topic |
|---|---|---|---|---|
| [config/fmmi/dev.json](../../config/fmmi/dev.json) | `FPACDEV` | `241533156429` | `FSA-CERT-Secrets` | `FSA-DEV-FMMI` |
| [config/fmmi/steamdev.json](../../config/fmmi/steamdev.json) | `steam-dev` | `335965711887` | `FSA-CERT-Secrets` | `FSA-DEV-FMMI` |
| [config/fmmi/prod.json](../../config/fmmi/prod.json) | `PROD` | `253490756794` | `FSA-PROD-secrets` | `FSA-PROD-FMMI` |

---

## Glue Jobs

| Job name (dev) | Script | Source | Target | Description |
|---|---|---|---|---|
| `FSA-FPACDEV-FMMI-LandingFiles` | `glue/FMMI-LandingFiles.py` | Echo FTPS `ftps://<host>/fmmi/in/` | `s3://<landing>/fmmi/fmmi_ocfo_files/<YYYYMMDD>/` | Downloads `FMMI.FSA*` CSV files via `curl --ftp-ssl`; timestamps into dated landing folder; writes `_NO_FILES` sentinel if nothing found; sends SNS alert on no-files. |
| `FSA-FPACDEV-FMMI-S3-STG-ODS-parquet` | `glue/S3-STG-ODS-parquet.py` | `s3://<landing>/fmmi/fmmi_ocfo_files/` | `s3://<cleansed>/fmmi_stg/` → `s3://<final>/fmmi_ods/` | Reads CSVs; applies per-table JSON schema configs; writes STG then ODS parquet; special GL and System Assurance reconciliation via `TMP_GL`/`TMP_SA` intermediates; adds 5 audit columns. 19 tables. |
| `FSA-FPACDEV-FMMI-Ora-DMS-ODS` | `glue/Ora-DMS-ODS.py` | `s3://<final>/fmmi/fmmi_ods/cs_tbi_*/` (headerless Oracle DMS CSVs) | `s3://<final>/fmmi/fmmi_ods/CS_TBL_*/` | Reads no-header CSVs; applies schema from `config/<table>_source.json`; adds 5 audit columns; writes Parquet. Processes 9 append tables. |

**Worker configuration (prod):**

| Job | WorkerType | Workers | AutoScaling | Timeout |
|---|---|---|---|---|
| LandingFiles | `G.2X` | 10 | true | 2880 min (48 h) |
| S3-STG-ODS-parquet | `G.4X` | 20 | true | 4880 min |
| Ora-DMS-ODS | `G.2X` | 2 | true | 480 min |

**FMMI table map (19 tables):**

| Source filename token | ODS table | Mode |
|---|---|---|
| `FMMI.FSA.MD.CMMITEM` | `CS_TBL_CMMT_ITEM` | reload |
| `FMMI.FSA.CMMT` | `CS_TBL_COMMITMENT` | append |
| `FMMI.FSA.MD_COSTCTR` | `CS_TBL_COST_CENTER` | reload |
| `FMMI.FSA.MD_CUSTOMER` | `CS_TBL_CUSTOMER` | reload |
| `FMMI.FSA.MD_FUNCAREA` | `CS_TBL_FUNC_AREA` | reload |
| `FMMI.FSA.MD_FUND` | `CS_TBL_FUND` | reload |
| `FMMI.FSA.FUNDEDPRG` | `CS_TBL_FUNDED_PROGRAM` | reload |
| `FMMI.FSA.MD_FUNDCTR` | `CS_TBL_FUND_CENTER` | reload |
| `FMMI.FSA.MD_GLACCT` | `CS_TBL_GL_ACCOUNT` | reload |
| `FMMI.FSA.MD.VENDOR` | `CS_TBL_VENDOR` | reload |
| `FMMI.FSA.MD_WBS` | `CS_TBL_WBS` | reload |
| `FMMI.FSA.GLITEM` | `CS_TBL_GL` | append (GL reconciliation) |
| `FMMI.FSA.SYSASSURANCE` | `CS_TBL_SYSTEM_ASSURANCE` | append (SA reconciliation) |
| `FMMI.FSA.INV_DIS` | `CS_TBL_INVOICE_DIS` | append |
| `FMMI.FSA.MATDOC` | `CS_TBL_MATERIAL_DOC` | append |
| `FMMI.FSA.PAYROLL` | `CS_TBL_PAYROLL` | append |
| `FMMI.FSA_POHEAD` | `CS_TBL_PO_HEADER` | append |
| `FMMI.FSA_POITEM` | `CS_TBL_PO_ITEM` | append |
| `FMMI.FSA.PURCH` | `CS_TBL_PURCHASING` | append |

---

## Lambda Functions

| Function name (dev) | Source | Purpose |
|---|---|---|
| `FSA-FPACDEV-FMMI-Check-ODS-NoFiles` | `lambda/Check-FMMI_ODS-NoFiles/lambda_function.py` | Checks S3 for `_NO_FILES` sentinel key; returns `{"no_files": true/false}`. Used by Step Function Choice state to short-circuit on no-file days. |
| `FSA-FPACDEV-FMMI-ODS-Crawler` | `lambda/FMMI_ODS-Crawler/lambda_function.py` | Triggers the Glue ODS crawler by name. Handles `CrawlerRunningException` gracefully. |

---

## Step Functions

### `FSA-{deployEnv}-FMMI-CSV-STG-ODS`

Defined in [`states/CSV-STG-ODS.asl.json`](states/CSV-STG-ODS.asl.json) with `__PLACEHOLDER__` tokens substituted at deploy time.

```
SaveInput
  └─► CheckSkipLanding (Choice)
        ├── use_existing_landing=true → InitArgs
        └── default → RunEchoLanding (Glue startJobRun.sync: LandingFiles)
                          └─► CheckNoFilesLambda (Lambda: Check-ODS-NoFiles)
                                └─► CheckNoFilesChoice (Choice)
                                      ├── no_files=true → SuccessSNS_NoFiles [End]
                                      └── default → InitArgs (Pass: assemble STG/ODS args)
                                                       └─► RunGlue (Glue: S3-STG-ODS-parquet)
                                                                └─► RunCrawler (Lambda: ODS-Crawler)
                                                                         └─► SuccessSNS [End]
(any failure) → FailureSNS [End]
```

Retry policy: 3 attempts, 60 s interval, backoff ×2, on all errors.  
Optional runtime input: `use_existing_landing: true` skips Echo FTPS ingest.

---

## Key Configuration (prod.json)

| Parameter | Value |
|---|---|
| Landing bucket | `c108-prod-fpacfsa-landing-zone` |
| Cleansed bucket | `c108-prod-fpacfsa-cleansed-zone` |
| Final bucket | `c108-prod-fpacfsa-final-zone` |
| Artifact bucket | `fsa-prod-ops` (prefix: `fmmi/`) |
| Glue catalog DB | `fsa-prod-fmmi_ods` |
| Spark UI logs | `s3://fsa-prod-ops/glue-logs/spark-ui/fmmi/` |
| JDBC connection (prod) | `FSA-PROD-PG-MDART-115` |
| SNS topic | `arn:aws:sns:us-east-1:253490756794:FSA-PROD-FMMI` |
| Glue role | `arn:aws:iam::253490756794:role/disc-fsa-prod-glue-servicerole` |
| Lambda role | `arn:aws:iam::253490756794:role/disc-fsa-prod-lambda-servicerole` |
| SFN role | `arn:aws:iam::253490756794:role/disc-fsa-prod-stepfunction-servicerole` |

---

## Deploying

```bash
cd deploy/

# Dev
python deploy.py --project-type fmmi --config config/fmmi/dev.json --region us-east-1

# Production
python deploy.py --project-type fmmi --config config/fmmi/prod.json --region us-east-1
```

The deployer creates/updates 3 Glue jobs, 2 Lambda functions, 1 Step Function, and optionally 1 Glue Crawler.

## Project Structure

```
fmmi/
├── deploy.py
├── deploy_config.sh
├── glue/
│   ├── FMMI-LandingFiles.py        # Echo FTPS → S3 landing
│   ├── S3-STG-ODS-parquet.py       # Landing CSVs → STG → ODS parquet
│   └── Ora-DMS-ODS.py              # Oracle DMS CSVs → ODS parquet
├── lambda/
│   ├── Check-FMMI_ODS-NoFiles/
│   └── FMMI_ODS-Crawler/
├── config/
│   └── config/                     # Per-table JSON schema definitions
│       ├── cs_tbl_*_source.json
│       └── cs_tbl_*_target.json
├── states/
│   ├── CSV-STG-ODS.asl.json        # Parameterised ASL (substituted at deploy)
│   └── stepfunctions/
│       └── FSA-DEV-FMMI-CSV-STG-ODS.py  # Reference DEV ASL builder
└── pipe/
    ├── dev/jenkins
    ├── cert/jenkins
    └── prod/jenkins
```
