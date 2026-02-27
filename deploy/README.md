# FSA FPAC ETL Deploy

Central deployer for all FSA/FPAC ETL pipeline projects. Each project is deployed independently using `deploy.py` with a project-specific config file.

## Quick Links

| Document | Description |
|---|---|
| [projects/README.md](projects/README.md) | Projects directory overview and deployer architecture |
| [config/README.md](config/README.md) | Config directory structure and parameter reference |

---

## Deploy Usage

```bash
cd deploy/
python deploy.py --config config/<project>/<env>.json --region us-east-1 --project-type <type>
```

See each project README for the exact command and available environments.

---

## Project READMEs

| Project | Type | Description |
|---|---|---|
| [athenafarm](projects/athenafarm/README.md) | Glue + SFN | Builds `tract_producer_year` / `farm_producer_year` Apache Iceberg reporting tables from SSS/PG sources |
| [cars](projects/cars/README.md) | Glue + Lambda + SFN | DMS S3 → Final Zone → PostgreSQL EDV (Data Vault) → DM → Redshift |
| [carsdm](projects/carsdm/README.md) | Glue + Lambda + SFN | DMS parquet → Consolidated Datasets → Final Zone + CDC Zone for CARS tables |
| [cnsv](projects/cnsv/README.md) | Glue + Lambda + SFN | 4 sub-pipelines for Conservation program data (Base, Contract Maintenance, Payments, EDW SQL) |
| [farm_records](projects/farm_records/README.md) | Glue + Lambda + SFN | 3-track pipeline: SAP Load, Postgres EDV Load, and Data Mart for Farm Records |
| [farmdm](projects/farmdm/README.md) | Config upload only | Uploads Farm DM SQL scripts to S3 via `deploy_config.sh`; no Glue/Lambda/SFN |
| [fbp_rpt](projects/fbp_rpt/README.md) | SQL reference | Draft Athena SQL for FBP reporting tables — no deployable infrastructure |
| [flpids](projects/flpids/README.md) | Lambda + SFN | FLPIDS FTPS file check pipeline (DynamoDB Streams → EventBridge Pipe → Lambda → SFN) |
| [fmmi](projects/fmmi/README.md) | Glue + Lambda + SFN | FMMI/SAP CSV ingestion from Echo FTPS → S3 Landing → STG → ODS Parquet (19 financial tables) |
| [fpac_pipeline](projects/fpac_pipeline/README.md) | Glue + Lambda + SFN | Shared base FPAC framework: Landing → Cleansed → Final, parameterised per downstream project |
| [nps](projects/nps/README.md) | Glue + SFN | NPS/FWADM financial archive table builder from PostgreSQL → S3 Parquet + Glue catalog |
| [pmrds](projects/pmrds/README.md) | Glue + SFN | Aggregates CPS/SBSD/ARCPLC/NPS/NRRS into 5 PMRDS financial summary tables |
| [sbsd](projects/sbsd/README.md) | Glue + Lambda + SFN | SBSD Subsidy pipeline: STG → EDV (Data Vault) → PYMT_DM → Redshift |
| [tsthooks](projects/tsthooks/README.md) | Lambda | CI utility: Jenkins webhook trigger + Echo DART FTPS test data file manager |

---

## Additional READMEs

The following per-project sub-directories also contain READMEs with pipeline-specific detail:

| Document | Description |
|---|---|
| [projects/carsdm/pipe/README.md](projects/carsdm/pipe/README.md) | carsdm pipeline run history / pipe notes |
| [projects/flpids/pipe/README.md](projects/flpids/pipe/README.md) | flpids pipeline details |
| [projects/flpids/stepfunctions/README.md](projects/flpids/stepfunctions/README.md) | flpids Step Functions reference |
| [projects/flpids/tests/README_FTPS_EXPECTED_FILES_AND_LAMBDA_PATTERNS.md](projects/flpids/tests/README_FTPS_EXPECTED_FILES_AND_LAMBDA_PATTERNS.md) | flpids FTPS expected file patterns |
| [projects/flpids/tests/tests-jenkins-hook/README.md](projects/flpids/tests/tests-jenkins-hook/README.md) | flpids Jenkins hook test notes |
| [projects/pmrds/pipe/README.md](projects/pmrds/pipe/README.md) | pmrds pipeline run notes |
