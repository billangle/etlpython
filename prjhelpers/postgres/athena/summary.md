# Athena SQL Analysis Summary

## SQL Files Analyzed

| File | Lines | Operation | Target Table |
|---|---|---|---|
| `TRACT_PRODUCER_YEAR_INSERT_ATHENA.sql` | 306 | CDC INSERT (with compare) | `tract_producer_year` |
| `TRACT_PRODUCER_YEAR_INSERT_NO_COMPARE_ATHENA.sql` | 336 | Bulk INSERT (no compare) | `tract_producer_year` |
| `TRACT_PRODUCER_YEAR_SELECT.sql` | ~160 | UPDATE detect | `farm_producer_year` |
| `TRACT_PRODUCER_YEAR_UPDATE_ATHENA.sql` | 310 | CDC UPDATE | `tract_producer_year` |

---

## Query Summaries

### 1. TRACT_PRODUCER_YEAR_INSERT_ATHENA.sql
Performs a **CDC-aware INSERT** into `tract_producer_year`. Traverses the full SAP IBase chain to resolve farm/tract/producer relationships, then cross-references ZMI exception tables (`zmi_farm_partn`, `zmi_field_excp`) for involvement dates and HEL/CW/PCW codes. Only inserts rows where `cdc_op='I'` and the comparison logic (`updte='DIFF'`) confirms the row has changed relative to what is already in the target table. Federated joins to PostgreSQL (`farm_records_reporting`) resolve `time_period`, `county_office_control`, `farm`, `tract`, `farm_year`, and `tract_year` surrogate keys.

### 2. TRACT_PRODUCER_YEAR_INSERT_NO_COMPARE_ATHENA.sql
A **bulk/initial-load variant** of the above INSERT. The comparison logic is commented out so all resolved rows are inserted regardless of whether they already exist. Uses the pre-built lookup table `tbl_fr_bp_relationship` to short-circuit the IBase chain traversal, reducing join complexity in exchange for dependence on a pre-computed table. Used for full reloads or first-time population of `tract_producer_year`.

### 3. TRACT_PRODUCER_YEAR_SELECT.sql
Targets the **`farm_producer_year`** table (not `tract_producer_year`). Executes the same IBase/ZMI/PG join chain but is scoped to detect rows requiring an UPDATE by selecting candidate records. Queries the native Athena catalog (`awsdatacatalog.fsa-prod-farm-records.farm`) and joins back to the federated PostgreSQL source to identify divergence between source and target state. Output drives the UPDATE pipeline.

### 4. TRACT_PRODUCER_YEAR_UPDATE_ATHENA.sql
Performs a **CDC-aware UPDATE** on `tract_producer_year`. Same join chain as the INSERT query. Filters on `cdc_op='U' AND updte='DIFF'` to only touch rows confirmed as changed. Updates all producer-year attributes (involvement dates, exception codes, administrative keys) for matching tract/producer/year combinations.

---

## Data Source Systems

### sss-farmrecords (Athena — SAP origin)

| Table | Purpose |
|---|---|
| `ibib` | Farm IBase — farm number, state/county, status |
| `ibsp` | Tract IBase Component — tract number, admin state/county |
| `ibst` | IBase Structure — component-to-IBase linkage |
| `ibin` | IBase Instance — instance-to-IBase mapping |
| `ibpart` | IBase Partner Segments — segment=2 identifies farm level |
| `crmd_partner` | CRM Partner Roles — ZFARMONR=162 / ZOTNT=163 |
| `z_ibase_comp_detail` | IBase component detail |
| `comm_pr_frg_rel` | Product-to-Fragment relationship |
| `zmi_farm_partn` | ZMI Farm Partner — exception codes, involvement dates |
| `zmi_field_excp` | ZMI Field Exceptions — HEL/CW/PCW codes and dates |
| `tbl_fr_bp_relationship` | Pre-built FR/BP lookup (no-compare variant only) |

### FSA-PROD-rds-pg-edv (Federated PostgreSQL)

| Schema | Tables |
|---|---|
| `farm_records_reporting` | `time_period`, `county_office_control`, `farm`, `tract`, `farm_year`, `tract_year`, `tract_producer_year`, `farm_producer_year` |
| `crm_ods` | `but000` |

---

## Performance Root Causes

1. **Federated JDBC joins to RDS PostgreSQL** — each join crosses the Athena-to-RDS boundary serially; non-parallelizable and subject to connection pool limits
2. **Full table scans on unpartitioned SSS Athena tables** — `ibib`, `ibst`, `ibsp`, `ibin`, `ibpart`, `crmd_partner`, `zmi_farm_partn`, and `zmi_field_excp` have no partition pruning
3. **Dual CDC passes** — INSERT and UPDATE queries execute the same expensive join chain twice over the same data
4. **Three separate SQL files** for what is logically a single upsert operation, tripling I/O and compile overhead

---

## Diagram Files

| File | Description |
|---|---|
| `diagrams/reference-data-lookups.mmd` | Mermaid flowchart of all reference data lookups and join paths across both source systems |
| `diagrams/aws-architecture.mmd` | Mermaid flowchart of the recommended target AWS architecture |

---

## Architecture Recommendations (see arch.md for full details)

- **Pre-materialize** all 11 SSS Athena reference tables to partitioned S3 Iceberg tables on a nightly schedule
- **Pre-materialize** all 9 PostgreSQL reference tables to S3 Iceberg via AWS DMS or Glue JDBC connector
- **Replace** all three CDC SQL files with a single **Glue Spark job** using the same CTE logic
- **Replace** separate INSERT + UPDATE queries with a single **Iceberg `MERGE INTO`** statement
- **Orchestrate** with Step Functions + EventBridge; run Glue ingest jobs in parallel before the transform job
- **Expected result:** Hours → 30–90 seconds end-to-end

See [arch.md](arch.md) for the complete architecture document including the `MERGE INTO` SQL template, partition/Z-order configuration per table, Step Functions DAG JSON, and file mapping from old SQL to new Glue jobs.
