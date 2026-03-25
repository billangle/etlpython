# farm_producer_year Schema and Lineage (AthenaFarm)

## Overview

This document describes:
- the final `farm_producer_year` output schema used by the athenafarm project,
- which ingested inputs are used to build it,
- and a text graph of how those inputs flow through the transform.

Source of truth used here:
- `deploy/projects/athenafarm/glue/Transform-Farm-Producer-Year.py`

---

## Final Output Table

Target table written by the transform job:

```text
glue_catalog.<target_database>.farm_producer_year
default target_database: athenafarm_prod_gold
```

Current runtime behavior:
- full-load mode is forced (`FULL_LOAD = True`)
- merge key uses `farm_year_identifier`
- inserts new rows when no match exists in target

---

## Final Schema (Column Order)

The transform builds `new_farm_producer_year` with this output projection:

```text
1.  farm_producer_year_identifier
2.  core_customer_identifier
3.  farm_year_identifier
4.  producer_involvement_code
5.  producer_involvement_interrupted_indicator
6.  producer_involvement_start_date
7.  producer_involvement_end_date
8.  farm_producer_hel_exception_code
9.  farm_producer_cw_exception_code
10. farm_producer_pcw_exception_code
11. data_status_code
12. creation_date
13. last_change_date
14. last_change_user_name
15. time_period_identifier
16. state_fsa_code
17. county_fsa_code
18. farm_identifier
19. farm_number
20. hel_appeals_exhausted_date
21. cw_appeals_exhausted_date
22. pcw_appeals_exhausted_date
23. farm_producer_rma_hel_exception_code
24. farm_producer_rma_cw_exception_code
25. farm_producer_rma_pcw_exception_code
```

Important mapping note from implementation:
- `core_customer_identifier` is currently `NULL`
- `producer_involvement_code` is currently `NULL`
- join/merge key is therefore `farm_year_identifier` only

---

## Inputs Used to Build farm_producer_year

## SSS/raw input (from `athenafarm_prod_raw`)

```text
fsa_farm_records_farm
```

This table is materialized by `Ingest-SSS-Farmrecords` from the
`fsa-prod-farm-records.farm` source.

## Reference inputs (from `athenafarm_prod_ref`)

```text
county_office_control
farm (aliased as farm_rds)
farm_year
```

Note:
- `athenafarm_prod_cdc` tables are not used by this transform.

---

## Transform Graph (How Inputs Become Final Output)

```text
athenafarm_prod_raw.fsa_farm_records_farm
        +
athenafarm_prod_ref.county_office_control
athenafarm_prod_ref.farm (farm_rds)
athenafarm_prod_ref.farm_year
        |
        v
Transform-Farm-Producer-Year
  |
  +--> register source views
  |
  +--> build farm_producer_year_tbl CTE
  |      - derive status/date/exception and geo fields from fsa_farm_records_farm
  |      - resolve county_office_control_identifier via padded state+county join
  |      - resolve farm_identifier via farm_rds join
  |      - resolve farm_year_identifier via farm_year join
  |
  +--> left join to farm_producer_year_existing
  |      on farm_year_identifier
  |      to carry farm_producer_year_identifier when present
  |
  +--> output temp view: new_farm_producer_year
  |
  +--> MERGE INTO glue_catalog.<target_database>.farm_producer_year
         ON t.farm_year_identifier = s.farm_year_identifier
         WHEN NOT MATCHED THEN INSERT
```

---

## Field-Level Lineage Map

```text
farm_producer_year_identifier
  <- existing target row id when farm_year_identifier matches
     else NULL for new insert candidates

core_customer_identifier
  <- CAST(NULL AS INT) (mapping pending in source SQL)

farm_year_identifier
  <- farm_year.farm_year_identifier
     joined from farm_rds.farm_identifier

producer_involvement_code
producer_involvement_interrupted_indicator
producer_involvement_start_date
producer_involvement_end_date
  <- currently NULL by design in this transform implementation

farm_producer_hel_exception_code
farm_producer_cw_exception_code
farm_producer_pcw_exception_code
  <- CAST from fsa_farm_records_farm:
     hel_exception, cw_exception, pcw_exception

data_status_code
  <- fsa_farm_records_farm.USER_STATUS mapped:
     ACTV->A, INCR->I, PEND->P, DRFT->D, X->I, else A

creation_date
last_change_date
last_change_user_name
  <- fsa_farm_records_farm.crtim / UPTIM / UPNAM|crnam

time_period_identifier
  <- YEAR(fsa_farm_records_farm.crtim) - 1998

state_fsa_code
county_fsa_code
  <- LPAD(fsa_farm_records_farm.administrative_state, 2, '0')
     LPAD(fsa_farm_records_farm.administrative_count, 3, '0')

farm_identifier
  <- farm_rds.farm_identifier
     via join on (farm_number + county_office_control_identifier)

farm_number
  <- fsa_farm_records_farm.farm_number

hel_appeals_exhausted_date
cw_appeals_exhausted_date
pcw_appeals_exhausted_date
  <- fsa_farm_records_farm.hel_appeals_exhausted_date
     fsa_farm_records_farm.cw_appeals_exhausted_date
     fsa_farm_records_farm.pcw_appeals_exhausted_date

farm_producer_rma_hel_exception_code
farm_producer_rma_cw_exception_code
farm_producer_rma_pcw_exception_code
  <- CAST from fsa_farm_records_farm:
     rma_hel_exceptions, rma_cw_exceptions, rma_pcw_exceptions
```

---

## Condensed One-Page Graph

```text
athenafarm_prod_raw
  fsa_farm_records_farm
            +
athenafarm_prod_ref
  county_office_control + farm_rds + farm_year
            |
            v
Transform-Farm-Producer-Year
  CTE: farm_producer_year_tbl
  -> left join existing target ids
  -> new_farm_producer_year
  -> MERGE INTO farm_producer_year (match on farm_year_identifier)
            |
            v
athenafarm_prod_gold.farm_producer_year
  (25 projected data fields + identifier carry-forward)
```
