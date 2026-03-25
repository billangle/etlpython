# tract_producer_year Schema and Lineage (AthenaFarm)

## Overview

This document describes:
- the final `tract_producer_year` output schema used by the athenafarm project,
- which ingested inputs are used to build it,
- and a text graph of how those inputs flow through the transform stages.

Source of truth used here:
- `deploy/projects/athenafarm/glue/Transform-Tract-Producer-Year.py`

---

## Final Output Table

Target table written by the transform job:

```text
glue_catalog.<target_database>.tract_producer_year
default target_database: athenafarm_prod_gold
```

Write behavior in current runtime path (`single_fast`):
- full-load overwrite (`mode("overwrite")`) of `tract_producer_year`
- incremental mode is disabled for this job path

---

## Final Schema (Column Order)

The final schema is the `SELECT` projection in `FINALIZE_RESOLVE_SQL`:

```text
1.  core_customer_identifier
2.  tract_year_identifier
3.  producer_involvement_start_date
4.  producer_involvement_end_date
5.  producer_involvement_interrupted_indicator
6.  tract_producer_hel_exception_code
7.  tract_producer_cw_exception_code
8.  tract_producer_pcw_exception_code
9.  data_status_code
10. creation_date
11. last_change_date
12. last_change_user_name
13. producer_involvement_code
14. time_period_identifier
15. state_fsa_code
16. county_fsa_code
17. farm_identifier
18. farm_number
19. tract_number
20. hel_appeals_exhausted_date
21. cw_appeals_exhausted_date
22. pcw_appeals_exhausted_date
23. tract_producer_rma_hel_exception_code
24. tract_producer_rma_cw_exception_code
25. tract_producer_rma_pcw_exception_code
```

De-duplication rule before publish:
- one row kept per
  `(core_customer_identifier, tract_year_identifier, producer_involvement_code)`
- winner chosen by latest `creation_date`, then latest `last_change_date`

---

## Inputs Used to Build tract_producer_year

## SSS raw inputs (from `athenafarm_prod_raw`)

```text
ibsp
ibst
ibib
ibin
ibpart
crmd_partner
z_ibase_comp_detail
comm_pr_frg_rel
zmi_farm_partn
```

## Reference inputs (from `athenafarm_prod_ref`)

```text
time_period
county_office_control
but000
farm
tract
farm_year
tract_year
```

Note:
- `athenafarm_prod_cdc` tables are not used to construct this transform output.
- CDC tables are used by sync workflows, not by `Transform-Tract-Producer-Year` creation logic.

---

## Stage Graph (How Inputs Become Final Output)

```text
SSS + REF ingested Iceberg tables
    |
    v
[1] PREPROCESS_SPINE
    ibsp
    -> normalized tract/admin/status/change-user fields
    -> stage: tract_producer_year_stage_spine

    |
    v
[2] PREPROCESS_STRUCTURE_LINK
    stage_spine + ibst
    -> links tract/admin details to f_ibase
    -> stage: tract_producer_year_stage_core1

    |
    v
[3] PREPROCESS_STRUCTURE_FARM_FILTER
    stage_core1 + ibib
    -> resolves farm_number by f_ibase
    -> stage: tract_producer_year_stage_structure_filter

    |
    v
[4] PREPROCESS_STRUCTURE_FARM
    stage_core1 + stage_structure_filter
    -> structure rows with farm_number + tract/admin/status metadata
    -> stage: tract_producer_year_stage_structure

    |
    v
[5] PREPROCESS_CORE2_EXTRACT
    stage_structure + ibin
    -> resolves in_guid by (instance, client, ibase)
    -> stage: tract_producer_year_stage_core2_source

    |
    v
[6] PREPROCESS_CORE2
    stage_structure + stage_core2_source
    -> enriches structure rows with in_guid lineage
    -> stage: tract_producer_year_stage_core2

    |
    v
[7] PREPROCESS_PARTNER
    stage_core2 + ibpart + crmd_partner
    -> resolves partner role + partner id
    -> stage: tract_producer_year_stage_base

    |
    v
[8] PREPROCESS_ENRICH
    stage_base + (z_ibase_comp_detail + comm_pr_frg_rel + zmi_farm_partn)
              + (time_period + county_office_control + but000)
    -> computes exceptions, producer dates, customer id, coc/time keys
    -> stage: tract_producer_year_stage

    |
    v
[9] FINALIZE_RESOLVE
    stage + (farm + tract + farm_year + tract_year)
    -> resolves farm_identifier and tract_year_identifier
    -> applies row_number de-dup logic
    -> stage: tract_producer_year_stage_resolve

    |
    v
[10] FINALIZE_PUBLISH
    stage_resolve
    -> overwrite target
    -> glue_catalog.<target_database>.tract_producer_year
```

---

## Field-Level Lineage Map

```text
core_customer_identifier
  <- CAST(but000.bpext AS INT)

tract_year_identifier
  <- join chain in FINALIZE_RESOLVE:
     county_office_control_identifier + tract_number -> tract
     farm_identifier + time_period_identifier -> farm_year
     farm_year + tract -> tract_year

producer_involvement_start_date
producer_involvement_end_date
  <- zmi_farm_partn.VALID_FROM / VALID_TO (via zmi_details CTE)

producer_involvement_interrupted_indicator
  <- CAST(NULL AS STRING) (currently null by design)

tract_producer_hel_exception_code
tract_producer_cw_exception_code
tract_producer_pcw_exception_code
  <- decoded from zmi_farm_partn code values (ZZ0010/ZZ0011/ZZ0012)

data_status_code
  <- ibsp.ZZFLD0000B8 mapped to A/I/D/P

creation_date
last_change_date
last_change_user_name
  <- ibsp.crtim / ibsp.UPTIM / ibsp.upnam|crnam

producer_involvement_code
  <- crmd_partner.partner_fct mapped:
     ZFARMONR -> 162
     ZOTNT    -> 163

time_period_identifier
  <- time_period.time_period_identifier joined by computed year name rule

state_fsa_code
county_fsa_code
  <- ibsp admin fields (padded)

farm_identifier
  <- ref farm table (join on coc + padded farm_number)

farm_number
  <- ibib.ZZFLD000000 (padded)

tract_number
  <- ibsp source fields with normalization logic

hel_appeals_exhausted_date
cw_appeals_exhausted_date
pcw_appeals_exhausted_date
  <- zmi_farm_partn ZZ0013/ZZ0014/ZZ0015

tract_producer_rma_hel_exception_code
tract_producer_rma_cw_exception_code
tract_producer_rma_pcw_exception_code
  <- decoded from zmi_farm_partn ZZ0016/ZZ0017/ZZ0018
```

---

## Condensed One-Page Graph

```text
athenafarm_prod_raw (SSS)
  ibsp, ibst, ibib, ibin, ibpart, crmd_partner,
  z_ibase_comp_detail, comm_pr_frg_rel, zmi_farm_partn
            +
athenafarm_prod_ref (PG refs)
  time_period, county_office_control, but000, farm, tract, farm_year, tract_year
            |
            v
Transform-Tract-Producer-Year (single_fast)
  preprocess_spine
  -> structure_link
  -> structure_farm_filter
  -> structure_farm
  -> core2_extract
  -> core2
  -> partner
  -> enrich
  -> finalize_resolve (dedupe)
  -> publish overwrite
            |
            v
athenafarm_prod_gold.tract_producer_year
  (25 columns, producer-year tract output)
```
