# tract_producer_year Multi-Job Redesign (Low-Shuffle Approach)

## Goal

Break `tract_producer_year` into multiple jobs so that each job uses:
- at most 4 inputs total
- at most 3 reference tables

and materially reduces repeated wide joins/shuffles that currently cause long
runtime and non-completion.

---

## Why This Works

The current tract flow does many wide joins in one long chain. The redesign
pushes complexity into narrow, reusable key maps so each heavy source is read
once, joined once, and then persisted for downstream use.

Core strategy:
- build small canonical maps early (`ibase`, `instance`, `in_guid`, partner)
- persist intermediate outputs as Iceberg tables partitioned on stable join keys
- avoid rejoining the same raw tables in later jobs
- defer only one final identifier resolution pass near the end

---

## What Was Already Attempted (from Errors + Code Comments)

The existing code and error artifacts show many in-job optimizations were already
tried inside one large tract job:

- Progressive bucket/shard splitting of structure processing (`b0..b15` and hot
  `b3` sharding) to address skew/timeouts.
- Multiple rounds of shuffle, AQE, checkpoint, and resiliency tuning in
  `single_fast` mode.
- Stage-level refactors that still execute inside one monolithic job graph.

Observed failure signatures in `deploy/projects/athenafarm/errors/` include:
- `States.Timeout` on long-running steps.
- `localCheckpoint` block-not-found failures (executor loss / checkpoint fragility).
- Iceberg clustered-writer failures (`Incoming records violate writer assumption`).
- Runtime/config churn issues (for example immutable Spark config set at runtime).

Interpretation:
- current attempts mostly optimize execution mechanics of one large DAG,
  not the DAG shape itself.

---

## How This Proposal Is Different

This redesign is intentionally different from prior attempts:

- It changes the physical architecture from one large tract transform to a
  multi-job persisted DAG.
- It enforces hard per-job limits (max 4 inputs, max 3 references) to bound
  join fan-in and shuffle width by design.
- It isolates heavy-source joins to one-time map-builder jobs (`J2`, `J3`, `J4`)
  so downstream jobs do not re-read and re-shuffle the same raw tables.
- It replaces repeated wide in-memory lineage rebuilding with keyed Iceberg
  contracts between jobs.

This is not another repartition/bucket tweak on the same monolithic plan; it is
an execution-model change.

---

## New Job DAG

```text
J1 SpineBase
  -> J2 InstanceGuidMap
  -> J3 PartnerMap
  -> J4 ZmiMap
  -> J6 TractCandidateAssemble

J5 CocTimeMap ----------------------/
J3 + J4 + J6 -> J7 PartnerCustomerResolve
J7 -> J8 FarmTractResolve
J8 -> J9 TractYearResolve
J9 -> J10 DedupAndPublish
```

---

## Job-by-Job Plan (with Input Limits)

## J1 SpineBase

Purpose:
- build normalized tract/farm structural base keys once.

Inputs (3 total, refs 0):
- `ibsp`
- `ibst`
- `ibib`

Output:
- `tpy_spine_base`

Primary keys carried forward:
- `instance`, `client`, `f_ibase`, `tract_number`, `farm_number`,
  `admin_state`, `admin_county`, status/date/user fields

---

## J2 InstanceGuidMap

Purpose:
- resolve `in_guid` from `ibin` only for keys present in J1.

Inputs (2 total, refs 0):
- `tpy_spine_base`
- `ibin`

Output:
- `tpy_instance_guid_map`

Key:
- `(instance, client, f_ibase) -> in_guid`

---

## J3 PartnerMap

Purpose:
- resolve partner role and partner number once.

Inputs (3 total, refs 0):
- `tpy_instance_guid_map`
- `ibpart`
- `crmd_partner`

Output:
- `tpy_partner_map`

Key:
- `in_guid -> partner_fct, partner_no`

---

## J4 ZmiMap

Purpose:
- resolve ZMI exception and producer date fields once.

Inputs (4 total, refs 0):
- `tpy_instance_guid_map`
- `z_ibase_comp_detail`
- `comm_pr_frg_rel`
- `zmi_farm_partn`

Output:
- `tpy_zmi_map`

Key:
- `(instance, f_ibase) -> exception/rma/appeal/producer-date columns`

---

## J5 CocTimeMap

Purpose:
- precompute active county-office/time lookup used later.

Inputs (2 total, refs 2):
- `time_period`
- `county_office_control`

Output:
- `tpy_coc_time_map`

Key:
- `(state_fsa_code, county_fsa_code, time_period_identifier)`

---

## J6 TractCandidateAssemble

Purpose:
- build tract candidates from structural base + partner + zmi + coc/time.

Inputs (4 total, refs 0):
- `tpy_spine_base`
- `tpy_partner_map`
- `tpy_zmi_map`
- `tpy_coc_time_map`

Output:
- `tpy_candidate_stage`

Notes:
- this is the first place where most output columns appear together
- still no PG id-resolution joins yet

---

## J7 PartnerCustomerResolve

Purpose:
- resolve customer id from partner attributes.

Inputs (2 total, refs 1):
- `tpy_candidate_stage`
- `but000` (reference)

Output:
- `tpy_candidate_with_customer`

---

## J8 FarmTractResolve

Purpose:
- resolve `farm_identifier`, `tract_identifier`, and `farm_year_identifier`.

Inputs (4 total, refs 3):
- `tpy_candidate_with_customer`
- `farm` (reference)
- `tract` (reference)
- `farm_year` (reference)

Output:
- `tpy_resolved_ids_stage`

---

## J9 TractYearResolve

Purpose:
- resolve final `tract_year_identifier`.

Inputs (2 total, refs 1):
- `tpy_resolved_ids_stage`
- `tract_year` (reference)

Output:
- `tpy_resolved_final`

---

## J10 DedupAndPublish

Purpose:
- apply final dedup logic and publish to target table.

Inputs (1 total, refs 0):
- `tpy_resolved_final`

Output:
- `athenafarm_prod_gold.tract_producer_year`

---

## How This Eliminates Repeated Wide Joins and Shuffles

## 1) One-time heavy-source scans

In the redesign, each expensive raw source is joined once in exactly one job:
- `ibin` only in J2
- `ibpart` + `crmd_partner` only in J3
- `z_ibase_comp_detail` + `comm_pr_frg_rel` + `zmi_farm_partn` only in J4

Later jobs consume slim, keyed Iceberg maps instead of rescanning heavy sources.

---

## 2) Narrow keyed outputs between jobs

Each job writes a minimal column set needed for next joins, not full wide rows.
This reduces shuffle payload size and spill risk.

Example:
- J2 writes `(instance, client, f_ibase, in_guid)`
- J3/J4 attach only partner/zmi attributes to those keys
- wide business columns are assembled late (J6 onward)

---

## 3) Stable partitioning by canonical keys

Partition outputs consistently:
- J1/J2/J4 by `(instance, client)` or hashed `f_ibase`
- J3 by hashed `in_guid`
- J6/J7 by `(county_office_control_identifier, time_period_identifier)`
- J8/J9 by `farm_identifier` / `tract_identifier` / `tract_year_identifier`

This avoids repeated random repartitioning and improves locality for next job.

---

## 4) Separate id-resolution from enrichment

Current tract flow mixes enrichment and PG id resolution deep in one chain.
Redesign isolates id resolution to J8/J9 so expensive dimension joins happen
after candidate rows are already reduced.

---

## 5) Replace wide window dedup with two-step dedup (recommended)

In J10, avoid one giant `ROW_NUMBER` over full-width rows. Use:
1. key-only aggregate for latest timestamp per business key
2. join back to filtered rows to pick winners

Business key:
- `(core_customer_identifier, tract_year_identifier, producer_involvement_code)`

This keeps the heaviest sort on narrow keys.

---

## Step Functions Orchestration (Practical)

Run as two controlled phases:

Phase A (parallel where safe):
- `J1 -> J2`
- then run `J3` and `J4` in parallel
- run `J5` in parallel with `J3/J4`

Phase B (serial resolution/publish):
- `J6 -> J7 -> J8 -> J9 -> J10`

Benefits:
- bounded fan-out
- clearer retry boundaries
- no single monolithic 90-minute critical section

---

## Output Contract (Unchanged)

This redesign keeps the same final `tract_producer_year` schema and business
rules; it only changes physical execution strategy to reduce shuffle-heavy
critical path behavior.

---

## Implementation Checklist

1. Create new Glue scripts/jobs `J1..J10` with table contracts above.
2. Add Iceberg table DDLs for all intermediate `tpy_*` outputs.
3. Enforce partition/bucket spec per intermediate table.
4. Update Step Functions to the new DAG.
5. Add row-count and key-cardinality metrics after each job.
6. Validate output equivalence against current tract output for a fixed time slice.
