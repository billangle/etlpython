# Why tract_producer_year Struggles While farm_producer_year Finishes Quickly

## Scope

This analysis is based on:
- `prjhelpers/farm-info/tract-producer-year-schema-readme.md`
- `prjhelpers/farm-info/farm-producer-year-schema-readme.md`
- implementation behavior in:
  - `deploy/projects/athenafarm/glue/Transform-Tract-Producer-Year.py`
  - `deploy/projects/athenafarm/glue/Transform-Farm-Producer-Year.py`
  - `deploy/projects/athenafarm/states/Main.param.asl.json`

This is a design-level bottleneck analysis (no CloudWatch run logs are embedded
here), so conclusions are evidence-based but still inferential.

---

## Executive Summary

`farm_producer_year` is a relatively narrow transform:
- 1 main SSS source table + 3 reference tables
- a single CTE path with modest joins
- no heavy multi-stage key expansion chain

`tract_producer_year` is a high-cardinality, multi-join graph:
- 9 SSS tables + 7 reference tables
- deep key-resolution chain across `ibase`/`instance`/`in_guid`/partner mappings
- additional dedup windowing and final resolution joins
- explicit history of repeated optimizations for skew and timeouts in code version notes

Result:
- farm path can complete in minutes (observed around 5 minutes)
- tract path can hit long-running shuffle and skew ceilings and fail to complete

---

## Side-by-Side Complexity Comparison

```text
farm_producer_year:
  Inputs: 1 raw + 3 ref
  Join depth: low
  Major expensive operators: moderate joins + merge insert
  Cardinality blow-up risk: low/moderate
  Runtime profile: short

tract_producer_year:
  Inputs: 9 raw + 7 ref
  Join depth: high (multi-hop lineage resolution)
  Major expensive operators:
    - repeated wide joins
    - repartition/shuffle-heavy stages
    - final ROW_NUMBER de-dup sort
    - full output overwrite
  Cardinality blow-up risk: high
  Runtime profile: long / unstable under skew
```

---

## Why the Tract Path Becomes Slow

## 1) Deep multi-hop lineage resolution creates large intermediate datasets

Tract flow resolves through these hops:

```text
ibsp -> ibst -> ibib -> ibin -> ibpart -> crmd_partner
       -> (z_ibase_comp_detail + comm_pr_frg_rel + zmi_farm_partn)
       -> (county_office_control + but000 + time_period)
       -> (farm + tract + farm_year + tract_year)
```

Each hop can widen row counts, especially where one-to-many relationships exist
for base keys (`ibase`, partner sets, fragment relationships).

Impact:
- large intermediate shuffles
- longer spill/sort phases
- increased executor memory pressure

---

## 2) Proven key skew in structure-farm stage

The tract job itself documents repeated changes specifically to address skew and
timeouts, including:
- splitting structure flow into multiple buckets
- hot bucket `b3` split into 4 shards
- repeated versions focused on reducing phase runtime/timeouts

This is strong evidence that one or more key distributions are highly uneven.

Impact:
- straggler tasks dominate stage completion time
- parallelism exists on paper but effective runtime is gated by hot partitions

---

## 3) Wide shuffle profile is intrinsic to current tract single_fast plan

In the state machine, tract runs with:
- `WorkerType=G.4X`
- `NumberOfWorkers=40`
- `--shuffle_partitions=1400`
- Step timeout `5400` seconds

The tract script then performs several repartitions across major join keys before
final publish. Even with AQE and skew options, many wide exchanges remain.

Impact:
- high network and shuffle I/O
- long tail on repartition+join stages
- timeout risk when one stage tails badly

---

## 4) Final dedup uses window sort on large resolved dataset

`FINALIZE_RESOLVE_SQL` applies:
- `ROW_NUMBER()` partitioned by
  `(core_customer_identifier, tract_year_identifier, producer_involvement_code)`
- ordered by `creation_date DESC, last_change_date DESC`

Window partitioning and sorting is expensive at scale.

Impact:
- additional global/local sorting cost near end of critical path
- often where jobs already memory-constrained become unstable

---

## 5) Full overwrite publish amplifies end-of-pipeline cost

Tract currently writes final target with full overwrite behavior.

Impact:
- expensive terminal write even after heavy upstream compute
- if pipeline is already near timeout, publish step can push over the limit

---

## Why Farm Finishes in About 5 Minutes

Farm transform is structurally simpler:
- single dominant source: `fsa_farm_records_farm`
- only three ref joins (`county_office_control`, `farm`, `farm_year`)
- no multi-hop `ibase -> in_guid -> partner -> zmi` chain
- no large window dedup stage comparable to tract `ROW_NUMBER` resolve
- fewer opportunities for key skew and cardinality explosion

In short, farm is primarily a straightforward projection+join+merge path, while
tract is a high-fanout lineage reconstruction workflow.

---

## Significant Tract Bottlenecks (Priority Order)

1. Join cardinality expansion across `ibase`/partner/fragment chains.
2. Hot-key skew in structure-farm processing (explicitly evidenced by bucket and shard mitigations).
3. Repeated wide repartition and shuffle boundaries through single_fast stages.
4. Final windowed dedup sort in `FINALIZE_RESOLVE_SQL`.
5. Full-table overwrite publish cost at the end of an already long critical path.

---

## Critical Path Graph (Performance View)

```text
ibsp + ibst + ibib
   -> structure resolution
   -> skew-prone bucket/shard processing
   -> merge buckets
   -> ibin expansion (in_guid)
   -> ibpart + crmd_partner partner resolution
   -> zmi + ref enrich joins
   -> farm/tract/farm_year/tract_year resolution
   -> row_number de-dup sort
   -> full overwrite publish
```

The longest observed tail is typically in the middle resolution joins and the
late dedup/write phases, where data volume and skew are both highest.

---

## Practical Interpretation

If farm succeeds and tract does not, that pattern is expected given design
complexity alone; it does not imply infrastructure failure by itself. The tract
path has materially higher computational load and skew sensitivity.
