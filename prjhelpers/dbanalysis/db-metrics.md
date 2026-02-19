# Aurora PostgreSQL — Resource Allocation Report

> Generated: 2026-02-19 15:19 UTC  
> Window: last 120 minutes  
> Cluster: `disc-fsa-prod-db-pg`  
> Region: `us-east-1`  
> Engine: `aurora-postgresql 15.12`  
> Status: `available`  
> ACU range: `0.5 – 64.0` (Serverless v2)  
> Instances: `disc-fsa-prod-db-pg-instance-1`, `disc-fsa-prod-db-pg-instance-1-us-east-1a`  

## Summary

| Severity | Count |
|----------|-------|
| 🔴 CRITICAL | 5 |
| 🟡 WARNING  | 1 |

---

## Issues & Recommendations

### Capacity & Scaling

| Severity | Scope | Metric | Observed Value |
|----------|-------|--------|----------------|
| 🔴 CRIT | `cluster` | `ACU Ceiling` | 90% of sample periods at ≥95% of 64.0 ACU  (min 47.36 / avg 62.77 / max 64.00) |
| 🔴 CRIT | `cluster` | `ACUUtilization` | 100.00% |

**Remediation steps:**

**`ACU Ceiling`**
- Raise the cluster max ACU limit: RDS console → Modify cluster → Serverless v2 capacity → increase Maximum ACU above 64.
- Add RDS Proxy in front of the cluster to decouple connection spikes from the scaling event lag.
- Identify the top CPU / I/O consuming queries using `pg_stat_statements` and optimize or add indexes.

**`ACUUtilization`**
- Raise max ACU (see ACU Ceiling above) or reduce query concurrency.
- Use connection pooling (RDS Proxy / PgBouncer) to limit active queries at peak.

---

### Per-Instance — Query Latency

| Severity | Scope | Metric | Observed Value |
|----------|-------|--------|----------------|
| 🔴 CRIT | `disc-fsa-prod-db-pg-instance-1` | `CommitLatency` | 600.83 ms |
| 🟡 WARN | `disc-fsa-prod-db-pg-instance-1-us-east-1a` | `CommitLatency` | 12.68 ms |

**Remediation steps:**

**`CommitLatency`**
- High commit latency indicates WAL I/O pressure. Check `VolumeWriteIOPs` and WAL volume.
- Review `synchronous_commit` setting — for non-critical writes consider `synchronous_commit = local`.
- Batch small, frequent transactions into larger commits to amortize WAL flush cost.
- Check for lock waits blocking commits: `SELECT * FROM pg_locks WHERE granted = false;`

---

### Storage I/O

| Severity | Scope | Metric | Observed Value |
|----------|-------|--------|----------------|
| 🔴 CRIT | `cluster` | `VolumeReadIOPs` | 1,780,740 IOPS |
| 🔴 CRIT | `cluster` | `VolumeWriteIOPs` | 4,754 IOPS |

**Remediation steps:**

**`VolumeReadIOPs`**
- Identify table/index scans driving IOPS: `SELECT relname, seq_scan, idx_scan FROM pg_stat_user_tables ORDER BY seq_scan DESC LIMIT 20;`
- Add indexes to convert sequential scans to index scans — each seq_scan on a large table = massive IOPS.
- After a scale-down event, the buffer pool is cold. Consider a warm-up query to pre-load hot pages.

**`VolumeWriteIOPs`**
- Check autovacuum activity: `SELECT relname, n_dead_tup, last_autovacuum FROM pg_stat_user_tables ORDER BY n_dead_tup DESC LIMIT 20;`
- Reduce checkpoint frequency if write IOPS are WAL-driven: review `checkpoint_completion_target`.
- Batch bulk insert/update operations and commit in larger chunks.

---

## Reference

| ACU | ~max\_connections |
|-----|-----------------|
| 0.5 | 189 |
| 1 | 270 |
| 2 | 405 |
| 4 | 660 |
| 8 | 1,290 |
| 16 | 2,560 |
| 32 | 5,120 |
| 64 | 5,000 |

**Aurora PostgreSQL Serverless v2 key facts**

- Scale-up is fast (~seconds) but not instant — queries queue at the ACU ceiling.
- `max_connections` is determined at instance start by the configured max ACU; use RDS Proxy to avoid reconnect storms during scale events.
- The shared buffer pool **does not persist** across scale-down events — expect cold-read latency spikes after idle periods.
- Autovacuum runs on the writer instance. Monitor `MaximumUsedTransactionIDs` to prevent transaction ID wraparound.

*Run `python3 aurora_serverless_metrics.py --minutes 120 --verbose` for a wider window with all metrics.*
