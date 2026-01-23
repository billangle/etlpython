# Aurora PostgreSQL Serverless Index Optimization
**HypoPG + pganalyze**
**Architecture • Runbooks • Change Management • Cost • Compliance**

This document extends the prior guide with **architecture diagrams**, **promotion policies**, **automation concepts**, **cost analysis**, and **SOC-aligned operational language**.  
It is suitable for **regulated production environments** and formal change control.

---

## 1. Reference Architecture

### Logical flow
1. Application issues queries to Aurora PostgreSQL Serverless
2. `pg_stat_statements` captures workload
3. pganalyze collector pulls stats + metadata
4. pganalyze Index Advisor generates recommendations
5. DBA validates recommendations using HypoPG
6. Approved indexes are promoted to production

### Key characteristics
- No automatic index creation
- Human-in-the-loop validation
- Zero-risk hypothesis testing
- Explicit rollback paths

---

## 2. Staging → Production Promotion Policy

### Environments
| Environment | Purpose |
|------------|---------|
| Dev | Query design, schema evolution |
| Staging | Performance validation, HypoPG testing |
| Production | Approved index deployment |

### Promotion rules
An index **MUST** meet all of the following before production:

- Identified by pganalyze Index Advisor
- Validated with HypoPG (`EXPLAIN` cost improvement)
- Reviewed by at least one peer DBA
- Approved via change ticket
- Scheduled during approved maintenance window

---

## 3. Automated Index Lifecycle (Recommended Pattern)

### Detection
- pganalyze flags:
  - Missing indexes
  - Duplicate / overlapping indexes
  - Unused indexes (low scan count)

### Evaluation (semi-automated)
- Script pulls candidate DDL
- DBA runs HypoPG test script
- Results attached to change record

### Promotion
- Index created using `CREATE INDEX CONCURRENTLY`
- Usage monitored via `pg_stat_statements`

### Retirement
- Unused indexes identified
- Drop scheduled using `DROP INDEX CONCURRENTLY`

> **Important:** Fully automated index creation is discouraged in regulated environments.

---

## 4. Cost Impact Analysis

### Index cost components

| Cost Type | Description |
|---------|-------------|
| Storage | Index size on disk |
| Write amplification | Additional work on INSERT/UPDATE/DELETE |
| Vacuum overhead | Index maintenance |
| Replication | WAL volume increase |

### Estimating index size

```sql
SELECT
  pg_size_pretty(
    pg_relation_size('orders_customer_created_at_idx')
  );
```

### Monitoring post-deploy
- Compare write latency before/after
- Monitor storage growth
- Review WAL generation trends

---

## 5. SOC / Change-Management Language (Template)

### Change description
> This change introduces a new PostgreSQL index to improve query performance for a high-cost production workload.  
> The index was evaluated using hypothetical index analysis (HypoPG) and validated against real workload data provided by pganalyze.

### Risk assessment
- Availability risk: Low (concurrent index build)
- Data integrity risk: None
- Security impact: None

### Rollback plan
> The index can be removed online using `DROP INDEX CONCURRENTLY` with no application downtime.

---

## 6. Standardized HypoPG Validation Script

```sql
-- Baseline
EXPLAIN
SELECT *
FROM orders
WHERE customer_id = 42
ORDER BY created_at DESC
LIMIT 50;

-- Hypothetical index
SELECT * FROM hypopg_create_index(
  'CREATE INDEX ON orders (customer_id, created_at DESC)'
);

-- Re-test
EXPLAIN
SELECT *
FROM orders
WHERE customer_id = 42
ORDER BY created_at DESC
LIMIT 50;

-- Cleanup
SELECT hypopg_reset();
```

Attach EXPLAIN output to change ticket.

---

## 7. Security & Compliance Summary

- No persistent objects created during evaluation
- No schema changes until approved
- No automatic DDL in production
- Supports least-privilege access models
- Compatible with SOC, FedRAMP, and internal audit controls

---

## 8. Diagram Deliverables

A companion **draw.io** architecture diagram should include:
- Aurora PostgreSQL cluster
- Writer + reader endpoints
- pg_stat_statements inside DB
- pganalyze collector (EC2/ECS)
- pganalyze SaaS
- Operator / DBA validation loop

---

## 9. Reference Links

- AWS Aurora PostgreSQL  
  https://aws.amazon.com/rds/aurora/postgresql/

- HypoPG  
  https://github.com/HypoPG/hypopg

- pganalyze Index Advisor  
  https://pganalyze.com/index-advisor

- pganalyze Documentation  
  https://pganalyze.com/docs
