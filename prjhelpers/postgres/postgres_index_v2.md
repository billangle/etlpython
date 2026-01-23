# Aurora PostgreSQL Serverless: HypoPG + pganalyze Index Advisor
**Architecture Guide • Operations Runbook • Worked Example**

This document consolidates **architecture**, **operations**, and **hands-on workflow** guidance for using **HypoPG** together with **pganalyze Index Advisor** on **Amazon Aurora PostgreSQL Serverless**.  
It is written to be suitable for **regulated / production environments** (change control, rollback, observability).

---

## 1. Architecture Overview

### Components and responsibilities

| Component | Responsibility |
|---------|----------------|
| Aurora PostgreSQL Serverless | Primary OLTP database |
| HypoPG (extension) | Session‑local hypothetical indexes for planner evaluation |
| pg_stat_statements | Query workload statistics |
| pganalyze Collector | Extracts query stats + metadata |
| pganalyze Index Advisor | Generates index recommendations |
| DBA / Operator | Validates recommendations with HypoPG |

### Design principles

* **Zero‑risk evaluation** – HypoPG creates no physical indexes
* **Workload‑driven recommendations** – pganalyze uses real query data
* **Controlled promotion** – only validated indexes are created
* **Rollback‑friendly** – every change has an exit path

---

## 2. Aurora Serverless v1 vs v2 considerations

### Serverless v1
* Single writer at all times
* Scale events are more disruptive
* Prefer **off‑peak index creation**
* Strongly recommend `CREATE INDEX CONCURRENTLY`

### Serverless v2
* Near‑continuous scaling
* Better for background maintenance
* Still prefer `CONCURRENTLY` for large tables
* HypoPG behavior identical to v1

---

## 3. AWS Console setup (one‑time)

### 3.1 Cluster parameter group

1. **RDS → Parameter groups**
2. Create **DB cluster parameter group**
3. Set:
   ```
   shared_preload_libraries = pg_stat_statements
   ```
4. Attach to cluster
5. Restart writer instance

### 3.2 Database extensions

Run once per database:

```sql
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
CREATE EXTENSION IF NOT EXISTS hypopg;
```

Verify:

```sql
SELECT extname, extversion FROM pg_extension;
```

---

## 4. pganalyze collector deployment (production model)

### Network
* TCP 5432 → Aurora cluster
* HTTPS 443 → pganalyze endpoints

### IAM (optional but recommended)
* RDS Describe*
* CloudWatch ReadOnly

### Supported runtimes
* EC2
* ECS / Fargate
* Docker host

---

## 5. Operations Runbook (Day‑2)

### 5.1 Normal operating flow

1. Queries execute in production
2. pg_stat_statements collects stats
3. pganalyze ingests workload
4. Index Advisor flags:
   * Missing indexes
   * Redundant indexes
   * Unused indexes

### 5.2 Change evaluation workflow (standard)

| Step | Action | Risk |
|----|------|----|
| 1 | Review recommendation | None |
| 2 | Reproduce query in staging | None |
| 3 | Test with HypoPG | None |
| 4 | Peer review | None |
| 5 | Create index concurrently | Low |
| 6 | Monitor | Low |
| 7 | Drop if negative | Low |

---

## 6. Worked end‑to‑end example

### 6.1 Identify slow query (from pg_stat_statements)

```sql
SELECT query, calls, total_exec_time
FROM pg_stat_statements
ORDER BY total_exec_time DESC
LIMIT 5;
```

Example query:

```sql
SELECT *
FROM orders
WHERE customer_id = $1
ORDER BY created_at DESC
LIMIT 50;
```

### 6.2 pganalyze recommendation

Suggested index:

```sql
CREATE INDEX ON orders (customer_id, created_at DESC);
```

---

## 7. HypoPG validation (critical step)

### 7.1 Baseline plan

```sql
EXPLAIN
SELECT *
FROM orders
WHERE customer_id = 42
ORDER BY created_at DESC
LIMIT 50;
```

### 7.2 Create hypothetical index

```sql
SELECT *
FROM hypopg_create_index(
  'CREATE INDEX ON orders (customer_id, created_at DESC)'
);
```

### 7.3 Re‑evaluate plan

```sql
EXPLAIN
SELECT *
FROM orders
WHERE customer_id = 42
ORDER BY created_at DESC
LIMIT 50;
```

Confirm:
* Index Scan or Bitmap Index Scan
* Lower estimated cost
* No regression in join strategy

### 7.4 Cleanup

```sql
SELECT hypopg_reset();
```

---

## 8. Production index creation checklist

### Pre‑change
- [ ] Off‑peak window selected
- [ ] Table size reviewed
- [ ] Replication lag checked
- [ ] Storage headroom confirmed

### Execution

```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS
orders_customer_created_at_idx
ON orders (customer_id, created_at DESC);
```

### Post‑change
- [ ] `pg_stat_statements` confirms usage
- [ ] Query latency reduced
- [ ] Write amplification acceptable

---

## 9. Rollback strategy

If negative impact observed:

```sql
DROP INDEX CONCURRENTLY IF EXISTS orders_customer_created_at_idx;
```

Rollback is:
* Online
* Fast
* Non‑blocking

---

## 10. Security and compliance notes

* HypoPG does **not** modify catalog tables
* No persistent objects created
* Safe for regulated environments
* Compatible with least‑privilege models

---

## 11. Troubleshooting guide

### pg_stat_statements missing
* Confirm `shared_preload_libraries`
* Restart writer

### HypoPG not affecting plans
* Same session required
* Use `EXPLAIN`, not `ANALYZE`

### No pganalyze index advice yet
* Wait for sufficient workload volume
* Ensure collector connectivity

---

## 12. Reference links

* AWS Aurora PostgreSQL  
  https://aws.amazon.com/rds/aurora/postgresql/

* HypoPG  
  https://github.com/HypoPG/hypopg

* pganalyze Index Advisor  
  https://pganalyze.com/index-advisor

* pganalyze Documentation  
  https://pganalyze.com/docs
