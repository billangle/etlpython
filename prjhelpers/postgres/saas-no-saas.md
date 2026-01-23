# SaaS vs Non-SaaS Index Advisory for Aurora PostgreSQL
**HypoPG • pganalyze • Fully Self‑Hosted Alternatives**

This document explains **SaaS vs non‑SaaS** approaches for PostgreSQL index analysis on **Amazon Aurora PostgreSQL Serverless**, with a focus on **regulatory, compliance, and air‑gapped environments**.

---

## Executive Summary

- **pganalyze is a SaaS product**  
  - You may self‑host the *collector*, but the **Index Advisor, analytics engine, and UI are SaaS‑hosted**.
- If **no SaaS / no external control plane** is allowed:
  - **Do not use pganalyze**
  - Use **native PostgreSQL tooling + HypoPG** instead
- A **fully non‑SaaS solution is achievable and common** in regulated environments.

---

## Option 1 — SaaS-Based (pganalyze)

### Architecture
```
Application
   |
Aurora PostgreSQL
   |-- pg_stat_statements
   |
pganalyze Collector (EC2 / ECS / Docker)
   |
pganalyze SaaS
   |
DBA / Operator
```

### Characteristics

| Aspect | Status |
|-----|------|
| External SaaS | YES |
| Index Advisor | YES |
| Human validation | YES |
| Automatic index creation | NO |
| FedRAMP / air‑gap friendly | NO |

### Notes
- Collector runs in your AWS account
- Query data is transmitted to pganalyze
- Index Advisor logic runs entirely in SaaS
- Commonly disallowed in federal / restricted environments

---

## Option 2 — Non‑SaaS (Recommended)

### Fully Self‑Hosted PostgreSQL + HypoPG

### Architecture
```
Application
   |
Aurora PostgreSQL Serverless
   |-- pg_stat_statements
   |-- HypoPG
   |-- auto_explain (optional)
   |
DBA Scripts / Runbooks
```

### Components

| Component | Purpose |
|--------|--------|
| pg_stat_statements | Identify expensive queries |
| HypoPG | Test hypothetical indexes |
| EXPLAIN | Validate planner behavior |
| auto_explain (optional) | Capture slow plans automatically |
| Custom SQL | Generate index candidates |

### Workflow

#### 1. Identify expensive queries
```sql
SELECT query, calls, total_exec_time, mean_exec_time
FROM pg_stat_statements
ORDER BY total_exec_time DESC
LIMIT 20;
```

#### 2. Derive candidate indexes
From predicates and ordering clauses:
```sql
(customer_id, created_at DESC)
```

#### 3. Validate using HypoPG
```sql
SELECT hypopg_create_index(
  'CREATE INDEX ON orders (customer_id, created_at DESC)'
);

EXPLAIN
SELECT *
FROM orders
WHERE customer_id = 42
ORDER BY created_at DESC
LIMIT 50;

SELECT hypopg_reset();
```

#### 4. Promote to production
```sql
CREATE INDEX CONCURRENTLY orders_customer_created_at_idx
ON orders (customer_id, created_at DESC);
```

### Characteristics

| Aspect | Status |
|-----|------|
| External SaaS | NO |
| Index Advisor | Semi‑manual |
| Human validation | REQUIRED |
| Audit friendly | YES |
| Air‑gapped | YES |

---

## Option 3 — Non‑SaaS with UI (PoWA)

### PostgreSQL Workload Analyzer (PoWA)

### Architecture
```
Aurora PostgreSQL
   |
PoWA Collector
   |
PoWA Web UI (self‑hosted)
```

### Capabilities

| Feature | Supported |
|------|-----------|
| Query statistics | YES |
| Dashboards | YES |
| Index advisor | NO |
| SaaS dependency | NO |

Use **PoWA for visibility**, then **HypoPG for validation**.

---

## Option 4 — Log‑Based (pgBadger)

### Architecture
```
Aurora PostgreSQL
   |
Postgres Logs
   |
pgBadger Reports
   |
DBA / HypoPG
```

### Use Cases
- Offline analysis
- Forensic / audit review
- Highly restricted environments

---

## Why pganalyze Is Not Non‑SaaS

Even when self‑hosting the collector:
- Index Advisor logic is SaaS
- UI and analytics are SaaS
- Historical query data is externalized

**Conclusion:** pganalyze ≠ self‑hosted

---

## Recommended Non‑SaaS Reference Architecture

```
Aurora PostgreSQL Serverless
  ├─ pg_stat_statements
  ├─ HypoPG
  ├─ auto_explain (optional)
  |
DBA‑Managed Tooling
  ├─ SQL analysis scripts
  ├─ HypoPG validation
  ├─ Change‑ticket artifacts
```

✔ No external services  
✔ Deterministic  
✔ Fully auditable  
✔ Regulator‑friendly  

---

## Compliance Positioning Language

> This solution performs index evaluation using hypothetical planner analysis without creating persistent database objects.  
> All changes are human‑reviewed, auditable, and reversible, and no query data leaves the AWS account.

---

## References

- HypoPG  
  https://github.com/HypoPG/hypopg

- AWS Aurora PostgreSQL  
  https://aws.amazon.com/rds/aurora/postgresql/

- PoWA  
  https://powa.readthedocs.io/
