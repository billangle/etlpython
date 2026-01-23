# Aurora PostgreSQL Serverless: HypoPG + pganalyze Index Advisor

This guide shows how to enable **HypoPG** on an **Amazon Aurora PostgreSQL (Serverless)** cluster and how to use it together with **pganalyze’s Index Advisor** to *suggest* and *trial* indexes before creating them for real.

> Key point: **HypoPG is a PostgreSQL extension installed on your Aurora cluster.**  
> **pganalyze Index Advisor is not a Postgres extension installed inside Aurora**—it is an external analysis tool (standalone web advisor or the pganalyze application).

---

## 1) Prerequisites and compatibility

### Aurora PostgreSQL versions that support HypoPG
Aurora PostgreSQL supports HypoPG in the following engine versions (and later patch levels):

- PostgreSQL 15.5+
- PostgreSQL 14.10+
- PostgreSQL 13.13+
- PostgreSQL 12.17+

Always confirm extension availability for your exact Aurora engine version.

### Required database privileges
You need a database role with **RDS superuser** privileges (typically the cluster master user) to:

- Install extensions (`CREATE EXTENSION`)
- Enable monitoring extensions
- Create helper functions (optional, for pganalyze)

---

## 2) AWS Console configuration (Aurora Serverless)

### 2.1 Enable `pg_stat_statements` (recommended)

pganalyze relies on **pg_stat_statements** to analyze real query workloads.

**Console steps**

1. Open **RDS → Parameter groups**
2. Create a **DB cluster parameter group**
   - Family must match your Aurora PostgreSQL major version
3. Edit parameters:
   - Set `shared_preload_libraries` to include:
     ```
     pg_stat_statements
     ```
     (Keep existing values and separate with commas.)
4. Save changes (will be **pending reboot**)

**Attach the parameter group**

1. Open **RDS → Databases → your Aurora cluster**
2. Choose **Modify**
3. Set the **DB cluster parameter group** to your custom group
4. Apply changes

**Restart requirement**

Changes to `shared_preload_libraries` require restarting the **writer instance** or performing a controlled failover.

---

### 2.2 Install required extensions

Extensions are installed **per database**, not per cluster.

Connect to the writer endpoint and run:

```sql
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
CREATE EXTENSION IF NOT EXISTS hypopg;

SELECT extname, extversion
FROM pg_extension
WHERE extname IN ('pg_stat_statements', 'hypopg');
```

---

## 3) How HypoPG works

- Hypothetical indexes exist **only in the current database session**
- No disk space is used
- No impact on other sessions
- Intended for use with `EXPLAIN` (not `EXPLAIN ANALYZE`)

This makes HypoPG ideal for validating index design decisions safely.

---

## 4) pganalyze Index Advisor + HypoPG workflow

### Option A — pganalyze Standalone Index Advisor
- Web-based, no database installation required
- You provide:
  - Schema-only dump (`pg_dump --schema-only`)
  - Query text
- Useful for ad-hoc analysis and design reviews

### Option B — pganalyze In-App Index Advisor
- Uses **real production query workloads**
- Requires deploying a **pganalyze collector**
- Provides continuous recommendations and tracking

---

## 5) HypoPG usage examples

### Example query

```sql
EXPLAIN
SELECT *
FROM orders
WHERE customer_id = 42
ORDER BY created_at DESC
LIMIT 50;
```

### Create a hypothetical index

```sql
SELECT *
FROM hypopg_create_index(
  'CREATE INDEX ON orders (customer_id, created_at DESC)'
);
```

### View hypothetical indexes

```sql
SELECT * FROM hypopg_list_indexes();
```

### Re-check the query plan

```sql
EXPLAIN
SELECT *
FROM orders
WHERE customer_id = 42
ORDER BY created_at DESC
LIMIT 50;
```

If the planner switches to an index scan with a lower estimated cost, the index is likely beneficial.

### Remove all hypothetical indexes

```sql
SELECT hypopg_reset();
```

---

## 6) Converting a hypothetical index into a real index

In production environments, prefer **non-blocking index creation**:

```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS
  orders_customer_created_at_idx
ON orders (customer_id, created_at DESC);
```

Then:

- Validate using `EXPLAIN (ANALYZE)` in staging
- Monitor write overhead and storage growth

---

## 7) pganalyze in-app setup: AWS requirements

### Database configuration
- `pg_stat_statements` enabled and loaded
- Extension installed in each monitored database

Optional helper function for improved recommendations:

```sql
CREATE SCHEMA IF NOT EXISTS pganalyze;
GRANT USAGE ON SCHEMA pganalyze TO pganalyze;

CREATE OR REPLACE FUNCTION pganalyze.get_column_stats()
RETURNS TABLE(
  schemaname name,
  tablename name,
  attname name,
  inherited bool,
  null_frac real,
  avg_width int,
  n_distinct real,
  correlation real
)
AS $$
  SELECT schemaname, tablename, attname, inherited,
         null_frac, avg_width, n_distinct, correlation
  FROM pg_catalog.pg_stats
  WHERE schemaname NOT IN ('pg_catalog', 'information_schema');
$$ LANGUAGE sql VOLATILE SECURITY DEFINER;
```

### Network and security
- Collector must reach Aurora on port **5432**
- Outbound HTTPS (**443**) allowed to pganalyze endpoints
- IAM role if collecting AWS metadata or CloudWatch metrics

### Collector deployment options
- EC2
- ECS / Fargate
- Docker host

---

## 8) Minimal implementation checklist

1. Verify Aurora PostgreSQL version supports HypoPG
2. Create and attach custom **DB cluster parameter group**
3. Enable `pg_stat_statements`
4. Restart writer instance
5. Install extensions:
   ```sql
   CREATE EXTENSION hypopg;
   CREATE EXTENSION pg_stat_statements;
   ```
6. Use pganalyze to generate index candidates
7. Validate candidates using HypoPG
8. Create real indexes only after validation

---

## 9) Troubleshooting

### pg_stat_statements not loading
- Ensure it is listed in `shared_preload_libraries`
- Restart the writer instance

### HypoPG appears ineffective
- Use `EXPLAIN`, not `EXPLAIN ANALYZE`
- Ensure hypothetical indexes exist in the same session

### No pganalyze recommendations yet
- Allow time for sufficient workload data collection

---

## Appendix: Useful queries

```sql
SELECT query, calls, total_exec_time
FROM pg_stat_statements
ORDER BY total_exec_time DESC
LIMIT 20;
```

```sql
SELECT * FROM hypopg_list_indexes();
```

---

## References

- https://aws.amazon.com/rds/aurora/postgresql/
- https://github.com/HypoPG/hypopg
- https://pganalyze.com/docs
- https://pganalyze.com/index-advisor
