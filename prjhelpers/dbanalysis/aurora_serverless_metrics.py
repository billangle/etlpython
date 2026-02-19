#!/usr/bin/env python3
"""
aurora_pg_serverless_v2_metrics.py

Analyzes AWS Aurora PostgreSQL Serverless v2 clusters (0.5-64 ACU range)
to surface resource allocation issues causing slow response times.

Metric groups:
  Scaling / Capacity
    ACUUtilization              % of max ACUs in use (v2 only)
    ServerlessDatabaseCapacity  Current allocated ACUs (live view + trend)

  Compute & Memory (per instance)
    CPUUtilization              Instance CPU %
    FreeableMemory              Available RAM (drops as ACU scales)

  Connections
    DatabaseConnections         Active connections (max_connections scales with ACU)

  Cache & I/O
    BufferCacheHitRatio         Shared buffer hit % -- low = storage round-trips
    ReadLatency                 Average storage read latency
    WriteLatency                Average storage write latency
    VolumeReadIOPs              Aurora storage read IOPS (cluster)
    VolumeWriteIOPs             Aurora storage write IOPS (cluster)

  Query Latency
    SelectLatency               Average SELECT latency
    DMLLatency                  Average INSERT/UPDATE/DELETE latency
    CommitLatency               Average COMMIT latency (WAL / lock pressure)
    DDLLatency                  Average DDL latency

  Reliability -- PostgreSQL-specific
    Deadlocks                   Deadlock count
    MaximumUsedTransactionIDs   Distance to transaction ID wraparound (CRITICAL)
    TransactionLogsDiskUsage    WAL / transaction log disk bytes
    ReplicationSlotDiskUsage    WAL retained by replication slots (blocks vacuum)

  Network (per instance)
    NetworkReceiveThroughput
    NetworkTransmitThroughput

Target cluster: disc-fsa-prod-db-pg  (us-east-1, Aurora PostgreSQL Serverless v2)
  Writer : disc-fsa-prod-db-pg-instance-1    (us-east-1c)
  Reader : disc-fsa-prod-db-pg-instance-1-us-e  (us-east-1a)

Usage (AWS CloudShell -- credentials are provided automatically):
    python3 aurora_serverless_metrics.py
    python3 aurora_serverless_metrics.py --minutes 60
    python3 aurora_serverless_metrics.py --verbose
    python3 aurora_serverless_metrics.py --cluster <other-cluster-id>
    python3 aurora_serverless_metrics.py --report /tmp/my-report.md

Options:
    --cluster    Aurora cluster identifier (default: disc-fsa-prod-db-pg)
    --region     AWS region (default: us-east-1)
    --minutes    Lookback window in minutes (default: 30)
    --profile    AWS CLI profile -- not needed in CloudShell
    --verbose    Show all metrics including those in OK state
    --report     Markdown report output path (default: db-metrics.md)

Requires: boto3  (pre-installed in AWS CloudShell)

ACU to max_connections reference (Aurora PostgreSQL Serverless v2):
    0.5 ACU  ~   189 connections
    1   ACU  ~   270 connections
    2   ACU  ~   405 connections
    4   ACU  ~   660 connections
    8   ACU  ~ 1,290 connections
   16   ACU  ~ 2,560 connections
   32   ACU  ~ 5,120 connections
   64   ACU  ~ 5,000 connections (platform ceiling)
"""

import argparse
import sys
from datetime import datetime, timezone, timedelta
from typing import Optional

try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
except ImportError:
    print("ERROR: boto3 is required.  Run: pip install boto3")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Environment constants -- disc-fsa-prod-db-pg
# ---------------------------------------------------------------------------
DEFAULT_CLUSTER      = "disc-fsa-prod-db-pg"
DEFAULT_REGION       = "us-east-1"
SERVERLESS_V2_MIN_ACU = 0.5
SERVERLESS_V2_MAX_ACU = 64.0

# Approximate max_connections at each ACU tier (Aurora PG Serverless v2)
ACU_CONN_REFERENCE = {
    0.5: 189, 1: 270, 2: 405, 4: 660, 8: 1290,
    16: 2560, 32: 5120, 64: 5000,
}

# ---------------------------------------------------------------------------
# Thresholds -- tune these for your workload
# ---------------------------------------------------------------------------
THRESHOLDS = {
    # Scaling ---
    "ACUUtilization": {
        "warn": 75.0,
        "crit": 92.0,
        "unit": "%",
        "note": (
            "Near 64 ACU ceiling -- cluster CANNOT scale further. "
            "Queries will queue. Raise max ACU or reduce peak concurrency."
        ),
    },
    # Compute ---
    "CPUUtilization": {
        "warn": 70.0,
        "crit": 85.0,
        "unit": "%",
        "note": (
            "High CPU can saturate the instance before ACU scaling completes. "
            "Check pg_stat_activity for runaway queries."
        ),
    },
    "FreeableMemory": {
        "warn": 512 * 1024 * 1024,   # 512 MB
        "crit": 256 * 1024 * 1024,   # 256 MB
        "unit": "bytes",
        "invert": True,
        "note": (
            "Low free memory causes shared_buffer eviction and forces storage reads. "
            "Aurora PG sets shared_buffers = 75% of instance RAM."
        ),
    },
    # Connections ---
    "DatabaseConnections": {
        "warn": 200,
        "crit": 400,
        "unit": "connections",
        "note": (
            "max_connections scales with ACU -- at 0.5 ACU the limit is ~189. "
            "Use RDS Proxy to absorb connection spikes during scale events."
        ),
    },
    # Cache & I/O ---
    "BufferCacheHitRatio": {
        "warn": 90.0,
        "crit": 80.0,
        "unit": "%",
        "invert": True,
        "note": (
            "Low shared_buffer hit ratio forces Aurora storage reads. "
            "Check for large sequential scans or missing indexes."
        ),
    },
    "ReadLatency": {
        "warn": 0.005,    # 5 ms
        "crit": 0.020,    # 20 ms
        "unit": "seconds",
        "note": "Elevated read I/O latency. Check buffer cache hit ratio and storage IOPS.",
    },
    "WriteLatency": {
        "warn": 0.005,
        "crit": 0.020,
        "unit": "seconds",
        "note": "Elevated write I/O latency. Check WAL volume, autovacuum activity, and IOPS.",
    },
    "VolumeReadIOPs": {
        "warn": 3000,
        "crit": 6000,
        "unit": "IOPS",
        "note": "High read IOPS -- likely missing index or cold buffer pool after scale-down.",
    },
    "VolumeWriteIOPs": {
        "warn": 2000,
        "crit": 4000,
        "unit": "IOPS",
        "note": "High write IOPS. Check autovacuum frequency, WAL volume, and batch sizes.",
    },
    # Query Latency ---
    "SelectLatency": {
        "warn": 0.020,    # 20 ms
        "crit": 0.100,    # 100 ms
        "unit": "seconds",
        "note": "High SELECT latency. Check pg_stat_statements for slow queries and index gaps.",
    },
    "DMLLatency": {
        "warn": 0.050,
        "crit": 0.200,
        "unit": "seconds",
        "note": "High DML latency. Check lock contention, write IOPS, and index bloat.",
    },
    "CommitLatency": {
        "warn": 0.005,    # 5 ms
        "crit": 0.020,    # 20 ms
        "unit": "seconds",
        "note": (
            "High COMMIT latency = I/O pressure on WAL writes or lock contention. "
            "Consider batching commits; review synchronous_commit setting."
        ),
    },
    "DDLLatency": {
        "warn": 0.500,
        "crit": 2.000,
        "unit": "seconds",
        "note": "High DDL latency. DDL acquires AccessExclusiveLock -- check for blocking queries.",
    },
    # Reliability / PostgreSQL-specific ---
    "Deadlocks": {
        "warn": 1,
        "crit": 5,
        "unit": "count",
        "note": "Deadlocks cause rollbacks and retry storms. Review locking order in the application.",
    },
    "MaximumUsedTransactionIDs": {
        "warn": 1_000_000_000,    # 1 B
        "crit": 1_500_000_000,    # 1.5 B (PG emergency autovacuum fires at ~1.6 B)
        "unit": "txid",
        "note": (
            "CRITICAL: at ~1.6 B the DB enters read-only emergency mode. "
            "Ensure autovacuum runs and is not blocked. Run VACUUM FREEZE on oldest tables."
        ),
    },
    "TransactionLogsDiskUsage": {
        "warn": 1 * 1024 ** 3,    # 1 GB
        "crit": 5 * 1024 ** 3,    # 5 GB
        "unit": "bytes",
        "note": (
            "Large WAL accumulation. Check for stale replication slots "
            "or long-running transactions blocking WAL recycling."
        ),
    },
    "ReplicationSlotDiskUsage": {
        "warn": 512 * 1024 * 1024,     # 512 MB
        "crit": 2 * 1024 ** 3,         # 2 GB
        "unit": "bytes",
        "note": (
            "Stale replication slot retaining WAL. Prevents autovacuum from reclaiming "
            "dead tuples and bloats pg_wal. Drop unused slots."
        ),
    },
    # Network ---
    "NetworkReceiveThroughput": {
        "warn": 50 * 1024 ** 2,     # 50 MB/s
        "crit": 100 * 1024 ** 2,
        "unit": "bytes/sec",
        "note": "High inbound network. Large COPY/bulk inserts or replication traffic.",
    },
    "NetworkTransmitThroughput": {
        "warn": 50 * 1024 ** 2,
        "crit": 100 * 1024 ** 2,
        "unit": "bytes/sec",
        "note": "High outbound network. Large result sets -- check for missing LIMIT clauses.",
    },
}

# Stat to pull per metric
METRIC_STAT = {
    "ACUUtilization":             "Maximum",
    "CPUUtilization":             "Maximum",
    "FreeableMemory":             "Minimum",
    "DatabaseConnections":        "Maximum",
    "BufferCacheHitRatio":        "Minimum",
    "ReadLatency":                "Average",
    "WriteLatency":               "Average",
    "VolumeReadIOPs":             "Average",
    "VolumeWriteIOPs":            "Average",
    "SelectLatency":              "Average",
    "DMLLatency":                 "Average",
    "CommitLatency":              "Average",
    "DDLLatency":                 "Average",
    "Deadlocks":                  "Sum",
    "MaximumUsedTransactionIDs":  "Maximum",
    "TransactionLogsDiskUsage":   "Maximum",
    "ReplicationSlotDiskUsage":   "Maximum",
    "NetworkReceiveThroughput":   "Average",
    "NetworkTransmitThroughput":  "Average",
    "ServerlessDatabaseCapacity": "Average",
}

# ---------------------------------------------------------------------------
# Terminal colors
# ---------------------------------------------------------------------------
RESET  = "\033[0m"
RED    = "\033[91m"
YELLOW = "\033[93m"
GREEN  = "\033[92m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
DIM    = "\033[2m"


def colorize(text: str, color: str) -> str:
    return f"{color}{text}{RESET}"


def fmt_value(metric_name: str, value: float) -> str:
    unit = THRESHOLDS.get(metric_name, {}).get("unit", "")
    if unit == "bytes":
        if value >= 1024 ** 3:
            return f"{value / 1024**3:.2f} GB"
        return f"{value / 1024**2:.1f} MB"
    if unit == "bytes/sec":
        return f"{value / 1024**2:.2f} MB/s"
    if unit == "%":
        return f"{value:.2f}%"
    if unit == "seconds":
        return f"{value * 1000:.2f} ms"
    if unit == "txid":
        limit = 2_147_483_648
        pct = value / limit * 100
        return f"{value:,.0f}  ({pct:.1f}% of 2^31 wraparound limit)"
    if unit in ("IOPS", "connections", "count"):
        return f"{value:,.0f} {unit}"
    return f"{value:.4f}"


def evaluate(metric_name: str, value: float) -> str:
    cfg = THRESHOLDS.get(metric_name)
    if cfg is None:
        return "OK"
    invert = cfg.get("invert", False)
    warn, crit = cfg["warn"], cfg["crit"]
    if invert:
        return "CRIT" if value <= crit else ("WARN" if value <= warn else "OK")
    return "CRIT" if value >= crit else ("WARN" if value >= warn else "OK")


def status_badge(status: str) -> str:
    return {
        "CRIT": colorize("[CRIT]", RED + BOLD),
        "WARN": colorize("[WARN]", YELLOW + BOLD),
        "OK":   colorize("[ OK ]", GREEN),
        "N/A":  colorize("[ N/A]", DIM),
    }.get(status, f"[{status}]")


# ---------------------------------------------------------------------------
# CloudWatch fetch
# ---------------------------------------------------------------------------

def fetch_metric(
    cw, metric_name: str, dimensions: list,
    stat: str, start: datetime, end: datetime, period: int,
) -> Optional[float]:
    try:
        resp = cw.get_metric_statistics(
            Namespace="AWS/RDS", MetricName=metric_name,
            Dimensions=dimensions, StartTime=start, EndTime=end,
            Period=period, Statistics=[stat],
        )
    except ClientError as exc:
        print(f"  CloudWatch error [{metric_name}]: {exc.response['Error']['Message']}")
        return None
    pts = resp.get("Datapoints", [])
    if not pts:
        return None
    pts.sort(key=lambda d: d["Timestamp"], reverse=True)
    return pts[0].get(stat)


def fetch_all_datapoints(cw, metric_name: str, dimensions: list,
                         stat: str, start: datetime, end: datetime, period: int) -> list:
    """Return all datapoint values (for trend analysis)."""
    try:
        resp = cw.get_metric_statistics(
            Namespace="AWS/RDS", MetricName=metric_name,
            Dimensions=dimensions, StartTime=start, EndTime=end,
            Period=period, Statistics=[stat],
        )
    except ClientError:
        return []
    return [d[stat] for d in resp.get("Datapoints", [])]


# ---------------------------------------------------------------------------
# RDS describe
# ---------------------------------------------------------------------------

def describe_cluster(rds, cluster_id: str) -> dict:
    try:
        resp = rds.describe_db_clusters(DBClusterIdentifier=cluster_id)
    except ClientError as exc:
        print(f"ERROR ({exc.response['Error']['Code']}): {exc.response['Error']['Message']}")
        sys.exit(1)
    clusters = resp.get("DBClusters", [])
    if not clusters:
        print(f"ERROR: No cluster returned for '{cluster_id}'.")
        sys.exit(1)
    return clusters[0]


def get_v2_scaling_config(cluster: dict) -> tuple:
    """Return (min_acu, max_acu) from the Serverless v2 config."""
    cfg = cluster.get("ServerlessV2ScalingConfiguration", {})
    return (
        cfg.get("MinCapacity", SERVERLESS_V2_MIN_ACU),
        cfg.get("MaxCapacity", SERVERLESS_V2_MAX_ACU),
    )


def get_instance_ids(cluster: dict) -> list:
    return [m["DBInstanceIdentifier"] for m in cluster.get("DBClusterMembers", [])]


# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------

def print_section(title: str):
    print(f"\n{BOLD}{CYAN}{'─' * 64}{RESET}")
    print(f"{BOLD}{CYAN}  {title}{RESET}")
    print(f"{BOLD}{CYAN}{'─' * 64}{RESET}")


def print_metric_row(metric_name: str, value: Optional[float], verbose: bool):
    if value is None:
        if verbose:
            print(f"  {status_badge('N/A')}  {metric_name:<42}  no data in window")
        return
    status    = evaluate(metric_name, value)
    note      = THRESHOLDS.get(metric_name, {}).get("note", "")
    formatted = fmt_value(metric_name, value)
    if status == "OK" and not verbose:
        return
    print(f"  {status_badge(status)}  {metric_name:<42}  {formatted}")
    if status in ("WARN", "CRIT") and note:
        print(f"             {DIM}{note}{RESET}")


# ---------------------------------------------------------------------------
# Capacity trend
# ---------------------------------------------------------------------------

def print_capacity_trend(cap_series: list, min_acu: float, max_acu: float):
    if not cap_series:
        return
    min_c   = min(cap_series)
    max_c   = max(cap_series)
    avg_c   = sum(cap_series) / len(cap_series)
    at_ceil = sum(1 for v in cap_series if v >= max_acu * 0.95)
    pct_ceil = at_ceil / len(cap_series) * 100

    print(f"\n  {BOLD}ACU allocation over window  "
          f"(configured: {min_acu} – {max_acu} ACU):{RESET}")
    print(f"    Min {min_c:.2f}  /  Avg {avg_c:.2f}  /  Max {max_c:.2f}")

    if at_ceil > 0:
        badge = colorize("[CRIT]", RED + BOLD) if pct_ceil >= 20 else colorize("[WARN]", YELLOW + BOLD)
        print(f"\n  {badge}  At ≥95% of max ACU ({max_acu}) for "
              f"{pct_ceil:.0f}% of sample points ({at_ceil}/{len(cap_series)} periods).")
        print(f"           {DIM}Queries are queuing at the ceiling. Options:{RESET}")
        print(f"           {DIM}  1. Raise max ACU above {max_acu} (modify cluster).{RESET}")
        print(f"           {DIM}  2. Use RDS Proxy to absorb connection bursts.{RESET}")
        print(f"           {DIM}  3. Tune slow queries to reduce compute per request.{RESET}")
    elif max_c <= min_acu + 0.5:
        print(f"  {colorize('[INFO]', CYAN)}  Cluster staying at min ACU ({min_acu}). "
              f"If latency is high, check connection count and shared_buffer hit ratio.")
    else:
        scale_range = max_c - min_acu
        if scale_range >= (max_acu - min_acu) * 0.5:
            print(f"  {colorize('[INFO]', CYAN)}  Wide ACU swing detected "
                  f"({min_c:.1f} – {max_c:.1f}). Bursty workload pattern.")


# ---------------------------------------------------------------------------
# Connection context vs current ACU
# ---------------------------------------------------------------------------

def print_connection_context(conn_count: float, current_acu: Optional[float], max_acu: float):
    acu = current_acu if current_acu is not None else max_acu
    nearest = min(ACU_CONN_REFERENCE, key=lambda k: abs(k - acu))
    approx_max = ACU_CONN_REFERENCE[nearest]
    pct = conn_count / approx_max * 100
    badge = (
        colorize("[CRIT]", RED + BOLD) if pct >= 90
        else colorize("[WARN]", YELLOW + BOLD) if pct >= 70
        else colorize("[ OK ]", GREEN)
    )
    print(f"\n  {badge}  Connection saturation: "
          f"{conn_count:.0f} / ~{approx_max} max  ({pct:.0f}% at ~{acu:.1f} ACU)")
    if pct >= 70:
        print(f"           {DIM}max_connections scales with ACU. At low ACU the limit is tight.{RESET}")
        print(f"           {DIM}RDS Proxy pools connections and decouples scale-out from connection count.{RESET}")


# ---------------------------------------------------------------------------
# Transaction ID wraparound advisory
# ---------------------------------------------------------------------------

def print_wraparound_advisory(txid_val: float):
    limit    = 2_147_483_648   # 2^31
    distance = limit - txid_val
    pct      = txid_val / limit * 100
    if pct >= 70:
        badge  = colorize("[CRIT]", RED + BOLD)
        advice = (
            "URGENT: run `VACUUM FREEZE` on tables with oldest relfrozenxid. "
            "Check pg_stat_activity for transactions blocking autovacuum."
        )
    elif pct >= 46.5:
        badge  = colorize("[WARN]", YELLOW + BOLD)
        advice = (
            "Schedule VACUUM FREEZE maintenance. "
            "Review autovacuum_freeze_max_age and autovacuum_vacuum_freeze_table_age."
        )
    else:
        return
    print(f"  {badge}  MaximumUsedTransactionIDs: {txid_val:,.0f} used, "
          f"{distance:,.0f} remaining ({pct:.1f}% of 2^31 limit)")
    print(f"           {DIM}{advice}{RESET}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Aurora PostgreSQL Serverless v2 -- disc-fsa-prod-db-pg resource analysis",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--cluster",  default=DEFAULT_CLUSTER,
                   help=f"Aurora cluster identifier (default: {DEFAULT_CLUSTER})")
    p.add_argument("--region",   default=DEFAULT_REGION,
                   help=f"AWS region (default: {DEFAULT_REGION})")
    p.add_argument("--minutes",  type=int, default=30,
                   help="Lookback window in minutes (default: 30)")
    p.add_argument("--profile",  default=None,
                   help="AWS CLI profile (not needed in CloudShell)")
    p.add_argument("--verbose",  action="store_true",
                   help="Show all metrics including OK")
    p.add_argument("--report",   default="db-metrics.md",
                   help="Markdown report output file (default: db-metrics.md)")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Markdown report writer
# ---------------------------------------------------------------------------

SEVERITY_EMOJI = {"CRIT": "🔴", "WARN": "🟡"}

REMEDIATION = {
    "ACU Ceiling": [
        "Raise the cluster max ACU limit: RDS console → Modify cluster → Serverless v2 capacity → increase Maximum ACU above 64.",
        "Add RDS Proxy in front of the cluster to decouple connection spikes from the scaling event lag.",
        "Identify the top CPU / I/O consuming queries using `pg_stat_statements` and optimize or add indexes.",
    ],
    "ACUUtilization": [
        "Raise max ACU (see ACU Ceiling above) or reduce query concurrency.",
        "Use connection pooling (RDS Proxy / PgBouncer) to limit active queries at peak.",
    ],
    "CPUUtilization": [
        "Run `SELECT query, calls, total_exec_time FROM pg_stat_statements ORDER BY total_exec_time DESC LIMIT 20;` to find runaway queries.",
        "Add or refine indexes on frequently queried columns to reduce full-table-scan CPU cost.",
        "Consider read replicas or routing read-heavy traffic to the reader instance.",
    ],
    "FreeableMemory": [
        "Increase min ACU so the cluster does not scale down to 0.5 ACU when idle — low ACU = very low shared_buffers.",
        "Look for queries with large `work_mem` multiplied by many parallel workers; reduce `work_mem` parameter.",
        "Check for memory-hungry extensions (e.g., `pg_trgm` on large tables).",
    ],
    "DatabaseConnections": [
        "Deploy RDS Proxy: connection pooling prevents exhausting max_connections at low ACU levels.",
        "Audit application connection pools — ensure they are using `min_pool_size` / `max_pool_size` conservatively.",
        "Check for idle connections: `SELECT count(*) FROM pg_stat_activity WHERE state = 'idle';`",
    ],
    "BufferCacheHitRatio": [
        "Identify tables driving cache misses: `SELECT relname, heap_blks_hit, heap_blks_read FROM pg_statio_user_tables ORDER BY heap_blks_read DESC LIMIT 20;`",
        "Add indexes to eliminate sequential scans on large tables.",
        "Increase min ACU so shared_buffers stays larger between requests.",
    ],
    "ReadLatency": [
        "Check BufferCacheHitRatio — low hit ratio forces storage reads.",
        "Run `EXPLAIN (ANALYZE, BUFFERS)` on slow queries to identify I/O-heavy plan steps.",
        "Ensure autovacuum is not bloating tables (dead tuples cause index bloat and larger reads).",
    ],
    "WriteLatency": [
        "Check WAL volume: `SELECT pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0'));`",
        "Review autovacuum settings — excessive dead tuple accumulation increases write amplification.",
        "Batch small writes into larger transactions to reduce WAL flush frequency.",
    ],
    "VolumeReadIOPs": [
        "Identify table/index scans driving IOPS: `SELECT relname, seq_scan, idx_scan FROM pg_stat_user_tables ORDER BY seq_scan DESC LIMIT 20;`",
        "Add indexes to convert sequential scans to index scans — each seq_scan on a large table = massive IOPS.",
        "After a scale-down event, the buffer pool is cold. Consider a warm-up query to pre-load hot pages.",
    ],
    "VolumeWriteIOPs": [
        "Check autovacuum activity: `SELECT relname, n_dead_tup, last_autovacuum FROM pg_stat_user_tables ORDER BY n_dead_tup DESC LIMIT 20;`",
        "Reduce checkpoint frequency if write IOPS are WAL-driven: review `checkpoint_completion_target`.",
        "Batch bulk insert/update operations and commit in larger chunks.",
    ],
    "SelectLatency": [
        "Enable and query `pg_stat_statements`: `SELECT query, mean_exec_time, calls FROM pg_stat_statements ORDER BY mean_exec_time DESC LIMIT 20;`",
        "Use `EXPLAIN (ANALYZE, BUFFERS)` on slow queries to pinpoint seq scans, hash joins, or sort spills.",
        "Add indexes on filter and join columns identified in slow queries.",
    ],
    "DMLLatency": [
        "Check for lock contention: `SELECT pid, wait_event_type, wait_event, query FROM pg_stat_activity WHERE wait_event IS NOT NULL;`",
        "Review index count on heavily written tables — too many indexes slow INSERT/UPDATE/DELETE.",
        "Check for bloated tables and run `VACUUM ANALYZE <table>` on the worst offenders.",
    ],
    "CommitLatency": [
        "High commit latency indicates WAL I/O pressure. Check `VolumeWriteIOPs` and WAL volume.",
        "Review `synchronous_commit` setting — for non-critical writes consider `synchronous_commit = local`.",
        "Batch small, frequent transactions into larger commits to amortize WAL flush cost.",
        "Check for lock waits blocking commits: `SELECT * FROM pg_locks WHERE granted = false;`",
    ],
    "DDLLatency": [
        "DDL acquires `AccessExclusiveLock` — check for long-running transactions blocking it.",
        "Run `SELECT pid, query, state, wait_event FROM pg_stat_activity WHERE state != 'idle' ORDER BY query_start;` to find blockers.",
        "Schedule DDL during low-traffic windows and use `lock_timeout` to avoid indefinite waits.",
    ],
    "Deadlocks": [
        "Review application locking order — deadlocks occur when transactions acquire locks in different orders.",
        "Enable deadlock logging: set `log_lock_waits = on` and `deadlock_timeout = 1s` in parameter group.",
        "Check `pg_stat_activity` during peak load to identify transactions waiting on locks.",
    ],
    "MaximumUsedTransactionIDs": [
        "URGENT if near 1.5B: Run `VACUUM FREEZE` on the table with the oldest `relfrozenxid`.",
        "Query: `SELECT relname, age(relfrozenxid) FROM pg_class WHERE relkind='r' ORDER BY age(relfrozenxid) DESC LIMIT 10;`",
        "Ensure no long-running transaction is blocking autovacuum: `SELECT pid, now() - xact_start AS duration, query FROM pg_stat_activity WHERE xact_start IS NOT NULL ORDER BY duration DESC;`",
        "Tune `autovacuum_freeze_max_age` to trigger freeze earlier if the table ages quickly.",
    ],
    "TransactionLogsDiskUsage": [
        "Check for stale replication slots retaining WAL: `SELECT slot_name, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained FROM pg_replication_slots;`",
        "Drop unused replication slots: `SELECT pg_drop_replication_slot('<slot_name>');`",
        "Check for long-running transactions preventing WAL recycling (see MaximumUsedTransactionIDs).",
    ],
    "ReplicationSlotDiskUsage": [
        "Identify the offending slot: `SELECT slot_name, active, restart_lsn FROM pg_replication_slots;`",
        "If the consumer is stuck or gone, drop the slot: `SELECT pg_drop_replication_slot('<slot_name>');`",
        "Set `max_slot_wal_keep_size` in the parameter group to cap WAL retained per slot.",
    ],
    "NetworkReceiveThroughput": [
        "Large COPY or bulk inserts drive inbound traffic — check if batch sizes can be throttled.",
        "Ensure S3 imports (aws_s3 extension) are not running concurrently with peak application load.",
    ],
    "NetworkTransmitThroughput": [
        "Large result sets drive outbound traffic — add LIMIT clauses or server-side cursors to large queries.",
        "Check for queries fetching entire tables: `SELECT query, rows FROM pg_stat_statements ORDER BY rows DESC LIMIT 10;`",
    ],
    "Connection Saturation": [
        "Deploy RDS Proxy: it pools idle connections and presents a stable connection count to Aurora.",
        "Review application pool configuration to ensure `max_pool_size` is set below Aurora's `max_connections`.",
    ],
}


def write_markdown_report(
    report_items: list,
    meta: dict,
    output_path: str,
) -> None:
    """
    Write a clean Markdown report containing only WARN and CRIT findings
    with actionable remediation steps.
    """
    crit_warn = [r for r in report_items if r["status"] in ("CRIT", "WARN")]

    lines = []
    ts = meta["run_time"].strftime("%Y-%m-%d %H:%M UTC")

    lines.append(f"# Aurora PostgreSQL — Resource Allocation Report")
    lines.append(f"")
    lines.append(f"> Generated: {ts}  ")
    lines.append(f"> Window: last {meta['minutes']} minutes  ")
    lines.append(f"> Cluster: `{meta['cluster']}`  ")
    lines.append(f"> Region: `{meta['region']}`  ")
    lines.append(f"> Engine: `{meta['engine']} {meta['engine_ver']}`  ")
    lines.append(f"> Status: `{meta['status']}`  ")
    lines.append(f"> ACU range: `{meta['min_acu']} – {meta['max_acu']}` (Serverless v2)  ")
    lines.append(f"> Instances: {', '.join(f'`{i}`' for i in meta['instances'])}  ")
    lines.append(f"")

    if not crit_warn:
        lines.append("## ✅ No Critical or Warning Issues Detected")
        lines.append(f"")
        lines.append(f"All monitored metrics are within acceptable thresholds for the {meta['minutes']}-minute window.")
    else:
        crit_count = sum(1 for r in crit_warn if r["status"] == "CRIT")
        warn_count = sum(1 for r in crit_warn if r["status"] == "WARN")
        lines.append(f"## Summary")
        lines.append(f"")
        lines.append(f"| Severity | Count |")
        lines.append(f"|----------|-------|")
        if crit_count:
            lines.append(f"| 🔴 CRITICAL | {crit_count} |")
        if warn_count:
            lines.append(f"| 🟡 WARNING  | {warn_count} |")
        lines.append(f"")

        # Group by category
        categories: dict = {}
        for item in crit_warn:
            categories.setdefault(item["category"], []).append(item)

        # CRIT sections first, then WARN
        def sort_key(cat_items):
            has_crit = any(i["status"] == "CRIT" for i in cat_items[1])
            return (0 if has_crit else 1, cat_items[0])

        lines.append(f"---")
        lines.append(f"")
        lines.append(f"## Issues & Recommendations")
        lines.append(f"")

        for category, items in sorted(categories.items(), key=sort_key):
            lines.append(f"### {category}")
            lines.append(f"")
            lines.append(f"| Severity | Scope | Metric | Observed Value |")
            lines.append(f"|----------|-------|--------|----------------|")
            for item in sorted(items, key=lambda x: (0 if x["status"] == "CRIT" else 1)):
                emoji = SEVERITY_EMOJI.get(item["status"], "")
                scope = f"`{item['scope']}`" if item["scope"] else "cluster"
                lines.append(
                    f"| {emoji} {item['status']} | {scope} | `{item['metric']}` | {item['value']} |"
                )
            lines.append(f"")

            # Deduplicate remediation keys across items in this category
            seen_keys: set = set()
            recs: list = []
            for item in items:
                key = item.get("remediation_key", item["metric"])
                if key not in seen_keys:
                    seen_keys.add(key)
                    steps = REMEDIATION.get(key, [])
                    if not steps and item.get("note"):
                        steps = [item["note"]]
                    if steps:
                        recs.append((item["metric"], steps))

            if recs:
                lines.append(f"**Remediation steps:**")
                lines.append(f"")
                for metric_name, steps in recs:
                    lines.append(f"**`{metric_name}`**")
                    for step in steps:
                        lines.append(f"- {step}")
                    lines.append(f"")

            lines.append(f"---")
            lines.append(f"")

    lines.append(f"## Reference")
    lines.append(f"")
    lines.append(f"| ACU | ~max\_connections |")
    lines.append(f"|-----|-----------------|")
    for acu, conns in ACU_CONN_REFERENCE.items():
        lines.append(f"| {acu} | {conns:,} |")
    lines.append(f"")
    lines.append(f"**Aurora PostgreSQL Serverless v2 key facts**")
    lines.append(f"")
    lines.append(f"- Scale-up is fast (~seconds) but not instant — queries queue at the ACU ceiling.")
    lines.append(f"- `max_connections` is determined at instance start by the configured max ACU; use RDS Proxy to avoid reconnect storms during scale events.")
    lines.append(f"- The shared buffer pool **does not persist** across scale-down events — expect cold-read latency spikes after idle periods.")
    lines.append(f"- Autovacuum runs on the writer instance. Monitor `MaximumUsedTransactionIDs` to prevent transaction ID wraparound.")
    lines.append(f"")
    lines.append(f"*Run `python3 aurora_serverless_metrics.py --minutes 120 --verbose` for a wider window with all metrics.*")

    with open(output_path, "w") as fh:
        fh.write("\n".join(lines) + "\n")

    print(f"\n  Report written to: {output_path}")


def main():
    args = parse_args()

    session_kwargs = {"region_name": args.region}
    if args.profile:
        session_kwargs["profile_name"] = args.profile
    session = boto3.Session(**session_kwargs)

    try:
        rds = session.client("rds")
        cw  = session.client("cloudwatch")
    except NoCredentialsError:
        print("ERROR: No AWS credentials. Use env vars, ~/.aws/credentials, or --profile.")
        sys.exit(1)

    end_time   = datetime.now(timezone.utc)
    start_time = end_time - timedelta(minutes=args.minutes)
    period     = max(60, (args.minutes * 60) // 20)   # ~20 data points across window

    # Accumulates all WARN/CRIT findings for the markdown report
    report_items: list = []

    def log_finding(category: str, scope: str, metric: str, value: Optional[float],
                    remediation_key: str = None):
        """Evaluate a metric value; if WARN/CRIT, add to the report_items list."""
        if value is None:
            return
        status = evaluate(metric, value)
        if status in ("WARN", "CRIT"):
            report_items.append({
                "category": category,
                "scope": scope,
                "metric": metric,
                "value": fmt_value(metric, value),
                "status": status,
                "note": THRESHOLDS.get(metric, {}).get("note", ""),
                "remediation_key": remediation_key or metric,
            })

    # Header
    print_section(f"Aurora PostgreSQL Serverless v2: {args.cluster}")
    print(f"  Region   : {args.region}")
    print(f"  Window   : last {args.minutes} min  "
          f"({start_time.strftime('%H:%M')} - {end_time.strftime('%H:%M')} UTC)")

    cluster      = describe_cluster(rds, args.cluster)
    engine       = cluster.get("Engine", "")
    engine_ver   = cluster.get("EngineVersion", "")
    status_str   = cluster.get("Status", "unknown")
    instance_ids = get_instance_ids(cluster)
    min_acu, max_acu = get_v2_scaling_config(cluster)

    if "postgres" not in engine.lower():
        print(f"{YELLOW}WARNING: Engine reported as '{engine}' -- expected aurora-postgresql.{RESET}")

    print(f"  Engine   : {engine} {engine_ver}")
    print(f"  Status   : {status_str}")
    print(f"  ACU range: {min_acu} - {max_acu}  (Serverless v2)")
    print(f"  Instances: {', '.join(sorted(instance_ids)) or 'none found'}")

    cluster_dim = [{"Name": "DBClusterIdentifier", "Value": args.cluster}]

    # ------------------------------------------------------------------
    # 1. Capacity & Scaling
    # ------------------------------------------------------------------
    print_section("Capacity & Scaling")

    cap_series = fetch_all_datapoints(
        cw, "ServerlessDatabaseCapacity", cluster_dim, "Average",
        start_time, end_time, period,
    )
    print_capacity_trend(cap_series, min_acu, max_acu)
    current_acu = (sum(cap_series) / len(cap_series)) if cap_series else None

    # Log ACU ceiling pressure as a named finding
    if cap_series:
        at_ceil  = sum(1 for v in cap_series if v >= max_acu * 0.95)
        pct_ceil = at_ceil / len(cap_series) * 100
        if at_ceil > 0:
            s = "CRIT" if pct_ceil >= 20 else "WARN"
            report_items.append({
                "category": "Capacity & Scaling",
                "scope": "cluster",
                "metric": "ACU Ceiling",
                "value": f"{pct_ceil:.0f}% of sample periods at ≥95% of {max_acu} ACU  "
                         f"(min {min(cap_series):.2f} / avg {current_acu:.2f} / max {max(cap_series):.2f})",
                "status": s,
                "note": "Cluster is pinned at its maximum ACU limit — queries are queuing.",
                "remediation_key": "ACU Ceiling",
            })

    acu_util = fetch_metric(cw, "ACUUtilization", cluster_dim,
                            "Maximum", start_time, end_time, period)
    print_metric_row("ACUUtilization", acu_util, args.verbose)
    log_finding("Capacity & Scaling", "cluster", "ACUUtilization", acu_util)

    # ------------------------------------------------------------------
    # 2. PostgreSQL-Specific Reliability
    # ------------------------------------------------------------------
    print_section("Reliability -- PostgreSQL-Specific")

    txid_val = fetch_metric(cw, "MaximumUsedTransactionIDs", cluster_dim,
                            "Maximum", start_time, end_time, period)
    if txid_val is not None:
        print_wraparound_advisory(txid_val)
        if evaluate("MaximumUsedTransactionIDs", txid_val) == "OK":
            print_metric_row("MaximumUsedTransactionIDs", txid_val, args.verbose)
        log_finding("Reliability — PostgreSQL-Specific", "cluster",
                    "MaximumUsedTransactionIDs", txid_val)

    for m in ("TransactionLogsDiskUsage", "ReplicationSlotDiskUsage", "Deadlocks"):
        val = fetch_metric(cw, m, cluster_dim, METRIC_STAT[m], start_time, end_time, period)
        print_metric_row(m, val, args.verbose)
        log_finding("Reliability — PostgreSQL-Specific", "cluster", m, val)

    # ------------------------------------------------------------------
    # 3. Storage I/O (cluster-level)
    # ------------------------------------------------------------------
    print_section("Storage I/O (Cluster)")

    for m in ("VolumeReadIOPs", "VolumeWriteIOPs"):
        val = fetch_metric(cw, m, cluster_dim, METRIC_STAT[m], start_time, end_time, period)
        print_metric_row(m, val, args.verbose)
        log_finding("Storage I/O", "cluster", m, val)

    # ------------------------------------------------------------------
    # 4. Per-instance metrics
    # ------------------------------------------------------------------
    print_section("Per-Instance Metrics")

    instance_groups = [
        ("Compute",        ["CPUUtilization", "FreeableMemory"]),
        ("Connections",    ["DatabaseConnections"]),
        ("Cache & I/O",    ["BufferCacheHitRatio", "ReadLatency", "WriteLatency"]),
        ("Query Latency",  ["SelectLatency", "DMLLatency", "CommitLatency", "DDLLatency"]),
        ("Network",        ["NetworkReceiveThroughput", "NetworkTransmitThroughput"]),
    ]

    if not instance_ids:
        print("  No instances found in this cluster.")
    else:
        for inst_id in sorted(instance_ids):
            print(f"\n  {BOLD}Instance: {inst_id}{RESET}")
            inst_dim  = [{"Name": "DBInstanceIdentifier", "Value": inst_id}]
            conn_val  = None

            for group_name, metrics in instance_groups:
                rows = []
                for m in metrics:
                    val = fetch_metric(cw, m, inst_dim, METRIC_STAT[m],
                                      start_time, end_time, period)
                    if m == "DatabaseConnections":
                        conn_val = val
                    status = evaluate(m, val) if val is not None else "N/A"
                    if status != "OK" or args.verbose:
                        rows.append((m, val))
                    log_finding(f"Per-Instance — {group_name}", inst_id, m, val)

                if rows:
                    print(f"\n  {DIM}  {group_name}{RESET}")
                    for m, val in rows:
                        print_metric_row(m, val, args.verbose)

            # Contextual connection vs ACU analysis
            if conn_val is not None and (conn_val > 0 or args.verbose):
                print_connection_context(conn_val, current_acu, max_acu)
                # Log connection saturation if concerning
                acu = current_acu if current_acu is not None else max_acu
                nearest = min(ACU_CONN_REFERENCE, key=lambda k: abs(k - acu))
                approx_max = ACU_CONN_REFERENCE[nearest]
                pct_conn = conn_val / approx_max * 100
                if pct_conn >= 70:
                    s = "CRIT" if pct_conn >= 90 else "WARN"
                    report_items.append({
                        "category": "Per-Instance — Connections",
                        "scope": inst_id,
                        "metric": "Connection Saturation",
                        "value": f"{conn_val:.0f} / ~{approx_max} max ({pct_conn:.0f}% at ~{acu:.1f} ACU)",
                        "status": s,
                        "note": "max_connections scales with ACU; RDS Proxy recommended.",
                        "remediation_key": "Connection Saturation",
                    })

    # ------------------------------------------------------------------
    # 5. Summary
    # ------------------------------------------------------------------
    print_section("Diagnostic Summary")

    crit_items = [r for r in report_items if r["status"] == "CRIT"]
    warn_items = [r for r in report_items if r["status"] == "WARN"]

    if report_items:
        print(f"\n  {BOLD}Findings requiring attention:{RESET}")
        for item in crit_items:
            print(f"    {colorize('!', RED + BOLD)}  [CRIT] {item['metric']} ({item['scope']}): {item['value']}")
        for item in warn_items:
            print(f"    {colorize('!', YELLOW + BOLD)}  [WARN] {item['metric']} ({item['scope']}): {item['value']}")
    else:
        print(f"\n  {colorize('No critical or warning issues detected', GREEN)} "
              f"in the last {args.minutes} min.")

    print(f"\n  Run with {BOLD}--verbose{RESET} to include OK metrics.")
    print(f"  Run with {BOLD}--minutes 120{RESET} for a longer lookback window.\n")

    # ------------------------------------------------------------------
    # 6. Write markdown report
    # ------------------------------------------------------------------
    meta = {
        "cluster":    args.cluster,
        "region":     args.region,
        "minutes":    args.minutes,
        "engine":     engine,
        "engine_ver": engine_ver,
        "status":     status_str,
        "min_acu":    min_acu,
        "max_acu":    max_acu,
        "instances":  sorted(instance_ids),
        "run_time":   end_time,
    }
    write_markdown_report(report_items, meta, args.report)


if __name__ == "__main__":
    main()
