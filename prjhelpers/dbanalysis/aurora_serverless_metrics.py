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

Options:
    --cluster    Aurora cluster identifier (default: disc-fsa-prod-db-pg)
    --region     AWS region (default: us-east-1)
    --minutes    Lookback window in minutes (default: 30)
    --profile    AWS CLI profile -- not needed in CloudShell
    --verbose    Show all metrics including those in OK state

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
    return p.parse_args()


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

    acu_util = fetch_metric(cw, "ACUUtilization", cluster_dim,
                            "Maximum", start_time, end_time, period)
    print_metric_row("ACUUtilization", acu_util, args.verbose)

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

    for m in ("TransactionLogsDiskUsage", "ReplicationSlotDiskUsage", "Deadlocks"):
        val = fetch_metric(cw, m, cluster_dim, METRIC_STAT[m], start_time, end_time, period)
        print_metric_row(m, val, args.verbose)

    # ------------------------------------------------------------------
    # 3. Storage I/O (cluster-level)
    # ------------------------------------------------------------------
    print_section("Storage I/O (Cluster)")

    for m in ("VolumeReadIOPs", "VolumeWriteIOPs"):
        val = fetch_metric(cw, m, cluster_dim, METRIC_STAT[m], start_time, end_time, period)
        print_metric_row(m, val, args.verbose)

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

                if rows:
                    print(f"\n  {DIM}  {group_name}{RESET}")
                    for m, val in rows:
                        print_metric_row(m, val, args.verbose)

            # Contextual connection vs ACU analysis
            if conn_val is not None and (conn_val > 0 or args.verbose):
                print_connection_context(conn_val, current_acu, max_acu)

    # ------------------------------------------------------------------
    # 5. Summary
    # ------------------------------------------------------------------
    print_section("Diagnostic Summary")

    findings = []

    if acu_util is not None and acu_util >= THRESHOLDS["ACUUtilization"]["warn"]:
        findings.append(f"ACU utilization peaked at {acu_util:.1f}% -- scaling ceiling pressure")

    if cap_series:
        at_ceil = sum(1 for v in cap_series if v >= max_acu * 0.95)
        if at_ceil:
            findings.append(
                f"At >= 95% of {max_acu} ACU max for {at_ceil}/{len(cap_series)} sample periods"
            )

    if txid_val is not None and txid_val >= THRESHOLDS["MaximumUsedTransactionIDs"]["warn"]:
        findings.append(
            f"Transaction ID wraparound risk: {txid_val:,.0f} IDs consumed "
            f"({txid_val/2_147_483_648*100:.1f}% of limit)"
        )

    if findings:
        print(f"\n  {BOLD}Findings requiring attention:{RESET}")
        for f in findings:
            print(f"    {colorize('!', RED + BOLD)}  {f}")
    else:
        print(f"\n  {colorize('No critical scaling or PostgreSQL-specific issues detected', GREEN)} "
              f"in the last {args.minutes} min.")

    print(f"\n  {DIM}Aurora PostgreSQL Serverless v2 key points:{RESET}")
    print(f"  {DIM}  - Scale-up is fast (~seconds) but not instant: queries queue at the ACU ceiling.{RESET}")
    print(f"  {DIM}  - max_connections is fixed at instance creation based on max ACU; use RDS Proxy{RESET}")
    print(f"  {DIM}    to avoid 'too many connections' errors during rapid scale events.{RESET}")
    print(f"  {DIM}  - Shared buffer pool does NOT persist across scale-down -- expect cold-read{RESET}")
    print(f"  {DIM}    latency spikes after the cluster idles and scales back to min ACU ({min_acu}).{RESET}")
    print(f"  {DIM}  - Autovacuum runs on the writer instance. Monitor MaximumUsedTransactionIDs.{RESET}")
    print(f"\n  Run with {BOLD}--verbose{RESET} to include OK metrics.")
    print(f"  Run with {BOLD}--minutes 120{RESET} for a longer lookback window.\n")


if __name__ == "__main__":
    main()
