#!/usr/bin/env python3
"""
create_cw_dashboard.py

Creates (or updates) an AWS CloudWatch dashboard for the Aurora PostgreSQL
Serverless v2 cluster disc-fsa-prod-db-pg based on the critical metrics
identified in db-metrics.md.

Dashboard sections (mirrors the db-metrics.md findings):
  1. ACU Scaling Ceiling       -- ACUUtilization, ServerlessDatabaseCapacity
  2. Storage I/O               -- VolumeReadIOPs, VolumeWriteIOPs
  3. Query Latency             -- CommitLatency (writer + reader), SelectLatency, DMLLatency
  4. Compute & Memory          -- CPUUtilization, FreeableMemory
  5. Connections               -- DatabaseConnections
  6. Cache Health              -- BufferCacheHitRatio, ReadLatency, WriteLatency
  7. Reliability               -- Deadlocks, MaximumUsedTransactionIDs,
                                  TransactionLogsDiskUsage, ReplicationSlotDiskUsage
  8. Network                   -- NetworkReceiveThroughput, NetworkTransmitThroughput

Usage (AWS CloudShell -- no profile needed):
    python3 create_cw_dashboard.py
    python3 create_cw_dashboard.py --dashboard MyStagingDB
    python3 create_cw_dashboard.py --cluster other-cluster-id --dashboard OtherDash

Options:
    --cluster    Aurora cluster identifier (default: disc-fsa-prod-db-pg)
    --region     AWS region              (default: us-east-1)
    --dashboard  CloudWatch dashboard name (default: Aurora-PG-disc-fsa-prod-db-pg)
    --period     Default metric period in seconds (default: 300 = 5 min)
    --profile    AWS CLI profile (not needed in CloudShell)
    --delete     Delete the dashboard instead of creating it
"""

import argparse
import json
import sys

try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
except ImportError:
    print("ERROR: boto3 is required.  Run: pip install boto3")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Defaults (disc-fsa-prod-db-pg)
# ---------------------------------------------------------------------------
DEFAULT_CLUSTER    = "disc-fsa-prod-db-pg"
DEFAULT_REGION     = "us-east-1"
DEFAULT_DASHBOARD  = f"Aurora-PG-{DEFAULT_CLUSTER}"
DEFAULT_PERIOD     = 300   # 5 minutes

WRITER_INSTANCE    = "disc-fsa-prod-db-pg-instance-1"
READER_INSTANCE    = "disc-fsa-prod-db-pg-instance-1-us-east-1a"

# Threshold annotation values (matching aurora_serverless_metrics.py)
WARN_COLOR = "#ff7f0e"   # orange
CRIT_COLOR = "#d62728"   # red

# ---------------------------------------------------------------------------
# Widget builder helpers
# ---------------------------------------------------------------------------

def cluster_dim(cluster_id: str) -> list:
    return ["DBClusterIdentifier", cluster_id]


def instance_dim(instance_id: str) -> list:
    return ["DBInstanceIdentifier", instance_id]


def metric(namespace: str, name: str, dims: list, stat: str = "Average",
           period: int = DEFAULT_PERIOD, label: str = None) -> list:
    """Return a CloudWatch metrics array entry."""
    entry = [namespace, name] + dims + [{"stat": stat, "period": period}]
    if label:
        entry[-1]["label"] = label
    return entry


def alarm_annotation(value: float, label: str, color: str) -> dict:
    return {"value": value, "label": label, "color": color}


def graph_widget(title: str, metrics: list, x: int, y: int,
                 width: int = 12, height: int = 6,
                 annotations: list = None,
                 view: str = "timeSeries",
                 left_label: str = None,
                 stat: str = "Average",
                 region: str = DEFAULT_REGION) -> dict:
    props = {
        "view": view,
        "stacked": False,
        "metrics": metrics,
        "period": DEFAULT_PERIOD,
        "title": title,
        "region": region,
    }
    if annotations:
        props["annotations"] = {"horizontal": annotations}
    if left_label:
        props["yAxis"] = {"left": {"label": left_label, "showUnits": False}}
    return {
        "type": "metric",
        "x": x,
        "y": y,
        "width": width,
        "height": height,
        "properties": props,
    }


def text_widget(markdown: str, x: int, y: int,
                width: int = 24, height: int = 2) -> dict:
    return {
        "type": "text",
        "x": x,
        "y": y,
        "width": width,
        "height": height,
        "properties": {"markdown": markdown},
    }


def alarm_widget(title: str, alarm_arns: list,
                 x: int, y: int,
                 width: int = 24, height: int = 3) -> dict:
    return {
        "type": "alarm",
        "x": x,
        "y": y,
        "width": width,
        "height": height,
        "properties": {
            "title": title,
            "alarms": alarm_arns,
        },
    }


# ---------------------------------------------------------------------------
# Dashboard body builder
# ---------------------------------------------------------------------------

def build_dashboard(cluster_id: str, writer: str, reader: str,
                    region: str, period: int) -> dict:
    ns = "AWS/RDS"
    widgets = []
    y = 0
    # Local wrapper so every graph_widget call automatically gets the correct region
    gw = lambda **kw: graph_widget(region=region, **kw)

    # ── Header ──────────────────────────────────────────────────────────────
    widgets.append(text_widget(
        f"## 🔴 Aurora PostgreSQL Serverless v2 — `{cluster_id}`\n"
        f"Region: `{region}` &nbsp;|&nbsp; "
        f"Writer: `{writer}` &nbsp;|&nbsp; "
        f"Reader: `{reader}` &nbsp;|&nbsp; "
        f"ACU range: 0.5 – 64 &nbsp;|&nbsp; "
        f"Engine: Aurora PostgreSQL (Serverless v2)\n\n"
        f"> **Dashboard mirrors critical findings from db-metrics.md. "
        f"Orange = WARN threshold, Red = CRIT threshold.**",
        x=0, y=y, width=24, height=3,
    ))
    y += 3

    # ── Section 1: ACU Scaling Ceiling ──────────────────────────────────────
    widgets.append(text_widget(
        "### 1 · ACU Scaling Ceiling\n"
        "Critical finding: cluster pinned at 64 ACU (max) for 90% of the 120-min window. "
        "Queries are queuing. **Raise max ACU or reduce concurrency.**",
        x=0, y=y, width=24, height=2,
    ))
    y += 2

    widgets.append(gw(
        title="ACUUtilization (% of max ACU)",
        metrics=[
            metric(ns, "ACUUtilization", cluster_dim(cluster_id),
                   stat="Maximum", period=period, label="ACU Utilization (max)"),
        ],
        annotations=[
            alarm_annotation(75, "WARN 75%", WARN_COLOR),
            alarm_annotation(92, "CRIT 92%", CRIT_COLOR),
        ],
        left_label="Percent",
        x=0, y=y, width=12, height=6,
    ))

    widgets.append(gw(
        title="ServerlessDatabaseCapacity (allocated ACUs)",
        metrics=[
            metric(ns, "ServerlessDatabaseCapacity", cluster_dim(cluster_id),
                   stat="Average", period=period, label="Allocated ACU (avg)"),
            metric(ns, "ServerlessDatabaseCapacity", cluster_dim(cluster_id),
                   stat="Maximum", period=period, label="Allocated ACU (max)"),
        ],
        annotations=[
            alarm_annotation(60.8, "WARN 95% of 64 ACU", WARN_COLOR),
            alarm_annotation(64,   "CRIT Max 64 ACU",    CRIT_COLOR),
        ],
        left_label="ACUs",
        x=12, y=y, width=12, height=6,
    ))
    y += 6

    # ── Section 2: Storage I/O ───────────────────────────────────────────────
    widgets.append(text_widget(
        "### 2 · Storage I/O\n"
        "Critical finding: `VolumeReadIOPs` at **1,780,740 IOPS** (threshold 6,000). "
        "Indicates massive sequential scans or cold buffer pool. "
        "`VolumeWriteIOPs` at **4,754 IOPS** (threshold 4,000 CRIT).",
        x=0, y=y, width=24, height=2,
    ))
    y += 2

    widgets.append(gw(
        title="VolumeReadIOPs (cluster)",
        metrics=[
            metric(ns, "VolumeReadIOPs", cluster_dim(cluster_id),
                   stat="Average", period=period, label="Read IOPS"),
        ],
        annotations=[
            alarm_annotation(3000, "WARN 3,000 IOPS", WARN_COLOR),
            alarm_annotation(6000, "CRIT 6,000 IOPS", CRIT_COLOR),
        ],
        left_label="IOPS",
        x=0, y=y, width=12, height=6,
    ))

    widgets.append(gw(
        title="VolumeWriteIOPs (cluster)",
        metrics=[
            metric(ns, "VolumeWriteIOPs", cluster_dim(cluster_id),
                   stat="Average", period=period, label="Write IOPS"),
        ],
        annotations=[
            alarm_annotation(2000, "WARN 2,000 IOPS", WARN_COLOR),
            alarm_annotation(4000, "CRIT 4,000 IOPS", CRIT_COLOR),
        ],
        left_label="IOPS",
        x=12, y=y, width=12, height=6,
    ))
    y += 6

    # ── Section 3: Query Latency ─────────────────────────────────────────────
    widgets.append(text_widget(
        "### 3 · Query Latency\n"
        "Critical finding: `CommitLatency` on writer at **600 ms** (CRIT threshold 20 ms). "
        "Indicates severe WAL I/O pressure or lock contention. "
        "Reader `CommitLatency` at 12 ms (WARN).",
        x=0, y=y, width=24, height=2,
    ))
    y += 2

    widgets.append(gw(
        title="CommitLatency — Writer vs Reader (ms)",
        metrics=[
            metric(ns, "CommitLatency", instance_dim(writer),
                   stat="Average", period=period, label=f"CommitLatency — {writer}"),
            metric(ns, "CommitLatency", instance_dim(reader),
                   stat="Average", period=period, label=f"CommitLatency — {reader}"),
        ],
        annotations=[
            alarm_annotation(5,  "WARN 5 ms",  WARN_COLOR),
            alarm_annotation(20, "CRIT 20 ms", CRIT_COLOR),
        ],
        left_label="Milliseconds",
        x=0, y=y, width=12, height=6,
    ))

    widgets.append(gw(
        title="SelectLatency / DMLLatency / DDLLatency — Writer (ms)",
        metrics=[
            metric(ns, "SelectLatency", instance_dim(writer),
                   stat="Average", period=period, label="SelectLatency"),
            metric(ns, "DMLLatency",    instance_dim(writer),
                   stat="Average", period=period, label="DMLLatency"),
            metric(ns, "DDLLatency",    instance_dim(writer),
                   stat="Average", period=period, label="DDLLatency"),
        ],
        annotations=[
            alarm_annotation(20,  "SELECT WARN 20 ms",  WARN_COLOR),
            alarm_annotation(100, "SELECT CRIT 100 ms", CRIT_COLOR),
        ],
        left_label="Milliseconds",
        x=12, y=y, width=12, height=6,
    ))
    y += 6

    # ── Section 4: Compute & Memory ─────────────────────────────────────────
    widgets.append(text_widget(
        "### 4 · Compute & Memory",
        x=0, y=y, width=24, height=1,
    ))
    y += 1

    widgets.append(gw(
        title="CPUUtilization — Writer vs Reader (%)",
        metrics=[
            metric(ns, "CPUUtilization", instance_dim(writer),
                   stat="Maximum", period=period, label=f"CPU — {writer}"),
            metric(ns, "CPUUtilization", instance_dim(reader),
                   stat="Maximum", period=period, label=f"CPU — {reader}"),
        ],
        annotations=[
            alarm_annotation(70, "WARN 70%", WARN_COLOR),
            alarm_annotation(85, "CRIT 85%", CRIT_COLOR),
        ],
        left_label="Percent",
        x=0, y=y, width=12, height=6,
    ))

    widgets.append(gw(
        title="FreeableMemory — Writer vs Reader (GB)",
        metrics=[
            metric(ns, "FreeableMemory", instance_dim(writer),
                   stat="Minimum", period=period, label=f"Free RAM — {writer}"),
            metric(ns, "FreeableMemory", instance_dim(reader),
                   stat="Minimum", period=period, label=f"Free RAM — {reader}"),
        ],
        annotations=[
            alarm_annotation(512 * 1024 * 1024, "WARN 512 MB", WARN_COLOR),
            alarm_annotation(256 * 1024 * 1024, "CRIT 256 MB", CRIT_COLOR),
        ],
        left_label="Bytes",
        x=12, y=y, width=12, height=6,
    ))
    y += 6

    # ── Section 5: Connections ───────────────────────────────────────────────
    widgets.append(text_widget(
        "### 5 · Connections\n"
        "At low ACU (0.5) max_connections ≈ 189. "
        "Use RDS Proxy to decouple connection count from ACU scaling.",
        x=0, y=y, width=24, height=2,
    ))
    y += 2

    widgets.append(gw(
        title="DatabaseConnections — Writer vs Reader",
        metrics=[
            metric(ns, "DatabaseConnections", instance_dim(writer),
                   stat="Maximum", period=period, label=f"Connections — {writer}"),
            metric(ns, "DatabaseConnections", instance_dim(reader),
                   stat="Maximum", period=period, label=f"Connections — {reader}"),
        ],
        annotations=[
            alarm_annotation(200, "WARN 200", WARN_COLOR),
            alarm_annotation(400, "CRIT 400", CRIT_COLOR),
        ],
        left_label="Count",
        x=0, y=y, width=24, height=6,
    ))
    y += 6

    # ── Section 6: Cache Health ──────────────────────────────────────────────
    widgets.append(text_widget(
        "### 6 · Cache & I/O Health\n"
        "Low `BufferCacheHitRatio` means Aurora is reading from storage instead of "
        "shared_buffers — directly drives the high `VolumeReadIOPs` finding.",
        x=0, y=y, width=24, height=2,
    ))
    y += 2

    widgets.append(gw(
        title="BufferCacheHitRatio — Writer vs Reader (%)",
        metrics=[
            metric(ns, "BufferCacheHitRatio", instance_dim(writer),
                   stat="Minimum", period=period, label=f"Cache Hit — {writer}"),
            metric(ns, "BufferCacheHitRatio", instance_dim(reader),
                   stat="Minimum", period=period, label=f"Cache Hit — {reader}"),
        ],
        annotations=[
            alarm_annotation(90, "WARN < 90%", WARN_COLOR),
            alarm_annotation(80, "CRIT < 80%", CRIT_COLOR),
        ],
        left_label="Percent",
        x=0, y=y, width=12, height=6,
    ))

    widgets.append(gw(
        title="Read / Write Latency — Writer (ms)",
        metrics=[
            metric(ns, "ReadLatency",  instance_dim(writer),
                   stat="Average", period=period, label="ReadLatency"),
            metric(ns, "WriteLatency", instance_dim(writer),
                   stat="Average", period=period, label="WriteLatency"),
        ],
        annotations=[
            alarm_annotation(5,  "WARN 5 ms",  WARN_COLOR),
            alarm_annotation(20, "CRIT 20 ms", CRIT_COLOR),
        ],
        left_label="Milliseconds",
        x=12, y=y, width=12, height=6,
    ))
    y += 6

    # ── Section 7: PostgreSQL Reliability ───────────────────────────────────
    widgets.append(text_widget(
        "### 7 · PostgreSQL Reliability\n"
        "`MaximumUsedTransactionIDs` approaching 1 B triggers WARN (autovacuum emergency at ~1.6 B). "
        "`ReplicationSlotDiskUsage` bloat blocks WAL recycling and autovacuum.",
        x=0, y=y, width=24, height=2,
    ))
    y += 2

    widgets.append(gw(
        title="MaximumUsedTransactionIDs (cluster)",
        metrics=[
            metric(ns, "MaximumUsedTransactionIDs", cluster_dim(cluster_id),
                   stat="Maximum", period=period, label="Max Used Transaction IDs"),
        ],
        annotations=[
            alarm_annotation(1_000_000_000, "WARN 1 B",   WARN_COLOR),
            alarm_annotation(1_500_000_000, "CRIT 1.5 B", CRIT_COLOR),
        ],
        left_label="Transaction IDs",
        x=0, y=y, width=8, height=6,
    ))

    widgets.append(gw(
        title="Deadlocks (cluster)",
        metrics=[
            metric(ns, "Deadlocks", cluster_dim(cluster_id),
                   stat="Sum", period=period, label="Deadlocks (sum)"),
        ],
        annotations=[
            alarm_annotation(1, "WARN >= 1", WARN_COLOR),
            alarm_annotation(5, "CRIT >= 5", CRIT_COLOR),
        ],
        left_label="Count",
        x=8, y=y, width=8, height=6,
    ))

    widgets.append(gw(
        title="WAL / Replication Slot Disk Usage (cluster)",
        metrics=[
            metric(ns, "TransactionLogsDiskUsage", cluster_dim(cluster_id),
                   stat="Maximum", period=period, label="WAL Disk Usage"),
            metric(ns, "ReplicationSlotDiskUsage", cluster_dim(cluster_id),
                   stat="Maximum", period=period, label="Replication Slot Usage"),
        ],
        annotations=[
            alarm_annotation(1 * 1024 ** 3, "WAL WARN 1 GB",  WARN_COLOR),
            alarm_annotation(5 * 1024 ** 3, "WAL CRIT 5 GB",  CRIT_COLOR),
        ],
        left_label="Bytes",
        x=16, y=y, width=8, height=6,
    ))
    y += 6

    # ── Section 8: Network ───────────────────────────────────────────────────
    widgets.append(text_widget(
        "### 8 · Network Throughput",
        x=0, y=y, width=24, height=1,
    ))
    y += 1

    widgets.append(gw(
        title="Network Receive Throughput — Writer vs Reader (MB/s)",
        metrics=[
            metric(ns, "NetworkReceiveThroughput", instance_dim(writer),
                   stat="Average", period=period, label=f"RX — {writer}"),
            metric(ns, "NetworkReceiveThroughput", instance_dim(reader),
                   stat="Average", period=period, label=f"RX — {reader}"),
        ],
        annotations=[
            alarm_annotation(50  * 1024 ** 2, "WARN 50 MB/s",  WARN_COLOR),
            alarm_annotation(100 * 1024 ** 2, "CRIT 100 MB/s", CRIT_COLOR),
        ],
        left_label="Bytes/sec",
        x=0, y=y, width=12, height=6,
    ))

    widgets.append(gw(
        title="Network Transmit Throughput — Writer vs Reader (MB/s)",
        metrics=[
            metric(ns, "NetworkTransmitThroughput", instance_dim(writer),
                   stat="Average", period=period, label=f"TX — {writer}"),
            metric(ns, "NetworkTransmitThroughput", instance_dim(reader),
                   stat="Average", period=period, label=f"TX — {reader}"),
        ],
        annotations=[
            alarm_annotation(50  * 1024 ** 2, "WARN 50 MB/s",  WARN_COLOR),
            alarm_annotation(100 * 1024 ** 2, "CRIT 100 MB/s", CRIT_COLOR),
        ],
        left_label="Bytes/sec",
        x=12, y=y, width=12, height=6,
    ))
    y += 6

    return {"widgets": widgets}


# ---------------------------------------------------------------------------
# CloudWatch alarms (for top-of-dashboard alarm status widget)
# ---------------------------------------------------------------------------

ALARMS_TO_CREATE = [
    {
        "name_suffix":  "ACUUtilization-CRIT",
        "metric":       "ACUUtilization",
        "dim_key":      "DBClusterIdentifier",
        "threshold":    92.0,
        "comparison":   "GreaterThanOrEqualToThreshold",
        "stat":         "Maximum",
        "description":  "Aurora Serverless v2 ACU utilization at or above 92% of max (64 ACU ceiling).",
        "period":       300,
        "eval_periods": 3,
    },
    {
        "name_suffix":  "CommitLatency-CRIT",
        "metric":       "CommitLatency",
        "dim_key":      "DBInstanceIdentifier",
        "dim_val":      WRITER_INSTANCE,
        "threshold":    0.020,   # 20 ms (seconds)
        "comparison":   "GreaterThanOrEqualToThreshold",
        "stat":         "Average",
        "description":  "Writer CommitLatency >= 20 ms — WAL I/O pressure or lock contention.",
        "period":       300,
        "eval_periods": 3,
    },
    {
        "name_suffix":  "VolumeReadIOPs-CRIT",
        "metric":       "VolumeReadIOPs",
        "dim_key":      "DBClusterIdentifier",
        "threshold":    6000,
        "comparison":   "GreaterThanOrEqualToThreshold",
        "stat":         "Average",
        "description":  "Cluster VolumeReadIOPs >= 6,000 — sequential scans or cold buffer pool.",
        "period":       300,
        "eval_periods": 3,
    },
    {
        "name_suffix":  "VolumeWriteIOPs-CRIT",
        "metric":       "VolumeWriteIOPs",
        "dim_key":      "DBClusterIdentifier",
        "threshold":    4000,
        "comparison":   "GreaterThanOrEqualToThreshold",
        "stat":         "Average",
        "description":  "Cluster VolumeWriteIOPs >= 4,000 — high autovacuum or WAL volume.",
        "period":       300,
        "eval_periods": 3,
    },
    {
        "name_suffix":  "BufferCacheHitRatio-WARN",
        "metric":       "BufferCacheHitRatio",
        "dim_key":      "DBInstanceIdentifier",
        "dim_val":      WRITER_INSTANCE,
        "threshold":    90.0,
        "comparison":   "LessThanOrEqualToThreshold",
        "stat":         "Minimum",
        "description":  "Writer BufferCacheHitRatio <= 90% — shared_buffer misses driving IOPS.",
        "period":       300,
        "eval_periods": 3,
    },
    {
        "name_suffix":  "Deadlocks-WARN",
        "metric":       "Deadlocks",
        "dim_key":      "DBClusterIdentifier",
        "threshold":    1,
        "comparison":   "GreaterThanOrEqualToThreshold",
        "stat":         "Sum",
        "description":  "Cluster has >= 1 deadlock in last 5 min.",
        "period":       300,
        "eval_periods": 1,
    },
]


def ensure_alarms(cw, cluster_id: str, region: str, alarm_prefix: str) -> list:
    """Create/update CloudWatch alarms and return their ARNs."""
    arns = []
    for spec in ALARMS_TO_CREATE:
        alarm_name = f"{alarm_prefix}-{spec['name_suffix']}"
        dim_key    = spec["dim_key"]
        dim_val    = spec.get("dim_val", cluster_id)
        try:
            cw.put_metric_alarm(
                AlarmName=alarm_name,
                AlarmDescription=spec["description"],
                Namespace="AWS/RDS",
                MetricName=spec["metric"],
                Dimensions=[{"Name": dim_key, "Value": dim_val}],
                Period=spec["period"],
                EvaluationPeriods=spec["eval_periods"],
                Threshold=spec["threshold"],
                ComparisonOperator=spec["comparison"],
                Statistic=spec["stat"],
                TreatMissingData="notBreaching",
            )
            # Fetch ARN
            resp = cw.describe_alarms(AlarmNames=[alarm_name])
            alarms_list = resp.get("MetricAlarms", [])
            if alarms_list:
                arns.append(alarms_list[0]["AlarmArn"])
            print(f"  alarm: {alarm_name}  [OK]")
        except ClientError as exc:
            print(f"  alarm: {alarm_name}  [ERROR] {exc.response['Error']['Message']}")
    return arns


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Create CloudWatch dashboard for Aurora PG disc-fsa-prod-db-pg",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--cluster",   default=DEFAULT_CLUSTER,
                   help=f"Aurora cluster ID (default: {DEFAULT_CLUSTER})")
    p.add_argument("--region",    default=DEFAULT_REGION,
                   help=f"AWS region (default: {DEFAULT_REGION})")
    p.add_argument("--dashboard", default=DEFAULT_DASHBOARD,
                   help=f"Dashboard name (default: {DEFAULT_DASHBOARD})")
    p.add_argument("--period",    type=int, default=DEFAULT_PERIOD,
                   help=f"Metric period in seconds (default: {DEFAULT_PERIOD})")
    p.add_argument("--writer",    default=WRITER_INSTANCE,
                   help=f"Writer instance ID (default: {WRITER_INSTANCE})")
    p.add_argument("--reader",    default=READER_INSTANCE,
                   help=f"Reader instance ID (default: {READER_INSTANCE})")
    p.add_argument("--profile",   default=None,
                   help="AWS CLI profile (not needed in CloudShell)")
    p.add_argument("--delete",    action="store_true",
                   help="Delete the dashboard instead of creating it")
    p.add_argument("--no-alarms", action="store_true",
                   help="Skip creating CloudWatch alarms")
    return p.parse_args()


def main():
    args = parse_args()

    session_kwargs = {"region_name": args.region}
    if args.profile:
        session_kwargs["profile_name"] = args.profile
    try:
        session = boto3.Session(**session_kwargs)
        cw = session.client("cloudwatch")
    except NoCredentialsError:
        print("ERROR: No AWS credentials found.")
        sys.exit(1)

    # ── Delete ──────────────────────────────────────────────────────────────
    if args.delete:
        try:
            cw.delete_dashboards(DashboardNames=[args.dashboard])
            print(f"Deleted dashboard: {args.dashboard}")
        except ClientError as exc:
            print(f"ERROR deleting dashboard: {exc.response['Error']['Message']}")
        sys.exit(0)

    # ── Alarms (created first so alarm ARNs exist before dashboard) ─────────
    alarm_arns = []
    if not args.no_alarms:
        alarm_prefix = args.dashboard
        print(f"\nCreating/updating CloudWatch alarms (prefix: {alarm_prefix})...")
        alarm_arns = ensure_alarms(cw, args.cluster, args.region, alarm_prefix)

    # ── Dashboard body ───────────────────────────────────────────────────────
    print(f"\nBuilding dashboard: {args.dashboard}...")

    # Inject alarm status widget at the very top if we have alarms
    dash_body = build_dashboard(
        cluster_id=args.cluster,
        writer=args.writer,
        reader=args.reader,
        region=args.region,
        period=args.period,
    )

    if alarm_arns:
        alarm_status_widget = alarm_widget(
            title="🔴 Critical Alarm Status",
            alarm_arns=alarm_arns,
            x=0, y=0,
            width=24, height=3,
        )
        # Shift all existing widgets down by 3
        for w in dash_body["widgets"]:
            w["y"] += 3
        # Insert at top
        dash_body["widgets"].insert(0, alarm_status_widget)

    body_json = json.dumps(dash_body)

    try:
        cw.put_dashboard(
            DashboardName=args.dashboard,
            DashboardBody=body_json,
        )
    except ClientError as exc:
        print(f"ERROR creating dashboard: {exc.response['Error']['Message']}")
        sys.exit(1)

    console_url = (
        f"https://{args.region}.console.aws.amazon.com/cloudwatch/home"
        f"?region={args.region}#dashboards:name={args.dashboard}"
    )

    print(f"\n{'='*64}")
    print(f"  Dashboard created: {args.dashboard}")
    print(f"  Alarms created:    {len(alarm_arns)}")
    print(f"\n  Open in CloudWatch console:")
    print(f"  {console_url}")
    print(f"{'='*64}\n")
    print(f"  Tip: set the time range to 'Last 3 hours' and auto-refresh")
    print(f"  to 1 minute for live scaling event visibility.\n")


if __name__ == "__main__":
    main()
