#!/usr/bin/env python3
"""
create_cw_dashboard_numbers.py

Creates (or updates) a CloudWatch "numbers" dashboard for Aurora PostgreSQL
Serverless v2 cluster disc-fsa-prod-db-pg.

All metric widgets use "view": "singleValue" — displays the current value as a
large number readout (closest to a gauge in CloudWatch), with an optional
sparkline trend below each number.

Sections (one singleValue widget per metric group):
  Row 1 — Scaling         : ACUUtilization, ServerlessDatabaseCapacity
  Row 2 — Storage I/O     : VolumeReadIOPs, VolumeWriteIOPs
  Row 3 — Query Latency   : CommitLatency (writer), CommitLatency (reader),
                             SelectLatency, DMLLatency
  Row 4 — Compute         : CPUUtilization (writer), CPUUtilization (reader),
                             FreeableMemory (writer), FreeableMemory (reader)
  Row 5 — Connections     : DatabaseConnections (writer + reader)
  Row 6 — Cache & I/O     : BufferCacheHitRatio (writer + reader),
                             ReadLatency, WriteLatency
  Row 7 — Reliability     : MaximumUsedTransactionIDs, Deadlocks,
                             TransactionLogsDiskUsage, ReplicationSlotDiskUsage
  Row 8 — Network         : NetworkReceiveThroughput (writer + reader),
                             NetworkTransmitThroughput (writer + reader)

Usage (CloudShell — no profile needed):
    python3 create_cw_dashboard_numbers.py
    python3 create_cw_dashboard_numbers.py --dashboard MyGaugeBoard
    python3 create_cw_dashboard_numbers.py --no-sparkline
    python3 create_cw_dashboard_numbers.py --delete

Options:
    --cluster      Aurora cluster ID      (default: disc-fsa-prod-db-pg)
    --region       AWS region             (default: us-east-1)
    --dashboard    Dashboard name         (default: Aurora-PG-Numbers-disc-fsa-prod-db-pg)
    --period       Metric period seconds  (default: 300)
    --no-sparkline Disable sparkline bar under each number
    --profile      AWS CLI profile        (not needed in CloudShell)
    --delete       Delete the dashboard
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
# Defaults
# ---------------------------------------------------------------------------
DEFAULT_CLUSTER   = "disc-fsa-prod-db-pg"
DEFAULT_REGION    = "us-east-1"
DEFAULT_DASHBOARD = f"Aurora-PG-Numbers-{DEFAULT_CLUSTER}"
DEFAULT_PERIOD    = 300

WRITER_INSTANCE   = "disc-fsa-prod-db-pg-instance-1"
READER_INSTANCE   = "disc-fsa-prod-db-pg-instance-1-us-east-1a"

WARN_COLOR = "#ff7f0e"
CRIT_COLOR = "#d62728"

# ---------------------------------------------------------------------------
# Widget builders
# ---------------------------------------------------------------------------

def text_widget(markdown: str, x: int, y: int,
                width: int = 24, height: int = 2) -> dict:
    return {
        "type": "text",
        "x": x, "y": y,
        "width": width, "height": height,
        "properties": {"markdown": markdown},
    }


def number_widget(title: str, metrics: list, x: int, y: int,
                  width: int = 6, height: int = 3,
                  region: str = DEFAULT_REGION,
                  period: int = DEFAULT_PERIOD,
                  sparkline: bool = True,
                  annotations: list = None) -> dict:
    """
    Single-value (gauge-style) widget. Each entry in `metrics` is a list:
    [namespace, metric_name, dim_key, dim_val, {options}]
    """
    props = {
        "view": "singleValue",
        "title": title,
        "metrics": metrics,
        "period": period,
        "region": region,
        "sparkline": sparkline,
        "setPeriodToTimeRange": True,
        "singleValueFullPrecision": False,
        "stacked": False,
    }
    if annotations:
        props["annotations"] = {"horizontal": annotations}
    return {
        "type": "metric",
        "x": x, "y": y,
        "width": width, "height": height,
        "properties": props,
    }


def alarm_annotation(value: float, label: str, color: str) -> dict:
    return {"value": value, "label": label, "color": color}


# ---------------------------------------------------------------------------
# Metric array entry helpers
# ---------------------------------------------------------------------------

def cluster_metric(ns: str, name: str, cluster_id: str,
                   stat: str, period: int, label: str = None) -> list:
    opts = {"stat": stat, "period": period}
    if label:
        opts["label"] = label
    return [ns, name, "DBClusterIdentifier", cluster_id, opts]


def instance_metric(ns: str, name: str, instance_id: str,
                    stat: str, period: int, label: str = None) -> list:
    opts = {"stat": stat, "period": period}
    if label:
        opts["label"] = label
    return [ns, name, "DBInstanceIdentifier", instance_id, opts]


# ---------------------------------------------------------------------------
# Dashboard builder
# ---------------------------------------------------------------------------

def build_dashboard(cluster_id: str, writer: str, reader: str,
                    region: str, period: int, sparkline: bool) -> dict:
    ns = "AWS/RDS"
    widgets = []
    y = 0

    # ── Header ──────────────────────────────────────────────────────────────
    widgets.append(text_widget(
        f"## Aurora PostgreSQL Serverless v2 — Numbers View\n"
        f"`{cluster_id}` &nbsp;|&nbsp; Region: `{region}` &nbsp;|&nbsp; "
        f"Writer: `{writer}` &nbsp;|&nbsp; Reader: `{reader}`\n\n"
        f"> Each tile shows the **current value** of the metric (singleValue / gauge view). "
        f"Orange annotation = WARN threshold · Red = CRIT threshold.",
        x=0, y=y, width=24, height=3,
    ))
    y += 3

    # ── Row 1: Scaling ───────────────────────────────────────────────────────
    widgets.append(text_widget(
        "### Scaling & Capacity",
        x=0, y=y, width=24, height=1,
    ))
    y += 1

    # ACUUtilization — % of max ACU in use
    widgets.append(number_widget(
        title="ACU Utilization % (max)",
        metrics=[
            cluster_metric(ns, "ACUUtilization", cluster_id, "Maximum", period,
                           label="ACU Utilization"),
        ],
        annotations=[
            alarm_annotation(75, "WARN 75%", WARN_COLOR),
            alarm_annotation(92, "CRIT 92%", CRIT_COLOR),
        ],
        x=0, y=y, width=6, height=3,
        region=region, period=period, sparkline=sparkline,
    ))

    # ServerlessDatabaseCapacity — allocated ACUs (avg)
    widgets.append(number_widget(
        title="Allocated ACUs (avg)",
        metrics=[
            cluster_metric(ns, "ServerlessDatabaseCapacity", cluster_id, "Average", period,
                           label="Allocated ACUs"),
        ],
        annotations=[
            alarm_annotation(60.8, "WARN 95% of 64", WARN_COLOR),
            alarm_annotation(64,   "CRIT Max 64 ACU", CRIT_COLOR),
        ],
        x=6, y=y, width=6, height=3,
        region=region, period=period, sparkline=sparkline,
    ))

    # ServerlessDatabaseCapacity — max in window
    widgets.append(number_widget(
        title="Allocated ACUs (max)",
        metrics=[
            cluster_metric(ns, "ServerlessDatabaseCapacity", cluster_id, "Maximum", period,
                           label="ACU Max"),
        ],
        annotations=[
            alarm_annotation(64, "CRIT Ceiling", CRIT_COLOR),
        ],
        x=12, y=y, width=6, height=3,
        region=region, period=period, sparkline=sparkline,
    ))
    y += 3

    # ── Row 2: Storage I/O ───────────────────────────────────────────────────
    widgets.append(text_widget(
        "### Storage I/O",
        x=0, y=y, width=24, height=1,
    ))
    y += 1

    widgets.append(number_widget(
        title="VolumeReadIOPs (avg)",
        metrics=[
            cluster_metric(ns, "VolumeReadIOPs", cluster_id, "Average", period,
                           label="Read IOPS"),
        ],
        annotations=[
            alarm_annotation(3000, "WARN 3,000", WARN_COLOR),
            alarm_annotation(6000, "CRIT 6,000", CRIT_COLOR),
        ],
        x=0, y=y, width=6, height=3,
        region=region, period=period, sparkline=sparkline,
    ))

    widgets.append(number_widget(
        title="VolumeWriteIOPs (avg)",
        metrics=[
            cluster_metric(ns, "VolumeWriteIOPs", cluster_id, "Average", period,
                           label="Write IOPS"),
        ],
        annotations=[
            alarm_annotation(2000, "WARN 2,000", WARN_COLOR),
            alarm_annotation(4000, "CRIT 4,000", CRIT_COLOR),
        ],
        x=6, y=y, width=6, height=3,
        region=region, period=period, sparkline=sparkline,
    ))
    y += 3

    # ── Row 3: Query Latency ─────────────────────────────────────────────────
    widgets.append(text_widget(
        "### Query Latency",
        x=0, y=y, width=24, height=1,
    ))
    y += 1

    latency_pairs = [
        ("CommitLatency — Writer",  "CommitLatency",  writer,  0.005, 0.020, 0),
        ("CommitLatency — Reader",  "CommitLatency",  reader,  0.005, 0.020, 6),
        ("SelectLatency — Writer",  "SelectLatency",  writer,  0.020, 0.100, 12),
        ("DMLLatency — Writer",     "DMLLatency",     writer,  0.050, 0.200, 18),
    ]
    for title, mname, inst, warn_v, crit_v, xpos in latency_pairs:
        widgets.append(number_widget(
            title=title,
            metrics=[
                instance_metric(ns, mname, inst, "Average", period, label=mname),
            ],
            annotations=[
                alarm_annotation(warn_v, f"WARN {warn_v*1000:.0f} ms", WARN_COLOR),
                alarm_annotation(crit_v, f"CRIT {crit_v*1000:.0f} ms", CRIT_COLOR),
            ],
            x=xpos, y=y, width=6, height=3,
            region=region, period=period, sparkline=sparkline,
        ))
    y += 3

    # ── Row 4: Compute & Memory ──────────────────────────────────────────────
    widgets.append(text_widget(
        "### Compute & Memory",
        x=0, y=y, width=24, height=1,
    ))
    y += 1

    compute_items = [
        ("CPU % — Writer",       "CPUUtilization",  writer, "Maximum", 70,          85,          0),
        ("CPU % — Reader",       "CPUUtilization",  reader, "Maximum", 70,          85,          6),
        ("Free RAM — Writer",    "FreeableMemory",  writer, "Minimum", 512*1024**2, 256*1024**2, 12),
        ("Free RAM — Reader",    "FreeableMemory",  reader, "Minimum", 512*1024**2, 256*1024**2, 18),
    ]
    for title, mname, inst, stat, warn_v, crit_v, xpos in compute_items:
        widgets.append(number_widget(
            title=title,
            metrics=[
                instance_metric(ns, mname, inst, stat, period, label=mname),
            ],
            annotations=[
                alarm_annotation(warn_v, "WARN", WARN_COLOR),
                alarm_annotation(crit_v, "CRIT", CRIT_COLOR),
            ],
            x=xpos, y=y, width=6, height=3,
            region=region, period=period, sparkline=sparkline,
        ))
    y += 3

    # ── Row 5: Connections ───────────────────────────────────────────────────
    widgets.append(text_widget(
        "### Connections",
        x=0, y=y, width=24, height=1,
    ))
    y += 1

    for title, inst, xpos in [
        ("Connections — Writer", writer, 0),
        ("Connections — Reader", reader, 6),
    ]:
        widgets.append(number_widget(
            title=title,
            metrics=[
                instance_metric(ns, "DatabaseConnections", inst, "Maximum", period,
                                label="Connections"),
            ],
            annotations=[
                alarm_annotation(200, "WARN 200", WARN_COLOR),
                alarm_annotation(400, "CRIT 400", CRIT_COLOR),
            ],
            x=xpos, y=y, width=6, height=3,
            region=region, period=period, sparkline=sparkline,
        ))
    y += 3

    # ── Row 6: Cache & I/O ───────────────────────────────────────────────────
    widgets.append(text_widget(
        "### Cache & I/O",
        x=0, y=y, width=24, height=1,
    ))
    y += 1

    cache_items = [
        ("Cache Hit % — Writer", "BufferCacheHitRatio", writer, "Minimum", 90,    80,    0),
        ("Cache Hit % — Reader", "BufferCacheHitRatio", reader, "Minimum", 90,    80,    6),
        ("Read Latency — Writer","ReadLatency",          writer, "Average", 0.005, 0.020, 12),
        ("Write Latency — Writer","WriteLatency",        writer, "Average", 0.005, 0.020, 18),
    ]
    for title, mname, inst, stat, warn_v, crit_v, xpos in cache_items:
        widgets.append(number_widget(
            title=title,
            metrics=[
                instance_metric(ns, mname, inst, stat, period, label=mname),
            ],
            annotations=[
                alarm_annotation(warn_v, "WARN", WARN_COLOR),
                alarm_annotation(crit_v, "CRIT", CRIT_COLOR),
            ],
            x=xpos, y=y, width=6, height=3,
            region=region, period=period, sparkline=sparkline,
        ))
    y += 3

    # ── Row 7: PostgreSQL Reliability ────────────────────────────────────────
    widgets.append(text_widget(
        "### PostgreSQL Reliability",
        x=0, y=y, width=24, height=1,
    ))
    y += 1

    widgets.append(number_widget(
        title="Max Transaction IDs Used",
        metrics=[
            cluster_metric(ns, "MaximumUsedTransactionIDs", cluster_id, "Maximum", period,
                           label="Max TxID Used"),
        ],
        annotations=[
            alarm_annotation(1_000_000_000, "WARN 1 B",   WARN_COLOR),
            alarm_annotation(1_500_000_000, "CRIT 1.5 B", CRIT_COLOR),
        ],
        x=0, y=y, width=6, height=3,
        region=region, period=period, sparkline=sparkline,
    ))

    widgets.append(number_widget(
        title="Deadlocks (sum)",
        metrics=[
            cluster_metric(ns, "Deadlocks", cluster_id, "Sum", period,
                           label="Deadlocks"),
        ],
        annotations=[
            alarm_annotation(1, "WARN ≥ 1", WARN_COLOR),
            alarm_annotation(5, "CRIT ≥ 5", CRIT_COLOR),
        ],
        x=6, y=y, width=6, height=3,
        region=region, period=period, sparkline=sparkline,
    ))

    widgets.append(number_widget(
        title="WAL Disk Usage",
        metrics=[
            cluster_metric(ns, "TransactionLogsDiskUsage", cluster_id, "Maximum", period,
                           label="WAL Usage"),
        ],
        annotations=[
            alarm_annotation(1 * 1024 ** 3, "WARN 1 GB", WARN_COLOR),
            alarm_annotation(5 * 1024 ** 3, "CRIT 5 GB", CRIT_COLOR),
        ],
        x=12, y=y, width=6, height=3,
        region=region, period=period, sparkline=sparkline,
    ))

    widgets.append(number_widget(
        title="Replication Slot Disk Usage",
        metrics=[
            cluster_metric(ns, "ReplicationSlotDiskUsage", cluster_id, "Maximum", period,
                           label="Slot Usage"),
        ],
        annotations=[
            alarm_annotation(512 * 1024 * 1024, "WARN 512 MB", WARN_COLOR),
            alarm_annotation(2 * 1024 ** 3,     "CRIT 2 GB",   CRIT_COLOR),
        ],
        x=18, y=y, width=6, height=3,
        region=region, period=period, sparkline=sparkline,
    ))
    y += 3

    # ── Row 8: Network ───────────────────────────────────────────────────────
    widgets.append(text_widget(
        "### Network Throughput",
        x=0, y=y, width=24, height=1,
    ))
    y += 1

    net_items = [
        ("Network RX — Writer", "NetworkReceiveThroughput",  writer, 0),
        ("Network RX — Reader", "NetworkReceiveThroughput",  reader, 6),
        ("Network TX — Writer", "NetworkTransmitThroughput", writer, 12),
        ("Network TX — Reader", "NetworkTransmitThroughput", reader, 18),
    ]
    for title, mname, inst, xpos in net_items:
        widgets.append(number_widget(
            title=title,
            metrics=[
                instance_metric(ns, mname, inst, "Average", period, label=mname),
            ],
            annotations=[
                alarm_annotation(50  * 1024 ** 2, "WARN 50 MB/s",  WARN_COLOR),
                alarm_annotation(100 * 1024 ** 2, "CRIT 100 MB/s", CRIT_COLOR),
            ],
            x=xpos, y=y, width=6, height=3,
            region=region, period=period, sparkline=sparkline,
        ))
    y += 3

    return {"widgets": widgets}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Create CloudWatch numbers/gauge dashboard for disc-fsa-prod-db-pg",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--cluster",      default=DEFAULT_CLUSTER,
                   help=f"Aurora cluster ID (default: {DEFAULT_CLUSTER})")
    p.add_argument("--region",       default=DEFAULT_REGION,
                   help=f"AWS region (default: {DEFAULT_REGION})")
    p.add_argument("--dashboard",    default=DEFAULT_DASHBOARD,
                   help=f"Dashboard name (default: {DEFAULT_DASHBOARD})")
    p.add_argument("--period",       type=int, default=DEFAULT_PERIOD,
                   help=f"Metric period in seconds (default: {DEFAULT_PERIOD})")
    p.add_argument("--writer",       default=WRITER_INSTANCE,
                   help=f"Writer instance ID (default: {WRITER_INSTANCE})")
    p.add_argument("--reader",       default=READER_INSTANCE,
                   help=f"Reader instance ID (default: {READER_INSTANCE})")
    p.add_argument("--no-sparkline", action="store_true",
                   help="Disable the sparkline bar under each number tile")
    p.add_argument("--profile",      default=None,
                   help="AWS CLI profile (not needed in CloudShell)")
    p.add_argument("--delete",       action="store_true",
                   help="Delete the dashboard instead of creating it")
    return p.parse_args()


def main():
    args = parse_args()
    sparkline = not args.no_sparkline

    session_kwargs = {"region_name": args.region}
    if args.profile:
        session_kwargs["profile_name"] = args.profile
    try:
        session = boto3.Session(**session_kwargs)
        cw = session.client("cloudwatch")
    except NoCredentialsError:
        print("ERROR: No AWS credentials found.")
        sys.exit(1)

    if args.delete:
        try:
            cw.delete_dashboards(DashboardNames=[args.dashboard])
            print(f"Deleted dashboard: {args.dashboard}")
        except ClientError as exc:
            print(f"ERROR deleting: {exc.response['Error']['Message']}")
        sys.exit(0)

    print(f"Building numbers dashboard: {args.dashboard}...")

    dash_body = build_dashboard(
        cluster_id=args.cluster,
        writer=args.writer,
        reader=args.reader,
        region=args.region,
        period=args.period,
        sparkline=sparkline,
    )

    try:
        cw.put_dashboard(
            DashboardName=args.dashboard,
            DashboardBody=json.dumps(dash_body),
        )
    except ClientError as exc:
        print(f"ERROR creating dashboard: {exc.response['Error']['Message']}")
        sys.exit(1)

    console_url = (
        f"https://{args.region}.console.aws.amazon.com/cloudwatch/home"
        f"?region={args.region}#dashboards:name={args.dashboard}"
    )

    print(f"\n{'='*64}")
    print(f"  Dashboard : {args.dashboard}")
    print(f"  Widgets   : {len(dash_body['widgets'])} tiles")
    print(f"  Sparkline : {'enabled' if sparkline else 'disabled'}")
    print(f"\n  Open in CloudWatch console:")
    print(f"  {console_url}")
    print(f"{'='*64}\n")
    print(f"  Tip: set auto-refresh to 1 minute for a live status board.\n")


if __name__ == "__main__":
    main()
