#!/usr/bin/env python3
"""
phase25/feature-2/poc/pipeline_health_check.py

Major Pipeline Failure Detection System — Proof of Concept

Checks whether the Spark analytics job has written fresh data to each of the
three TimescaleDB hypertables. Marks each table as HEALTHY, STALE, or EMPTY
based on the age of the most recent row and the total row count.

Exit codes:
  0 — all tables healthy
  1 — one or more tables STALE or EMPTY (pipeline unhealthy)

Two modes:
  --demo   Run against synthetic timestamps (no database required)
  --live   Connect to TimescaleDB using environment variables

Environment variables for --live mode:
  DB_HOST      (default: localhost)
  DB_PORT      (default: 5432)
  DB_NAME      (default: analytics)
  DB_USER      (default: postgres)
  DB_PASSWORD  (default: team05-epidemic)

Usage:
  python pipeline_health_check.py --demo
  python pipeline_health_check.py --live

Dependencies:
  pip install psycopg2-binary
"""

import os
import sys
import argparse
from datetime import datetime, timezone, timedelta

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Maximum age of the most recent row before a table is considered STALE.
# Set slightly above the 6-hour CronJob cadence to allow for normal run time.
STALE_THRESHOLD_HOURS = 8

# Each table and the column that holds the event time
TABLES = [
    {"name": "hourly_event_rate", "time_col": "hour"},
    {"name": "severity_trend",    "time_col": "window_start"},
    {"name": "bed_availability",  "time_col": "hour"},
]

# ---------------------------------------------------------------------------
# Demo data — simulates what TimescaleDB would return
# Adjust offsets to exercise different health states:
#   < STALE_THRESHOLD_HOURS  → HEALTHY
#   > STALE_THRESHOLD_HOURS  → STALE
#   row_count = 0            → EMPTY
# ---------------------------------------------------------------------------

def demo_table_states():
    now = datetime.now(timezone.utc)
    return {
        "hourly_event_rate": {
            "latest_time": now - timedelta(hours=4),   # fresh — HEALTHY
            "row_count":   3840,
        },
        "severity_trend": {
            "latest_time": now - timedelta(hours=13),  # stale — STALE
            "row_count":   2210,
        },
        "bed_availability": {
            "latest_time": None,                        # no rows — EMPTY
            "row_count":   0,
        },
    }

# ---------------------------------------------------------------------------
# Database queries
# ---------------------------------------------------------------------------

QUERY_TEMPLATE = """
SELECT
    MAX({time_col})     AS latest_time,
    COUNT(*)            AS row_count
FROM {table};
"""


def fetch_table_state(cur, table_name, time_col):
    """Return (latest_time, row_count) for a single table."""
    query = QUERY_TEMPLATE.format(table=table_name, time_col=time_col)
    cur.execute(query)
    row = cur.fetchone()
    latest_time, row_count = row
    return latest_time, int(row_count)


def fetch_all_live():
    """Connect to TimescaleDB and fetch state for all tables."""
    try:
        import psycopg2
    except ImportError:
        print("ERROR: psycopg2 not installed. Run: pip install psycopg2-binary")
        sys.exit(1)

    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "analytics"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "team05-epidemic"),
    )
    cur = conn.cursor()

    states = {}
    for tbl in TABLES:
        latest_time, row_count = fetch_table_state(cur, tbl["name"], tbl["time_col"])
        # psycopg2 returns timezone-naive datetimes for TIMESTAMPTZ; make aware
        if latest_time is not None and latest_time.tzinfo is None:
            latest_time = latest_time.replace(tzinfo=timezone.utc)
        states[tbl["name"]] = {"latest_time": latest_time, "row_count": row_count}

    cur.close()
    conn.close()
    return states

# ---------------------------------------------------------------------------
# Health evaluation
# ---------------------------------------------------------------------------

def evaluate_table(table_name, latest_time, row_count, now):
    """
    Determine health status for one table.
    Returns a result dict with status, age_hours, row_count, and a message.
    """
    if row_count == 0 or latest_time is None:
        return {
            "table":     table_name,
            "status":    "EMPTY",
            "age_hours": None,
            "row_count": row_count,
            "message":   "Table has no rows — Spark job may never have run or table was truncated",
        }

    age = now - latest_time
    age_hours = age.total_seconds() / 3600

    if age_hours > STALE_THRESHOLD_HOURS:
        return {
            "table":     table_name,
            "status":    "STALE",
            "age_hours": age_hours,
            "row_count": row_count,
            "message":   (
                f"Latest row is {age_hours:.1f}h old "
                f"(threshold: {STALE_THRESHOLD_HOURS}h) — "
                "Spark CronJob may have failed"
            ),
        }

    return {
        "table":     table_name,
        "status":    "HEALTHY",
        "age_hours": age_hours,
        "row_count": row_count,
        "message":   f"Latest row is {age_hours:.1f}h old — within threshold",
    }


def run_health_check(states):
    """Evaluate all tables and print a structured report. Returns overall healthy bool."""
    now = datetime.now(timezone.utc)
    results = []

    for tbl in TABLES:
        name = tbl["name"]
        state = states[name]
        result = evaluate_table(name, state["latest_time"], state["row_count"], now)
        results.append(result)

    # Print report
    print(f"\n{'='*62}")
    print(f"  PIPELINE HEALTH CHECK REPORT")
    print(f"  Generated:  {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"  Stale threshold: {STALE_THRESHOLD_HOURS} hours")
    print(f"{'='*62}\n")

    status_icons = {"HEALTHY": "✅", "STALE": "🟡", "EMPTY": "🔴"}

    for r in results:
        icon = status_icons.get(r["status"], "❓")
        age_str = f"{r['age_hours']:.1f}h" if r["age_hours"] is not None else "N/A"
        print(f"  {icon}  [{r['status']:7s}]  {r['table']}")
        print(f"           Age:       {age_str}")
        print(f"           Row count: {r['row_count']:,}")
        print(f"           {r['message']}")
        print()

    healthy_count   = sum(1 for r in results if r["status"] == "HEALTHY")
    unhealthy_count = len(results) - healthy_count
    overall         = "HEALTHY" if unhealthy_count == 0 else "UNHEALTHY"
    overall_icon    = "✅" if overall == "HEALTHY" else "🔴"

    print(f"{'='*62}")
    print(f"  {overall_icon}  OVERALL STATUS: {overall}")
    print(f"      Tables healthy:   {healthy_count}/{len(results)}")
    print(f"      Tables unhealthy: {unhealthy_count}/{len(results)}")
    print(f"{'='*62}\n")

    return overall == "HEALTHY"

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Pipeline Health Check — Phase 2.5 POC"
    )
    parser.add_argument(
        "--demo", action="store_true",
        help="Run against synthetic timestamps (no database required)"
    )
    parser.add_argument(
        "--live", action="store_true",
        help="Connect to TimescaleDB using DB_* environment variables"
    )
    args = parser.parse_args()

    if not args.demo and not args.live:
        print("Usage: python pipeline_health_check.py --demo | --live")
        sys.exit(1)

    if args.demo:
        print("[demo mode] Using synthetic timestamps — no database connection required.")
        states = demo_table_states()
    else:
        print("[live mode] Connecting to TimescaleDB...")
        try:
            states = fetch_all_live()
        except Exception as e:
            print(f"ERROR: Could not connect to database: {e}")
            print("Tip: set DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD or use --demo")
            sys.exit(1)

    healthy = run_health_check(states)
    sys.exit(0 if healthy else 1)


if __name__ == "__main__":
    main()
