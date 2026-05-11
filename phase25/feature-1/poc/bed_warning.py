#!/usr/bin/env python3
"""
phase25/feature-1/poc/bed_warning.py

Bed Availability Warning System — Proof of Concept

Reads the most recent rows from the TimescaleDB tables written by the Phase 2
Spark job and raises warnings when bed availability or hospital admission volume
crosses configurable thresholds.

Two modes:
  --demo   Run against synthetic data (no database required)
  --live   Connect to TimescaleDB using environment variables

Environment variables for --live mode:
  DB_HOST      (default: localhost)
  DB_PORT      (default: 5432)
  DB_NAME      (default: analytics)
  DB_USER      (default: postgres)
  DB_PASSWORD  (default: team05-epidemic)

Usage:
  python bed_warning.py --demo
  python bed_warning.py --live

Dependencies:
  pip install psycopg2-binary
"""

import os
import sys
import argparse
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Thresholds — these would come from a ConfigMap in production
# ---------------------------------------------------------------------------
MIN_BEDS_WARNING  = 10   # warn if min_available_beds drops below this
MIN_BEDS_CRITICAL = 5    # escalate to CRITICAL below this
ADMISSION_WARNING = 20   # warn if hospital admissions per hour exceed this
ADMISSION_CRITICAL = 40  # escalate to CRITICAL above this

# ---------------------------------------------------------------------------
# Demo data — mirrors the schema written by spark/etl.py
# ---------------------------------------------------------------------------
DEMO_BED_ROWS = [
    # (hour, region, avg_available_beds, min_available_beds, sample_count)
    (datetime(2026, 4, 28, 22, 0, tzinfo=timezone.utc), "Boston",      8.3,  3, 12),
    (datetime(2026, 4, 28, 22, 0, tzinfo=timezone.utc), "Cambridge",  22.1, 18,  9),
    (datetime(2026, 4, 28, 22, 0, tzinfo=timezone.utc), "Springfield",  4.0,  2,  7),
    (datetime(2026, 4, 28, 22, 0, tzinfo=timezone.utc), "Worcester",   15.5, 11,  5),
]

DEMO_RATE_ROWS = [
    # (hour, event_type, region, event_count)
    (datetime(2026, 4, 28, 22, 0, tzinfo=timezone.utc), "hospital_admission", "Boston",      45),
    (datetime(2026, 4, 28, 22, 0, tzinfo=timezone.utc), "hospital_admission", "Cambridge",   12),
    (datetime(2026, 4, 28, 22, 0, tzinfo=timezone.utc), "hospital_admission", "Springfield", 38),
    (datetime(2026, 4, 28, 22, 0, tzinfo=timezone.utc), "hospital_admission", "Worcester",   18),
]


# ---------------------------------------------------------------------------
# Database queries — read latest hour per region from Spark output tables
# ---------------------------------------------------------------------------

QUERY_BED = """
SELECT DISTINCT ON (region)
    hour, region, avg_available_beds, min_available_beds, sample_count
FROM bed_availability
ORDER BY region, hour DESC;
"""

QUERY_RATE = """
SELECT DISTINCT ON (region)
    hour, event_type, region, event_count
FROM hourly_event_rate
WHERE event_type = 'hospital_admission'
ORDER BY region, hour DESC;
"""


def fetch_live(query):
    """Connect to TimescaleDB and return rows for a query."""
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
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


# ---------------------------------------------------------------------------
# Warning logic
# ---------------------------------------------------------------------------

def severity_label(value, warning_threshold, critical_threshold, low_is_bad=True):
    """
    Return (severity_str, triggered) based on thresholds.
    low_is_bad=True  → lower values are worse (beds remaining)
    low_is_bad=False → higher values are worse (admission counts)
    """
    if low_is_bad:
        if value <= critical_threshold:
            return "CRITICAL", True
        if value <= warning_threshold:
            return "WARNING", True
        return "OK", False
    else:
        if value >= critical_threshold:
            return "CRITICAL", True
        if value >= warning_threshold:
            return "WARNING", True
        return "OK", False


def check_bed_availability(bed_rows):
    """
    Check each region's latest bed availability against thresholds.
    Returns list of warning dicts.
    """
    warnings = []
    for row in bed_rows:
        hour, region, avg_beds, min_beds, sample_count = row
        sev, triggered = severity_label(
            min_beds, MIN_BEDS_WARNING, MIN_BEDS_CRITICAL, low_is_bad=True
        )
        if triggered:
            warnings.append({
                "region":   region,
                "severity": sev,
                "metric":   "min_available_beds",
                "observed": min_beds,
                "threshold": MIN_BEDS_CRITICAL if sev == "CRITICAL" else MIN_BEDS_WARNING,
                "hour":     hour,
                "detail":   f"avg={avg_beds:.1f}, min={min_beds}, samples={sample_count}",
            })
    return warnings


def check_admission_rate(rate_rows):
    """
    Check each region's latest hospital admission count against thresholds.
    Returns list of warning dicts.
    """
    warnings = []
    for row in rate_rows:
        hour, event_type, region, event_count = row
        sev, triggered = severity_label(
            event_count, ADMISSION_WARNING, ADMISSION_CRITICAL, low_is_bad=False
        )
        if triggered:
            warnings.append({
                "region":    region,
                "severity":  sev,
                "metric":    "hospital_admission_count",
                "observed":  event_count,
                "threshold": ADMISSION_CRITICAL if sev == "CRITICAL" else ADMISSION_WARNING,
                "hour":      hour,
                "detail":    f"event_type=hospital_admission, count={event_count}",
            })
    return warnings


def print_report(bed_rows, rate_rows):
    """Run both checks and print a formatted warning report."""
    now = datetime.now(timezone.utc)
    print(f"\n{'='*60}")
    print(f"  BED AVAILABILITY WARNING REPORT")
    print(f"  Generated: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"{'='*60}\n")

    bed_warnings  = check_bed_availability(bed_rows)
    rate_warnings = check_admission_rate(rate_rows)
    all_warnings  = bed_warnings + rate_warnings

    if not all_warnings:
        print("  ✓  All regions within normal thresholds. No warnings raised.\n")
    else:
        criticals = [w for w in all_warnings if w["severity"] == "CRITICAL"]
        standard  = [w for w in all_warnings if w["severity"] == "WARNING"]

        for w in criticals + standard:
            icon = "🔴" if w["severity"] == "CRITICAL" else "🟡"
            print(f"  {icon}  [{w['severity']}] Region: {w['region']}")
            print(f"        Metric:    {w['metric']}")
            print(f"        Observed:  {w['observed']}  (threshold: {w['threshold']})")
            print(f"        Hour:      {w['hour'].strftime('%Y-%m-%d %H:%M UTC')}")
            print(f"        Detail:    {w['detail']}")
            print()

    print(f"  Regions checked (beds):      {len(bed_rows)}")
    print(f"  Regions checked (admissions):{len(rate_rows)}")
    print(f"  Warnings raised:             {len(all_warnings)}")
    print(f"  Criticals raised:            {len([w for w in all_warnings if w['severity'] == 'CRITICAL'])}")
    print(f"\n{'='*60}\n")

    # Exit non-zero if any CRITICAL so a scheduler can detect failure
    if any(w["severity"] == "CRITICAL" for w in all_warnings):
        sys.exit(2)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Bed Availability Warning System — Phase 2.5 POC"
    )
    parser.add_argument(
        "--demo", action="store_true",
        help="Run against synthetic demo data (no database required)"
    )
    parser.add_argument(
        "--live", action="store_true",
        help="Connect to TimescaleDB using DB_* environment variables"
    )
    args = parser.parse_args()

    if not args.demo and not args.live:
        print("Usage: python bed_warning.py --demo | --live")
        sys.exit(1)

    if args.demo:
        print("[demo mode] Using synthetic data — no database connection required.")
        bed_rows  = DEMO_BED_ROWS
        rate_rows = DEMO_RATE_ROWS
    else:
        print("[live mode] Connecting to TimescaleDB...")
        try:
            bed_rows  = fetch_live(QUERY_BED)
            rate_rows = fetch_live(QUERY_RATE)
        except Exception as e:
            print(f"ERROR: Could not connect to database: {e}")
            print("Tip: set DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD or use --demo")
            sys.exit(1)

    print_report(bed_rows, rate_rows)


if __name__ == "__main__":
    main()
