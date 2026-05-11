#!/usr/bin/env python3
"""
Phase 2.5 / Phase 3 data quality monitor for Team 05.

Runs in two modes:
  python quality/data_quality_monitor.py --demo
  python quality/data_quality_monitor.py --live

Live mode reads TimescaleDB connection settings from environment variables:
DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, STALE_HOURS.
It writes each check result to data_quality_checks when that table exists.
"""

import argparse
import os
import sys
from datetime import datetime, timezone, timedelta


STALE_HOURS = int(os.getenv("STALE_HOURS", "8"))

EXPECTED_TABLES = [
    {
        "name": "hourly_event_rate",
        "time_col": "hour",
        "key_cols": ["hour", "event_type", "region", "event_count"],
    },
    {
        "name": "severity_trend",
        "time_col": "window_start",
        "key_cols": ["window_start", "window_end", "region", "event_type", "avg_severity", "sample_count"],
    },
    {
        "name": "bed_availability",
        "time_col": "hour",
        "key_cols": ["hour", "region", "avg_available_beds", "min_available_beds", "sample_count"],
    },
    {
        "name": "vaccination_trend",
        "time_col": "hour",
        "key_cols": ["hour", "region", "vaccine_type", "dose_number", "vaccination_count"],
    },
]


def result(check_name, table_name, status, row_count=None, latest_timestamp=None, message=""):
    return {
        "checked_at": datetime.now(timezone.utc),
        "check_name": check_name,
        "table_name": table_name,
        "status": status,
        "row_count": row_count,
        "latest_timestamp": latest_timestamp,
        "message": message,
    }


def table_exists(cur, table_name):
    cur.execute(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = %s
        );
        """,
        (table_name,),
    )
    return bool(cur.fetchone()[0])


def column_exists(cur, table_name, column_name):
    cur.execute(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
              AND column_name = %s
        );
        """,
        (table_name, column_name),
    )
    return bool(cur.fetchone()[0])


def count_nulls(cur, table_name, columns):
    predicates = " OR ".join(f"{col} IS NULL" for col in columns)
    cur.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {predicates};")
    return int(cur.fetchone()[0])


def table_state_checks(cur, table_name, time_col, key_cols):
    checks = []
    if not table_exists(cur, table_name):
        return [
            result(
                "table_exists",
                table_name,
                "FAIL",
                message=f"Expected table {table_name} does not exist",
            )
        ]

    cur.execute(f"SELECT COUNT(*), MAX({time_col}) FROM {table_name};")
    row_count, latest_timestamp = cur.fetchone()
    row_count = int(row_count)

    if latest_timestamp is not None and latest_timestamp.tzinfo is None:
        latest_timestamp = latest_timestamp.replace(tzinfo=timezone.utc)

    checks.append(result("table_exists", table_name, "PASS", row_count, latest_timestamp, "Table exists"))

    if row_count == 0:
        checks.append(result("row_count_nonzero", table_name, "FAIL", row_count, latest_timestamp, "Table has no rows"))
        return checks
    checks.append(result("row_count_nonzero", table_name, "PASS", row_count, latest_timestamp, "Table has data"))

    age_hours = (datetime.now(timezone.utc) - latest_timestamp).total_seconds() / 3600
    if age_hours > STALE_HOURS:
        checks.append(
            result(
                "freshness",
                table_name,
                "FAIL",
                row_count,
                latest_timestamp,
                f"Latest data is {age_hours:.1f}h old; threshold is {STALE_HOURS}h",
            )
        )
    else:
        checks.append(
            result(
                "freshness",
                table_name,
                "PASS",
                row_count,
                latest_timestamp,
                f"Latest data is {age_hours:.1f}h old",
            )
        )

    missing_columns = [col for col in key_cols if not column_exists(cur, table_name, col)]
    if missing_columns:
        checks.append(
            result(
                "required_columns",
                table_name,
                "FAIL",
                row_count,
                latest_timestamp,
                f"Missing columns: {', '.join(missing_columns)}",
            )
        )
        return checks
    checks.append(result("required_columns", table_name, "PASS", row_count, latest_timestamp, "Required columns present"))

    null_count = count_nulls(cur, table_name, key_cols)
    if null_count:
        checks.append(
            result(
                "null_key_values",
                table_name,
                "WARN",
                row_count,
                latest_timestamp,
                f"{null_count} rows have null values in key columns",
            )
        )
    else:
        checks.append(result("null_key_values", table_name, "PASS", row_count, latest_timestamp, "No null key values"))

    return checks


def domain_checks(cur):
    checks = []

    if table_exists(cur, "bed_availability"):
        cur.execute("SELECT COUNT(*) FROM bed_availability WHERE min_available_beds < 0 OR avg_available_beds < 0;")
        bad_beds = int(cur.fetchone()[0])
        checks.append(
            result(
                "bed_availability_nonnegative",
                "bed_availability",
                "FAIL" if bad_beds else "PASS",
                bad_beds,
                message=f"{bad_beds} rows have negative bed counts",
            )
        )

    if table_exists(cur, "severity_trend"):
        cur.execute("SELECT COUNT(*) FROM severity_trend WHERE avg_severity < 0 OR avg_severity > 4;")
        bad_severity = int(cur.fetchone()[0])
        checks.append(
            result(
                "severity_range",
                "severity_trend",
                "FAIL" if bad_severity else "PASS",
                bad_severity,
                message=f"{bad_severity} rows have avg_severity outside 0..4",
            )
        )

    if table_exists(cur, "vaccination_trend"):
        cur.execute("SELECT COUNT(*) FROM vaccination_trend WHERE dose_number < 1 OR vaccination_count < 0;")
        bad_vax = int(cur.fetchone()[0])
        checks.append(
            result(
                "vaccination_values",
                "vaccination_trend",
                "FAIL" if bad_vax else "PASS",
                bad_vax,
                message=f"{bad_vax} rows have invalid dose_number or vaccination_count",
            )
        )

    if table_exists(cur, "outbreak_predictions"):
        checks.append(result("outbreak_prediction_table", "outbreak_predictions", "PASS", message="Outbreak table exists"))
    else:
        checks.append(result("outbreak_prediction_table", "outbreak_predictions", "WARN", message="ML has not created outbreak_predictions yet"))

    return checks


def connect_live():
    try:
        import psycopg2
    except ImportError:
        print("ERROR: psycopg2 is not installed. Install psycopg2-binary for live mode.")
        sys.exit(1)

    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "analytics"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", ""),
    )


def persist_results(cur, checks):
    if not table_exists(cur, "data_quality_checks"):
        print("WARN: data_quality_checks table does not exist; results printed only.")
        return

    for check in checks:
        cur.execute(
            """
            INSERT INTO data_quality_checks (
                checked_at, check_name, table_name, status,
                row_count, latest_timestamp, message
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s);
            """,
            (
                check["checked_at"],
                check["check_name"],
                check["table_name"],
                check["status"],
                check["row_count"],
                check["latest_timestamp"],
                check["message"],
            ),
        )


def run_live():
    conn = connect_live()
    conn.autocommit = True
    cur = conn.cursor()
    checks = []

    for table in EXPECTED_TABLES:
        checks.extend(table_state_checks(cur, table["name"], table["time_col"], table["key_cols"]))
    checks.extend(domain_checks(cur))
    persist_results(cur, checks)

    cur.close()
    conn.close()
    return checks


def run_demo():
    now = datetime.now(timezone.utc)
    return [
        result("table_exists", "hourly_event_rate", "PASS", 128, now - timedelta(hours=2), "Table exists"),
        result("row_count_nonzero", "hourly_event_rate", "PASS", 128, now - timedelta(hours=2), "Table has data"),
        result("freshness", "hourly_event_rate", "PASS", 128, now - timedelta(hours=2), "Latest data is 2.0h old"),
        result("severity_range", "severity_trend", "PASS", 0, now - timedelta(hours=2), "0 rows outside 0..4"),
        result("bed_availability_nonnegative", "bed_availability", "PASS", 0, now - timedelta(hours=2), "0 rows have negative bed counts"),
        result("table_exists", "vaccination_trend", "PASS", 36, now - timedelta(hours=2), "Vaccination analytics table exists"),
        result("row_count_nonzero", "vaccination_trend", "PASS", 36, now - timedelta(hours=2), "Vaccination analytics has rows"),
        result("vaccination_values", "vaccination_trend", "PASS", 0, now - timedelta(hours=2), "0 invalid vaccination rows"),
        result("outbreak_prediction_table", "outbreak_predictions", "PASS", 24, now - timedelta(hours=2), "Outbreak table exists"),
        result("freshness", "bed_availability", "WARN", 42, now - timedelta(hours=7), "Latest data is close to 8h threshold"),
    ]


def print_summary(checks):
    counts = {"PASS": 0, "WARN": 0, "FAIL": 0}
    for check in checks:
        counts[check["status"]] = counts.get(check["status"], 0) + 1

    print("\nDATA QUALITY MONITOR SUMMARY")
    print(f"checked_at: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"total_checks: {len(checks)}")
    print(f"passed: {counts.get('PASS', 0)}")
    print(f"warned: {counts.get('WARN', 0)}")
    print(f"failed: {counts.get('FAIL', 0)}")
    print("\nSample check rows:")
    print("status | check_name | table_name | row_count | latest_timestamp | message")
    for check in checks[:12]:
        latest = check["latest_timestamp"].isoformat() if check["latest_timestamp"] else ""
        row_count = "" if check["row_count"] is None else str(check["row_count"])
        print(
            f"{check['status']} | {check['check_name']} | {check['table_name']} | "
            f"{row_count} | {latest} | {check['message']}"
        )

    if counts.get("FAIL", 0):
        return 2
    return 0


def main():
    parser = argparse.ArgumentParser(description="Team 05 Phase 3 data quality monitor")
    parser.add_argument("--demo", action="store_true", help="Run with built-in sample check results")
    parser.add_argument("--live", action="store_true", help="Connect to TimescaleDB using DB_* environment variables")
    args = parser.parse_args()

    if args.live:
        checks = run_live()
    else:
        checks = run_demo()

    exit_code = print_summary(checks)
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
