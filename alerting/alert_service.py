#!/usr/bin/env python3
"""
Advanced alerting service for Team 05 Phase 3.

Live mode polls TimescaleDB analytics tables. Demo mode prints representative
alerts without requiring Kafka, OpenShift, or database credentials.

Examples:
  python alerting/alert_service.py --demo --once
  python alerting/alert_service.py --once
"""

import argparse
import json
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone, timedelta


DB_HOST = os.getenv("DB_HOST", "timescaledb-service")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "analytics")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

POLL_SECONDS = int(os.getenv("POLL_SECONDS", "60"))
COOLDOWN_SECONDS = int(os.getenv("COOLDOWN_SECONDS", "300"))
STALE_HOURS = int(os.getenv("STALE_HOURS", "8"))

EVENT_WARNING_THRESHOLD = int(os.getenv("EVENT_WARNING_THRESHOLD", "5"))
EVENT_CRITICAL_THRESHOLD = int(os.getenv("EVENT_CRITICAL_THRESHOLD", "10"))

SEVERITY_WARNING_THRESHOLD = float(os.getenv("SEVERITY_WARNING_THRESHOLD", "2.5"))
SEVERITY_CRITICAL_THRESHOLD = float(os.getenv("SEVERITY_CRITICAL_THRESHOLD", "3.5"))

BED_WARNING_THRESHOLD = int(os.getenv("BED_WARNING_THRESHOLD", "100"))
BED_CRITICAL_THRESHOLD = int(os.getenv("BED_CRITICAL_THRESHOLD", "50"))

VACCINATION_LOW_THRESHOLD = int(os.getenv("VACCINATION_LOW_THRESHOLD", "1"))
VACCINATION_SURGE_THRESHOLD = int(os.getenv("VACCINATION_SURGE_THRESHOLD", "30"))
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")

last_alert_time = {}


def connect():
    import psycopg2

    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def should_send(alert_key):
    now = datetime.now(timezone.utc)
    last_seen = last_alert_time.get(alert_key)

    if last_seen is None or now - last_seen >= timedelta(seconds=COOLDOWN_SECONDS):
        last_alert_time[alert_key] = now
        return True

    return False


def emit_alert(level, metric, region, message):
    alert_key = f"{level}:{metric}:{region}"

    if should_send(alert_key):
        alert_text = (
            f"ALERT level={level} metric={metric} region={region} "
            f"time={datetime.now(timezone.utc).isoformat()} message='{message}'"
        )
        print(alert_text, flush=True)
        send_slack_notification(alert_text)
    else:
        print(
            f"SUPPRESSED level={level} metric={metric} region={region} "
            f"reason=cooldown",
            flush=True,
        )


def send_slack_notification(alert_text):
    """Optionally send alerts to Slack without making Slack a hard dependency."""
    if not SLACK_WEBHOOK_URL:
        return

    payload = json.dumps({"text": alert_text}).encode("utf-8")
    request = urllib.request.Request(
        SLACK_WEBHOOK_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            if response.status >= 400:
                print(f"WARN slack_webhook_status={response.status}", flush=True)
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        print(
            f"WARN slack_webhook_failed exception_type={type(e).__name__} message='{e}'",
            flush=True,
        )


def classify_event_count(event_count):
    if event_count >= EVENT_CRITICAL_THRESHOLD:
        return "critical"
    if event_count >= EVENT_WARNING_THRESHOLD:
        return "warning"
    return None


def classify_severity(avg_severity):
    if avg_severity >= SEVERITY_CRITICAL_THRESHOLD:
        return "critical"
    if avg_severity >= SEVERITY_WARNING_THRESHOLD:
        return "warning"
    return None


def classify_beds(min_beds):
    if min_beds <= BED_CRITICAL_THRESHOLD:
        return "critical"
    if min_beds <= BED_WARNING_THRESHOLD:
        return "warning"
    return None


def check_event_rate(cur):
    cur.execute(
        """
        SELECT hour, region, event_type, event_count
        FROM hourly_event_rate
        ORDER BY hour DESC, event_count DESC
        LIMIT 20;
        """
    )

    for hour, region, event_type, event_count in cur.fetchall():
        level = classify_event_count(event_count)
        if level:
            emit_alert(level, "event_rate", region, f"{event_type} had {event_count} events at {hour}")


def check_severity(cur):
    cur.execute(
        """
        SELECT window_start, region, event_type, avg_severity, sample_count
        FROM severity_trend
        ORDER BY window_start DESC, avg_severity DESC
        LIMIT 20;
        """
    )

    for window_start, region, event_type, avg_severity, sample_count in cur.fetchall():
        level = classify_severity(avg_severity)
        if level:
            emit_alert(
                level,
                "severity",
                region,
                f"{event_type} avg_severity={avg_severity:.2f} from {sample_count} samples at {window_start}",
            )


def check_bed_availability(cur):
    cur.execute(
        """
        SELECT hour, region, min_available_beds, avg_available_beds
        FROM bed_availability
        ORDER BY hour DESC, min_available_beds ASC
        LIMIT 20;
        """
    )

    for hour, region, min_beds, avg_beds in cur.fetchall():
        level = classify_beds(min_beds)
        if level:
            emit_alert(
                level,
                "bed_availability",
                region,
                f"min_available_beds={min_beds}, avg_available_beds={avg_beds:.2f} at {hour}",
            )


def check_vaccination_activity(cur):
    cur.execute(
        """
        WITH latest AS (
            SELECT MAX(hour) AS latest_hour FROM vaccination_trend
        )
        SELECT v.hour, v.region, v.vaccine_type, SUM(v.vaccination_count)::int AS doses
        FROM vaccination_trend v
        JOIN latest l ON v.hour = l.latest_hour
        GROUP BY v.hour, v.region, v.vaccine_type
        ORDER BY doses ASC
        LIMIT 20;
        """
    )

    for hour, region, vaccine_type, doses in cur.fetchall():
        if doses <= VACCINATION_LOW_THRESHOLD:
            emit_alert(
                "warning",
                "vaccination_low",
                region,
                f"Only {doses} {vaccine_type} vaccination records at {hour}",
            )
        elif doses >= VACCINATION_SURGE_THRESHOLD:
            emit_alert(
                "info",
                "vaccination_surge",
                region,
                f"{doses} {vaccine_type} vaccination records at {hour}",
            )


def check_pipeline_health(cur):
    cur.execute(
        """
        SELECT checked_at, check_name, table_name, status, message
        FROM data_quality_checks
        WHERE checked_at >= now() - interval '12 hours'
          AND status IN ('WARN', 'FAIL')
        ORDER BY checked_at DESC
        LIMIT 20;
        """
    )
    rows = cur.fetchall()
    for checked_at, check_name, table_name, status, message in rows:
        level = "critical" if status == "FAIL" else "warning"
        emit_alert(
            level,
            "data_quality",
            table_name or "pipeline",
            f"{check_name}={status} at {checked_at}: {message}",
        )

    cur.execute("SELECT MAX(hour) FROM hourly_event_rate;")
    latest = cur.fetchone()[0]
    if latest is None:
        emit_alert("critical", "pipeline_freshness", "all", "hourly_event_rate has no rows")
        return
    if latest.tzinfo is None:
        latest = latest.replace(tzinfo=timezone.utc)
    age_hours = (datetime.now(timezone.utc) - latest).total_seconds() / 3600
    if age_hours > STALE_HOURS:
        emit_alert(
            "critical",
            "pipeline_freshness",
            "all",
            f"hourly_event_rate latest row is {age_hours:.1f}h old; threshold is {STALE_HOURS}h",
        )


def run_once():
    conn = connect()
    cur = conn.cursor()

    check_event_rate(cur)
    check_severity(cur)
    check_bed_availability(cur)
    check_vaccination_activity(cur)
    check_pipeline_health(cur)

    cur.close()
    conn.close()


def run_demo():
    print("Starting Team 05 advanced alerting service in demo mode", flush=True)
    emit_alert("critical", "event_rate", "Boston", "hospital_admission had 14 events at demo-hour")
    emit_alert("warning", "severity", "Cambridge", "symptom_report avg_severity=2.80 from 18 samples at demo-hour")
    emit_alert("critical", "bed_availability", "Boston", "min_available_beds=42, avg_available_beds=63.20 at demo-hour")
    emit_alert("warning", "vaccination_low", "Springfield", "Only 0 influenza vaccination records at demo-hour")
    emit_alert("critical", "data_quality", "severity_trend", "freshness=FAIL: latest row is 13.0h old")


def main():
    parser = argparse.ArgumentParser(description="Team 05 advanced alerting service")
    parser.add_argument("--demo", action="store_true", help="Print sample alerts without connecting to the database")
    parser.add_argument("--once", action="store_true", help="Run one polling cycle and exit")
    args = parser.parse_args()

    if args.demo:
        run_demo()
        return

    print("Starting Team 05 advanced alerting service", flush=True)
    print(
        f"Config: poll={POLL_SECONDS}s cooldown={COOLDOWN_SECONDS}s "
        f"event_warning={EVENT_WARNING_THRESHOLD} event_critical={EVENT_CRITICAL_THRESHOLD} "
        f"severity_warning={SEVERITY_WARNING_THRESHOLD} severity_critical={SEVERITY_CRITICAL_THRESHOLD} "
        f"bed_warning={BED_WARNING_THRESHOLD} bed_critical={BED_CRITICAL_THRESHOLD} "
        f"vaccination_low={VACCINATION_LOW_THRESHOLD} vaccination_surge={VACCINATION_SURGE_THRESHOLD} "
        f"stale_hours={STALE_HOURS}",
        flush=True,
    )

    while True:
        try:
            run_once()
        except Exception as e:
            print(f"ALERT_SERVICE_ERROR {type(e).__name__}: {e}", flush=True)

        if args.once:
            break
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
