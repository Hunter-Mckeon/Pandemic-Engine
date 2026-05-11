#!/usr/bin/env python3
"""
Rule-based outbreak prediction for Team 05 Phase 3.

The project uses a transparent scoring model rather than a trained black-box
model so the team can defend the feature in the Phase 3 interview. Live mode
reads Phase 2 analytics from TimescaleDB and writes outbreak_predictions.
Demo mode prints representative predictions without any database.

Examples:
  python ml/outbreak_prediction.py --demo
  python ml/outbreak_prediction.py --live
"""

import argparse
import os


DB_HOST = os.getenv("DB_HOST", "timescaledb-service")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "analytics")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS outbreak_predictions (
    predicted_at TIMESTAMPTZ DEFAULT now(),
    hour TIMESTAMPTZ NOT NULL,
    region TEXT NOT NULL,
    total_events INTEGER,
    avg_severity DOUBLE PRECISION,
    min_available_beds INTEGER,
    vaccination_count INTEGER DEFAULT 0,
    event_score DOUBLE PRECISION,
    severity_score DOUBLE PRECISION,
    bed_pressure_score DOUBLE PRECISION,
    vaccination_score DOUBLE PRECISION,
    outbreak_risk_score DOUBLE PRECISION,
    outbreak_risk_level TEXT
);
"""


PREDICTION_SQL = """
WITH event_features AS (
    SELECT
        hour,
        region,
        SUM(event_count)::int AS total_events
    FROM hourly_event_rate
    GROUP BY hour, region
),
severity_features AS (
    SELECT
        window_start AS hour,
        region,
        AVG(avg_severity)::float AS avg_severity
    FROM severity_trend
    GROUP BY window_start, region
),
bed_features AS (
    SELECT
        hour,
        region,
        MIN(min_available_beds)::int AS min_available_beds
    FROM bed_availability
    GROUP BY hour, region
),
vaccination_features AS (
    SELECT
        hour,
        region,
        SUM(vaccination_count)::int AS vaccination_count
    FROM vaccination_trend
    GROUP BY hour, region
),
joined_features AS (
    SELECT
        e.hour,
        e.region,
        e.total_events,
        COALESCE(s.avg_severity, 0) AS avg_severity,
        COALESCE(b.min_available_beds, 999) AS min_available_beds,
        COALESCE(v.vaccination_count, 0) AS vaccination_count
    FROM event_features e
    LEFT JOIN severity_features s
      ON e.hour = s.hour
     AND e.region = s.region
    LEFT JOIN bed_features b
      ON e.hour = b.hour
     AND e.region = b.region
    LEFT JOIN vaccination_features v
      ON e.hour = v.hour
     AND e.region = v.region
),
scored AS (
    SELECT
        hour,
        region,
        total_events,
        avg_severity,
        min_available_beds,
        vaccination_count,

        -- Event volume: saturates after 10 events in an hour.
        LEAST(total_events / 10.0, 1.0) AS event_score,

        -- Severity labels were converted to 1..4 by Spark.
        LEAST(avg_severity / 4.0, 1.0) AS severity_score,

        -- Fewer beds means higher pressure and higher outbreak risk.
        CASE
            WHEN min_available_beds <= 50 THEN 1.0
            WHEN min_available_beds <= 100 THEN 0.7
            WHEN min_available_beds <= 200 THEN 0.4
            ELSE 0.1
        END AS bed_pressure_score,

        -- Vaccination activity contextualizes risk. Low vaccination activity
        -- increases risk; high activity reduces this component toward zero.
        GREATEST(0.0, 1.0 - LEAST(vaccination_count / 20.0, 1.0)) AS vaccination_score
    FROM joined_features
),
predictions AS (
    SELECT
        hour,
        region,
        total_events,
        avg_severity,
        min_available_beds,
        vaccination_count,
        event_score,
        severity_score,
        bed_pressure_score,
        vaccination_score,
        (
            0.35 * event_score +
            0.35 * severity_score +
            0.20 * bed_pressure_score +
            0.10 * vaccination_score
        ) AS outbreak_risk_score
    FROM scored
)
INSERT INTO outbreak_predictions (
    hour,
    region,
    total_events,
    avg_severity,
    min_available_beds,
    vaccination_count,
    event_score,
    severity_score,
    bed_pressure_score,
    vaccination_score,
    outbreak_risk_score,
    outbreak_risk_level
)
SELECT
    hour,
    region,
    total_events,
    avg_severity,
    min_available_beds,
    vaccination_count,
    event_score,
    severity_score,
    bed_pressure_score,
    vaccination_score,
    outbreak_risk_score,
    CASE
        WHEN outbreak_risk_score >= 0.75 THEN 'critical'
        WHEN outbreak_risk_score >= 0.50 THEN 'high'
        WHEN outbreak_risk_score >= 0.30 THEN 'medium'
        ELSE 'low'
    END AS outbreak_risk_level
FROM predictions;
"""


SELECT_TOP_SQL = """
SELECT
    hour,
    region,
    total_events,
    ROUND(avg_severity::numeric, 2),
    min_available_beds,
    vaccination_count,
    ROUND(outbreak_risk_score::numeric, 2),
    outbreak_risk_level
FROM outbreak_predictions
ORDER BY outbreak_risk_score DESC
LIMIT 10;
"""


def connect_live():
    import psycopg2

    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def run_live():
    conn = connect_live()
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(CREATE_TABLE_SQL)
    cur.execute("TRUNCATE TABLE outbreak_predictions;")
    cur.execute(PREDICTION_SQL)
    cur.execute(SELECT_TOP_SQL)

    print("Top outbreak predictions:")
    for row in cur.fetchall():
        print(row)

    cur.close()
    conn.close()
    print("Outbreak prediction completed")


def run_demo():
    print("Top outbreak predictions (demo data):")
    rows = [
        ("2026-04-28 22:00:00+00", "Boston", 18, 3.2, 42, 3, 0.78, "critical"),
        ("2026-04-28 22:00:00+00", "Cambridge", 11, 2.7, 88, 9, 0.58, "high"),
        ("2026-04-28 22:00:00+00", "Worcester", 7, 2.0, 145, 16, 0.36, "medium"),
        ("2026-04-28 22:00:00+00", "Springfield", 3, 1.6, 230, 24, 0.18, "low"),
    ]
    print("hour | region | total_events | avg_severity | min_beds | vaccination_count | risk_score | risk_level")
    for row in rows:
        print(" | ".join(str(value) for value in row))
    print("Scoring uses event volume, severity, bed pressure, and vaccination activity.")


def main():
    parser = argparse.ArgumentParser(description="Team 05 rule-based outbreak prediction")
    parser.add_argument("--demo", action="store_true", help="Print predictions from sample data")
    parser.add_argument("--live", action="store_true", help="Read/write TimescaleDB using DB_* environment variables")
    args = parser.parse_args()

    if args.live:
        run_live()
    else:
        run_demo()


if __name__ == "__main__":
    main()
