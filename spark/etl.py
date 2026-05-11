# =============================================================================
# spark/etl.py — Phase 2 Spark Batch Analytics Job
#
# Reads from typed Kafka topics produced by the Phase 1 Tekton pipeline,
# computes analytics, and writes results to TimescaleDB.
#
# Analytics:
#   1. Hourly event rate by type and region
#   2. Severity trend over time (rolling avg severity by region)
#   3. Bed availability pressure by region over time
#   4. Vaccination trend by hour, region, vaccine_type, and dose_number
#
# Run via CronJob every 6 hours. Each run reprocesses all available Kafka data
# from the earliest offset, truncates the result tables, and rewrites fresh
# aggregated results so the database always reflects the full event history.
#
# Dependencies (included in Dockerfile):
#   pyspark==3.4.0, psycopg2-binary, kafka-python-ng
#   Spark package: org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0
#   JDBC driver:   org.postgresql:postgresql:42.6.0
# =============================================================================

import os
import sys
import logging
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, date_trunc, window, avg, min as spark_min,
    count, when, lit, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger("epidemic-etl")

# -----------------------------------------------------------------------------
# Configuration — read from environment variables set by the CronJob manifest
# -----------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka.ds551-2026-spring-7726b8.svc.cluster.local:9092")
DB_HOST         = os.getenv("DB_HOST", "timescaledb-service.ds551-2026-spring-7726b8.svc.cluster.local")
DB_PORT         = os.getenv("DB_PORT", "5432")
DB_NAME         = os.getenv("DB_NAME", "analytics")
DB_USER         = os.getenv("DB_USER", "postgres")
DB_PASSWORD     = os.getenv("DB_PASSWORD", "team05-epidemic")

JDBC_URL   = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
JDBC_PROPS = {
    "user":     DB_USER,
    "password": DB_PASSWORD,
    "driver":   "org.postgresql.Driver"
}

# Typed Kafka topics from Phase 1
SYMPTOM_TOPIC  = "ds551-s26.team05.symptom_reports"
CLINIC_TOPIC   = "ds551-s26.team05.clinic_visits"
HOSPITAL_TOPIC = "ds551-s26.team05.hospital_admissions"
VACCINATION_TOPIC = "ds551-s26.team05.vaccination_records"

# -----------------------------------------------------------------------------
# Schemas — expected JSON structure for each typed topic after Phase 1 enrichment
# -----------------------------------------------------------------------------

# Base fields present on all events after Phase 1 enrichment
BASE_SCHEMA = StructType([
    StructField("event_id",              StringType()),
    StructField("event_type",            StringType()),
    StructField("schema_version",        IntegerType()),
    StructField("patient_id",            StringType()),
    StructField("timestamp",             StringType()),
    StructField("region",                StringType()),
    StructField("team_id",               StringType()),
    StructField("processing_timestamp",  StringType()),
    StructField("event_source",          StringType()),
])

# symptom_report events — adds severity and available_beds
SYMPTOM_SCHEMA = StructType(BASE_SCHEMA.fields + [
    StructField("severity",       StringType()),
    StructField("available_beds", IntegerType()),
    StructField("age",            IntegerType()),
    StructField("duration_days",  IntegerType()),
])

# clinic_visit events — adds available_beds (no severity field)
CLINIC_SCHEMA = StructType(BASE_SCHEMA.fields + [
    StructField("available_beds", IntegerType()),
    StructField("visit_type",     StringType()),
    StructField("diagnosis_code", StringType()),
])

# hospital_admission events — adds severity and available_beds
HOSPITAL_SCHEMA = StructType(BASE_SCHEMA.fields + [
    StructField("severity",        StringType()),
    StructField("available_beds",  IntegerType()),
    StructField("oxygen_level",    DoubleType()),
    StructField("expected_los_days", IntegerType()),
])

# vaccination_record events from CO-2026-01
VACCINATION_SCHEMA = StructType(BASE_SCHEMA.fields + [
    StructField("record_id",       StringType()),
    StructField("vaccine_type",    StringType()),
    StructField("dose_number",     IntegerType()),
    StructField("administered_at", StringType()),
])

# -----------------------------------------------------------------------------
# Severity mapping — converts string severity labels to a numeric scale
# so we can compute averages. Used in Analytics 1 and 2.
#
# Scale: low=1, moderate=2, high=3, severe=3, critical=4
# -----------------------------------------------------------------------------
def severity_to_numeric(severity_col):
    return (
        when(severity_col == "low",      lit(1.0))
        .when(severity_col == "moderate", lit(2.0))
        .when(severity_col == "high",     lit(3.0))
        .when(severity_col == "severe",   lit(3.0))
        .when(severity_col == "critical", lit(4.0))
        .otherwise(lit(None))
    )

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def read_kafka_topic(spark, topic, schema):
    """
    Read all messages from a Kafka topic in batch mode (earliest to latest).
    Parses the JSON value field using the provided schema.
    Returns a DataFrame with typed columns.
    """
    raw = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets",   "latest")
        .load()
    )
    return (
        raw.select(
            from_json(col("value").cast("string"), schema).alias("data")
        )
        .select("data.*")
        .withColumn("timestamp", to_timestamp(col("timestamp")))
        .filter(col("timestamp").isNotNull())
        .filter(col("region").isNotNull())
        .filter(col("event_type").isNotNull())
    )


def empty_topic_df(spark, schema):
    """Return an empty normalized event DataFrame for optional topics."""
    return (
        spark.createDataFrame([], schema)
        .withColumn("timestamp", to_timestamp(col("timestamp")))
        .filter(col("timestamp").isNotNull())
    )


def read_optional_kafka_topic(spark, topic, schema):
    """
    Read an optional Kafka topic. If the topic is not yet present in an older
    deployment, return an empty DataFrame so the original Phase 2 analytics still
    run. The change-order topic is then picked up automatically once it exists.
    """
    try:
        return read_kafka_topic(spark, topic, schema)
    except Exception as e:
        log.warning(
            "Optional topic unavailable; using empty DataFrame "
            f"topic={topic} exception_type={type(e).__name__} exception_message={e}"
        )
        return empty_topic_df(spark, schema)


def truncate_tables(tables):
    """
    Truncate result tables before each run so the database reflects
    a fresh recomputation from all available Kafka data.
    """
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD
    )
    conn.autocommit = True
    cur = conn.cursor()
    for table in tables:
        cur.execute(f"TRUNCATE TABLE {table};")
        log.info(f"Truncated table: {table}")
    cur.close()
    conn.close()


def write_to_db(df, table_name):
    """Write a DataFrame to TimescaleDB via JDBC in append mode."""
    df.write \
        .format("jdbc") \
        .option("url",      JDBC_URL) \
        .option("dbtable",  table_name) \
        .option("user",     DB_USER) \
        .option("password", DB_PASSWORD) \
        .option("driver",   "org.postgresql.Driver") \
        .mode("append") \
        .save()
    log.info(f"Wrote results to {table_name}: {df.count()} rows")


# =============================================================================
# Analytics
# =============================================================================

def compute_hourly_event_rate(symptom_df, clinic_df, hospital_df, vaccination_df=None):
    """
    Analytics 1 — Hourly Event Rate by Type and Region

    Combines typed topics and counts events per hour, event type,
    and region. This is the foundation metric for Phase 3 alerting thresholds
    (e.g., "symptom reports in Boston exceed 50 in one hour") and provides
    the event rate trend feature for the outbreak prediction model.

    Output schema: hour, event_type, region, event_count
    """
    log.info("Computing Analytics 1: Hourly Event Rate by Type and Region")

    # Combine all events — only need the shared base fields
    base_cols = ["timestamp", "event_type", "region"]

    all_events = (
        symptom_df.select(base_cols)
        .union(clinic_df.select(base_cols))
        .union(hospital_df.select(base_cols))
    )
    if vaccination_df is not None:
        all_events = all_events.union(vaccination_df.select(base_cols))

    hourly_rate = (
        all_events
        .withColumn("hour", date_trunc("hour", col("timestamp")))
        .groupBy("hour", "event_type", "region")
        .agg(count("*").alias("event_count"))
        .filter(col("hour").isNotNull())
        .orderBy("hour", "region", "event_type")
    )

    return hourly_rate


def compute_severity_trend(symptom_df, hospital_df):
    """
    Analytics 2 — Severity Trend Over Time

    Computes a rolling average of numeric severity scores from symptom_report
    and hospital_admission events, grouped by region in 1-hour windows.
    Captures whether conditions are escalating, stable, or improving —
    the key leading indicator for the Phase 3 outbreak prediction model.

    Output schema: window_start, window_end, region, event_type, avg_severity, sample_count
    """
    log.info("Computing Analytics 2: Severity Trend Over Time")

    # Add numeric severity to both event types, then union
    symptom_with_severity = (
        symptom_df
        .filter(col("severity").isNotNull())
        .withColumn("severity_score", severity_to_numeric(col("severity")))
        .filter(col("severity_score").isNotNull())
        .select("timestamp", "region", "event_type", "severity_score")
    )

    hospital_with_severity = (
        hospital_df
        .filter(col("severity").isNotNull())
        .withColumn("severity_score", severity_to_numeric(col("severity")))
        .filter(col("severity_score").isNotNull())
        .select("timestamp", "region", "event_type", "severity_score")
    )

    severity_events = symptom_with_severity.union(hospital_with_severity)

    severity_trend = (
        severity_events
        .groupBy(
            window(col("timestamp"), "1 hour").alias("time_window"),
            col("region"),
            col("event_type")
        )
        .agg(
            avg("severity_score").alias("avg_severity"),
            count("*").alias("sample_count")
        )
        .select(
            col("time_window.start").alias("window_start"),
            col("time_window.end").alias("window_end"),
            col("region"),
            col("event_type"),
            col("avg_severity"),
            col("sample_count")
        )
        .filter(col("window_start").isNotNull())
        .orderBy("window_start", "region")
    )

    return severity_trend


def compute_bed_availability_pressure(hospital_df):
    """
    Analytics 3 — Bed Availability Pressure

    Aggregates the available_beds field from hospital_admission events per
    hour and region, tracking both average and minimum available beds.
    Declining bed availability is a concrete downstream consequence of a
    surge in serious cases and is a critical signal for Phase 3 alerting.
    It is independent of event rate and severity — a region can have high
    event volume while beds remain plentiful, making this a distinct metric.

    Output schema: hour, region, avg_available_beds, min_available_beds, sample_count
    """
    log.info("Computing Analytics 3: Bed Availability Pressure")

    bed_pressure = (
        hospital_df
        .filter(col("available_beds").isNotNull())
        .withColumn("hour", date_trunc("hour", col("timestamp")))
        .groupBy("hour", "region")
        .agg(
            avg("available_beds").alias("avg_available_beds"),
            spark_min("available_beds").alias("min_available_beds"),
            count("*").alias("sample_count")
        )
        .filter(col("hour").isNotNull())
        .orderBy("hour", "region")
    )

    return bed_pressure


def compute_vaccination_trend(vaccination_df):
    """
    Analytics 4 / Change Order CO-2026-01 — Vaccination Trend

    Counts vaccination_record events by hour, region, vaccine_type, and
    dose_number. This produces a queryable downstream output using the new
    vaccination-specific fields required by the change order.

    Output schema: hour, region, vaccine_type, dose_number, vaccination_count
    """
    log.info("Computing Analytics 4: Vaccination Trend")

    vaccination_trend = (
        vaccination_df
        .filter(col("vaccine_type").isNotNull())
        .filter(col("dose_number").isNotNull())
        .filter(col("dose_number") >= 1)
        .withColumn("hour", date_trunc("hour", col("timestamp")))
        .groupBy("hour", "region", "vaccine_type", "dose_number")
        .agg(count("*").alias("vaccination_count"))
        .filter(col("hour").isNotNull())
        .orderBy("hour", "region", "vaccine_type", "dose_number")
    )

    return vaccination_trend


# =============================================================================
# Main
# =============================================================================

def main():
    log.info("=== Epidemic Engine — Phase 2 Spark Analytics Job starting ===")

    # -------------------------------------------------------------------------
    # Spark session
    # Packages are passed at spark-submit time via --packages flag in Dockerfile
    # -------------------------------------------------------------------------
    spark = (
        SparkSession.builder
        .appName("epidemic-analytics-team05")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info(f"Spark version: {spark.version}")

    # -------------------------------------------------------------------------
    # Read typed topics from Kafka
    # -------------------------------------------------------------------------
    log.info("Reading Kafka topics...")
    try:
        symptom_df  = read_kafka_topic(spark, SYMPTOM_TOPIC,  SYMPTOM_SCHEMA)
        clinic_df   = read_kafka_topic(spark, CLINIC_TOPIC,   CLINIC_SCHEMA)
        hospital_df = read_kafka_topic(spark, HOSPITAL_TOPIC, HOSPITAL_SCHEMA)
        vaccination_df = read_optional_kafka_topic(spark, VACCINATION_TOPIC, VACCINATION_SCHEMA)
    except Exception as e:
        log.error(f"Failed to read Kafka topics: {e}")
        sys.exit(1)

    log.info(f"symptom_reports:    {symptom_df.count()} events")
    log.info(f"clinic_visits:      {clinic_df.count()} events")
    log.info(f"hospital_admissions:{hospital_df.count()} events")
    log.info(f"vaccination_records:{vaccination_df.count()} events")

    # -------------------------------------------------------------------------
    # Compute analytics
    # -------------------------------------------------------------------------
    hourly_rate      = compute_hourly_event_rate(symptom_df, clinic_df, hospital_df, vaccination_df)
    severity_trend   = compute_severity_trend(symptom_df, hospital_df)
    bed_pressure     = compute_bed_availability_pressure(hospital_df)
    vaccination_trend = compute_vaccination_trend(vaccination_df)

    # -------------------------------------------------------------------------
    # Truncate result tables before writing so results reflect full history
    # -------------------------------------------------------------------------
    log.info("Truncating result tables...")
    try:
        truncate_tables([
            "hourly_event_rate",
            "severity_trend",
            "bed_availability",
            "vaccination_trend"
        ])
    except Exception as e:
        log.error(f"Failed to truncate tables: {e}")
        sys.exit(1)

    # -------------------------------------------------------------------------
    # Write results to TimescaleDB
    # -------------------------------------------------------------------------
    log.info("Writing analytics results to TimescaleDB...")
    try:
        write_to_db(hourly_rate,    "hourly_event_rate")
        write_to_db(severity_trend, "severity_trend")
        write_to_db(bed_pressure,   "bed_availability")
        write_to_db(vaccination_trend, "vaccination_trend")
    except Exception as e:
        log.error(f"Failed to write to database: {e}")
        sys.exit(1)

    log.info("=== Analytics job completed successfully ===")
    spark.stop()


if __name__ == "__main__":
    main()
