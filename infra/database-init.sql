-- TimescaleDB Schema for Epidemic Engine Phase 2
-- This file is a standalone copy of the init SQL embedded in the ConfigMap.
-- The ConfigMap mounts this into /docker-entrypoint-initdb.d/ on first boot.

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Analytics 1: Hourly event rate by type and region
CREATE TABLE IF NOT EXISTS hourly_event_rate (
    hour        TIMESTAMPTZ NOT NULL,
    event_type  TEXT        NOT NULL,
    region      TEXT        NOT NULL,
    event_count INTEGER     NOT NULL
);
SELECT create_hypertable('hourly_event_rate', 'hour', if_not_exists => TRUE);

-- Analytics 2: Severity trend over time
CREATE TABLE IF NOT EXISTS severity_trend (
    window_start TIMESTAMPTZ      NOT NULL,
    window_end   TIMESTAMPTZ      NOT NULL,
    region       TEXT              NOT NULL,
    event_type   TEXT              NOT NULL,
    avg_severity DOUBLE PRECISION  NOT NULL,
    sample_count BIGINT            NOT NULL DEFAULT 0
);
SELECT create_hypertable('severity_trend', 'window_start', if_not_exists => TRUE);

-- Analytics 3: Bed availability pressure
CREATE TABLE IF NOT EXISTS bed_availability (
    hour              TIMESTAMPTZ      NOT NULL,
    region            TEXT              NOT NULL,
    avg_available_beds DOUBLE PRECISION NOT NULL,
    min_available_beds INTEGER          NOT NULL,
    sample_count       BIGINT           NOT NULL DEFAULT 0
);
SELECT create_hypertable('bed_availability', 'hour', if_not_exists => TRUE);

-- Analytics 4 / CO-2026-01: Vaccination trend
CREATE TABLE IF NOT EXISTS vaccination_trend (
    hour              TIMESTAMPTZ NOT NULL,
    region            TEXT        NOT NULL,
    vaccine_type      TEXT        NOT NULL,
    dose_number       INTEGER     NOT NULL,
    vaccination_count INTEGER     NOT NULL
);
SELECT create_hypertable('vaccination_trend', 'hour', if_not_exists => TRUE);

-- Phase 3: Outbreak prediction output
CREATE TABLE IF NOT EXISTS outbreak_predictions (
    predicted_at          TIMESTAMPTZ DEFAULT now(),
    hour                  TIMESTAMPTZ NOT NULL,
    region                TEXT        NOT NULL,
    total_events          INTEGER,
    avg_severity          DOUBLE PRECISION,
    min_available_beds    INTEGER,
    vaccination_count     INTEGER DEFAULT 0,
    event_score           DOUBLE PRECISION,
    severity_score        DOUBLE PRECISION,
    bed_pressure_score    DOUBLE PRECISION,
    vaccination_score     DOUBLE PRECISION,
    outbreak_risk_score   DOUBLE PRECISION,
    outbreak_risk_level   TEXT
);
SELECT create_hypertable('outbreak_predictions', 'predicted_at', if_not_exists => TRUE);

-- Phase 2.5 / Phase 3 Category D: persistent data-quality monitoring
CREATE TABLE IF NOT EXISTS data_quality_checks (
    checked_at       TIMESTAMPTZ DEFAULT now(),
    check_name       TEXT        NOT NULL,
    table_name       TEXT,
    status           TEXT        NOT NULL,
    row_count        BIGINT,
    latest_timestamp TIMESTAMPTZ,
    message          TEXT
);
SELECT create_hypertable('data_quality_checks', 'checked_at', if_not_exists => TRUE);

-- Indexes for Phase 3 alerting and dashboard queries
CREATE INDEX IF NOT EXISTS idx_hourly_rate_region ON hourly_event_rate (region, hour DESC);
CREATE INDEX IF NOT EXISTS idx_severity_region    ON severity_trend (region, window_start DESC);
CREATE INDEX IF NOT EXISTS idx_bed_region         ON bed_availability (region, hour DESC);
CREATE INDEX IF NOT EXISTS idx_vaccination_region ON vaccination_trend (region, hour DESC);
CREATE INDEX IF NOT EXISTS idx_outbreak_region    ON outbreak_predictions (region, hour DESC);
CREATE INDEX IF NOT EXISTS idx_quality_status     ON data_quality_checks (status, checked_at DESC);
