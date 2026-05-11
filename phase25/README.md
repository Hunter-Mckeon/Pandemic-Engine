# Phase 2.5 — Feature Proposals

**Team 05 | DS-551 Data Engineering at Scale — Spring 2026**

---

## Feature 1: Bed Availability Warning System

Our Spark job already computes `avg_available_beds` and `min_available_beds` per region per hour and writes them to the `bed_availability` hypertable in TimescaleDB (Analytics 3). The gap is that nothing acts on those numbers. Phase 3 dashboards can display them, but if a region drops below a safe bed threshold at 3 AM, no signal is raised. This feature closes that gap by reading the latest rows from `bed_availability` and `hourly_event_rate`, comparing them against configurable thresholds, and emitting warnings when a region is under pressure. We chose this over a general alerting service because the data is already there — the POC requires no new pipeline components, only logic that reads what Spark already writes.

## Feature 2: Major Pipeline Failure Detection System

Our Spark CronJob runs every 6 hours and writes to three TimescaleDB hypertables: `hourly_event_rate`, `severity_trend`, and `bed_availability`. Nothing currently checks whether those writes happened successfully. If the CronJob silently fails — due to a Kafka connectivity error, a JDBC timeout, or an OOM kill — Phase 3 alerting and ML features continue reading the last successfully written rows and produce results that look current but are not. This feature detects that condition by comparing the most recent timestamp in each table against the current time and flagging the pipeline as unhealthy when data is stale beyond a configurable window. We chose this over a more complex checkpoint system because the failure signal is already latent in the database — the latest `hour` in `hourly_event_rate` tells you exactly when Spark last ran successfully.

Phase 3 extends Feature 2 into `quality/data_quality_monitor.py`, which writes live check results to `data_quality_checks`.
