# Bed Availability Warning System

## What is currently broken or missing

Our Spark job computes bed availability pressure (Analytics 3) every 6 hours and writes
the results to the `bed_availability` hypertable in TimescaleDB. Each row contains
`avg_available_beds` and `min_available_beds` per region per hour. The problem is that
nothing reads those values and acts on them. If the `min_available_beds` for Boston
drops to 2 at 4 AM, the number sits in the database. Phase 3 dashboards might display
it in a notebook, but there is no explicit warning raised and no signal sent. The same
Spark job also writes `hourly_event_rate` broken down by region — so we can see both
that hospital admissions in a region are spiking and that bed availability is eroding,
but those two signals are never combined into a warning.

## What this feature does

The POC reads the most recent rows from the `bed_availability` and `hourly_event_rate`
tables in TimescaleDB, groups them by region, and raises a warning when either
`min_available_beds` falls below a configurable floor or the hospital admission event
count in the last hour exceeds a configurable ceiling. Warnings are printed to stdout
with a severity level (WARNING or CRITICAL), the region name, the triggering metric,
and the observed value vs. the threshold. The POC runs locally against TimescaleDB via
a direct psycopg2 connection or in demo mode using synthetic data, so it can be
demonstrated without a live cluster.

## How it connects to our existing pipeline

- **Reads from:** `bed_availability` hypertable (written by `compute_bed_availability_pressure`
  in `spark/etl.py`) and `hourly_event_rate` hypertable (written by
  `compute_hourly_event_rate` in `spark/etl.py`), both in TimescaleDB at
  `timescaledb-service.ds551-2026-spring-7726b8.svc.cluster.local:5432/analytics`
- **Writes to:** stdout (POC); a `bed_warnings` table in TimescaleDB (production)
- **Touches:** Spark analytics output layer and Phase 3 alerting logic
- **Uses fields:** `region`, `min_available_beds`, `avg_available_beds`, `event_count`,
  `event_type`, `hour` — all of which are already present in the Spark output tables

## What would be needed to make this production-ready

1. **Store warnings in TimescaleDB** — write each triggered warning to a `bed_warnings`
   hypertable so Phase 3 dashboards can query warning history over time
2. **Replace static thresholds with real capacity data** — the current threshold is a
   fixed integer; production would join against a `hospital_capacity` reference table
   keyed by region to compute a percentage-of-capacity metric
3. **Add alert delivery** — wire the warning output into the Phase 3 notification
   path: structured Kubernetes logs by default, with optional Slack webhook support
4. **Add time-windowed logic** — rather than checking only the latest hour, check
   whether the condition has persisted for N consecutive hours before escalating to
   CRITICAL, reducing false positives from single-hour spikes
5. **Containerize and deploy as a Kubernetes Deployment** — the script should run as a
   long-lived polling loop in a pod, re-checking on a configurable interval
