# Major Pipeline Failure Detection System

## What is currently broken or missing

Our Spark CronJob runs every 6 hours and writes to three TimescaleDB hypertables:
`hourly_event_rate`, `severity_trend`, and `bed_availability`. Nothing currently checks
whether those writes happened. If the CronJob is OOM-killed, fails to connect to Kafka,
or crashes mid-write, the job exits with a non-zero code but no alert is raised and no
record of the failure is stored. The next time the Phase 3 alerting service polls
`hourly_event_rate`, or the Jupyter notebooks query `severity_trend`, they read the last
successfully written rows — which could be 12 or 24 hours old — and produce results that
look current. There is currently no way for Phase 3 features to know whether the data
they are reading is fresh. A dashboard showing a flat severity trend is indistinguishable
from a real flat trend and a dead pipeline.

## What this feature does

The POC queries the most recent timestamp in each of the three Spark output tables and
compares it against the current time. If any table's latest row is older than a
configurable staleness window (default: 8 hours, slightly longer than the 6-hour CronJob
cadence to allow for normal run time), the pipeline is marked unhealthy for that table.
The check also validates that each table has a non-zero row count — catching the case
where a table was accidentally truncated and not repopulated. The result is a structured
health report printed to stdout: each table is marked HEALTHY, STALE, or EMPTY, along
with the age of the most recent data and the row count. The script exits with a non-zero
code when any table is unhealthy so it can be used as a readiness gate in CI or a
Tekton post-step.

## How it connects to our existing pipeline

- **Reads from:** `hourly_event_rate`, `severity_trend`, and `bed_availability`
  hypertables in TimescaleDB at
  `timescaledb-service.ds551-2026-spring-7726b8.svc.cluster.local:5432/analytics` —
  the same tables written by `spark/etl.py` in the `write_to_db` calls
- **Writes to:** stdout (POC); a `pipeline_health` table in TimescaleDB (production)
- **Touches:** Spark analytics output layer, TimescaleDB schema, and Phase 3
  dashboard/alerting logic (which currently has no signal for data freshness)
- **Uses fields:** `hour` (from `hourly_event_rate` and `bed_availability`) and
  `window_start` (from `severity_trend`) — the time-partition columns already present
  in each hypertable

## What would be needed to make this production-ready

1. **Integrate into the Tekton pipeline as a post-step** — run the health check
   immediately after each Spark CronJob completes; fail the job if the check reports
   STALE or EMPTY so failures are surfaced in Tekton's task status
2. **Write health status to a TimescaleDB table** — store each check result with a
   timestamp so Phase 3 dashboards can display pipeline health history and alert on
   repeated failures
3. **Add automatic alerts on unhealthy status** — wire the non-zero exit code into
   the Phase 3 notification path: structured Kubernetes logs by default, with
   optional Slack webhook support
4. **Add per-table retry logic** — if a table is STALE, optionally trigger a Spark
   job re-run via the Kubernetes API before declaring the pipeline unhealthy
5. **Expose health status via an API endpoint** — a lightweight FastAPI endpoint
   serving the latest health check result so Phase 3 dashboards can show a live
   pipeline status indicator without querying TimescaleDB directly
