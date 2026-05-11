# Phase 3 Architecture

## Component Diagram

```text
infra namespace
  Event Generator
      |
      v
team namespace: ds551-2026-spring-7726b8
  Kafka broker/service
      |
      v
  Tekton Pipeline: epidemic-engine-phase1
      |
      +-- Task: validate-events
      +-- Task: route-and-enrich
      |
      v
  Kafka typed topics
      |
      v
  Kubernetes CronJob: epidemic-analytics
      |
      v
  TimescaleDB StatefulSet + Service + PVC
      |
      +-- Deployment: epidemic-alerting
      +-- Batch script: ml/outbreak_prediction.py
      +-- Batch script: quality/data_quality_monitor.py
      +-- Notebook: notebooks/phase3_visualizations.ipynb
```

## Phase Relationships

Phase 1 is the ingestion contract. It validates raw JSON, enriches trusted events,
and produces typed topics.

Phase 2 is the analytics layer. Spark reads typed topics and rewrites aggregate
tables in TimescaleDB.

Phase 2.5 is the trust layer. Data quality checks tell Phase 3 features whether
the analytics outputs are fresh, populated, and internally consistent.

Phase 3 is the product layer. Alerting, outbreak prediction, and visualization read
the database and turn analytics into operational signals.

## Technology Choices

| Layer | Technology | Reason |
|---|---|---|
| Streaming | Kafka | Course-provided event backbone and typed topic pattern. |
| Workflow | Tekton | Kubernetes-native batch pipeline for validation/routing. |
| Analytics | Spark / PySpark | Batch aggregations over multiple typed topics. |
| Storage | TimescaleDB | Time-series tables and SQL compatibility for pandas/alerts/ML. |
| Alerting | Python service | Simple polling fits the six-hour Spark cadence. |
| Prediction | Rule-based Python/SQL | Transparent scoring is easy to defend and inspect. |
| Visualization | Jupyter + pandas + matplotlib | Mandatory, lightweight, and reproducible. |
| Data quality | Python monitor + `data_quality_checks` table | Directly addresses Phase 2.5 stale/empty-data risk. |

## CO-2026-01 Handling

`vaccination_record` is handled as a new typed path:

1. `tekton/tasks.yaml` validates base fields for every event type and applies
   `schema_version = 2` only to `vaccination_record`.
2. `route-and-enrich` sends records to `ds551-s26.team05.vaccination_records`.
3. `spark/etl.py` reads the topic and writes `vaccination_trend`.
4. `alerting/alert_service.py`, `ml/outbreak_prediction.py`, and the notebook
   use vaccination activity downstream.

This is intentionally localized: old event types keep the same validation,
routing, and analytics behavior.

## Reliability Controls

- Tekton consumers use named consumer groups and manual commits.
- Spark CronJob uses `concurrencyPolicy: Forbid` and `backoffLimit: 1`.
- `quality/data_quality_monitor.py` checks freshness, row counts, nulls, domain
  ranges, and vaccination table readiness.
- Alerting reads `data_quality_checks` and emits warnings/critical alerts when
  the pipeline is stale or unhealthy.
- Demo modes exist for alerting, ML, data quality, and visualization so logic can
  be tested locally without credentials.

## Limitations

- Repo code alone cannot produce live cluster evidence. Collected OpenShift
  evidence is documented in `docs/phase3-runbook.md` and
  `docs/PHASE3-DATAFLOW.md`.
- Spark still uses a truncate-and-rewrite strategy; if the job fails after
  truncation and before all writes complete, tables may be empty or partial.
- Alert notifications are structured Kubernetes logs by default. Slack is
  optional through the `SLACK_WEBHOOK_URL` environment variable and is not
  required for demo mode.
- Runtime dependency installation in cluster manifests may require network access;
  the existing Dockerfile remains the more production-friendly path.
- `alerting/alert-configmap.yaml` duplicates the alerting script for deployment;
  if `alert_service.py` changes later, regenerate the ConfigMap.

## Failure Modes and Recovery

Kafka down:
- Symptoms: Tekton/Spark logs show connection errors or topic reads fail.
- Recovery: verify broker pod/service, then rerun Tekton and Spark.

Tekton validation/routing failure:
- Symptoms: PipelineRun or TaskRun failed; typed topics stop receiving data.
- Recovery: inspect TaskRun logs, fix manifest/code, rerun PipelineRun.

Spark failure:
- Symptoms: failed Job pod, stale data quality checks, no new DB rows.
- Recovery: inspect Spark logs, fix DB/Kafka/resource issue, trigger manual job
  from the CronJob.

Database down:
- Symptoms: Spark/alerting/ML connection errors.
- Recovery: inspect TimescaleDB pod/PVC, restore pod, rerun Spark and quality
  monitor.

Stale or empty analytics:
- Symptoms: `data_quality_checks` has `WARN` or `FAIL`; alerting emits
  `data_quality` or `pipeline_freshness` alerts.
- Recovery: run Phase 1 if typed topics are empty, then rerun Spark and the data
  quality monitor.
