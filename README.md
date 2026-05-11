# Team 05 - Epidemic Engine

**DS-551 Data Engineering at Scale - Spring 2026**

## Project Overview

The Epidemic Engine is a cloud-native data engineering system that ingests,
validates, enriches, analyzes, monitors, and visualizes synthetic epidemiological
events.

The system uses:

- Kafka for event streaming
- Tekton for validation and routing
- Spark for batch analytics
- TimescaleDB for time-series storage
- Python services for data quality, alerting, and outbreak prediction
- Jupyter for visualization

All components run in the OpenShift namespace:

```text
ds551-2026-spring-7726b8
```

## Phase 3 Change Order: Vaccination Records

The pipeline was updated for CO-2026-01 to support the official upstream
`vaccination_record` event type.

Vaccination data path:

```text
ds551-s26.team05.raw
-> ds551-s26.team05.validated
-> ds551-s26.team05.vaccination_records
-> Spark analytics
-> TimescaleDB vaccination_trend
```

Implementation summary:

- `validate-events` accepts `vaccination_record` events only when the required
  schema-version-2 fields are present: `schema_version`, `record_id`,
  `patient_id`, `vaccine_type`, `dose_number`, and `administered_at`.
- `route-and-enrich` routes `vaccination_record` events into
  `ds551-s26.team05.vaccination_records`.
- Placeholder `vaccination` events are not official CO-2026-01 records. They are
  kept separate from `vaccination_record` and are dropped during routing because
  they lack the official vaccination fields.
- Spark reads `ds551-s26.team05.vaccination_records` and writes vaccination
  aggregates to `vaccination_trend`.
- The downstream analytic groups by `hour`, `region`, `vaccine_type`, and
  `dose_number`.

Final live verification:

```text
VALID forwarded event_type=vaccination_record to ds551-s26.team05.validated
ROUTED event_type=vaccination_record to ds551-s26.team05.vaccination_records
SUMMARY processed=7574 routed=5279 dropped=2295
ds551-s26.team05.vaccination_records:0:616
vaccination_trend | 129
```

Final database counts:

```text
hourly_event_rate    | 56
severity_trend       | 28
bed_availability     | 15
vaccination_trend    | 129
outbreak_predictions | 5
data_quality_checks  | 63
```

For detailed commands and verification steps, see `docs/phase3-runbook.md`.

## Repository Structure

```text
.
|-- alerting/
|   |-- alert_service.py
|   |-- alert-configmap.yaml
|   `-- alert-deployment.yaml
|-- docs/
|   |-- architecture.md
|   |-- genai-usage.md
|   |-- PHASE3-ARCHITECTURE.md
|   |-- PHASE3-DATAFLOW.md
|   |-- phase3-runbook.md
|   `-- visualization-guide.md
|-- infra/
|   |-- database-deployment.yaml
|   |-- database-init.sql
|   |-- spark-cronjob.yaml
|   |-- spark-rbac.yaml
|   `-- spark-test-job.yaml
|-- ml/
|   |-- Dockerfile
|   |-- ml-job.yaml
|   `-- outbreak_prediction.py
|-- notebooks/
|   `-- phase3_visualizations.ipynb
|-- phase25/
|   |-- feature-1/
|   |-- feature-2/
|   `-- README.md
|-- quality/
|   |-- data_quality_monitor.py
|   `-- data-quality-job.yaml
|-- spark/
|   |-- etl.py
|   `-- README.md
|-- tekton/
|   |-- pipeline.yaml
|   |-- pipelinerun.yaml
|   |-- pipelinerun-vax-real.yaml
|   |-- route-vax-real-logs-taskrun.yaml
|   |-- README.md
|   `-- tasks.yaml
|-- CHANGE-ORDER-CO-2026-01.md
|-- PHASE3-FEATURES-SCORECARD.md
`-- README.md
```

## Team Members

| Team Member | Primary Responsibility |
| --- | --- |
| Hunter | Architecture, Phase 2 Spark/TimescaleDB, Phase 3 integration and presentation |
| Josh | Spark analytics, database schema, Phase 3 deliverables |
| Khushbu | Tekton pipeline, Phase 3 integration, data quality/ML/alerting evidence, submission |
| Rohan | Tekton pipeline, routing/enrichment, Phase 3 integration and submission |

## System Data Flow

```text
Event Generator
-> Kafka raw topic: ds551-s26.team05.raw
-> Tekton validate-events
-> Kafka validated topic: ds551-s26.team05.validated
-> Tekton route-and-enrich
-> Typed Kafka topics
-> Spark analytics
-> TimescaleDB
-> Phase 3 features
```

Typed Kafka topics:

```text
ds551-s26.team05.symptom_reports
ds551-s26.team05.clinic_visits
ds551-s26.team05.hospital_admissions
ds551-s26.team05.vaccination_records
```

TimescaleDB tables:

```text
hourly_event_rate
severity_trend
bed_availability
vaccination_trend
outbreak_predictions
data_quality_checks
```

## Phase 1 - Tekton Pipeline

Main files:

```text
tekton/tasks.yaml
tekton/pipeline.yaml
tekton/pipelinerun.yaml
tekton/README.md
```

The pipeline validates raw events, forwards valid events to the validated topic,
enriches records with metadata, and routes them into typed Kafka topics.

For Phase 3 evidence, `tekton/tasks.yaml` intentionally uses the consumer groups
`team05-validate-events-phase3-vax` and `team05-route-enrich-phase3-vax` so stale
committed Kafka offsets do not hide current `vaccination_record` records.

`tekton/pipelinerun.yaml` intentionally uses `batch-size: "5000"` for Phase 3
evidence/backlog processing.

## Phase 2 - Spark Analytics + TimescaleDB

Main files:

```text
spark/etl.py
spark/README.md
infra/database-deployment.yaml
infra/database-init.sql
infra/spark-cronjob.yaml
```

Spark computes:

- hourly event rate
- severity trend
- bed availability pressure
- vaccination trend

Results are written to TimescaleDB.

## Phase 2.5 / Category D - Data Quality Monitoring

Main files:

```text
quality/data_quality_monitor.py
quality/data-quality-job.yaml
```

The data quality monitor checks table existence, row counts, freshness, required
columns, nulls, severity values, bed availability values, vaccination analytics,
and outbreak prediction readiness.

Results are written to:

```text
data_quality_checks
```

## Phase 3 Features

### Advanced Alerting

Main files:

```text
alerting/alert_service.py
alerting/alert-configmap.yaml
alerting/alert-deployment.yaml
```

The alerting service monitors event rate, severity, bed pressure, vaccination
activity, and data quality status.

### Outbreak Prediction

Main files:

```text
ml/outbreak_prediction.py
ml/ml-job.yaml
```

The outbreak prediction job writes risk scores to:

```text
outbreak_predictions
```

It uses event volume, severity, bed pressure, and vaccination activity.

### Visualization

Main file:

```text
notebooks/phase3_visualizations.ipynb
```

The notebook visualizes event rate, severity, bed availability, outbreak risk,
vaccination analytics, and data quality health.

## Evidence Manifests

The repo includes two manual Phase 3 evidence manifests:

- `tekton/pipelinerun-vax-real.yaml`: CO-2026-01 validation evidence run using
  the real raw topic.
- `tekton/route-vax-real-logs-taskrun.yaml`: CO-2026-01 route evidence run using
  the real validated topic. This can duplicate routed output if rerun, so use it
  intentionally for evidence capture only.

These are not temp-topic demos and are not the default everyday production flow.

## Running the System

Use the Phase 3 runbook for commands:

```text
docs/phase3-runbook.md
```

Common checks include:

```bash
oc get pods -n ds551-2026-spring-7726b8
oc get jobs -n ds551-2026-spring-7726b8
oc get svc -n ds551-2026-spring-7726b8
```

For database verification:

```bash
oc exec -it timescaledb-0 -n ds551-2026-spring-7726b8 -- \
  psql -U postgres -d analytics -c "\dt"
```

## Documentation

Key docs:

```text
docs/architecture.md
docs/PHASE3-DATAFLOW.md
docs/PHASE3-ARCHITECTURE.md
docs/phase3-runbook.md
docs/visualization-guide.md
docs/genai-usage.md
```
