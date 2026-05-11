# Epidemic Engine — System Architecture

**Team 05 | DS-551 Data Engineering at Scale — Spring 2026**

---

## System Overview

The Epidemic Engine is a cloud-native, event-driven data platform for ingesting, validating, enriching, analyzing, and acting on synthetic epidemiological events. The system is built across three phases:

1. **Phase 1:** Tekton-based ingestion, validation, enrichment, and Kafka topic routing
2. **Phase 2:** Spark batch analytics and TimescaleDB storage
3. **Phase 3:** Advanced features including alerting, outbreak prediction, visualization, and data quality monitoring

All components run on OpenShift in the team namespace:

```text
ds551-2026-spring-7726b8
````

Events originate from the course-managed event generator and flow through Kafka into our team pipeline.

---

## Full System Data Flow

```text
Event Generator
    |
    | synthetic epidemiological JSON events
    v
Kafka raw topic:
ds551-s26.team05.raw
    |
    | Phase 1 — Tekton Pipeline
    v
Tekton Task 1: validate-events
    - validates base fields: event_type, timestamp, region
    - validates ISO 8601 timestamp format
    - validates vaccination_record schema_version 2 fields
    - forwards valid events
    - logs and drops invalid events
    |
    v
Kafka validated topic:
ds551-s26.team05.validated
    |
    v
Tekton Task 2: route-and-enrich
    - adds team_id
    - adds processing_timestamp
    - adds event_source
    - routes by event_type
    |
    +---> ds551-s26.team05.symptom_reports
    +---> ds551-s26.team05.clinic_visits
    +---> ds551-s26.team05.hospital_admissions
    +---> ds551-s26.team05.vaccination_records
    |
    | Phase 2 — Spark Batch Analytics
    v
Spark analytics job / CronJob
    - reads typed Kafka topics
    - computes event rate, severity trend, bed availability, and vaccination trend
    - writes analytics to TimescaleDB
    |
    v
TimescaleDB:
timescaledb-0 / timescaledb-service
database: analytics
    |
    +---> hourly_event_rate
    +---> severity_trend
    +---> bed_availability
    +---> vaccination_trend
    +---> outbreak_predictions
    +---> data_quality_checks
    |
    | Phase 3 — Advanced Features
    v
Advanced Alerting Service
Outbreak Prediction Job
Jupyter Visualization Notebook
Data Quality Monitor
```

---

## Phase 1: Tekton Ingestion Pipeline

### Kafka Topic Design

The pipeline uses separate Kafka topics for raw events, validated events, and typed routed events.

| Topic                                  | Purpose                              |
| -------------------------------------- | ------------------------------------ |
| `ds551-s26.team05.raw`                 | Raw events from the event generator  |
| `ds551-s26.team05.validated`           | Events that passed validation        |
| `ds551-s26.team05.symptom_reports`     | Enriched `symptom_report` events     |
| `ds551-s26.team05.clinic_visits`       | Enriched `clinic_visit` events       |
| `ds551-s26.team05.hospital_admissions` | Enriched `hospital_admission` events |
| `ds551-s26.team05.vaccination_records` | Enriched `vaccination_record` events |

The intermediate `validated` topic separates validation from routing. This makes the system easier to debug and makes it possible to replay validated events without rereading the raw topic.

---

### Task 1 — `validate-events`

The `validate-events` task reads from:

```text
ds551-s26.team05.raw
```

It checks the required base fields:

```text
event_type
timestamp
region
```

It also checks that `timestamp` is parseable as an ISO 8601 timestamp.

For the CO-2026-01 change order event type `vaccination_record`, the validator applies additional schema checks:

```text
schema_version
record_id
patient_id
vaccine_type
dose_number
administered_at
```

A valid `vaccination_record` must have:

```text
schema_version = 2
dose_number >= 1
administered_at as a parseable timestamp
```

Valid events are forwarded to:

```text
ds551-s26.team05.validated
```

Invalid events are logged and discarded.

---

### Task 2 — `route-and-enrich`

The `route-and-enrich` task reads from:

```text
ds551-s26.team05.validated
```

It enriches every routed event with:

```text
team_id
processing_timestamp
event_source
```

Then it routes events by `event_type`.

| Event Type           | Output Topic                           |
| -------------------- | -------------------------------------- |
| `symptom_report`     | `ds551-s26.team05.symptom_reports`     |
| `clinic_visit`       | `ds551-s26.team05.clinic_visits`       |
| `hospital_admission` | `ds551-s26.team05.hospital_admissions` |
| `vaccination_record` | `ds551-s26.team05.vaccination_records` |

The official CO-2026-01 event type is `vaccination_record`. Placeholder
`vaccination` events are not treated as valid vaccination records because they do
not include `record_id`, `patient_id`, `vaccine_type`, `dose_number`, or
`administered_at`. They are logged/dropped during routing, the same way as
`general_health_report` and `emergency_incident`.

Final live routing evidence showed real `vaccination_record` events routed successfully:

```text
ROUTED event_type=vaccination_record to ds551-s26.team05.vaccination_records
SUMMARY processed=7574 routed=5279 dropped=2295
ds551-s26.team05.vaccination_records:0:616
```

---

## Phase 2: Spark Analytics and Storage

### Database Choice — TimescaleDB

We use TimescaleDB as the analytics database. TimescaleDB is PostgreSQL-based and optimized for time-series data, which fits this project because all analytics are grouped by timestamps and time windows.

TimescaleDB runs in OpenShift as:

```text
pod: timescaledb-0
service: timescaledb-service
database: analytics
```

Spark connects with:

```text
jdbc:postgresql://timescaledb-service.ds551-2026-spring-7726b8.svc.cluster.local:5432/analytics
```

TimescaleDB was chosen because:

1. The project data is naturally time-series based.
2. SQL makes the data easy to query from Spark, Python, and Jupyter.
3. Hypertables support efficient time-based queries.
4. Phase 3 alerting and visualization can query aggregated outputs directly.

---

### Spark Analytics

The Spark job reads the typed Kafka topics and writes aggregated analytics to TimescaleDB.

Input Kafka topics:

```text
ds551-s26.team05.symptom_reports
ds551-s26.team05.clinic_visits
ds551-s26.team05.hospital_admissions
ds551-s26.team05.vaccination_records
```

Output tables:

```text
hourly_event_rate
severity_trend
bed_availability
vaccination_trend
```

---

### Analytics 1 — Hourly Event Rate

Table:

```text
hourly_event_rate
```

Schema:

```text
hour TIMESTAMPTZ
event_type TEXT
region TEXT
event_count INTEGER
```

This table counts events per hour, event type, and region. It is used by alerting and outbreak prediction to detect unusual event volume.

---

### Analytics 2 — Severity Trend

Table:

```text
severity_trend
```

Schema:

```text
window_start TIMESTAMPTZ
window_end TIMESTAMPTZ
region TEXT
event_type TEXT
avg_severity DOUBLE PRECISION
sample_count BIGINT
```

This table tracks average severity over time by region and event type. It helps identify whether conditions are worsening or improving.

---

### Analytics 3 — Bed Availability Pressure

Table:

```text
bed_availability
```

Schema:

```text
hour TIMESTAMPTZ
region TEXT
avg_available_beds DOUBLE PRECISION
min_available_beds INTEGER
sample_count BIGINT
```

This table measures hospital capacity pressure. Lower available beds indicate higher operational risk.

---

### Analytics 4 — Vaccination Trend

Table:

```text
vaccination_trend
```

Schema:

```text
hour TIMESTAMPTZ
region TEXT
vaccine_type TEXT
dose_number INTEGER
vaccination_count INTEGER
```

This table satisfies the CO-2026-01 downstream analytics requirement. It uses vaccination-specific fields, especially:

```text
vaccine_type
dose_number
```

The table is used by:

```text
alerting
outbreak prediction
visualization
data quality monitoring
```

Final live Spark evidence showed:

```text
vaccination_trend | 129
```

---

## Phase 3: Advanced Features

### Advanced Alerting

The alerting service is implemented in:

```text
alerting/alert_service.py
```

It runs as an OpenShift deployment and polls TimescaleDB. It monitors:

```text
hourly_event_rate
severity_trend
bed_availability
vaccination_trend
data_quality_checks
```

It emits structured logs such as:

```text
ALERT level=critical metric=event_rate region=Boston ...
SUPPRESSED level=critical metric=data_quality region=hourly_event_rate reason=cooldown
```

Alerting supports:

* warning and critical thresholds
* event-rate alerts
* severity alerts
* bed availability alerts
* vaccination activity alerts
* data quality / pipeline health alerts
* cooldown suppression to avoid repeated alert spam
* optional Slack webhook via `SLACK_WEBHOOK_URL`

---

### Outbreak Prediction

The outbreak prediction job is implemented in:

```text
ml/outbreak_prediction.py
```

It reads analytics tables from TimescaleDB and writes predictions to:

```text
outbreak_predictions
```

Features used:

```text
total_events from hourly_event_rate
avg_severity from severity_trend
min_available_beds from bed_availability
vaccination_count from vaccination_trend
```

The model is a transparent rule-based scoring model. The score combines:

```text
event volume
severity
bed pressure
vaccination activity
```

Output columns include:

```text
hour
region
total_events
avg_severity
min_available_beds
vaccination_count
event_score
severity_score
bed_pressure_score
vaccination_score
outbreak_risk_score
outbreak_risk_level
```

Risk levels are assigned as:

```text
critical
high
medium
low
```

This was chosen instead of a black-box model because the team can explain each input feature and scoring decision during the interview.

---

### Visualization

The visualization notebook is:

```text
notebooks/phase3_visualizations.ipynb
```

It can run in demo mode or live database mode.

The notebook visualizes:

1. Hourly event rate by event type and region
2. Severity trend by region
3. Bed availability pressure by region
4. Outbreak risk score by region and hour
5. Vaccination analytics by region, vaccine type, and dose
6. Pipeline/data quality health status

The notebook reads from:

```text
hourly_event_rate
severity_trend
bed_availability
vaccination_trend
outbreak_predictions
data_quality_checks
```

---

### Data Quality Monitoring

The data quality monitor is implemented in:

```text
quality/data_quality_monitor.py
```

It writes persistent check results to:

```text
data_quality_checks
```

It checks:

```text
table existence
nonzero row counts
freshness
required columns
null key values
severity value range
nonnegative bed availability
vaccination analytics validity
outbreak prediction table readiness
```

This is our Phase 2.5 / Category D reliability layer. It prevents Phase 3 from silently trusting stale or incomplete data.

In the final live evidence run, the monitor wrote rows to `data_quality_checks`, and the table contained persisted monitoring results.

---

## Database Tables

The final TimescaleDB database contains:

| Table                  | Purpose                                                    |
| ---------------------- | ---------------------------------------------------------- |
| `hourly_event_rate`    | Event counts by hour, event type, and region               |
| `severity_trend`       | Average severity by time window and region                 |
| `bed_availability`     | Bed pressure metrics by hour and region                    |
| `vaccination_trend`    | Vaccination counts by hour, region, vaccine type, and dose |
| `outbreak_predictions` | Phase 3 outbreak risk scores                               |
| `data_quality_checks`  | Persistent reliability and freshness checks                |

Final live database evidence showed:

```text
hourly_event_rate    | 56
severity_trend       | 28
bed_availability     | 15
vaccination_trend    | 129
outbreak_predictions | 5
data_quality_checks  | 63
```

---

## Technology Choices

| Component              | Technology                      | Rationale                                              |
| ---------------------- | ------------------------------- | ------------------------------------------------------ |
| Event streaming        | Apache Kafka                    | Course-provided event streaming platform               |
| Pipeline orchestration | Tekton Pipelines                | Kubernetes-native workflow execution                   |
| Batch analytics        | Apache Spark / PySpark          | Handles multi-topic analytics and grouped aggregations |
| Database               | TimescaleDB                     | PostgreSQL-compatible time-series database             |
| Container platform     | OpenShift Kubernetes            | Course-provided deployment environment                 |
| Alerting               | Python service                  | Lightweight polling service with structured logs       |
| ML / Prediction        | Python + SQL rule-based scoring | Transparent and explainable                            |
| Visualization          | Jupyter + pandas + matplotlib   | Reproducible analytics visualization                   |
| Data quality           | Python + SQL checks             | Persistent operational reliability layer               |

---

## Deployment Summary

All components are deployed in:

```text
ds551-2026-spring-7726b8
```

Key deployed resources:

```text
Kafka pod/service:
kafka-team05
kafka-team05:9092

Tekton Pipeline:
epidemic-engine-phase1

TimescaleDB:
timescaledb-0
timescaledb-service

Spark analytics:
epidemic-analytics CronJob
phase3-vax-real manual evidence job

Data quality:
epidemic-data-quality-monitor Job

ML:
epidemic-outbreak-prediction Job

Alerting:
epidemic-alerting Deployment

Visualization:
notebooks/phase3_visualizations.ipynb
```

---

## Operational Evidence Summary

The final system evidence showed:

* Kafka was running in pod `kafka-team05-558fd785fb-g4g6l`.
* TimescaleDB was running in pod `timescaledb-0`.
* The raw topic contained real generator-produced `vaccination_record` events.
* Tekton validation forwarded `vaccination_record` events to `ds551-s26.team05.validated`.
* Tekton routing sent `vaccination_record` events to `ds551-s26.team05.vaccination_records`.
* The vaccination topic offset increased to `616`.
* Spark completed successfully after the vaccination routing run.
* TimescaleDB contained populated Phase 2 and Phase 3 tables.
* `vaccination_trend` contained `129` rows.
* `outbreak_predictions` contained queryable risk scores.
* `data_quality_checks` contained persisted monitoring results.
* The Jupyter notebook displayed charts for all major Phase 3 outputs.

---

## CO-2026-01: `vaccination_record` Change Order

The upstream event generator introduced a new event type:

```text
vaccination_record
```

Expected schema:

```json
{
  "event_type": "vaccination_record",
  "schema_version": 2,
  "timestamp": "2026-04-27T14:32:00Z",
  "record_id": "VR123456",
  "patient_id": "P54321",
  "region": "Boston",
  "vaccine_type": "influenza",
  "dose_number": 1,
  "administered_at": "2026-04-27T14:30:00Z"
}
```

Team 05 handled this as a localized extension.

### Validation

`validate-events` applies base validation to all events and extra schema validation to `vaccination_record` events.

Additional required fields:

```text
schema_version
record_id
patient_id
vaccine_type
dose_number
administered_at
```

### Routing

`route-and-enrich` routes:

```text
vaccination_record → ds551-s26.team05.vaccination_records
```

Placeholder `vaccination` events are kept separate from `vaccination_record`
because they do not contain the required CO-2026-01 fields. They are dropped
during routing.

### Storage

Spark reads:

```text
ds551-s26.team05.vaccination_records
```

and writes:

```text
vaccination_trend
```

### Downstream Use

Vaccination data is used by:

```text
Spark analytics
outbreak prediction
alerting
visualization
data quality monitoring
```

The notebook includes a vaccination chart grouped by:

```text
region
vaccine_type
dose_number
```

This demonstrates that vaccination-specific fields are used downstream, not merely routed.

### Final Live Evidence

```text
VALID forwarded event_type=vaccination_record to ds551-s26.team05.validated
ROUTED event_type=vaccination_record to ds551-s26.team05.vaccination_records
SUMMARY processed=7574 routed=5279 dropped=2295
ds551-s26.team05.vaccination_records:0:616
vaccination_trend | 129
```

---

## Team Roles

| Team Member | Primary Responsibility                                                              |
| ----------- | ----------------------------------------------------------------------------------- |
| Hunter      | Architecture, Phase 2 Spark/TimescaleDB, Phase 3 integration and presentation       |
| Josh        | Spark analytics, database schema, Phase 3 deliverables                              |
| Khushbu     | Tekton pipeline, Phase 3 integration, data quality/ML/alerting evidence, submission |
| Rohan       | Tekton pipeline, routing/enrichment, Phase 3 integration and submission             |

All team members participated in the final system integration and interview preparation.
