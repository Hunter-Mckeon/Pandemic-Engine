# Phase 3 Data Flow

Team 05 namespace: `ds551-2026-spring-7726b8`

This document describes the final live system flow, repo evidence, and CO-2026-01 vaccination change-order evidence.

## End-to-End Flow

```text
Event Generator (course infra namespace)
  |
  v
Kafka raw topic: ds551-s26.team05.raw
  |
  v
Tekton Task: validate-events
  - validates base fields: event_type, timestamp, region
  - validates ISO 8601 timestamp format
  - validates schema_version = 2 only for vaccination_record
  - validates vaccination_record required fields for CO-2026-01
  - forwards valid events
  - logs and drops invalid events
  |
  v
Kafka validated topic: ds551-s26.team05.validated
  |
  v
Tekton Task: route-and-enrich
  - adds team_id, processing_timestamp, event_source
  - routes supported event types to typed topics
  - drops unroutable legacy/noise event types
  |
  +--> ds551-s26.team05.symptom_reports
  +--> ds551-s26.team05.clinic_visits
  +--> ds551-s26.team05.hospital_admissions
  +--> ds551-s26.team05.vaccination_records
        |
        v
Spark CronJob: epidemic-analytics
  - reads typed Kafka topics
  - computes hourly event rate, severity trend, bed availability, and vaccination trend
  |
  v
TimescaleDB service: timescaledb-service:5432 / database=analytics
  |
  +--> hourly_event_rate
  +--> severity_trend
  +--> bed_availability
  +--> vaccination_trend
  +--> outbreak_predictions
  +--> data_quality_checks
  |
  v
Phase 3 consumers
  +--> alerting/alert_service.py
  +--> ml/outbreak_prediction.py
  +--> notebooks/phase3_visualizations.ipynb
  +--> quality/data_quality_monitor.py
````

## Kafka Topics

| Topic                                  | Producer               | Consumer           | Notes                                            |
| -------------------------------------- | ---------------------- | ------------------ | ------------------------------------------------ |
| `ds551-s26.team05.raw`                 | Course event generator | `validate-events`  | Raw JSON events.                                 |
| `ds551-s26.team05.validated`           | `validate-events`      | `route-and-enrich` | Valid base schema.                               |
| `ds551-s26.team05.symptom_reports`     | `route-and-enrich`     | Spark ETL          | Original Phase 1 typed topic.                    |
| `ds551-s26.team05.clinic_visits`       | `route-and-enrich`     | Spark ETL          | Original Phase 1 typed topic.                    |
| `ds551-s26.team05.hospital_admissions` | `route-and-enrich`     | Spark ETL          | Original Phase 1 typed topic.                    |
| `ds551-s26.team05.vaccination_records` | `route-and-enrich`     | Spark ETL          | CO-2026-01 typed topic for `vaccination_record`. |

## Schemas

Base fields validated for all events:

```json
{
  "event_type": "symptom_report",
  "timestamp": "2026-04-27T14:32:00Z",
  "region": "Boston"
}
```

Vaccination change-order event:

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

Additional validation for `vaccination_record`:

```text
schema_version must equal 2
record_id must be present
patient_id must be present
vaccine_type must be present
dose_number must be present and >= 1
administered_at must be present and parseable as a timestamp
```

Enrichment fields added by Tekton routing:

```json
{
  "team_id": "team05",
  "processing_timestamp": "2026-04-27T14:33:01Z",
  "event_source": "ds551-event-generator"
}
```

## Routing Behavior

Supported routed event types:

| Event Type           | Routed Topic                           |
| -------------------- | -------------------------------------- |
| `symptom_report`     | `ds551-s26.team05.symptom_reports`     |
| `clinic_visit`       | `ds551-s26.team05.clinic_visits`       |
| `hospital_admission` | `ds551-s26.team05.hospital_admissions` |
| `vaccination_record` | `ds551-s26.team05.vaccination_records` |

Legacy/noise event types:

| Event Type              | Behavior                                                                                                    |
| ----------------------- | ----------------------------------------------------------------------------------------------------------- |
| `general_health_report` | Valid base schema but unroutable; dropped during routing.                                                   |
| `vaccination`           | Placeholder event; dropped because it does not contain CO-2026-01 vaccination fields.                      |
| `emergency_incident`    | Valid base schema but unroutable; dropped during routing.                                                   |

Placeholder `vaccination` events are kept separate from `vaccination_record`
because they do not include `record_id`, `patient_id`, `vaccine_type`,
`dose_number`, or `administered_at`.

## TimescaleDB Tables

| Table                  | Writer                            | Purpose                                                          |
| ---------------------- | --------------------------------- | ---------------------------------------------------------------- |
| `hourly_event_rate`    | `spark/etl.py`                    | Event counts by hour/type/region, including vaccination records. |
| `severity_trend`       | `spark/etl.py`                    | Hourly severity trend for symptom and admission events.          |
| `bed_availability`     | `spark/etl.py`                    | Bed pressure by hour/region.                                     |
| `vaccination_trend`    | `spark/etl.py`                    | Vaccination counts by hour/region/type/dose.                     |
| `outbreak_predictions` | `ml/outbreak_prediction.py`       | Rule-based outbreak risk scores.                                 |
| `data_quality_checks`  | `quality/data_quality_monitor.py` | Data quality and freshness check history.                        |

## Vaccination Analytics

Spark reads:

```text
ds551-s26.team05.vaccination_records
```

and writes:

```text
vaccination_trend
```

The vaccination analytic groups by:

```text
hour
region
vaccine_type
dose_number
```

Output schema:

```text
hour TIMESTAMPTZ
region TEXT
vaccine_type TEXT
dose_number INTEGER
vaccination_count INTEGER
```

This satisfies the CO-2026-01 downstream requirement because the output uses vaccination-specific fields, not just the event type.

## Phase 3 Feature Consumers

Alerting:

* Reads `hourly_event_rate`, `severity_trend`, `bed_availability`, `vaccination_trend`, and `data_quality_checks`.
* Emits structured Kubernetes-log alerts with severity and cooldown suppression.
* Monitors vaccination low/surge activity using `vaccination_trend`.
* If `SLACK_WEBHOOK_URL` is configured, also posts alert text to Slack.

Outbreak prediction:

* Reads event volume, severity, bed pressure, and vaccination activity.
* Uses `vaccination_count` from `vaccination_trend` as one risk feature.
* Writes `outbreak_predictions`.

Visualization:

* `notebooks/phase3_visualizations.ipynb` reads all six core Phase 3 tables.
* The notebook includes demo/sample data so it can run without TimescaleDB.
* The vaccination chart shows counts by region, `vaccine_type`, and `dose_number`.

Data quality monitoring:

* Checks table existence, row counts, freshness, nulls, severity range, nonnegative beds, vaccination values, and ML output table presence.
* Writes persistent check rows to `data_quality_checks` in live mode.
* Checks `vaccination_trend` for required columns and valid vaccination values.

## Final CO-2026-01 Live Evidence

The final live run verified the real event-generator path.

Raw topic contained real `vaccination_record` events:

```text
event_type=vaccination_record
schema_version=2
record_id=...
patient_id=...
vaccine_type=...
dose_number=...
administered_at=...
```

Validation forwarded real vaccination records:

```text
VALID forwarded event_type=vaccination_record to ds551-s26.team05.validated
```

Routing sent real vaccination records to the typed vaccination topic:

```text
ROUTED event_type=vaccination_record to ds551-s26.team05.vaccination_records
SUMMARY processed=7574 routed=5279 dropped=2295
```

Kafka topic offset after routing:

```text
ds551-s26.team05.vaccination_records:0:616
```

TimescaleDB row counts after rerunning Spark analytics:

```text
hourly_event_rate    | 56
severity_trend       | 28
bed_availability     | 15
vaccination_trend    | 129
outbreak_predictions | 5
data_quality_checks  | 63
```

## What Requires OpenShift to Verify Live

* Event generator producing fresh raw events.
* Tekton TaskRun logs and Kafka message movement.
* Spark CronJob execution and DB writes.
* TimescaleDB pod readiness and table row counts.
* Alerting Deployment pod logs.
* One-hour continuous flow evidence.

Use `docs/phase3-runbook.md` for exact commands to collect or reproduce this evidence.
