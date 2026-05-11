# Phase 2 — Spark Analytics Job

**Team 05 | DS-551 Data Engineering at Scale — Spring 2026**

---

## Overview

The Phase 2 Spark batch job reads from the typed Kafka topics produced by the Phase 1 Tekton pipeline, computes analytics, and writes results to TimescaleDB. It runs every 6 hours as a Kubernetes CronJob. Phase 3 CO-2026-01 adds the `vaccination_record` topic and vaccination analytics without removing the original three Phase 2 analytics.

---

## Analytics

**Analytics 1 — Hourly Event Rate by Type and Region**
Counts events per hour, event type, and region across all three typed topics. Feeds into Phase 3 alerting thresholds and provides event rate trend features for the outbreak prediction model.

**Analytics 2 — Severity Trend Over Time**
Computes a rolling average of severity scores from `symptom_report` and `hospital_admission` events per region in 1-hour windows. Captures whether conditions are escalating or improving — the key leading indicator for outbreak prediction.

**Analytics 3 — Bed Availability Pressure**
Aggregates average and minimum available beds from `hospital_admission` events per hour and region. Declining bed availability is both an alert trigger and a strong outbreak predictor independent of event rate and severity.

**Analytics 4 — Vaccination Trend**
Counts `vaccination_record` events per hour, region, `vaccine_type`, and `dose_number`. This satisfies CO-2026-01 by creating a queryable downstream output from vaccination-specific fields.

---

## Data Flow and Kafka Connectivity

**Bootstrap server:** `kafka.ds551-2026-spring-7726b8.svc.cluster.local:9092`

**Input topics:**

| Topic | Event Type | Key fields used |
|---|---|---|
| `ds551-s26.team05.symptom_reports` | `symptom_report` | timestamp, region, severity |
| `ds551-s26.team05.clinic_visits` | `clinic_visit` | timestamp, region |
| `ds551-s26.team05.hospital_admissions` | `hospital_admission` | timestamp, region, severity, available_beds |
| `ds551-s26.team05.vaccination_records` | `vaccination_record` | timestamp, region, vaccine_type, dose_number |

**Expected event structure (after Phase 1 enrichment):**
```json
{
  "event_id": "evt-abc123",
  "event_type": "hospital_admission",
  "patient_id": "P12345",
  "timestamp": "2026-04-02T21:54:26Z",
  "region": "Boston",
  "severity": "high",
  "available_beds": 12,
  "team_id": "team05",
  "processing_timestamp": "2026-04-03T07:51:49Z",
  "event_source": "ds551-event-generator"
}
```

---

## Database Schema

**TimescaleDB connection:**
```
jdbc:postgresql://timescaledb-service.ds551-2026-spring-7726b8.svc.cluster.local:5432/analytics
```

**Table: `hourly_event_rate`**
```sql
hour         TIMESTAMPTZ   -- truncated to the hour
event_type   TEXT          -- symptom_report, clinic_visit, hospital_admission
region       TEXT          -- e.g. Boston, Chicago
event_count  INTEGER       -- number of events in that hour/type/region
```

Sample output:
```
2026-04-03 10:00:00+00 | symptom_report    | Boston  | 42
2026-04-03 10:00:00+00 | clinic_visit      | Chicago | 17
2026-04-03 10:00:00+00 | hospital_admission| Boston  | 8
```

**Table: `severity_trend`**
```sql
window_start  TIMESTAMPTZ     -- start of 1-hour window
window_end    TIMESTAMPTZ     -- end of 1-hour window
region        TEXT
event_type    TEXT            -- symptom_report or hospital_admission
avg_severity  DOUBLE PRECISION -- numeric scale: low=1, moderate=2, high=3, critical=4
sample_count  INTEGER
```

Sample output:
```
2026-04-03 10:00:00+00 | 2026-04-03 11:00:00+00 | Boston | symptom_report | 2.4 | 35
```

**Table: `bed_availability`**
```sql
hour               TIMESTAMPTZ
region             TEXT
avg_available_beds DOUBLE PRECISION
min_available_beds INTEGER
sample_count       INTEGER
```

Sample output:
```
2026-04-03 10:00:00+00 | Boston | 18.3 | 4 | 8
```

**Table: `vaccination_trend`**
```sql
hour              TIMESTAMPTZ
region            TEXT
vaccine_type      TEXT
dose_number       INTEGER
vaccination_count INTEGER
```

Sample output:
```
2026-04-27 14:00:00+00 | Boston | influenza | 1 | 12
```

**Phase 3 tables:** `outbreak_predictions` stores rule-based outbreak risk scores and `data_quality_checks` stores Category D monitoring results.

---

## Manual Run Command

To trigger the Spark job manually outside the CronJob schedule:

```bash
# Trigger a one-off job from the CronJob definition
oc create job --from=cronjob/epidemic-analytics manual-run-1 \
  -n ds551-2026-spring-7726b8

# Watch the job pod
oc get pods -n ds551-2026-spring-7726b8 | grep manual-run

# Stream logs
oc logs -f <pod-name> -n ds551-2026-spring-7726b8
```

---

## Monitoring and Health Checks

**Check the CronJob is scheduled:**
```bash
oc get cronjob epidemic-analytics -n ds551-2026-spring-7726b8
```

**Check recent job runs:**
```bash
oc get jobs -n ds551-2026-spring-7726b8
```

**Expected log messages on a successful run:**
```
=== Epidemic Engine — Phase 2 Spark Analytics Job starting ===
Reading Kafka topics...
symptom_reports:     450 events
clinic_visits:       310 events
hospital_admissions: 180 events
Computing Analytics 1: Hourly Event Rate by Type and Region
Computing Analytics 2: Severity Trend Over Time
Computing Analytics 3: Bed Availability Pressure
Truncating result tables...
Writing analytics results to TimescaleDB...
Wrote results to hourly_event_rate: 126 rows
Wrote results to severity_trend: 84 rows
Wrote results to bed_availability: 42 rows
=== Analytics job completed successfully ===
```

**Verify results were written to the database:**
```bash
# Open a psql session in the TimescaleDB pod
oc exec -it <timescaledb-pod> -n ds551-2026-spring-7726b8 -- \
  psql -U postgres -d analytics

# Check row counts
SELECT COUNT(*) FROM hourly_event_rate;
SELECT COUNT(*) FROM severity_trend;
SELECT COUNT(*) FROM bed_availability;
SELECT COUNT(*) FROM vaccination_trend;
SELECT COUNT(*) FROM outbreak_predictions;
SELECT COUNT(*) FROM data_quality_checks;

# Sample the most recent hourly event rates
SELECT * FROM hourly_event_rate ORDER BY hour DESC LIMIT 10;
```

---

## Deployment

**Step 1 — Deploy TimescaleDB and create schema:**
```bash
oc apply -f infra/database-deployment.yaml -n ds551-2026-spring-7726b8

# Wait for pod to be ready
oc get pods -n ds551-2026-spring-7726b8 | grep timescaledb

# Run schema init script
oc exec -it <timescaledb-pod> -n ds551-2026-spring-7726b8 -- \
  psql -U postgres -d analytics -f /tmp/database-init.sql
```

**Step 2 — Build and push the container image:**
```bash
# Get the OpenShift internal registry route
REGISTRY=$(oc get route default-route -n openshift-image-registry \
  --template='{{ .spec.host }}')

docker build -t ${REGISTRY}/ds551-2026-spring-7726b8/epidemic-analytics:latest .
docker push ${REGISTRY}/ds551-2026-spring-7726b8/epidemic-analytics:latest
```

**Step 3 — Deploy RBAC and CronJob:**
```bash
oc apply -f infra/spark-rbac.yaml -n ds551-2026-spring-7726b8
oc apply -f infra/spark-cronjob.yaml -n ds551-2026-spring-7726b8
```

**Step 4 — Trigger a manual run to verify:**
```bash
oc create job --from=cronjob/epidemic-analytics manual-run-1 \
  -n ds551-2026-spring-7726b8
```
