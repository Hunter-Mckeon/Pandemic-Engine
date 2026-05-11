# Tekton Pipeline — Phase 1

**Team 05 | DS-551 Data Engineering at Scale — Spring 2026**

---

## 1. Pipeline Design

The Phase 1 pipeline consumes synthetic epidemiological events from Kafka, validates them, enriches them with metadata, and routes them into three typed output topics. It is implemented using two Tekton Tasks connected through a Pipeline.

### Data Flow

```text
Kafka: ds551-s26.team05.raw
    |
    v  [Task 1: validate-events]
    - Checks required base fields: event_type, timestamp, region
    - Validates timestamp format (ISO 8601)
    - Forwards valid events; logs and drops invalid ones
    |
    v
Kafka: ds551-s26.team05.validated
    |
    v  [Task 2: route-and-enrich]
    - Adds enrichment fields
    - Routes routable event types
    - Logs and drops unroutable events
    |
    +---> Kafka: ds551-s26.team05.symptom_reports
    +---> Kafka: ds551-s26.team05.clinic_visits
    +---> Kafka: ds551-s26.team05.hospital_admissions
    +---> Kafka: ds551-s26.team05.vaccination_records
````

---

## 2. Task Design

### `validate-events`

This task reads events from the raw Kafka topic and validates them.

For each event:

* Ensures required base fields are present: `event_type`, `timestamp`, `region`
* Validates that `timestamp` follows ISO 8601 format
* For CO-2026-01 `vaccination_record` events, additionally validates
  `schema_version = 2`, `record_id`, `patient_id`, `vaccine_type`,
  `dose_number`, and `administered_at`

Valid events are forwarded to the validated topic. Invalid events are logged with reasons such as:

* `missing_fields`
* `bad_timestamp`
* `bad_json`

This stage does **not** filter by event type. Both routable and unroutable event types are allowed to pass validation as long as the required base fields are present.

The task processes a fixed batch and commits offsets using the consumer group:

```text
team05-validate-events-phase3-vax
```

This `phase3-vax` consumer group is intentional for the final Phase 3 evidence
run. It avoids stale committed Kafka offsets from earlier debugging while still
using the real raw topic and production routing logic.

### `route-and-enrich`

This task reads validated events and prepares them for downstream use.

Each event is enriched with:

* `team_id = "team05"`
* `processing_timestamp` (current UTC time)
* `event_source = "ds551-event-generator"`

Routing is based on `event_type`:

| Event Type         | Output Topic                         |
| ------------------ | ------------------------------------ |
| symptom_report     | ds551-s26.team05.symptom_reports     |
| clinic_visit       | ds551-s26.team05.clinic_visits       |
| hospital_admission | ds551-s26.team05.hospital_admissions |
| vaccination_record | ds551-s26.team05.vaccination_records |

The following upstream event types are valid but unroutable and are logged and dropped during routing:

* `general_health_report`
* `vaccination`
* `emergency_incident`

`vaccination_record` is not dropped. It is the Phase 3 change-order event type
and is routed to its own typed topic for Spark vaccination analytics.

Consumer group used:

```text
team05-route-enrich-phase3-vax
```

This `phase3-vax` consumer group is intentional for the final Phase 3 evidence
run. It avoids stale committed Kafka offsets while routing real
`vaccination_record` messages from the real validated topic.

---

## 3. Pipeline Structure

The pipeline ensures sequential execution:

1. `validate-events`
2. `route-and-enrich`

This is enforced using:

```text
runAfter: [validate-events-task]
```

All Kafka topics and configuration values are passed through parameters defined in `pipelinerun.yaml`.

---

## 4. Batch Processing Model

Since Tekton tasks must terminate, we simulate streaming using batch processing:

* `batch-size` controls how many messages are processed per run
* `consumer_timeout_ms=15000` ensures the task exits if no new messages arrive

After processing:

* Offsets are committed (`consumer.commit()`)
* Future runs resume from the last processed offset

`tekton/pipelinerun.yaml` intentionally uses `batch-size: "5000"` for Phase 3
evidence/backlog processing so official `vaccination_record` records appear in
the run. This is an evidence-oriented batch size, not a statement that every
small manual test must process 5000 messages.

The repo also includes evidence-only manifests:

* `tekton/pipelinerun-vax-real.yaml` is a CO-2026-01 validation evidence run
  against the real raw topic.
* `tekton/route-vax-real-logs-taskrun.yaml` is a manual route evidence/replay
  TaskRun against the real validated topic with `batch-size: "12000"`. Rerunning
  it can duplicate routed output, so use it intentionally for evidence capture
  only.

These evidence manifests are not temp-topic demos and are not the default
everyday production flow.

---

## 5. Deployment Commands

All commands use namespace:

```text
ds551-2026-spring-7726b8
```

### Initial setup

```bash
oc apply -f tekton/tasks.yaml -n ds551-2026-spring-7726b8
oc apply -f tekton/pipeline.yaml -n ds551-2026-spring-7726b8
oc create -f tekton/pipelinerun.yaml -n ds551-2026-spring-7726b8
```

### Monitor execution

```bash
oc get pipelinerun -n ds551-2026-spring-7726b8
oc get taskrun -n ds551-2026-spring-7726b8
```

Stream logs:

```bash
oc logs -f -l tekton.dev/task=validate-events -n ds551-2026-spring-7726b8
oc logs -f -l tekton.dev/task=route-and-enrich -n ds551-2026-spring-7726b8
```

### Re-run pipeline

```bash
oc create -f tekton/pipelinerun.yaml -n ds551-2026-spring-7726b8
```

---

## 6. Verification

### Check output topics

```bash
kafka-console-consumer \
  --bootstrap-server kafka.ds551-2026-spring-7726b8.svc.cluster.local:9092 \
  --topic ds551-s26.team05.symptom_reports \
  --max-messages 5
```

Vaccination change-order topic:

```bash
kafka-console-consumer \
  --bootstrap-server kafka.ds551-2026-spring-7726b8.svc.cluster.local:9092 \
  --topic ds551-s26.team05.vaccination_records \
  --max-messages 5
```

### Example output event

```json
{
  "event_type": "symptom_report",
  "timestamp": "2026-04-02T21:54:26Z",
  "patient_id": "P78146",
  "region": "Boston",
  "team_id": "team05",
  "processing_timestamp": "2026-04-03T07:51:49Z",
  "event_source": "ds551-event-generator"
}
```

### Log verification

A successful run shows:

```text
SUMMARY total_seen=50 valid=50 invalid=0
SUMMARY processed=50 routed=26 dropped=24
```

The exact counts may vary across runs depending on which event types arrive in the batch, but valid events should be forwarded to the validated topic and routable events should be written to the three typed topics.

---

## 7. Challenges and Solutions

### 1. Ensuring tasks terminate

Tekton tasks cannot run indefinitely. We implemented:

* batch limits
* timeout-based exit

### 2. Python Kafka compatibility

`kafka-python` breaks on Python 3.11.

Fix:

* Used `kafka-python-ng`
* Installed dynamically inside container

### 3. Spec corrections and generator schema mismatch

The corrected Phase 1 spec requires only the base fields `event_type`, `timestamp`, and `region` during validation. The upstream generator also emits unroutable event types that only contain base fields plus `data: "placeholder"`.

Fix:

* Validated only the corrected base fields
* Allowed all event types to pass validation if base fields were present
* Dropped unroutable types during routing instead of validation

### 4. Avoiding duplicate processing across runs

Tekton tasks are short-lived batch consumers. To avoid replaying the same messages on every run, both tasks use named consumer groups and commit offsets after processing.

---

## 8. Final Outcome

The pipeline successfully:

* Consumes raw Kafka events
* Validates and filters malformed data
* Enriches valid events
* Routes supported event types into structured typed topics
* Drops unroutable event types during the routing stage

This completes Phase 1 of the Epidemic Engine system.
