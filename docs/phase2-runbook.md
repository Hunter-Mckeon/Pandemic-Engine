# Phase 2 — Runbook

**Team 05 | DS-551 Data Engineering at Scale — Spring 2026**

**Namespace:** `ds551-2026-spring-7726b8`

---

## Startup Procedure

Follow these steps in order. Each step depends on the previous one completing successfully.

### Step 1 — Deploy TimescaleDB

```bash
oc apply -f infra/database-deployment.yaml -n ds551-2026-spring-7726b8
```

Wait for the pod to be ready:
```bash
oc get pods -n ds551-2026-spring-7726b8 | grep timescaledb
# Expected: timescaledb-0   1/1   Running
```

### Step 2 — Initialize the database schema

Copy the init script into the pod and run it:
```bash
oc cp infra/database-init.sql \
  ds551-2026-spring-7726b8/timescaledb-0:/tmp/database-init.sql

oc exec -it timescaledb-0 -n ds551-2026-spring-7726b8 -- \
  psql -U postgres -d analytics -f /tmp/database-init.sql
```

Verify tables were created:
```bash
oc exec -it timescaledb-0 -n ds551-2026-spring-7726b8 -- \
  psql -U postgres -d analytics -c "\dt"
# Expected: hourly_event_rate, severity_trend, bed_availability
```

### Step 3 — Build and push the container image

```bash
REGISTRY=$(oc get route default-route -n openshift-image-registry \
  --template='{{ .spec.host }}')

docker build -t ${REGISTRY}/ds551-2026-spring-7726b8/epidemic-analytics:latest .
docker push ${REGISTRY}/ds551-2026-spring-7726b8/epidemic-analytics:latest
```

### Step 4 — Deploy RBAC and CronJob

```bash
oc apply -f infra/spark-rbac.yaml -n ds551-2026-spring-7726b8
oc apply -f infra/spark-cronjob.yaml -n ds551-2026-spring-7726b8
```

Verify CronJob is scheduled:
```bash
oc get cronjob epidemic-analytics -n ds551-2026-spring-7726b8
# Expected: SCHEDULE = 0 */6 * * *   SUSPEND = False   ACTIVE = 0
```

### Step 5 — Trigger a manual run to verify end-to-end

```bash
oc create job --from=cronjob/epidemic-analytics manual-run-1 \
  -n ds551-2026-spring-7726b8
```

Stream logs:
```bash
oc logs -f -l job-name=manual-run-1 -n ds551-2026-spring-7726b8
```

Verify results in the database:
```bash
oc exec -it timescaledb-0 -n ds551-2026-spring-7726b8 -- \
  psql -U postgres -d analytics \
  -c "SELECT COUNT(*) FROM hourly_event_rate;"
# Expected: count > 0
```

---

## Shutdown Procedure

### Graceful shutdown

Suspend the CronJob so no new runs are triggered:
```bash
oc patch cronjob epidemic-analytics \
  -p '{"spec":{"suspend":true}}' \
  -n ds551-2026-spring-7726b8
```

Wait for any currently running job to complete:
```bash
oc get jobs -n ds551-2026-spring-7726b8
# Wait until no jobs show ACTIVE > 0
```

Scale down TimescaleDB:
```bash
oc scale statefulset timescaledb --replicas=0 \
  -n ds551-2026-spring-7726b8
```

### Resume after shutdown

Scale TimescaleDB back up:
```bash
oc scale statefulset timescaledb --replicas=1 \
  -n ds551-2026-spring-7726b8
```

Re-enable the CronJob:
```bash
oc patch cronjob epidemic-analytics \
  -p '{"spec":{"suspend":false}}' \
  -n ds551-2026-spring-7726b8
```

---

## Routine Troubleshooting

### Job is not running

**Check CronJob status:**
```bash
oc get cronjob epidemic-analytics -n ds551-2026-spring-7726b8
```
If `SUSPEND = True`, re-enable it (see Resume above).

**Check recent job history:**
```bash
oc get jobs -n ds551-2026-spring-7726b8
```

**Check pod logs for the most recent run:**
```bash
oc get pods -n ds551-2026-spring-7726b8 | grep epidemic-analytics
oc logs <pod-name> -n ds551-2026-spring-7726b8
```

---

### Database is down or unreachable

**Check pod status:**
```bash
oc get pods -n ds551-2026-spring-7726b8 | grep timescaledb
```

If `CrashLoopBackOff`:
```bash
oc logs timescaledb-0 -n ds551-2026-spring-7726b8
oc describe pod timescaledb-0 -n ds551-2026-spring-7726b8
```

If the PVC is full:
```bash
oc get pvc timescaledb-pvc -n ds551-2026-spring-7726b8
```
Expand the PVC request in `infra/database-deployment.yaml` and reapply, or clean old analytics rows:
```bash
oc exec -it timescaledb-0 -n ds551-2026-spring-7726b8 -- \
  psql -U postgres -d analytics \
  -c "DELETE FROM hourly_event_rate WHERE hour < now() - interval '7 days';"
```

---

### Kafka topics are lagging (Spark reads stale data)

**Verify Phase 1 pipeline is running:**
```bash
oc get pipelinerun -n ds551-2026-spring-7726b8
```

**Check that typed topics have messages:**
```bash
oc run kafka-check --image=confluentinc/cp-kafka:7.4.0 -it --rm \
  --restart=Never -n ds551-2026-spring-7726b8 -- \
  kafka-console-consumer \
  --bootstrap-server kafka-team05:9092 \
  --topic ds551-s26.team05.hospital_admissions \
  --max-messages 3
```

If topics are empty, re-run the Phase 1 pipeline:
```bash
oc create -f tekton/pipelinerun.yaml -n ds551-2026-spring-7726b8
```

---

### Database writes are slow

**Check for missing indexes:**
```bash
oc exec -it timescaledb-0 -n ds551-2026-spring-7726b8 -- \
  psql -U postgres -d analytics \
  -c "\d hourly_event_rate"
```
Expected indexes: `idx_hourly_event_rate_region`, `idx_hourly_event_rate_event_type`.
If missing, re-run `database-init.sql`.

**Check table sizes:**
```bash
oc exec -it timescaledb-0 -n ds551-2026-spring-7726b8 -- \
  psql -U postgres -d analytics \
  -c "SELECT hypertable_name, pg_size_pretty(total_bytes)
      FROM timescaledb_information.hypertable_detailed_size
      ORDER BY total_bytes DESC;"
```

---

### Spark job OOMKilled

**Identify the failure:**
```bash
oc get pods -n ds551-2026-spring-7726b8 | grep epidemic-analytics
oc describe pod <pod-name> -n ds551-2026-spring-7726b8
# Look for: OOMKilled in Last State
```

**Fix:** Increase memory limit in `infra/spark-cronjob.yaml`:
```yaml
resources:
  limits:
    memory: "4Gi"   # increase from 2Gi
```

Reapply and trigger a new run:
```bash
oc apply -f infra/spark-cronjob.yaml -n ds551-2026-spring-7726b8
oc create job --from=cronjob/epidemic-analytics manual-run-2 \
  -n ds551-2026-spring-7726b8
```
