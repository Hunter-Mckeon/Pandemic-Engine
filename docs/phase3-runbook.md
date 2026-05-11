# Phase 3 Runbook

This runbook separates local repo checks from OpenShift checks. Do not run the OpenShift section until you are logged in with team credentials.

---

## Local Checks

Syntax and manifest checks:

```bash
python -m py_compile alerting/alert_service.py ml/outbreak_prediction.py quality/data_quality_monitor.py
python quality/data_quality_monitor.py --demo
python alerting/alert_service.py --demo --once
python ml/outbreak_prediction.py --demo
````

Notebook JSON check:

```bash
python -m json.tool notebooks/phase3_visualizations.ipynb > /tmp/phase3_notebook_check.json
```

---

## After `oc login`: Cluster Status

```bash
NS=ds551-2026-spring-7726b8

oc project $NS
oc get pods -n $NS
oc get pvc -n $NS
oc get svc -n $NS
```

Set the Kafka pod variable:

```bash
KAFKA_POD=$(oc get pods -n $NS --no-headers | awk '/^kafka-team05/{print $1; exit}')
echo $KAFKA_POD
```

Expected Kafka pod pattern:

```text
kafka-team05-...
```

---

## Kafka Topic Checks

Check raw topic offset:

```bash
oc exec $KAFKA_POD -n $NS -- sh -lc \
'kafka-get-offsets --bootstrap-server localhost:9092 --topic ds551-s26.team05.raw'
```

Check raw topic sample:

```bash
oc exec $KAFKA_POD -n $NS -- sh -lc \
'kafka-console-consumer --bootstrap-server localhost:9092 --topic ds551-s26.team05.raw --from-beginning --max-messages 20 --timeout-ms 10000'
```

Search raw topic for real `vaccination_record` events:

```bash
oc exec $KAFKA_POD -n $NS -- sh -lc \
'kafka-console-consumer --bootstrap-server localhost:9092 --topic ds551-s26.team05.raw --from-beginning --timeout-ms 15000' \
| grep -m 5 vaccination_record
```

Check validated topic for real `vaccination_record` events:

```bash
oc exec $KAFKA_POD -n $NS -- sh -lc \
'kafka-console-consumer --bootstrap-server localhost:9092 --topic ds551-s26.team05.validated --from-beginning --timeout-ms 15000' \
| grep -m 5 vaccination_record
```

Check vaccination topic offset:

```bash
oc exec $KAFKA_POD -n $NS -- sh -lc \
'kafka-get-offsets --bootstrap-server localhost:9092 --topic ds551-s26.team05.vaccination_records'
```

Check vaccination topic sample:

```bash
oc exec $KAFKA_POD -n $NS -- sh -lc \
'kafka-console-consumer --bootstrap-server localhost:9092 --topic ds551-s26.team05.vaccination_records --from-beginning --max-messages 10 --timeout-ms 10000'
```

---

## Tekton Checks

Apply only after review:

```bash
oc apply -f tekton/tasks.yaml -n $NS
oc apply -f tekton/pipeline.yaml -n $NS
oc create -f tekton/pipelinerun.yaml -n $NS
```

Phase 3 evidence note:

* `tekton/tasks.yaml` intentionally uses the consumer groups
  `team05-validate-events-phase3-vax` and
  `team05-route-enrich-phase3-vax` for the final vaccination evidence run. These
  groups avoid stale committed Kafka offsets from earlier debug runs while still
  using the real topics.
* `tekton/pipelinerun.yaml` intentionally uses `batch-size: "5000"` for Phase 3
  evidence/backlog processing so official `vaccination_record` records appear in
  the run.

Watch and capture evidence:

```bash
oc get pipelinerun -n $NS
oc get taskrun -n $NS
oc logs -l tekton.dev/task=validate-events -n $NS --tail=100
oc logs -l tekton.dev/task=route-and-enrich -n $NS --tail=100
```

Look for:

```text
VALID forwarded event_type=vaccination_record to ds551-s26.team05.validated
ROUTED event_type=vaccination_record to ds551-s26.team05.vaccination_records
TOPICS ... vaccination=ds551-s26.team05.vaccination_records
```

---

## Final CO-2026-01 Vaccination Verification

This section records the final live verification for the Phase 3 vaccination change order.

### 1. Confirm raw topic contains real `vaccination_record` events

```bash
NS=ds551-2026-spring-7726b8
KAFKA_POD=$(oc get pods -n $NS --no-headers | awk '/^kafka-team05/{print $1; exit}')

oc exec $KAFKA_POD -n $NS -- sh -lc \
'kafka-console-consumer --bootstrap-server localhost:9092 --topic ds551-s26.team05.raw --from-beginning --timeout-ms 15000' \
| grep -m 5 vaccination_record
```

Observed real generator records with:

```text
event_type=vaccination_record
schema_version=2
record_id=...
patient_id=...
vaccine_type=...
dose_number=...
administered_at=...
```

### 2. Confirm validation forwarded vaccination records

```bash
tkn pipelinerun logs epidemic-engine-phase1-vax-real-tnfbg -n $NS \
| grep vaccination_record
```

Expected evidence:

```text
VALID forwarded event_type=vaccination_record to ds551-s26.team05.validated
```

### 3. Route real validated records into the vaccination topic

Use a route-only TaskRun from the real validated topic. This does not use a
temporary topic; it routes from the actual `ds551-s26.team05.validated` topic.
This is a manual evidence/replay TaskRun with `batch-size: "12000"`. Rerunning
it can duplicate routed output, so use it intentionally for evidence capture
only.

```bash
cat <<'EOF' | oc create -f -
apiVersion: tekton.dev/v1
kind: TaskRun
metadata:
  generateName: route-vax-real-logs-
spec:
  taskRef:
    name: route-and-enrich
  params:
    - name: kafka-bootstrap
      value: "kafka-team05:9092"
    - name: input-topic
      value: "ds551-s26.team05.validated"
    - name: symptom-topic
      value: "ds551-s26.team05.symptom_reports"
    - name: clinic-topic
      value: "ds551-s26.team05.clinic_visits"
    - name: hospital-topic
      value: "ds551-s26.team05.hospital_admissions"
    - name: vaccination-topic
      value: "ds551-s26.team05.vaccination_records"
    - name: team-id
      value: "team05"
    - name: batch-size
      value: "12000"
EOF
```

Check route logs:

```bash
POD=$(oc get pods -n $NS --sort-by=.metadata.creationTimestamp | awk '/route-vax-real-logs/{print $1}' | tail -1)

oc logs -f $POD -n $NS | grep --line-buffered -E "vaccination_record|vaccination_records|SUMMARY"
```

Final observed evidence:

```text
ROUTED event_type=vaccination_record to ds551-s26.team05.vaccination_records
SUMMARY processed=7574 routed=5279 dropped=2295
TOPICS symptom=ds551-s26.team05.symptom_reports clinic=ds551-s26.team05.clinic_visits hospital=ds551-s26.team05.hospital_admissions vaccination=ds551-s26.team05.vaccination_records
```

### 4. Confirm vaccination topic offset increased

```bash
oc exec $KAFKA_POD -n $NS -- sh -lc \
'kafka-get-offsets --bootstrap-server localhost:9092 --topic ds551-s26.team05.vaccination_records'
```

Final observed offset:

```text
ds551-s26.team05.vaccination_records:0:616
```

### 5. Rerun Spark analytics

```bash
JOB=phase3-vax-real-$(date +%H%M%S)
oc create job --from=cronjob/epidemic-analytics $JOB -n $NS
oc logs -f job/$JOB -n $NS
```

If logs are requested before the container starts, check the pod and retry:

```bash
oc get pods -n $NS | grep $JOB
oc logs -f job/$JOB -n $NS
```

### 6. Verify `vaccination_trend`

```bash
oc exec -it timescaledb-0 -n $NS -- \
psql -U postgres -d analytics -c "
SELECT hour, region, vaccine_type, dose_number, vaccination_count
FROM vaccination_trend
ORDER BY hour DESC
LIMIT 20;"
```

Final observed result included vaccination analytics by hour, region, vaccine type, and dose number.

Final table counts:

```text
hourly_event_rate    | 56
severity_trend       | 28
bed_availability     | 15
vaccination_trend    | 129
outbreak_predictions | 5
data_quality_checks  | 63
```

---

## Spark Checks

Update the ETL ConfigMap after changing `spark/etl.py`:

```bash
oc create configmap etl-script --from-file=etl.py=spark/etl.py \
  -n $NS --dry-run=client -o yaml | oc apply -f -
```

Confirm the live ConfigMap contains vaccination logic:

```bash
oc get configmap etl-script -n $NS -o yaml | grep -E "vaccination_records|vaccination_trend|compute_vaccination"
```

Deploy/redeploy:

```bash
oc apply -f infra/spark-rbac.yaml -n $NS
oc apply -f infra/spark-cronjob.yaml -n $NS
oc create job --from=cronjob/epidemic-analytics phase3-manual-run -n $NS
```

Logs:

```bash
oc logs -f -l job-name=phase3-manual-run -n $NS
```

Look for:

```text
vaccination_records:<N> events
Computing Analytics 4: Vaccination Trend
Wrote results to vaccination_trend
```

---

## Database Query Checks

```bash
oc exec -it timescaledb-0 -n $NS -- \
  psql -U postgres -d analytics -c "\dt"

oc exec -it timescaledb-0 -n $NS -- \
  psql -U postgres -d analytics -c "
    SELECT 'hourly_event_rate' AS table_name, COUNT(*) FROM hourly_event_rate
    UNION ALL SELECT 'severity_trend', COUNT(*) FROM severity_trend
    UNION ALL SELECT 'bed_availability', COUNT(*) FROM bed_availability
    UNION ALL SELECT 'vaccination_trend', COUNT(*) FROM vaccination_trend
    UNION ALL SELECT 'outbreak_predictions', COUNT(*) FROM outbreak_predictions
    UNION ALL SELECT 'data_quality_checks', COUNT(*) FROM data_quality_checks;"
```

Vaccination sample:

```bash
oc exec -it timescaledb-0 -n $NS -- \
  psql -U postgres -d analytics -c \
  "SELECT * FROM vaccination_trend ORDER BY hour DESC LIMIT 10;"
```

---

## Data Quality Checks

Local demo:

```bash
python quality/data_quality_monitor.py --demo
```

Create/update the data quality ConfigMap and run the Kubernetes Job after login:

```bash
oc create configmap data-quality-code \
  --from-file=data_quality_monitor.py=quality/data_quality_monitor.py \
  -n $NS --dry-run=client -o yaml | oc apply -f -

oc apply -f quality/data-quality-job.yaml -n $NS
oc logs -l app=epidemic-data-quality-monitor -n $NS --tail=150
```

To re-run the data quality Job after a failure:

```bash
oc delete job epidemic-data-quality-monitor -n $NS || true
oc apply -f quality/data-quality-job.yaml -n $NS
```

Live, from a pod or port-forwarded database:

```bash
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=analytics
export DB_USER=postgres
export DB_PASSWORD=<course-provided-db-password>
python quality/data_quality_monitor.py --live
```

Then verify persisted results:

```bash
oc exec -it timescaledb-0 -n $NS -- \
  psql -U postgres -d analytics -c \
  "SELECT checked_at, check_name, table_name, status, message
   FROM data_quality_checks
   ORDER BY checked_at DESC
   LIMIT 20;"
```

---

## Alerting Checks

Create/update the code ConfigMap and deployment:

```bash
oc apply -f alerting/alert-configmap.yaml -n $NS
oc apply -f alerting/alert-deployment.yaml -n $NS
```

Notification method: structured Kubernetes logs by default. Optional Slack webhook can be configured by setting `SLACK_WEBHOOK_URL` in `alerting/alert-deployment.yaml` or a Secret-backed environment variable.

Check logs:

```bash
oc get pods -l app=epidemic-alerting -n $NS
oc logs -l app=epidemic-alerting -n $NS --tail=100
```

Local demo:

```bash
python alerting/alert_service.py --demo --once
```

---

## Outbreak Prediction Checks

Local demo:

```bash
python ml/outbreak_prediction.py --demo
```

Build and push the ML image, then apply the Job:

```bash
REGISTRY=$(oc get route default-route -n openshift-image-registry \
  --template='{{ .spec.host }}')

docker build -f ml/Dockerfile \
  -t ${REGISTRY}/ds551-2026-spring-7726b8/epidemic-ml:latest .
docker push ${REGISTRY}/ds551-2026-spring-7726b8/epidemic-ml:latest

oc apply -f ml/ml-job.yaml -n $NS
oc logs -l app=epidemic-outbreak-prediction -n $NS --tail=150
```

To re-run the ML Job after a failure:

```bash
oc delete job epidemic-outbreak-prediction -n $NS || true
oc apply -f ml/ml-job.yaml -n $NS
```

Live mode from a local shell after port-forwarding the database:

```bash
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=analytics
export DB_USER=postgres
export DB_PASSWORD=<course-provided-db-password>
python ml/outbreak_prediction.py --live
```

Verify:

```bash
oc exec -it timescaledb-0 -n $NS -- \
  psql -U postgres -d analytics -c \
  "SELECT hour, region, outbreak_risk_score, outbreak_risk_level
   FROM outbreak_predictions
   ORDER BY outbreak_risk_score DESC
   LIMIT 10;"
```

---

## Visualization Checks

Demo:

```bash
jupyter notebook notebooks/phase3_visualizations.ipynb
```

Live DB mode:

```bash
export PHASE3_LIVE_DB=1
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=analytics
export DB_USER=postgres
export DB_PASSWORD=<course-provided-db-password>
jupyter notebook notebooks/phase3_visualizations.ipynb
```

---

## One-Hour Evidence Checklist

Capture screenshots/logs with timestamps showing:

1. `oc get pods` with Kafka, database, Spark/alerting components healthy.
2. Raw topic has fresh messages.
3. Tekton TaskRun logs show validation/routing.
4. Typed topics have fresh messages, including `vaccination_records`.
5. Spark job completed and wrote all analytics tables.
6. Database row counts are nonzero.
7. Alerting/data quality/ML/notebook output is connected to DB data.

---

## Troubleshooting

No vaccination records:

* Check raw topic contains `event_type=vaccination_record`.
* Check validator logs for `bad_schema_version`, `bad_dose_number`, or `bad_administered_at`.
* Check validated topic contains `vaccination_record`.
* Check route logs for `ROUTED event_type=vaccination_record`.
* Check vaccination topic offset with `kafka-get-offsets`.

Spark table empty:

* Confirm typed topic has messages.
* Confirm `etl-script` ConfigMap includes vaccination logic.
* Trigger a manual Spark job.
* Query `vaccination_trend`.
* Run `quality/data_quality_monitor.py --live` and inspect failed checks.

Alerting silent:

* Check `data_quality_checks` has recent rows.
* Lower thresholds temporarily with env vars in `alert-deployment.yaml`.
* Use `python alerting/alert_service.py --demo --once` locally to prove logic.
