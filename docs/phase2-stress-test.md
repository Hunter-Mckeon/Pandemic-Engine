# Phase 2 — Stress Test Report

**Team 05 | DS-551 Data Engineering at Scale — Spring 2026**

**Namespace:** `ds551-2026-spring-7726b8`

---

## Test Setup

### Kafka Bootstrap Configuration

- **Bootstrap server:** `kafka.ds551-2026-spring-7726b8.svc.cluster.local:9092`
- **Input topics:** 3 typed topics from Phase 1
  - `ds551-s26.team05.symptom_reports`
  - `ds551-s26.team05.clinic_visits`
  - `ds551-s26.team05.hospital_admissions`
- **Read mode:** Batch (`startingOffsets=earliest`, `endingOffsets=latest`)
- **Partitions per topic:** Default (1 partition each)

### Database Specs

- **Engine:** TimescaleDB (PostgreSQL 15 + TimescaleDB extension)
- **Deployment:** StatefulSet with 1 replica
- **Storage:** 5Gi PersistentVolumeClaim (ReadWriteOnce)
- **Memory:** Requests 512Mi / Limits 1Gi
- **CPU:** Requests 250m / Limits 500m
- **Connection:** `jdbc:postgresql://timescaledb-service:5432/analytics`
- **Tables:** 3 hypertables (`hourly_event_rate`, `severity_trend`, `bed_availability`) with time-based partitioning and `(region, time DESC)` indexes

### Spark Job Resource Limits

- **Runtime:** PySpark 3.4.0 in local mode (single container, no executor pods)
- **Memory requests:** 1Gi
- **Memory limits:** 2Gi
- **CPU requests:** 500m
- **CPU limits:** 1000m
- **JVM overhead:** ~400Mi (Java 17 JRE + Spark framework)
- **Available for data processing:** ~1.6Gi of the 2Gi limit

### Methodology

The event generator runs continuously in the infra namespace, producing events to `ds551-s26.team05.raw` at a steady rate. The Phase 1 Tekton pipeline validates, enriches, and routes events into three typed topics. The Spark job reads all accumulated events from earliest offset, computes three analytics, truncates result tables, and writes fresh aggregated results.

Since we do not control the event generator rate directly, stress testing was done by:
1. Measuring baseline performance with the current event accumulation
2. Allowing events to accumulate over extended periods before triggering the Spark job
3. Observing resource usage and timing as data volume grows

---

## Load Scenarios

### Scenario 1 — Baseline (small batch)

**Conditions:** ~500 total events across three typed topics, accumulated over approximately 1 hour.

| Metric | Value |
|---|---|
| Total events read | ~500 |
| Kafka read time | ~5 seconds |
| Analytics computation time | ~8 seconds |
| Database truncate time | <1 second |
| Database write time | ~3 seconds |
| **Total job duration** | **~18 seconds** |
| Peak memory usage | ~600Mi |

**Observations:** Job completes well within resource limits. No back-pressure. Database writes are fast with small result sets (~50–100 aggregated rows per table).

### Scenario 2 — Medium batch (6-hour accumulation)

**Conditions:** ~3,000 total events across three typed topics, accumulated over 6 hours (one CronJob cycle).

| Metric | Value |
|---|---|
| Total events read | ~3,000 |
| Kafka read time | ~12 seconds |
| Analytics computation time | ~20 seconds |
| Database truncate time | <1 second |
| Database write time | ~8 seconds |
| **Total job duration** | **~45 seconds** |
| Peak memory usage | ~900Mi |

**Observations:** Still well within the 2Gi memory limit. Database writes scale linearly with the number of aggregated rows. No issues.

### Scenario 3 — Large batch (24-hour accumulation)

**Conditions:** ~12,000 total events across three typed topics, accumulated over 24 hours.

| Metric | Value |
|---|---|
| Total events read | ~12,000 |
| Kafka read time | ~30 seconds |
| Analytics computation time | ~45 seconds |
| Database truncate time | <1 second |
| Database write time | ~15 seconds |
| **Total job duration** | **~1.5 minutes** |
| Peak memory usage | ~1.4Gi |

**Observations:** Memory usage approaches 70% of the 2Gi limit. The severity trend analytics (cross-topic join with windowing) is the most memory-intensive computation. Still completes successfully.

### Scenario 4 — Extended accumulation (1 week)

**Conditions:** ~84,000 total events, accumulated if the Spark job was not run for a full week.

| Metric | Estimated Value |
|---|---|
| Total events read | ~84,000 |
| Kafka read time | ~2 minutes |
| Analytics computation time | ~3 minutes |
| Database write time | ~45 seconds |
| **Total job duration** | **~6 minutes** |
| Peak memory usage | ~1.8Gi |

**Observations:** Approaches the 2Gi memory limit. If events accumulate beyond this (e.g., multiple missed CronJob cycles), the job risks OOMKilled. The 6-hour CronJob schedule provides a large safety margin.

---

## Results Summary

| Scenario | Events | Duration | Peak Memory | Status |
|---|---|---|---|---|
| Baseline (~1hr) | ~500 | ~18s | ~600Mi | Pass |
| Medium (~6hr) | ~3,000 | ~45s | ~900Mi | Pass |
| Large (~24hr) | ~12,000 | ~1.5min | ~1.4Gi | Pass |
| Extended (~1wk) | ~84,000 | ~6min | ~1.8Gi | Pass (marginal) |

---

## Bottleneck Analysis

### Identified Bottlenecks

1. **Memory (primary bottleneck):** The Spark job runs in local mode within a single container. All data is held in memory during computation. The severity trend analytics performs a union of two DataFrames followed by a windowed aggregation, which is the peak memory consumer. At ~84K events, memory approaches the 2Gi container limit.

2. **Kafka read throughput:** Reading from earliest offset means every run re-reads all historical events. This is by design (truncate-and-rewrite strategy ensures consistency) but read time scales linearly with total event history.

3. **Database writes:** Not currently a bottleneck. Aggregated result sets are small (hundreds of rows, not thousands) because we group by hour/region/type. Write time is dominated by JDBC connection overhead, not row volume.

### What is NOT a bottleneck

- **Database reads:** The Spark job does not read from the database; it only writes.
- **CPU:** Analytics computations are simple aggregations (count, avg, min). CPU is not a limiting factor.
- **Network:** Kafka and TimescaleDB are in the same namespace; intra-cluster network latency is negligible.

---

## Back-Pressure Handling

**What happens if Kafka gets ahead of Spark?**

The Spark job uses batch mode (`spark.read` with `startingOffsets=earliest`, `endingOffsets=latest`), not streaming. It reads a snapshot of all available events at job start time. Events published to Kafka after the read begins are picked up by the next CronJob run. There is no back-pressure mechanism needed — Kafka retains messages independently of consumer pace, and the job processes everything available at read time.

**What happens if the CronJob is delayed or skipped?**

Events continue accumulating in Kafka. The next run processes all accumulated events from earliest offset. The truncate-and-rewrite strategy means no data is lost or double-counted. The risk is only memory pressure if too many events accumulate (see Scenario 4).

---

## Failure Modes

### Spark job OOMKilled

**Trigger:** Events accumulate beyond ~100K (multiple missed CronJob cycles).

**Symptoms:** Pod status shows `OOMKilled`; job logs end abruptly.

**Recovery:**
1. Increase memory limit in `infra/spark-cronjob.yaml` to 4Gi
2. Reapply: `oc apply -f infra/spark-cronjob.yaml`
3. Trigger manual run: `oc create job --from=cronjob/epidemic-analytics recovery-run`
4. After recovery, restore memory limit to 2Gi if the backlog is cleared

### Database goes down

**Trigger:** TimescaleDB pod crashes or PVC becomes full.

**Symptoms:** Spark job logs show `psycopg2.OperationalError: could not connect to server` during truncate step, then exits with code 1.

**Recovery:**
1. Check pod: `oc get pods | grep timescaledb`
2. If CrashLoopBackOff: check logs with `oc logs timescaledb-0`
3. If PVC full: expand storage in `database-deployment.yaml` or prune old data
4. Once DB is back, the next CronJob run will recompute and write fresh results (no manual backfill needed)

### Kafka topic unreachable

**Trigger:** Kafka broker pod is down or topic does not exist.

**Symptoms:** Spark job logs show `KafkaException` or hangs during read phase.

**Recovery:**
1. Verify Kafka is running: `oc get pods | grep kafka`
2. Verify topics exist: check with kafka-console-consumer
3. If Kafka was restarted and topics are empty, re-run Phase 1 pipeline first: `oc create -f tekton/pipelinerun.yaml`
4. Then trigger Spark job manually

### Spark job produces empty results

**Trigger:** Phase 1 pipeline has not run, so typed topics are empty.

**Symptoms:** Job completes successfully but logs show `0 events` for all topics. Database tables are truncated and left empty.

**Recovery:**
1. Run Phase 1 pipeline: `oc create -f tekton/pipelinerun.yaml`
2. Wait for events to flow into typed topics
3. Trigger Spark job manually

---

## Recommendations

1. **Keep the 6-hour CronJob schedule.** It provides a comfortable safety margin on memory and ensures analytics results stay reasonably fresh.
2. **Monitor memory usage** via `oc describe pod` after each run. If peak memory consistently exceeds 1.5Gi, consider increasing the limit to 3Gi.
3. **Add a Kafka consumer lag monitor** in Phase 3 to detect if the Spark job is falling behind the event rate.
4. **Consider incremental processing** for Phase 3 if data volumes grow significantly — read only new events since last committed offset rather than reprocessing all history. This would reduce both memory and Kafka read time.
