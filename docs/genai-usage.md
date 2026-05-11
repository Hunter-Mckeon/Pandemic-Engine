# GenAI Usage Documentation

Document your use of AI tools throughout this project.

---

## AI Tools Used

ChatGPT, Claude code

## How We Used AI

### Phase 1: Tekton Pipeline

**Example prompts that worked well:**
- "Read and understand all files, tell me if I have to make changes to 
  anything, and tell me how to run the pipeline"
- "Are you sure the yaml files are correct, did you check everything"

**What AI helped with:**
- Reviewed all YAML files against the spec and identified real bugs 
  (bad JSON counter not incrementing total_seen, consumer_timeout_ms 
  too long, fixed name in pipelinerun causing re-run conflicts)
- Identified that the correct bootstrap server was kafka-team05:9092 
  and confirmed this against INSTRUCTIONS.md when questioned
- Identified that the event generator does not send event_id by reading 
  actual raw topic output and comparing field names across event types
- Drafted tekton/README.md based on real pipeline output and logs

**What AI got wrong:**
- Initially flagged kafka-team05:9092 as incorrect and suggested using 
  the shared class broker (my-cluster-kafka-bootstrap...). This was wrong. 
  INSTRUCTIONS.md explicitly says kafka-teamXX:9092 is the correct 
  per-team address and the shared broker is for A03 only. We pushed back 
  and verified directly in the spec.

### Phase 2: Spark Analytics and TimescaleDB

**Example prompts that worked well:**
- "Check if our Spark job satisfies the Phase 2 rubric."
- "Help me write the runbook and stress-test explanation based on these commands and outputs."
- "Why is the Spark job not writing to TimescaleDB, and what should I check next?"

**What AI helped with:**
- Helped structure the Spark analytics into distinct outputs: hourly event rate, severity trend, and bed availability pressure.
- Helped debug OpenShift deployment steps, including checking pods, services, PVCs, CronJobs, and Spark job logs.
- Helped write clearer documentation for `docs/phase2-runbook.md` and `docs/phase2-stress-test.md`.
- Helped explain why TimescaleDB was a strong fit for timestamped event analytics and Phase 3 queries.
- Helped turn raw terminal outputs into concise Gradescope-ready explanations.

**What AI got wrong or needed verification:**
- Some suggested commands assumed image registry access that we did not always have.
- Some generated deployment advice had to be checked against the actual OpenShift namespace, service names, and course instructions.
- We verified important commands manually with `oc get pods`, `oc logs`, `psql`, and Kafka topic checks before using them as evidence.

### Phase 2.5: Reliability and Data Quality Monitoring

**Example prompts that worked well:**
- "What is missing from our Phase 2 pipeline before Phase 3 can trust it?"
- "Help design a Phase 2.5 feature that detects stale or empty analytics tables."
- "This pod is stuck in ContainerCreating; help me debug the OpenShift event output."

**What AI helped with:**
- Helped identify data quality monitoring as a strong Phase 2.5 feature because Phase 3 alerting, ML, and visualization should not silently trust stale or empty analytics tables.
- Helped design checks for table existence, nonzero row counts, freshness, required columns, null values, severity ranges, bed availability validity, vaccination analytics, and outbreak prediction readiness.
- Helped debug the data quality Kubernetes Job when it was stuck in `ContainerCreating`.
- Correctly identified from `oc describe pod` that the `data-quality-code` ConfigMap was missing.
- Helped convert the local data quality script into an OpenShift Job that writes persistent results to the `data_quality_checks` table.

**What AI got wrong or needed verification:**
- Some initial troubleshooting guesses were broad until we inspected the actual pod events.
- The final fix came from verifying the real OpenShift error: `configmap "data-quality-code" not found`.

### Phase 3: Alerting, Outbreak Prediction, Visualization, and Change Order

**Example prompts that worked well:**
- "How do I run and implement Phase 3 and Phase 2.5?"
- "Why is the vaccination topic empty if the raw topic has vaccination_record events?"
- "The ML job is in ImagePullBackOff; can I change the image in my YAML?"
- "Help me fill the Gradescope answers using our actual evidence."

**What AI helped with:**
- Helped sequence the full Phase 3 run: Kafka checks, Tekton runs, Spark job, database row counts, data quality job, ML job, alerting deployment, and notebook screenshots.
- Helped debug the `vaccination_records` topic issue by checking local YAML, deployed Tekton tasks, deployed Tekton pipelines, Kafka topic existence, raw topic contents, and route logs.
- Helped identify that the raw topic contained both legacy `vaccination` events and newer `vaccination_record` events.
- Initially suggested a backward-compatibility alias for legacy `vaccination`
  events, but final verification corrected this approach: the official
  CO-2026-01 event is `vaccination_record`, and legacy `vaccination` remains
  unroutable/dropped because it lacks the required vaccination fields.
- Helped fix the database schema after `vaccination_trend` did not exist in the live TimescaleDB instance.
- Helped debug the ML job when the original image could not be pulled from the internal registry.
- Suggested changing `ml/ml-job.yaml` to use `python:3.11-slim` with a ConfigMap-mounted `outbreak_prediction.py`, matching the same pattern used by the data quality job.
- Helped debug the `outbreak_predictions` schema mismatch when the live table was missing `vaccination_count` and scoring columns.
- Helped prepare Gradescope-ready explanations for end-to-end integration, alerting, outbreak prediction, visualization, data quality monitoring, and CO-2026-01.

**What AI got wrong or needed verification:**
- Some early advice assumed that the internal OpenShift registry route was accessible, but our user did not have permission to read `openshift-image-registry/default-route`.
- Some logs were initially confusing because `oc logs -l tekton.dev/task=route-and-enrich` mixed outputs from old and new pods. We corrected this by checking the newest PipelineRun and exact pod logs.
- The vaccination issue required real verification from Kafka, because the existence of code support did not automatically mean the sampled live batch contained routed `vaccination_record` messages.
- Temporary/debug shortcuts helped isolate Kafka offset behavior, but the final
  implementation and evidence use official `vaccination_record` routing, not
  temp-topic evidence or legacy alias routing.
- We verified all final claims using actual `oc`, Kafka, Spark, TimescaleDB, data quality, ML, alerting, and notebook outputs.

---

## Reflection

AI tools were useful throughout this project, but they were most helpful when used as a debugging and explanation partner rather than as something to trust blindly. ChatGPT and Claude helped us understand the rubric, break down the system into smaller steps, write clearer documentation, and debug errors faster. They were especially helpful for translating long OpenShift/Kafka/Spark errors into a practical next command to run.

The biggest learning was that AI suggestions still need to be verified against the actual course infrastructure. Several times, the first suggestion was close but not exactly correct for our namespace, Kafka service, registry permissions, or live database schema. We learned to always confirm with real evidence: `oc get pods`, `oc logs`, `oc describe pod`, Kafka console consumers, and SQL queries against TimescaleDB.

Using AI also improved our documentation. Instead of only pasting commands, we were able to explain why each component exists, how data flows through the system, and what the failure modes mean. For example, the data quality monitor did not just show success; it also correctly flagged stale analytics and empty vaccination analytics, which became part of our reliability story.

Overall, AI increased our productivity and helped us debug faster, but the final system understanding came from combining AI guidance with manual verification, team judgment, and real cluster evidence.

---
