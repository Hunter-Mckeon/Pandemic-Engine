# Phase 3 Features Scorecard

This scorecard maps Team 05 Phase 3 grading categories to repo evidence and the
final collected CO-2026-01 evidence.

## Final Evidence Summary

Tekton vaccination evidence:

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

## Q2 End-to-End Integration

Claim: Event Generator -> Kafka -> Tekton -> typed Kafka topics -> Spark ->
TimescaleDB -> Phase 3 features.

Repo evidence:

- `tekton/tasks.yaml`
- `tekton/pipeline.yaml`
- `tekton/pipelinerun.yaml`
- `tekton/pipelinerun-vax-real.yaml`
- `tekton/route-vax-real-logs-taskrun.yaml`
- `spark/etl.py`
- `infra/database-deployment.yaml`
- `infra/database-init.sql`
- `infra/spark-cronjob.yaml`
- `docs/PHASE3-DATAFLOW.md`
- `docs/phase3-runbook.md`

Final evidence:

- Tekton validation forwarded `vaccination_record` to the validated topic.
- Tekton routing sent `vaccination_record` to
  `ds551-s26.team05.vaccination_records`.
- Spark rerun populated all analytics and Phase 3 tables listed above.

## Q3 Category A: Advanced Alerting

Claim: Advanced alerting monitors multiple metrics with severity levels and
cooldown/suppression.

Repo evidence:

- `alerting/alert_service.py`
- `alerting/alert-deployment.yaml`
- `alerting/alert-configmap.yaml`

Implemented monitors:

- event-rate thresholds from `hourly_event_rate`
- severity thresholds from `severity_trend`
- bed-pressure thresholds from `bed_availability`
- vaccination low/surge thresholds from `vaccination_trend`
- stale/unhealthy pipeline alerts from `data_quality_checks`
- structured Kubernetes logs by default
- optional Slack webhook through `SLACK_WEBHOOK_URL`

Final evidence:

- Alerting code reads the populated Phase 3 tables.
- `data_quality_checks` contained `63` rows in the final database count.
- Local demo command remains available:

```bash
python alerting/alert_service.py --demo --once
```

## Q4 Category B: Outbreak Prediction

Claim: Rule-based outbreak prediction generates queryable risk scores using
Phase 2 analytics and vaccination activity.

Repo evidence:

- `ml/outbreak_prediction.py`
- `ml/Dockerfile`
- `ml/ml-job.yaml`
- `infra/database-init.sql` table `outbreak_predictions`

Features:

- event volume
- average severity
- minimum available beds
- vaccination activity from `vaccination_trend`

Final evidence:

- `outbreak_predictions | 5`
- Local demo command remains available:

```bash
python ml/outbreak_prediction.py --demo
```

## Q5 Category C: Mandatory Visualization

Claim: Mandatory notebook visualization exists and can run locally or against DB.

Repo evidence:

- `notebooks/phase3_visualizations.ipynb`
- `docs/visualization-guide.md`

Charts:

- hourly event rate
- severity trend
- bed availability
- outbreak risk
- vaccination analytics
- pipeline/data quality health

Final evidence:

- Notebook reads the same six Phase 3 tables shown in the final DB counts.
- Demo mode runs without TimescaleDB; live mode uses `PHASE3_LIVE_DB=1`.

## Q6 Category D: Data Quality Monitoring

Claim: Phase 2.5 reliability work is implemented as data quality monitoring.

Repo evidence:

- `quality/data_quality_monitor.py`
- `quality/data-quality-job.yaml`
- `phase25/feature-2/poc/pipeline_health_check.py`
- `infra/database-init.sql` table `data_quality_checks`
- `docs/PHASE3-ARCHITECTURE.md`

Checks:

- expected analytics tables exist
- row counts are nonzero
- latest timestamps are fresh
- vaccination analytics table exists
- key columns are not null
- bed availability is nonnegative
- severity values are in range
- vaccination values are valid
- outbreak prediction table exists

Final evidence:

- `data_quality_checks | 63`
- Local demo command remains available:

```bash
python quality/data_quality_monitor.py --demo
```

## Q9 Architecture Documentation

Repo evidence:

- `docs/architecture.md`
- `docs/PHASE3-ARCHITECTURE.md`
- `docs/PHASE3-DATAFLOW.md`
- `docs/phase3-runbook.md`
- `docs/visualization-guide.md`

Final evidence:

- `docs/PHASE3-DATAFLOW.md` and `docs/phase3-runbook.md` document the final
  live CO-2026-01 flow and database counts.
- `docs/architecture.md` documents component relationships, reliability controls,
  and final operational evidence.

## Q10 CO-2026-01 Vaccination Record

Claim: `vaccination_record` with `schema_version: 2` is handled end-to-end.

Repo evidence:

- Validator: `tekton/tasks.yaml`
- Routing: `tekton/tasks.yaml`
- Pipeline params: `tekton/pipeline.yaml`
- Standard PipelineRun: `tekton/pipelinerun.yaml`
- Evidence PipelineRun: `tekton/pipelinerun-vax-real.yaml`
- Evidence route TaskRun: `tekton/route-vax-real-logs-taskrun.yaml`
- Spark analytic: `spark/etl.py`
- DB table: `infra/database-init.sql`, `infra/database-deployment.yaml`
- Documentation: `docs/PHASE3-DATAFLOW.md`, `docs/PHASE3-ARCHITECTURE.md`

Official event type:

```text
vaccination_record
```

Topic:

```text
ds551-s26.team05.vaccination_records
```

Database table:

```text
vaccination_trend(hour, region, vaccine_type, dose_number, vaccination_count)
```

Final evidence:

```text
VALID forwarded event_type=vaccination_record to ds551-s26.team05.validated
ROUTED event_type=vaccination_record to ds551-s26.team05.vaccination_records
ds551-s26.team05.vaccination_records:0:616
vaccination_trend | 129
```

Placeholder `vaccination` records are not official CO-2026-01 events. They are
kept separate from `vaccination_record` and are dropped during routing because
they lack `record_id`, `patient_id`, `vaccine_type`, `dose_number`, and
`administered_at`.
