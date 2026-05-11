# Phase 3 Visualization Guide

## Notebook

Primary notebook:

```text
notebooks/phase3_visualizations.ipynb
```

The notebook runs in demo mode by default and uses built-in sample data. To run
against TimescaleDB, set:

```bash
export PHASE3_LIVE_DB=1
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=analytics
export DB_USER=postgres
export DB_PASSWORD=<password>
jupyter notebook notebooks/phase3_visualizations.ipynb
```

On Windows PowerShell:

```powershell
$env:PHASE3_LIVE_DB="1"
$env:DB_HOST="localhost"
$env:DB_PORT="5432"
$env:DB_NAME="analytics"
$env:DB_USER="postgres"
$env:DB_PASSWORD="<password>"
jupyter notebook notebooks\phase3_visualizations.ipynb
```

## Charts

1. **Hourly Event Rate by Event Type**
   - Source: `hourly_event_rate`
   - Shows the volume of symptom, clinic, admission, and vaccination events.

2. **Severity Trend by Region**
   - Source: `severity_trend`
   - Shows whether symptom/admission severity is rising over time.

3. **Bed Availability Pressure**
   - Source: `bed_availability`
   - Shows minimum available beds by region.

4. **Outbreak Risk Score**
   - Source: `outbreak_predictions`
   - Shows the Phase 3 risk score generated from event volume, severity, bed
     pressure, and vaccination activity.

5. **Vaccination Analytics**
   - Source: `vaccination_trend`
   - Shows vaccination counts by region, `vaccine_type`, and `dose_number`.

6. **Pipeline / Data Quality Health**
   - Source: `data_quality_checks`
   - Shows PASS/WARN/FAIL status for freshness, row counts, and domain checks.

## Tables Used

- `hourly_event_rate`
- `severity_trend`
- `bed_availability`
- `vaccination_trend`
- `outbreak_predictions`
- `data_quality_checks`

## What to Screenshot for Gradescope

- Notebook first cell showing demo or live mode.
- The six charts.
- If using live mode, a terminal showing `oc port-forward` or DB connection
  setup and a DB query proving row counts are nonzero.
