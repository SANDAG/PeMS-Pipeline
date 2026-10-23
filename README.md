# PeMS Pipeline

Databricks Lakeflow pipeline for processing PeMS traffic count data and
producing analysis-ready weekday traffic counts.

## Pipeline

Raw PeMS
→ Bronze
→ Silver station-day processing
→ QA/QC
→ Gold weekday counts

## Reporting

A Quarto QA/QC report is available under `reports/pems_quality/`.

An interactive map of 2022-2025 weekday volume trends by station, with a
switch to color stations by average weekday speed instead, is built by
`analysis/station_volume_trends.py` and published at
https://sandag.github.io/PeMS-Pipeline/station_volume_trends/.

### Running the station volume trends script

The script reads from Databricks. Settings come from a `.env` file in the repo
root (git-ignored), loaded with `uv run --env-file .env`:

```
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/<warehouse id>
DATABRICKS_CONFIG_PROFILE=<profile name>
```

`DATABRICKS_HTTP_PATH` is required (Databricks > SQL Warehouses > Connection
details). `DATABRICKS_CONFIG_PROFILE` is optional (default `dev`); use any
valid profile from `databricks auth profiles`.

Build the map and CSVs:

```bash
uv run --env-file .env python analysis/station_volume_trends.py
```

Limit to specific lane types (e.g. ML mainline, HV HOV):

```bash
uv run --env-file .env python analysis/station_volume_trends.py --lane-types ML,HV
```

## Development

See `docs/developer_setup.md` for environment setup, Databricks bundle
deployment, and development workflow.