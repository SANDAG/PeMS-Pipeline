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

An interactive map of 2022-2025 weekday volume trends by station is built by
`analysis/station_volume_trends.py` and published at
https://sandag.github.io/PeMS-Pipeline/station_volume_trends/.

## Development

See `docs/developer_setup.md` for environment setup, Databricks bundle
deployment, and development workflow.