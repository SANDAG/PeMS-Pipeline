Purpose
-------
Quarto QA/QC report for PeMS data used by the pipeline.

Data flow
---------
bronze_raw_pems
      ↓
silver_pems_station_days
      ↓
silver_pems_quality
      ↓
gold_pems_weekday_counts

The report primarily summarizes silver_pems_quality.

Rendering
---------
uv run quarto render reports/pems_quality

Output
------
docs/pems_quality/