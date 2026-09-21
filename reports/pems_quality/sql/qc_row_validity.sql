-- Raw 5-minute record validity: how many bronze rows fail basic sanity
-- checks -- the same conditions filtered out before station-day
-- aggregation in transformations/02-silver_pems_station_days.py -- plus a
-- couple of extra checks worth tracking (negative flow, missing
-- percent_observed). Scoped to 2025 to match the rest of this report's
-- narrative (see reports/pems_quality/index.qmd); the pipeline itself
-- covers 2022-2025.
-- Assumes `USE CATALOG` / `USE SCHEMA` have already set the session context.
SELECT
  COUNT(*) AS total_rows,
  SUM(CASE WHEN timestamp IS NULL THEN 1 ELSE 0 END) AS null_timestamp,
  SUM(CASE WHEN station IS NULL THEN 1 ELSE 0 END) AS null_station_ID,
  SUM(CASE WHEN samples IS NULL OR samples <= 0 THEN 1 ELSE 0 END) AS zero_samples,
  SUM(CASE WHEN total_flow IS NULL THEN 1 ELSE 0 END) AS null_total_flow,
  SUM(CASE WHEN total_flow < 0 THEN 1 ELSE 0 END) AS negative_total_flow,
  SUM(CASE WHEN percent_observed IS NULL THEN 1 ELSE 0 END) AS null_percent_observed
FROM bronze_raw_pems
WHERE YEAR(timestamp) = 2025
