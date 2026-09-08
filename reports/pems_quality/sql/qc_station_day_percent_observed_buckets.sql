-- Distribution of station-days by their AVERAGE percent_observed, over the
-- same nonholiday Sep-Oct 2025 weekday population as the completeness
-- checks. "100" = fully observed all day, no imputation; lower buckets =
-- station-days relying on increasing amounts of imputed/estimated values.
-- Assumes `USE CATALOG` / `USE SCHEMA` have already set the session context.
SELECT
  CASE
    WHEN average_percent_observed IS NULL THEN 'null'
    WHEN average_percent_observed >= 100 THEN '100'
    WHEN average_percent_observed >= 75 THEN '75-99'
    WHEN average_percent_observed >= 50 THEN '50-75'
    WHEN average_percent_observed >= 25 THEN '25-50'
    WHEN average_percent_observed = 0 THEN '0'
    WHEN average_percent_observed > 0 THEN '<25'
    ELSE 'negative'
  END AS bucket,
  COUNT(*) AS station_days
FROM silver_pems_quality
GROUP BY bucket
