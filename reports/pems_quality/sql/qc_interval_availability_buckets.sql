-- Distribution of station-days by % of the 288 daily 5-minute intervals
-- present (periods_day / 288), bucketed. More informative than a binary
-- complete/incomplete count -- shows whether incomplete days are mostly
-- near-complete or mostly empty. Scoped to 2025 to match the rest of this
-- report's narrative (see reports/pems_quality/index.qmd); the pipeline
-- itself covers 2022-2025.
-- Assumes `USE CATALOG` / `USE SCHEMA` have already set the session context.
WITH pct AS (
  SELECT ROUND(periods_day * 100.0 / 288, 2) AS pct_intervals_present
  FROM silver_pems_quality
  WHERE year = 2025
)
SELECT
  CASE
    WHEN pct_intervals_present = 100 THEN '100'
    WHEN pct_intervals_present >= 75 THEN '75-99'
    WHEN pct_intervals_present >= 50 THEN '50-75'
    WHEN pct_intervals_present >= 25 THEN '25-50'
    WHEN pct_intervals_present > 0 THEN '<25'
    ELSE '0'
  END AS bucket,
  COUNT(*) AS station_days
FROM pct
GROUP BY bucket
