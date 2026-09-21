-- Fine-grained distribution of average_percent_observed at the
-- station-day level, in 10-point buckets. Companion to
-- qc_percent_observed_histogram.sql, which buckets individual 5-minute
-- records instead of per-station-day averages. silver_pems_quality already
-- restricts to the nonholiday September/October weekday, valid-record
-- population (samples > 0, total_flow not null) used throughout this
-- report, with one row per station-day.
-- Assumes `USE CATALOG` / `USE SCHEMA` have already set the session context.
SELECT
  CASE
    WHEN average_percent_observed IS NULL THEN 'null'
    WHEN average_percent_observed < 0 THEN 'negative'
    WHEN average_percent_observed >= 100 THEN '100'
    ELSE
      CAST(FLOOR(average_percent_observed / 10) * 10 AS INT)
      || '-'
      || CAST(FLOOR(average_percent_observed / 10) * 10 + 9 AS INT)
  END AS bucket,
  COUNT(*) AS station_days
FROM silver_pems_quality
GROUP BY bucket
