-- Monthly station-day detector health: share of station-days whose
-- average_percent_observed is at or above a threshold. Companion to
-- qc_monthly_detector_health.sql, which measures the same thing at the
-- individual 5-minute interval level instead of per-station-day average.
-- silver_pems_quality already restricts to the nonholiday September/October
-- weekday, valid-record population used throughout this report, with one
-- row per station-day.
-- Assumes `USE CATALOG` / `USE SCHEMA` have already set the session context.
SELECT
  year,
  month_number,
  DATE_FORMAT(MAKE_DATE(year, month_number, 1), 'MMMM yyyy') AS month,
  COUNT(DISTINCT station) AS stations,
  COUNT(*) AS station_days,
  SUM(CASE WHEN average_percent_observed >= 100 THEN 1 ELSE 0 END) / COUNT(*) AS detector_health_100,
  SUM(CASE WHEN average_percent_observed >= 80 THEN 1 ELSE 0 END) / COUNT(*) AS detector_health_80,
  SUM(CASE WHEN average_percent_observed >= 50 THEN 1 ELSE 0 END) / COUNT(*) AS detector_health_50
FROM silver_pems_quality
GROUP BY year, month_number
ORDER BY year, month_number
