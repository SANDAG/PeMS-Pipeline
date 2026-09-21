-- Fine-grained distribution of percent_observed at the individual
-- 5-minute record level, over the same nonholiday September/October
-- weekday, valid-record population used in qc_monthly_detector_health.sql
-- (this is what that table's health percentages are computed from).
-- Assumes `USE CATALOG` / `USE SCHEMA` have already set the session context.

WITH bronze_dated AS (
  SELECT
    percent_observed,
    CASE
      WHEN (HOUR(timestamp) * 60 + MINUTE(timestamp)) < 180
        THEN DATE_SUB(TO_DATE(timestamp), 1)
      ELSE TO_DATE(timestamp)
    END AS service_date
  FROM bronze_raw_pems
  WHERE timestamp IS NOT NULL
    AND station IS NOT NULL
    AND samples > 0
    AND total_flow IS NOT NULL
),

nonholiday_weekdays AS (
  SELECT b.*
  FROM bronze_dated b
  LEFT ANTI JOIN ref_pems_holidays h
    ON b.service_date = h.holiday_date
  WHERE YEAR(b.service_date) IN (2022, 2023, 2024, 2025)
    AND MONTH(b.service_date) IN (9, 10)
    AND DAYOFWEEK(b.service_date) NOT IN (1, 7)
)

SELECT
  CASE
    WHEN percent_observed IS NULL THEN 'null'
    WHEN percent_observed < 0 THEN 'negative'
    WHEN percent_observed >= 100 THEN '100'
    ELSE
      CAST(FLOOR(percent_observed / 10) * 10 AS INT)
      || '-'
      || CAST(FLOOR(percent_observed / 10) * 10 + 9 AS INT)
  END AS bucket,
  COUNT(*) AS records
FROM nonholiday_weekdays
GROUP BY bucket
