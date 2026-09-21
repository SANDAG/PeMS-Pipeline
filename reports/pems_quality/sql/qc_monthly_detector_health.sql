-- Monthly detector health: share of expected five-minute intervals
-- (stations x nonholiday weekdays x 288) observed at or above a
-- percent_observed threshold. Uses the same record-validity filter as
-- silver_pems_station_days (samples > 0, total_flow not null) and the
-- same service-date, weekday, and holiday rules as the silver pipeline,
-- but is computed directly off bronze so it reflects interval-level
-- percent_observed rather than the per-station-day average.
-- Assumes `USE CATALOG` / `USE SCHEMA` have already set the session context.

WITH bronze_dated AS (
  SELECT
    station,
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
),

monthly AS (
  SELECT
    YEAR(service_date) AS year,
    MONTH(service_date) AS month_number,
    DATE_FORMAT(service_date, 'MMMM yyyy') AS month,
    COUNT(DISTINCT station) AS stations,
    COUNT(DISTINCT service_date) AS days,
    SUM(CASE WHEN percent_observed >= 100 THEN 1 ELSE 0 END) AS intervals_100,
    SUM(CASE WHEN percent_observed >= 80 THEN 1 ELSE 0 END) AS intervals_80,
    SUM(CASE WHEN percent_observed >= 50 THEN 1 ELSE 0 END) AS intervals_50
  FROM nonholiday_weekdays
  GROUP BY year, month_number, month
)

SELECT
  year,
  month_number,
  month,
  stations,
  days,
  stations * days * 288 AS expected_station_day_intervals,
  intervals_100 / (stations * days * 288) AS detector_health_100,
  intervals_80 / (stations * days * 288) AS detector_health_80,
  intervals_50 / (stations * days * 288) AS detector_health_50
FROM monthly
ORDER BY year, month_number
