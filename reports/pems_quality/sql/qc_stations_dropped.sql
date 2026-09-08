-- Stations with raw bronze records that are entirely filtered out before
-- station-day aggregation (the samples > 0 AND total_flow IS NOT NULL
-- filters in transformations/02-silver_pems_station_days.py) -- i.e.
-- stations invisible to every downstream table.
-- Assumes `USE CATALOG` / `USE SCHEMA` have already set the session context.
SELECT
  COUNT(DISTINCT station) AS total_stations,
  COUNT(DISTINCT CASE WHEN samples > 0 AND total_flow IS NOT NULL THEN station END)
    AS stations_with_surviving_rows
FROM bronze_raw_pems
