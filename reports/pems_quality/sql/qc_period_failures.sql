-- Count of station-days with incomplete five-minute observations, per
-- model time period.
-- Assumes `USE CATALOG` / `USE SCHEMA` have already set the session context.
SELECT
  SUM(CASE WHEN NOT qc_complete_ea THEN 1 ELSE 0 END) AS ea_failures,
  SUM(CASE WHEN NOT qc_complete_am THEN 1 ELSE 0 END) AS am_failures,
  SUM(CASE WHEN NOT qc_complete_md THEN 1 ELSE 0 END) AS md_failures,
  SUM(CASE WHEN NOT qc_complete_pm THEN 1 ELSE 0 END) AS pm_failures,
  SUM(CASE WHEN NOT qc_complete_ev THEN 1 ELSE 0 END) AS ev_failures
FROM silver_pems_quality
