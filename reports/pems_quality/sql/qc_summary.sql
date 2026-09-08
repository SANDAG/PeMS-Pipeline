-- Station-day counts by overall QC result.
-- Assumes `USE CATALOG` / `USE SCHEMA` have already set the session context.
SELECT qc_pass, COUNT(*) AS station_days
FROM silver_pems_quality
GROUP BY qc_pass
