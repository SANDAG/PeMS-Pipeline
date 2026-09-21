-- Station-day counts by overall QC result. Scoped to 2025 to match the
-- rest of this report's narrative (see reports/pems_quality/index.qmd);
-- the pipeline itself covers 2022-2025.
-- Assumes `USE CATALOG` / `USE SCHEMA` have already set the session context.
SELECT qc_pass, COUNT(*) AS station_days
FROM silver_pems_quality
WHERE year = 2025
GROUP BY qc_pass
