-- Overlap between the two dominant row-drop conditions (samples > 0 and
-- total_flow IS NOT NULL): how much each filter catches independently vs.
-- together. These conditions aren't mutually exclusive, so the per-reason
-- counts in qc_row_validity.sql can't just be summed for a total. Scoped
-- to 2025 to match the rest of this report's narrative (see
-- reports/pems_quality/index.qmd); the pipeline itself covers 2022-2025.
-- Assumes `USE CATALOG` / `USE SCHEMA` have already set the session context.
SELECT
  CASE WHEN samples > 0 THEN '>0' ELSE '0 or less' END AS samples_bucket,
  CASE WHEN total_flow IS NULL THEN 'NULL' ELSE 'NOT NULL' END AS total_flow_bucket,
  COUNT(*) AS records
FROM bronze_raw_pems
WHERE YEAR(timestamp) = 2025
GROUP BY samples_bucket, total_flow_bucket
ORDER BY samples_bucket, total_flow_bucket
