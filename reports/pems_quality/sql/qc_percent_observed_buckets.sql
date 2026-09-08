-- Distribution of raw bronze records by percent_observed bucket. Different
-- from qc_row_validity.sql's null check -- this shows the shape of sensor
-- reliability/imputation across all raw records, not just whether the
-- value is missing.
-- Assumes `USE CATALOG` / `USE SCHEMA` have already set the session context.
SELECT
  CASE
    WHEN percent_observed IS NULL THEN 'null'
    WHEN percent_observed >= 100 THEN '100'
    WHEN percent_observed >= 75 THEN '75-99'
    WHEN percent_observed >= 50 THEN '50-75'
    WHEN percent_observed >= 25 THEN '25-50'
    WHEN percent_observed = 0 THEN '0'
    WHEN percent_observed > 0 THEN '<25'
    ELSE 'negative'
  END AS bucket,
  COUNT(*) AS records
FROM bronze_raw_pems
GROUP BY bucket
