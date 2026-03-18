-- sample_queries.sql
-- Example analytical queries to run in Amazon Athena after the pipeline has processed data.

-- 1. Preview the first 10 records
SELECT *
FROM etl_pipeline_db.processed_data
LIMIT 10;

-- 2. Total discharges by state
SELECT
    state,
    SUM(total_discharges) AS total_discharges
FROM etl_pipeline_db.processed_data
GROUP BY state
ORDER BY total_discharges DESC;

-- 3. Top 10 providers by average Medicare payments
SELECT
    provider_name,
    state,
    ROUND(avg_medicare_payments, 2) AS avg_medicare_payments
FROM etl_pipeline_db.processed_data
ORDER BY avg_medicare_payments DESC
LIMIT 10;

-- 4. Average payment gap (covered charges vs Medicare payments) by state
SELECT
    state,
    ROUND(AVG(avg_covered_charges - avg_medicare_payments), 2) AS avg_payment_gap
FROM etl_pipeline_db.processed_data
GROUP BY state
ORDER BY avg_payment_gap DESC;

-- 5. Top 10 DRGs by total discharges nationwide
SELECT
    drg_code,
    drg_description,
    SUM(total_discharges) AS total_discharges,
    ROUND(AVG(avg_medicare_payments), 2) AS avg_medicare_payments
FROM etl_pipeline_db.processed_data
GROUP BY drg_code, drg_description
ORDER BY total_discharges DESC
LIMIT 10;

-- 6. Count of records processed per ETL run
SELECT
    DATE_TRUNC('minute', _etl_processed_at) AS etl_run_time,
    COUNT(*) AS records_processed
FROM etl_pipeline_db.processed_data
GROUP BY DATE_TRUNC('minute', _etl_processed_at)
ORDER BY etl_run_time DESC;
