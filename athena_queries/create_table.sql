-- create_table.sql
-- Run this in Amazon Athena to register the processed S3 data as a queryable table.
-- Replace YOUR_PROCESSED_BUCKET with your actual bucket name.

CREATE DATABASE IF NOT EXISTS etl_pipeline_db;

CREATE EXTERNAL TABLE IF NOT EXISTS etl_pipeline_db.processed_data (
    -- Update these columns to match your actual dataset schema
    provider_id         STRING,
    provider_name       STRING,
    state               STRING,
    total_discharges    INT,
    avg_covered_charges DOUBLE,
    avg_total_payments  DOUBLE,
    avg_medicare_payments DOUBLE,
    _etl_processed_at   TIMESTAMP,
    _etl_source         STRING
)
STORED AS PARQUET
LOCATION 's3://YOUR_PROCESSED_BUCKET/data/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');
