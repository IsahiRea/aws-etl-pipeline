-- create_table.sql
-- Run this in Amazon Athena to register the processed S3 data as a queryable table.
-- Replace YOUR_PROCESSED_BUCKET with your actual bucket name.

CREATE DATABASE IF NOT EXISTS etl_pipeline_db;

CREATE EXTERNAL TABLE IF NOT EXISTS etl_pipeline_db.processed_data (
    provider_id           STRING,
    provider_name         STRING,
    provider_city         STRING,
    provider_street       STRING,
    provider_zip          STRING,
    provider_ruca         STRING,
    provider_ruca_desc    STRING,
    drg_code              STRING,
    drg_description       STRING,
    total_discharges      INT,
    avg_covered_charges   DOUBLE,
    avg_total_payments    DOUBLE,
    avg_medicare_payments DOUBLE,
    _etl_processed_at     TIMESTAMP,
    _etl_source           STRING
)
PARTITIONED BY (state STRING)
STORED AS PARQUET
LOCATION 's3://YOUR_PROCESSED_BUCKET/data/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');

-- After creating the table, load partitions:
MSCK REPAIR TABLE etl_pipeline_db.processed_data;
