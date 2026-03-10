"""
AWS Glue ETL Job: etl_transform.py
------------------------------------
Reads raw CSV data from S3, performs cleaning and transformation,
and writes the result as Parquet to the processed S3 bucket.

To run locally (mock): pip install pyspark
To deploy: Upload to S3 and reference in Glue job config.
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

# ── Job Parameters ────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, ["JOB_NAME", "raw_bucket", "processed_bucket"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

RAW_PATH = f"s3://{args['raw_bucket']}/incoming/"
PROCESSED_PATH = f"s3://{args['processed_bucket']}/data/"

REQUIRED_COLUMNS = [
    "provider_id",
    "provider_name",
    "state",
    "total_discharges",
    "avg_covered_charges",
    "avg_total_payments",
    "avg_medicare_payments",
]


def normalize_column_name(col_name):
    return col_name.strip().lower().replace(" ", "_").replace("-", "_")


# ── Extract ───────────────────────────────────────────────────────────────────
print(f"[ETL] Reading raw data from: {RAW_PATH}")

raw_df = spark.read.option("header", "true").option("inferSchema", "true").csv(RAW_PATH)

print(f"[ETL] Raw record count: {raw_df.count()}")
print(f"[ETL] Schema:")
raw_df.printSchema()

# ── Validate ─────────────────────────────────────────────────────────────────
normalized_columns = [normalize_column_name(c) for c in raw_df.columns]
missing_columns = [col for col in REQUIRED_COLUMNS if col not in normalized_columns]

if missing_columns:
    raise ValueError(f"[ETL] Missing required columns: {missing_columns}")

print(f"[ETL] Schema validation passed. All {len(REQUIRED_COLUMNS)} required columns present.")

# ── Transform ─────────────────────────────────────────────────────────────────

# 1. Drop rows where all values are null
cleaned_df = raw_df.dropna(how="all")

# 2. Standardize column names: lowercase, replace spaces with underscores
cleaned_df = cleaned_df.toDF(*[normalize_column_name(c) for c in cleaned_df.columns])

# 3. Add metadata columns for pipeline tracking
cleaned_df = cleaned_df.withColumn("_etl_processed_at", F.current_timestamp()) \
                        .withColumn("_etl_source", F.lit(RAW_PATH))

# 4. Deduplicate records
cleaned_df = cleaned_df.dropDuplicates()

print(f"[ETL] Cleaned record count: {cleaned_df.count()}")

# ── Load ──────────────────────────────────────────────────────────────────────
print(f"[ETL] Writing processed data to: {PROCESSED_PATH}")

cleaned_df.write \
    .mode("overwrite") \
    .partitionBy("state") \
    .option("compression", "snappy") \
    .parquet(PROCESSED_PATH)

print("[ETL] Job complete.")
print(f"[ETL] Pipeline summary: {raw_df.count()} raw → {cleaned_df.count()} processed")
job.commit()
