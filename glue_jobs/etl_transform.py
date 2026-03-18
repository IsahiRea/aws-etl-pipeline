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
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import ApplyMapping
from pyspark.sql import functions as F

# ── Job Parameters ────────────────────────────────────────────────────────────
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "raw_bucket", "processed_bucket", "catalog_database", "catalog_table"],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

RAW_PATH = f"s3://{args['raw_bucket']}/incoming/"
PROCESSED_PATH = f"s3://{args['processed_bucket']}/data/"
CATALOG_DATABASE = args["catalog_database"]
CATALOG_TABLE = args["catalog_table"]

# CMS Medicare column name → pipeline column name + target type
COLUMN_MAPPING = [
    ("Rndrng_Prvdr_CCN", "string", "provider_id", "string"),
    ("Rndrng_Prvdr_Org_Name", "string", "provider_name", "string"),
    ("Rndrng_Prvdr_City", "string", "provider_city", "string"),
    ("Rndrng_Prvdr_St", "string", "provider_street", "string"),
    ("Rndrng_Prvdr_State_Abrvtn", "string", "state", "string"),
    ("Rndrng_Prvdr_Zip5", "string", "provider_zip", "string"),
    ("Rndrng_Prvdr_RUCA", "string", "provider_ruca", "string"),
    ("Rndrng_Prvdr_RUCA_Desc", "string", "provider_ruca_desc", "string"),
    ("DRG_Cd", "string", "drg_code", "string"),
    ("DRG_Desc", "string", "drg_description", "string"),
    ("Tot_Dschrgs", "string", "total_discharges", "int"),
    ("Avg_Submtd_Cvrd_Chrg", "string", "avg_covered_charges", "double"),
    ("Avg_Tot_Pymt_Amt", "string", "avg_total_payments", "double"),
    ("Avg_Mdcr_Pymt_Amt", "string", "avg_medicare_payments", "double"),
]


# ── Extract ───────────────────────────────────────────────────────────────────
print(f"[ETL] Reading raw data from: {RAW_PATH}")

raw_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": [RAW_PATH],
        "recurse": True,
    },
    format="csv",
    format_options={"withHeader": True},
    transformation_ctx="raw_dyf",
)

print(f"[ETL] Raw record count: {raw_dyf.count()}")
print(f"[ETL] Schema:")
raw_dyf.printSchema()

# ── Validate & Transform ────────────────────────────────────────────────────

# 1. ApplyMapping: renames CMS columns, casts types, and validates schema
#    (fails if a source column is missing from the data)
mapped_dyf = ApplyMapping.apply(
    frame=raw_dyf,
    mappings=COLUMN_MAPPING,
    transformation_ctx="mapped_dyf",
)

print(f"[ETL] Schema mapping applied. Columns: {[f[2] for f in COLUMN_MAPPING]}")

cleaned_df = mapped_dyf.toDF()

# 2. Drop rows where all values are null
cleaned_df = cleaned_df.dropna(how="all")

# 3. Add metadata columns for pipeline tracking
cleaned_df = cleaned_df.withColumn("_etl_processed_at", F.current_timestamp()) \
                        .withColumn("_etl_source", F.lit(RAW_PATH))

# 4. Deduplicate records
cleaned_df = cleaned_df.dropDuplicates()

print(f"[ETL] Cleaned record count: {cleaned_df.count()}")

# ── Load ──────────────────────────────────────────────────────────────────────
print(f"[ETL] Writing processed data to: {PROCESSED_PATH}")

cleaned_dyf = DynamicFrame.fromDF(cleaned_df, glueContext, "cleaned_dyf")

sink = glueContext.getSink(
    path=PROCESSED_PATH,
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["state"],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="sink",
)
sink.setCatalogInfo(
    catalogDatabase=CATALOG_DATABASE,
    catalogTableName=CATALOG_TABLE,
)
sink.setFormat("glueparquet")
sink.writeFrame(cleaned_dyf)

print("[ETL] Job complete.")
print(f"[ETL] Pipeline summary: {raw_dyf.count()} raw → {cleaned_df.count()} processed")
job.commit()
