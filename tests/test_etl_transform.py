"""Tests for Glue ETL transform logic."""

import pytest

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType, StructField, StringType

    _spark_session = (
        SparkSession.builder.master("local[1]")
        .appName("etl-tests")
        .getOrCreate()
    )
    SPARK_AVAILABLE = True
except Exception:
    _spark_session = None
    SPARK_AVAILABLE = False

requires_spark = pytest.mark.skipif(
    not SPARK_AVAILABLE, reason="PySpark/Java not available"
)


@pytest.fixture(scope="module")
def spark():
    """Provide the local SparkSession for testing."""
    if _spark_session is None:
        pytest.skip("SparkSession could not be created")
    yield _spark_session


# CMS column mapping (mirrors COLUMN_MAPPING in etl_transform.py)
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


# --- Pure-Python tests (always run) ---


def test_column_mapping_has_no_duplicate_targets():
    targets = [m[2] for m in COLUMN_MAPPING]
    assert len(targets) == len(set(targets))


def test_column_mapping_has_no_duplicate_sources():
    sources = [m[0] for m in COLUMN_MAPPING]
    assert len(sources) == len(set(sources))


def test_column_mapping_contains_required_fields():
    targets = {m[2] for m in COLUMN_MAPPING}
    required = {"provider_id", "provider_name", "state", "total_discharges",
                "avg_covered_charges", "avg_total_payments", "avg_medicare_payments"}
    assert required.issubset(targets)


# --- Spark-dependent tests (skipped when Java unavailable) ---


@requires_spark
def test_drops_all_null_rows(spark):
    schema = StructType([
        StructField("provider_id", StringType(), True),
        StructField("provider_name", StringType(), True),
        StructField("state", StringType(), True),
    ])
    df = spark.createDataFrame(
        [("10001", "Hospital A", "TX"), (None, None, None)],
        schema,
    )
    result = df.dropna(how="all")
    assert result.count() == 1
    assert result.first()["provider_id"] == "10001"


@requires_spark
def test_keeps_partial_null_rows(spark):
    schema = StructType([
        StructField("provider_id", StringType(), True),
        StructField("provider_name", StringType(), True),
        StructField("state", StringType(), True),
    ])
    df = spark.createDataFrame(
        [("10001", None, "TX"), (None, None, None)],
        schema,
    )
    result = df.dropna(how="all")
    assert result.count() == 1


@requires_spark
def test_column_rename_via_mapping(spark):
    """Simulate what ApplyMapping does: rename CMS columns to pipeline names."""
    df = spark.createDataFrame(
        [("010001", "Hospital A", "TX")],
        ["Rndrng_Prvdr_CCN", "Rndrng_Prvdr_Org_Name", "Rndrng_Prvdr_State_Abrvtn"],
    )
    renamed = df.toDF("provider_id", "provider_name", "state")
    assert renamed.columns == ["provider_id", "provider_name", "state"]
    assert renamed.first()["provider_id"] == "010001"


@requires_spark
def test_deduplication(spark):
    df = spark.createDataFrame(
        [
            ("10001", "Hospital A", "TX"),
            ("10001", "Hospital A", "TX"),
            ("10002", "Hospital B", "CA"),
        ],
        ["provider_id", "provider_name", "state"],
    )
    result = df.dropDuplicates()
    assert result.count() == 2


@requires_spark
def test_metadata_columns_added(spark):
    df = spark.createDataFrame(
        [("10001", "Hospital A")],
        ["provider_id", "provider_name"],
    )
    result = df.withColumn("_etl_processed_at", F.current_timestamp()) \
               .withColumn("_etl_source", F.lit("s3://test/path/"))

    assert "_etl_processed_at" in result.columns
    assert "_etl_source" in result.columns
    row = result.first()
    assert row["_etl_source"] == "s3://test/path/"
    assert row["_etl_processed_at"] is not None


@requires_spark
def test_partitioned_write(spark, tmp_path):
    df = spark.createDataFrame(
        [
            ("10001", "Hospital A", "TX", 100),
            ("10002", "Hospital B", "CA", 200),
            ("10003", "Hospital C", "TX", 150),
        ],
        ["provider_id", "provider_name", "state", "total_discharges"],
    )

    output_path = str(tmp_path / "output")
    df.write.mode("overwrite").partitionBy("state").parquet(output_path)

    result = spark.read.parquet(output_path)
    assert result.count() == 3
    states = {row["state"] for row in result.select("state").distinct().collect()}
    assert states == {"TX", "CA"}
