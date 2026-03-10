"""Tests for Glue ETL transform logic."""

import pytest

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

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


def normalize_column_name(col_name):
    """Mirror of the function in etl_transform.py."""
    return col_name.strip().lower().replace(" ", "_").replace("-", "_")


# --- Pure-Python tests (always run) ---


def test_normalize_column_name():
    assert normalize_column_name("Provider Name") == "provider_name"
    assert normalize_column_name("  State  ") == "state"
    assert normalize_column_name("avg-covered-charges") == "avg_covered_charges"
    assert normalize_column_name("already_clean") == "already_clean"


def test_schema_validation_catches_missing_columns():
    required = ["provider_id", "provider_name", "state"]
    actual = ["provider_id", "provider_name"]
    missing = [col for col in required if col not in actual]
    assert missing == ["state"]


def test_schema_validation_passes_with_all_columns():
    required = ["provider_id", "provider_name", "state"]
    actual = ["provider_id", "provider_name", "state", "extra_col"]
    missing = [col for col in required if col not in actual]
    assert missing == []


# --- Spark-dependent tests (skipped when Java unavailable) ---


@requires_spark
def test_drops_all_null_rows(spark):
    df = spark.createDataFrame(
        [("10001", "Hospital A", "TX"), (None, None, None)],
        ["provider_id", "provider_name", "state"],
    )
    result = df.dropna(how="all")
    assert result.count() == 1
    assert result.first()["provider_id"] == "10001"


@requires_spark
def test_keeps_partial_null_rows(spark):
    df = spark.createDataFrame(
        [("10001", None, "TX"), (None, None, None)],
        ["provider_id", "provider_name", "state"],
    )
    result = df.dropna(how="all")
    assert result.count() == 1


@requires_spark
def test_column_normalization_applied(spark):
    df = spark.createDataFrame(
        [("10001", "Hospital A")],
        ["Provider ID", "Provider Name"],
    )
    normalized = df.toDF(*[normalize_column_name(c) for c in df.columns])
    assert normalized.columns == ["provider_id", "provider_name"]


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
    assert set(result.select("state").distinct().toPandas()["state"]) == {"TX", "CA"}
