import pytest
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable
import pyspark.sql.functions as F
from unittest.mock import patch
from datetime import datetime

from esource_pipeline import (
    read_csv_to_df,
    hash_dataframe,
    calculate_file_hash,
    check_file_already_loaded,
    validate_primary_key,
    write_or_merge_to_delta,
    log_file_load,
)


@pytest.fixture(scope="session")
def spark():
    spark_session = SparkSession.builder \
        .master("local[2]") \
        .appName("unit-tests") \
        .getOrCreate()
    yield spark_session
    spark_session.stop()


def test_read_csv_to_df(spark):
    df = read_csv_to_df(spark, "tests/data/sample.csv")
    assert df.count() == 3
    assert set(df.columns) == {"id", "name", "age"}


def test_hash_dataframe(spark):
    df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
    hashed_df = hash_dataframe(df)

    assert "row_hash" in hashed_df.columns
    assert hashed_df.select("row_hash").distinct().count() == 2


def test_calculate_file_hash(spark):
    df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
    df = hash_dataframe(df)

    file_hash = calculate_file_hash(df)
    assert isinstance(file_hash, str)
    assert len(file_hash) == 64


def test_check_file_already_loaded_empty(spark):
    load_log_table = "test.load_log"

    spark.sql(f"DROP TABLE IF EXISTS {load_log_table}")
    result = check_file_already_loaded(spark, load_log_table, "dummy_hash")

    assert not result


def test_check_file_already_loaded_present(spark):
    load_log_table = "test.load_log"
    spark.createDataFrame(
        [("file1.csv", "existing_hash", datetime.now())],
        schema="file_name STRING, file_hash STRING, load_time TIMESTAMP"
    ).write.format("delta").mode("overwrite").saveAsTable(load_log_table)

    assert check_file_already_loaded(spark, load_log_table, "existing_hash")
    assert not check_file_already_loaded(spark, load_log_table, "new_hash")


def test_validate_primary_key_no_issues(spark):
    df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
    validate_primary_key(df, "id")  # Should not raise any error


def test_validate_primary_key_duplicate(spark):
    df = spark.createDataFrame([(1, "Alice"), (1, "Bob")], ["id", "name"])
    with pytest.raises(ValueError, match="Duplicate primary keys detected"):
        validate_primary_key(df, "id")


def test_validate_primary_key_nulls(spark):
    df = spark.createDataFrame([(None, "Alice"), (2, "Bob")], ["id", "name"])
    with pytest.raises(ValueError, match="Found NULL primary keys"):
        validate_primary_key(df, "id")


@patch("your_module.DeltaTable")
def test_write_or_merge_to_delta_new_table(mock_delta, spark):
    table_name = "test.new_table"
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    df = spark.createDataFrame([(1, "Alice")], ["id", "name"])
    write_or_merge_to_delta(spark, df, table_name, "id")

    assert spark.table(table_name).count() == 1


@patch("your_module.DeltaTable")
def test_log_file_load(spark):
    load_log_table = "test.load_log"
    spark.sql(f"DROP TABLE IF EXISTS {load_log_table}")

    log_file_load(spark, load_log_table, "test_file.csv", "dummy_hash")

    log_df = spark.table(load_log_table)
    assert log_df.count() == 1
    assert log_df.filter(F.col("file_hash") == "dummy_hash").count() == 1
