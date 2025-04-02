from pyspark.sql import SparkSession, DataFrame

def read_incremental_changes(spark: SparkSession, table_name: str, last_version: int) -> DataFrame:
    return spark.read.format("delta") \
        .option("readChangeFeed", "true") \
        .option("startingVersion", last_version + 1) \
        .table(table_name)
