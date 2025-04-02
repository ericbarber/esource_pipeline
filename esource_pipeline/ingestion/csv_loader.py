import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from datetime import datetime


def read_csv_to_df(spark: SparkSession, file_location: str, infer_schema: str = "true",
                   header: str = "true", delimiter: str = ",", drop_index_column: bool = False) -> DataFrame:
    df = spark.read.format("csv") \
        .option("inferSchema", infer_schema) \
        .option("header", header) \
        .option("sep", delimiter) \
        .load(file_location)

    if drop_index_column and "_c0" in df.columns:
        df = df.drop("_c0")

    return df


def hash_dataframe(df: DataFrame) -> DataFrame:
    return df.withColumn("row_hash", F.sha2(F.concat_ws("||", *df.columns), 256))


def calculate_file_hash(df: DataFrame) -> str:
    return df.select(F.sha2(F.concat_ws("||", F.collect_list("row_hash")), 256).alias("file_hash")) \
             .collect()[0]["file_hash"]


def check_file_already_loaded(spark: SparkSession, load_log_table: str, file_hash: str) -> bool:
    if spark.catalog.tableExists(load_log_table):
        log_df = spark.table(load_log_table)
        return log_df.filter(F.col("file_hash") == file_hash).count() > 0
    else:
        spark.createDataFrame([], schema="file_name STRING, file_hash STRING, load_time TIMESTAMP") \
             .write.format("delta").mode("overwrite").saveAsTable(load_log_table)
        return False


def write_or_merge_to_delta(spark: SparkSession, df: DataFrame, table_name: str, primary_key: str = None):
    timestamp = F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss")
    df = df.withColumn("load_datetime", timestamp).withColumn("updated_datetime", timestamp)

    if not spark.catalog.tableExists(table_name):
        df.write.format("delta") \
            .option("delta.enableChangeDataFeed", "true") \
            .mode("overwrite") \
            .saveAsTable(table_name)
    else:
        delta_table = DeltaTable.forName(spark, table_name)
        if primary_key:
            merge_condition = f"target.{primary_key} = source.{primary_key}"
            update_condition = "target.row_hash != source.row_hash"

            delta_table.alias("target").merge(
                source=df.alias("source"),
                condition=merge_condition
            ).whenMatchedUpdate(
                condition=update_condition,
                set={col: f"source.{col}" for col in df.columns if col != "load_datetime"}
            ).whenNotMatchedInsertAll().execute()
        else:
            merge_condition = "target.row_hash = source.row_hash"

            delta_table.alias("target").merge(
                source=df.alias("source"),
                condition=merge_condition
            ).whenNotMatchedInsertAll().execute()


def log_file_load(spark: SparkSession, load_log_table: str, file_location: str, file_hash: str):
    new_log_entry = spark.createDataFrame(
        [(file_location, file_hash, datetime.now())],
        schema="file_name STRING, file_hash STRING, load_time TIMESTAMP"
    )
    new_log_entry.write.format("delta").mode("append").saveAsTable(load_log_table)


# Unified reusable function for files with or without primary key
def load_csv_to_delta(spark: SparkSession, file_location: str, table_name: str,
                      load_log_table: str, primary_key: str = None, infer_schema: str = "true",
                      header: str = "true", delimiter: str = ",", drop_index_column: bool = False):

    df = read_csv_to_df(spark, file_location, infer_schema, header, delimiter, drop_index_column)
    df = hash_dataframe(df)

    file_hash = calculate_file_hash(df)
    if check_file_already_loaded(spark, load_log_table, file_hash):
        print(f"Duplicate file detected: File '{file_location}' already loaded—skipping.")
        return

    df = df.dropDuplicates(["row_hash"])
    write_or_merge_to_delta(spark, df, table_name, primary_key)
    log_file_load(spark, load_log_table, file_location, file_hash)


# Example usage:
if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    file_location = "/FileStore/tables/table_name.csv"
    table_name = "schema.table_name"
    load_log_table = "schema.load_log"

    # With primary key
    load_csv_to_delta(spark, file_location, table_name, load_log_table, primary_key="ID")

    # Without primary key
    load_csv_to_delta(spark, file_location, table_name, load_log_table, header="false", drop_index_column=True)
