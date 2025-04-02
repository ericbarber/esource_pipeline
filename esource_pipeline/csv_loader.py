import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from datetime import datetime


def read_csv_to_df(spark: SparkSession, file_location: str, infer_schema: str = "true",
                   header: str = "true", delimiter: str = ",") -> DataFrame:
    return spark.read.format("csv") \
        .option("inferSchema", infer_schema) \
        .option("header", header) \
        .option("sep", delimiter) \
        .load(file_location)


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


def validate_primary_key(df: DataFrame, primary_key: str):
    duplicate_keys_df = df.groupBy(primary_key).agg(F.count("*").alias("count")) \
                          .filter(F.col("count") > 1)

    if duplicate_keys_df.count() > 0:
        sample_keys = duplicate_keys_df.select(primary_key).limit(5).toPandas()[primary_key].tolist()
        raise ValueError(f"Duplicate primary keys detected: {sample_keys}")

    if df.filter(F.col(primary_key).isNull()).count() > 0:
        raise ValueError("Found NULL primary keys — aborting load.")


def write_or_merge_to_delta(spark: SparkSession, df: DataFrame, table_name: str, primary_key: str):
    timestamp = F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss")
    df = df.withColumn("load_datetime", timestamp).withColumn("updated_datetime", timestamp)

    if not spark.catalog.tableExists(table_name):
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)
    else:
        delta_table = DeltaTable.forName(spark, table_name)
        merge_condition = f"target.{primary_key} = source.{primary_key}"
        update_condition = f"target.row_hash != source.row_hash"

        delta_table.alias("target").merge(
            source=df.alias("source"),
            condition=merge_condition
        ).whenMatchedUpdate(
            condition=update_condition,
            set={col: f"source.{col}" for col in df.columns if col != "load_datetime"}
        ).whenNotMatchedInsertAll().execute()


def log_file_load(spark: SparkSession, load_log_table: str, file_location: str, file_hash: str):
    new_log_entry = spark.createDataFrame(
        [(file_location, file_hash, datetime.now())],
        schema="file_name STRING, file_hash STRING, load_time TIMESTAMP"
    )
    new_log_entry.write.format("delta").mode("append").saveAsTable(load_log_table)


# Main reusable function
def load_csv_to_delta(spark: SparkSession, file_location: str, table_name: str,
                      primary_key: str, load_log_table: str,
                      infer_schema: str = "true", header: str = "true", delimiter: str = ","):

    df = read_csv_to_df(spark, file_location, infer_schema, header, delimiter)
    df = hash_dataframe(df)

    file_hash = calculate_file_hash(df)
    if check_file_already_loaded(spark, load_log_table, file_hash):
        raise Exception("Duplicate file detected — skipping load.")

    df = df.dropDuplicates(["row_hash"])
    validate_primary_key(df, primary_key)
    write_or_merge_to_delta(spark, df, table_name, primary_key)
    log_file_load(spark, load_log_table, file_location, file_hash)


# Example usage:
if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    file_location = "/FileStore/tables/esource/v1/flatfiles/utility2_planned_der.csv"
    table_name = "esource.utility2_planned_der_source"
    primary_key = "INTERCONNECTION_QUEUE_REQUEST_ID"
    load_log_table = "esource.load_log"

    load_csv_to_delta(spark, file_location, table_name, primary_key, load_log_table)
