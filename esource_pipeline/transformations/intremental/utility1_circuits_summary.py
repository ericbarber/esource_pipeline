from pyspark.sql import SparkSession
from pyspark.sql.types import *
from delta.tables import DeltaTable
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql import DataFrame

def run_data_quality_checks(df: DataFrame, primary_key: str, required_columns: list) -> (DataFrame, DataFrame):
    print("Running data quality checks and applying fixes...\n")

    # Track rejected rows
    rejected_rows = spark.createDataFrame([], df.schema)  # empty frame with same schema

    # Nulls in required columns
    for col_name in required_columns:
        nulls_df = df.filter(F.col(col_name).isNull())
        null_count = nulls_df.count()
        if null_count > 0:
            print(f"Dropping {null_count} rows with null in required column: {col_name}")
            rejected_rows = rejected_rows.union(nulls_df)
            df = df.subtract(nulls_df)
        else:
            print(f"Column '{col_name}' passed null check.")

    # Check Full duplicate rows
    # Duplicate primary keys (keep first, reject others)
    window = Window.partitionBy(primary_key).orderBy(F.monotonically_increasing_id())
    df = df.withColumn("row_num", F.row_number().over(window))
    pk_dupe_rows_df = df.filter(F.col("row_num") > 1).drop("row_num")
    pk_dupe_count = pk_dupe_rows_df.count()
    if pk_dupe_count > 0:
        print(f"Dropping {pk_dupe_count} duplicate rows based on primary key: {primary_key}")
        rejected_rows = rejected_rows.union(pk_dupe_rows_df)

    # Keep only first of each primary key group
    clean_df = df.filter(F.col("row_num") == 1).drop("row_num")

    print("\nData quality checks complete. Returning clean and rejected DataFrames.")
    return clean_df, rejected_rows

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# # Column mapping: old name ➜ new name
# COLUMN_MAPPING = {
#     "_c1":  "CIRCUIT_ID",
#     "_c2":  "NUM_PHASES",
#     "_c3":  "OVERUNDER",
#     "_c4":  "PHASES_PRESENT",
#     "_c5":  "SECTION_ID",
#     "_c6":  "N_FEEDER_ID",
#     "_c7":  "N_VOLTAGE_KV",
#     "_c8":  "N_MAX_HC",
#     "_c9":  "N_MAP_COLOR",
#     "_c10":  "F_FEEDER_ID",
#     "_c11": "F_VOLTAGE_KV",
#     "_c12": "F_MAX_HC",
#     "_c13": "F_MIN_HC",
#     "_c14": "F_HCA_DATE",
#     "_c15": "F_NOTES",
#     "_c16": "SHAPE_LENGTH"
# }

# def apply_column_mapping(df, schema, mapping):
#     for old_col, new_col in mapping.items():
#         target_type = schema[new_col].dataType
#         df_renamed = df.withColumnRenamed(old_col, new_col)
#         df_typed = df_renamed.withColumn(new_col, F.col(new_col).cast(target_type))
#     return df_typed

# Define schema
utility1_circuit_schema = StructType([
    StructField("CIRCUIT_ID", LongType(), False),
    StructField("NUM_PHASES", IntegerType(), True),
    StructField("OVERUNDER", StringType(), True),
    StructField("PHASES_PRESENT", StringType(), True),
    StructField("SECTION_ID", LongType(), False),
    StructField("N_FEEDER_ID", LongType(), True),
    StructField("N_VOLTAGE_KV", DoubleType(), True),
    StructField("N_MAX_HC", DoubleType(), True),
    StructField("N_MAP_COLOR", StringType(), True),
    StructField("F_FEEDER_ID", LongType(), True),
    StructField("F_VOLTAGE_KV", DoubleType(), True),
    StructField("F_MAX_HC", DoubleType(), True),
    StructField("F_MIN_HC", DoubleType(), True),
    StructField("F_HCA_DATE", TimestampType(), True),
    StructField("F_NOTES", StringType(), True),
    StructField("SHAPE_LENGTH", DoubleType(), True),
])

utility1_circuit_summary_schema = StructType([
    StructField("CIRCUIT_ID", LongType(), False),
    StructField("F_VOLTAGE_KV", DoubleType(), True),
    StructField("F_MAX_HC", DoubleType(), True),
    StructField("F_MIN_HC", DoubleType(), True),
    StructField("F_HCA_DATE", TimestampType(), True),
    StructField("SHAPE_LENGTH", DoubleType(), True),
])

source_table = 'esource.utility1_circuits_source'
target_table = 'esource.utility1_circuits_by_section'
version_tracking_table = 'esource.version_tracking'

# Initialize version tracking table if not exists
if not spark.catalog.tableExists(version_tracking_table):
    tracking_schema = StructType([
        StructField("table_name", StringType(), False),
        StructField("last_processed_version", LongType(), False),
        StructField("last_updated", TimestampType(), True)
    ])

    spark.createDataFrame([(source_table, -1, F.current_timestamp())], schema=tracking_schema) \
         .write.format("delta").mode("overwrite").saveAsTable(version_tracking_table)

def get_last_processed_version(spark, table_name):
    df = spark.table(version_tracking_table).filter(col("table_name") == table_name)
    records = df.collect()
    if records:
        return records[0]["last_processed_version"]
    else:
        # Use actual Python datetime value
        spark.createDataFrame(
            [(table_name, -1, datetime.now())],
            schema=df.schema
        ).write.format("delta").mode("append").saveAsTable(version_tracking_table)
        return -1

def update_last_processed_version(spark, table_name, new_version):
    delta_table = DeltaTable.forName(spark, version_tracking_table)
    delta_table.update(
        condition=f"table_name = '{table_name}'",
        set={
            "last_processed_version": f"{new_version}",
            "last_updated": "current_timestamp()"
        }
    )


# Incremental processing with data quality checks
def incremental_processing():
    last_version = get_last_processed_version(spark, source_table)

    current_version = spark.sql(f"DESCRIBE HISTORY {source_table}") \
                           .selectExpr("max(version) as current_version") \
                           .collect()[0]["current_version"]

    for version in range(last_version + 1, current_version + 1):
        original_df = spark.read.format("delta") \
            .option("readChangeFeed", "true") \
            .option("startingVersion", version) \
            .option("endingVersion", version) \
            .table(source_table)

        # Apply the column renaming and type enforcement
        # Start with your raw dataframe with _c1 to _c16
        # TODO: write a functino to do this
        incremental_df = original_df \
            .withColumnRenamed("_c1", "CIRCUIT_ID") \
            .withColumn("CIRCUIT_ID", F.col("CIRCUIT_ID").cast(LongType())) \
            .withColumnRenamed("_c2", "NUM_PHASES") \
            .withColumn("NUM_PHASES", F.col("NUM_PHASES").cast(IntegerType())) \
            .withColumnRenamed("_c3", "OVERUNDER") \
            .withColumn("OVERUNDER", F.col("OVERUNDER").cast(StringType())) \
            .withColumnRenamed("_c4", "PHASES_PRESENT") \
            .withColumn("PHASES_PRESENT", F.col("PHASES_PRESENT").cast(StringType())) \
            .withColumnRenamed("_c5", "SECTION_ID") \
            .withColumn("SECTION_ID", F.col("SECTION_ID").cast(LongType())) \
            .withColumnRenamed("_c6", "N_FEEDER_ID") \
            .withColumn("N_FEEDER_ID", F.col("N_FEEDER_ID").cast(LongType())) \
            .withColumnRenamed("_c7", "N_VOLTAGE_KV") \
            .withColumn("N_VOLTAGE_KV", F.col("N_VOLTAGE_KV").cast(DoubleType())) \
            .withColumnRenamed("_c8", "N_MAX_HC") \
            .withColumn("N_MAX_HC", F.col("N_MAX_HC").cast(DoubleType())) \
            .withColumnRenamed("_c9", "N_MAP_COLOR") \
            .withColumn("N_MAP_COLOR", F.col("N_MAP_COLOR").cast(StringType())) \
            .withColumnRenamed("_c10", "F_FEEDER_ID") \
            .withColumn("F_FEEDER_ID", F.col("F_FEEDER_ID").cast(LongType())) \
            .withColumnRenamed("_c11", "F_VOLTAGE_KV") \
            .withColumn("F_VOLTAGE_KV", F.col("F_VOLTAGE_KV").cast(DoubleType())) \
            .withColumnRenamed("_c12", "F_MAX_HC") \
            .withColumn("F_MAX_HC", F.col("F_MAX_HC").cast(DoubleType())) \
            .withColumnRenamed("_c13", "F_MIN_HC") \
            .withColumn("F_MIN_HC", F.col("F_MIN_HC").cast(DoubleType())) \
            .withColumnRenamed("_c14", "F_HCA_DATE") \
            .withColumn("F_HCA_DATE", F.col("F_HCA_DATE").cast(TimestampType())) \
            .withColumnRenamed("_c15", "F_NOTES") \
            .withColumn("F_NOTES", F.col("F_NOTES").cast(StringType())) \
            .withColumnRenamed("_c16", "SHAPE_LENGTH") \
            .withColumn("SHAPE_LENGTH", F.col("SHAPE_LENGTH").cast(DoubleType()))

        if incremental_df.isEmpty():
            print(f"No incremental changes in version {version}.")
            update_last_processed_version(spark, source_table, version)
            continue

        primary_column = "row_hash"
        required_columns = ["CIRCUIT_ID", "SECTION_ID"]
        clean_df, bad_records_df = run_data_quality_checks(incremental_df, primary_column, target_primary_key, required_columns)

        if not bad_records_df.isEmpty():
            bad_records_df.write.format("delta").mode("append") \
                .saveAsTable("esource.audit_records_utility1_circuits")
       
        # Define window partitioned by CIRCUIT_ID
        window_spec = Window.partitionBy("CIRCUIT_ID")
        
        # Add a column to flag rows where the F_HCA_DATE is the latest for that circuit
        with_latest_date = clean_df.withColumn(
            "max_hca_date", F.max("F_HCA_DATE").over(window_spec)
        ).filter(F.col("F_HCA_DATE") == F.col("max_hca_date"))
        
        # Now do the aggregation on just the latest rows per circuit
        circuit_data = with_latest_date.groupBy("CIRCUIT_ID").agg(
            F.max("N_VOLTAGE_KV").alias("normal_voltage"),
            F.max("F_VOLTAGE_KV").alias("feeder_voltage"),
            F.max("F_MAX_HC").alias("feeder_max_hc"),
            F.min("F_MIN_HC").alias("feeder_min_hc"),
            F.max("F_HCA_DATE").alias("hca_refresh_date"),  # same as max_hca_date
            F.sum("SHAPE_LENGTH").alias("shape_length"),
        )

        # Write to target table
        if not spark.catalog.tableExists(target_table):
            circuit_data.write.format("delta").mode("overwrite").saveAsTable(target_table)
        else:
            DeltaTable.forName(spark, target_table).alias("target").merge(
                circuit_data.alias("source"),
                "target.CIRCUIT_ID = source.CIRCUIT_ID AND target.SECTION_ID = source.SECTION_ID"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        update_last_processed_version(spark, source_table, version)
        print(f"Successfully processed version {version}.")
