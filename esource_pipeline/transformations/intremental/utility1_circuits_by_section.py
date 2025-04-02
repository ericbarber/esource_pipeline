from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, current_timestamp, max as spark_max
from delta.tables import DeltaTable

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

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

    spark.createDataFrame([(source_table, -1, current_timestamp())], schema=tracking_schema) \
         .write.format("delta").mode("overwrite").saveAsTable(version_tracking_table)


def get_last_processed_version(spark, table_name):
    df = spark.table(version_tracking_table).filter(col("table_name") == table_name)
    return df.collect()[0]["last_processed_version"]


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
        incremental_df = spark.read.format("delta") \
            .option("readChangeFeed", "true") \
            .option("startingVersion", version) \
            .option("endingVersion", version) \
            .schema(utility1_circuit_schema) \
            .table(source_table)

        if incremental_df.isEmpty():
            print(f"No incremental changes in version {version}.")
            update_last_processed_version(spark, source_table, version)
            continue

        # Data quality checks
        clean_df = incremental_df.filter(
            col("CIRCUIT_ID").isNotNull() &
            col("SECTION_ID").isNotNull()
        )

        # Handle bad records
        bad_records_df = incremental_df.exceptAll(clean_df)
        if not bad_records_df.isEmpty():
            bad_records_df.write.format("delta").mode("append") \
                .saveAsTable("esource.bad_records_utility1_circuits")

        # Write to target (silver) table
        if not spark.catalog.tableExists(target_table):
            clean_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
        else:
            DeltaTable.forName(spark, target_table).alias("target").merge(
                clean_df.alias("source"),
                "target.CIRCUIT_ID = source.CIRCUIT_ID AND target.SECTION_ID = source.SECTION_ID"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        update_last_processed_version(spark, source_table, version)
        print(f"Successfully processed version {version}.")


# Execute incremental processing
incremental_processing()
