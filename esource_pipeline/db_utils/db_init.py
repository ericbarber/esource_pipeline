from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

def db_init():
    # Initialize 
    esource_schema = 'esource'
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {esource_schema}")
    spark.catalog.setCurrentDatabase("esource")

    version_tracking_table = f"{esource_schema}.version_tracking"
    if not spark.catalog.tableExists(version_tracking_table):
        tracking_schema = StructType([
            StructField("table_name", StringType(), False),
            StructField("last_processed_version", LongType(), False),
            StructField("last_updated", TimestampType(), True)
        ])
        tracking_df = spark.createDataFrame([], schema=tracking_schema)
        tracking_df.write.format("delta").mode("overwrite").saveAsTable(version_tracking_table)

    load_log_table = f"{esource_schema}.load_log"
    if not spark.catalog.tableExists(load_log_table):
        load_log_schema = StructType([
            StructField("file_name", StringType(), False),
            StructField("file_hash", LongType(), False),
            StructField("load_time", TimestampType(), True)
        ])
        load_log_df = spark.createDataFrame([], schema=load_log_schema)
        load_log_df.write.format("delta").mode("overwrite").saveAsTable(load_log_table)
        
    print(
        "DATABASE INITIALIZED",
        f"COMPLETE CREATE SCHEMA: {esource_schema}",
        f"COMPLETE CREATE TABLE: {load_log_table}"
        f"COMPLETE CREATE TABLE: {version_tracking_table}",
        sep='\n\t'
    )

    return True
