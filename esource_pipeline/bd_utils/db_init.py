from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

spark.sql("CREATE SCHEMA IF NOT EXISTS esource")
spark.catalog.setCurrentDatabase("esource")

from pyspark.sql.types import StringType, LongType, TimestampType
version_tracking_table_name = "esource.version_tracking"
if not spark.catalog.tableExists(version_tracking_table_name):
    tracking_schema = StructType([
        StructField("table_name", StringType(), False),
        StructField("last_processed_version", LongType(), False),
        StructField("last_updated", TimestampType(), True)
    ])
    tracking_df = spark.createDataFrame([], schema=tracking_schema)
    tracking_df.write.format("delta").mode("overwrite").saveAsTable(version_tracking_table_name)
