tracking_schema = StructType([
    StructField("table_name", StringType(), False),
    StructField("last_processed_version", LongType(), False),
    StructField("last_updated", TimestampType(), True)
])

tracking_df = spark.createDataFrame([], schema=tracking_schema)
tracking_df.write.format("delta").mode("overwrite").saveAsTable("esource.version_tracking")
