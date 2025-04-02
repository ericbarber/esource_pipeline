def get_last_version(spark, table_name):
    df = spark.table("esource.version_tracking").filter(col("table_name") == table_name)
    return df.collect()[0]["last_processed_version"]

def update_last_version(spark, table_name, new_version):
    delta_table = DeltaTable.forName(spark, "esource.version_tracking")
    delta_table.update(
        condition=f"table_name = '{table_name}'",
        set={
            "last_processed_version": f"{new_version}",
            "last_updated": "current_timestamp()"
        }
    )
