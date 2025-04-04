import sys
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

def drop_table_and_clean_storage(database: str, table: str):
    spark = SparkSession.builder.appName("CleanupScript").getOrCreate()
    
    full_table = f"{database}.{table}"
    print(f"\n Checking for table: {full_table}")

    try:
        spark.sql(f"DROP TABLE IF EXISTS {full_table}")
        print(f" Dropped table: {full_table}")
    except AnalysisException as e:
        print(f" Error dropping table {full_table}: {str(e)}")

    # Try to infer the location
    try:
        location_df = spark.sql(f"DESCRIBE DETAIL {full_table}")
        location = location_df.select("location").collect()[0]["location"]
    except Exception:
        # Default to hive path if DESCRIBE DETAIL fails
        location = f"dbfs:/user/hive/warehouse/{database}.db/{table}"
    
    print(f" Attempting to delete data files at: {location}")
    try:
        dbutils.fs.rm(location, recurse=True)
        print(f" Deleted table data at: {location}")
    except Exception as e:
        print(f" Failed to delete files at {location}: {str(e)}")

# if __name__ == "__main__":
    # database = "esource"
    # table = "load_log"
    # drop_table_and_clean_storage(database, table)
