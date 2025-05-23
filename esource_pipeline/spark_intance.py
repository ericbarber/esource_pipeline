from pyspark.sql import SparkSession

def get_spark_session():
    return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
