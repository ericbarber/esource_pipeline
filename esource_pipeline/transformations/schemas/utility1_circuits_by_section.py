from pyspark.sql.types import *

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
