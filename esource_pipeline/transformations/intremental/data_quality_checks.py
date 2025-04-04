from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql import DataFrame

def run_data_quality_checks(df: DataFrame, primary_key: str, required_columns: list) -> (DataFrame, DataFrame):
    print("Running data quality checks and applying fixes...\n")

    # Track rejected rows
    rejected_rows = spark.createDataFrame([], df.schema)  # empty frame with same schema

    # Step 1: Nulls in required columns
    for col_name in required_columns:
        nulls_df = df.filter(F.col(col_name).isNull())
        null_count = nulls_df.count()
        if null_count > 0:
            print(f"Dropping {null_count} rows with null in required column: {col_name}")
            rejected_rows = rejected_rows.union(nulls_df)
            df = df.subtract(nulls_df)
        else:
            print(f"Column '{col_name}' passed null check.")

    # Step 2: Full duplicate rows
    dup_rows_df = (
        df.groupBy(df.columns)
        .count()
        .filter("count > 1")
        .drop("count")
    )
    dup_rows_count = dup_rows_df.count()
    if dup_rows_count > 0:
        print(f"Removing {dup_rows_count} full duplicate rows.")
        rejected_rows = rejected_rows.union(dup_rows_df)
        df = df.subtract(dup_rows_df)
    else:
        print("No full duplicate rows.")

    # Step 3: Duplicate primary keys (keep first, reject others)
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

