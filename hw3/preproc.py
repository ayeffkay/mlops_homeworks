"""
Script to process raw data from s3 with PySpark
Example usage:
    python preproc.py "s3a://otus-mlops-course/raw/" "processed" "processed_data.parquet"
"""


import sys

import findspark

findspark.init()
findspark.find()

from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import floor
from pyspark.sql.types import (
    BooleanType,
    LongType,
    TimestampType,
    DecimalType,
    IntegerType,
    ShortType,
    StructType,
)

SCHEMA = (
    StructType()
    .add("tx_id", LongType(), True)
    .add("tx_datetime", TimestampType(), True)
    .add("customer_id", LongType(), True)
    .add("terminal_id", IntegerType(), True)
    .add("tx_amount", DecimalType(precision=10, scale=2), True)
    .add("tx_time_seconds", IntegerType(), True)
    .add("tx_time_days", IntegerType(), True)
    .add("tx_fraud", BooleanType(), True)
    .add("tx_fraud_scenario", ShortType(), True)
)

SPARK = (
    SparkSession.builder.appName("preproc")
    .config("spark.executor.memory", "8g")
    .config("spark.driver.memory", "8g")
    .config("spark.driver.maxResultSize", "8g")
    .getOrCreate()
)

SQL = SQLContext(SPARK)


def process_bucket(
    raw_bucket_name: str, processed_folder_name: str, processed_dataset_name: str
):
    df = (
        SPARK.read.format("csv")
        .option("header", True)
        .schema(SCHEMA)
        .load(raw_bucket_name)
    )
    n_sec_day = 3600 * 24
    df.tx_time_days = floor(df.tx_time_seconds / n_sec_day)
    df = df.dropDuplicates(["tx_id"])
    df = df.na.drop(subset=["tx_datetime", "terminal_id"])
    df.write.parquet(f"{processed_folder_name}/{processed_dataset_name}")


if __name__ == "__main__":
    raw_bucket_name = sys.argv[1]
    processed_folder_name = sys.argv[2]
    processed_dataset_name = sys.argv[3]

    process_bucket(raw_bucket_name, processed_folder_name, processed_dataset_name)
