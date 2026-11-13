"""Create a consistent Spark session for jobs and notebooks."""

import os

from pyspark.sql import SparkSession


def create_spark(app_name: str) -> SparkSession:
    """Use local threads by default; the Kafka connector is image-configured."""
    spark = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[2]"))
        .config(
            "spark.sql.shuffle.partitions",
            os.environ.get("SPARK_SHUFFLE_PARTITIONS", "4"),
        )
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
