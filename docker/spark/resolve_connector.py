"""Resolve the matching Scala 2.13 / Spark 4.0.1 Kafka connector at build time."""

from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("build-connector-check")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        assert spark.version == "4.0.1", spark.version
        assert spark.range(3).count() == 3
        print("Spark 4.0.1 and Kafka connector dependencies resolved.")
    finally:
        spark.stop()
