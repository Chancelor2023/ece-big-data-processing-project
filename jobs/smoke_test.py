"""Check Spark, Parquet persistence, and Python -> Kafka -> Spark streaming.

Uses three synthetic messages on a unique temporary topic, not IMDb/Wikipedia.
Leaves a machine-readable verification result and a unique checkpoint on disk.
"""

import json
from datetime import datetime, timezone
from uuid import uuid4

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from imdb_pipeline.config import (
    CHECKPOINT_DIR,
    KAFKA_BOOTSTRAP_SERVERS,
    RESULTS_DIR,
)
from imdb_pipeline.spark import create_spark


def main() -> None:
    run_id = uuid4().hex
    topic = f"environment-smoke-{run_id}"
    output = RESULTS_DIR / "environment" / run_id
    output.mkdir(parents=True, exist_ok=False)
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    created = False
    spark = None
    query = None
    try:
        admin.create_topics(
            [NewTopic(topic, num_partitions=1, replication_factor=1)],
            request_timeout=30,
        )[topic].result(timeout=40)
        created = True
        delivery_errors = []

        def on_delivery(error, message):
            if error is not None:
                delivery_errors.append(str(error))

        producer = Producer(
            {"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS, "acks": "all"}
        )
        for number in range(3):
            payload = json.dumps({"run_id": run_id, "number": number})
            producer.produce(
                topic, value=payload.encode("utf-8"), on_delivery=on_delivery
            )
            producer.poll(0)
        pending = producer.flush(timeout=30)
        if pending or delivery_errors:
            raise RuntimeError(f"Kafka delivery failed: {pending=}, {delivery_errors=}")

        spark = create_spark("imdb-environment-smoke-test")
        if spark.version != "4.0.1":
            raise RuntimeError(f"Unexpected Spark version: {spark.version}")
        parquet_path = str(output / "parquet")
        spark.range(5).write.mode("errorifexists").parquet(parquet_path)
        parquet_count = spark.read.parquet(parquet_path).count()
        if parquet_count != 5:
            raise RuntimeError(f"Parquet round trip failed: {parquet_count}")

        schema = StructType(
            [
                StructField("run_id", StringType()),
                StructField("number", IntegerType()),
            ]
        )
        events = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "true")
            .load()
            .select(F.from_json(F.col("value").cast("string"), schema).alias("event"))
            .select("event.*")
            .filter(F.col("run_id") == run_id)
        )
        received = []

        def capture_batch(batch, batch_id):
            # Deliberately bounded: this test topic contains exactly 3 messages.
            received.extend(row.number for row in batch.collect())

        query = (
            events.writeStream.foreachBatch(capture_batch)
            .option("checkpointLocation", str(CHECKPOINT_DIR / "environment" / run_id))
            .trigger(availableNow=True)
            .start()
        )
        if not query.awaitTermination(180):
            raise TimeoutError("Spark streaming test did not finish within 180s.")
        if sorted(received) != [0, 1, 2]:
            raise RuntimeError(f"Unexpected streamed messages: {received}")

        report = {
            "status": "passed",
            "run_id": run_id,
            "verified_at_utc": datetime.now(timezone.utc).isoformat(),
            "spark_version": spark.version,
            "kafka_bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
            "parquet_rows": parquet_count,
            "streamed_messages": sorted(received),
            "synthetic_test_data": True,
        }
        destination = output / "verification.json"
        destination.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(report, indent=2))
        print(f"PASS: verification saved to {destination}")
    finally:
        if query is not None and query.isActive:
            query.stop()
        if spark is not None:
            spark.stop()
        if created:
            # Only remove the topic created by this particular test invocation.
            admin.delete_topics([topic], request_timeout=30)[topic].result(timeout=40)


if __name__ == "__main__":
    main()
