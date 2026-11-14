"""Consume Kafka with Spark, deduplicate with event time and verify IMDb matches."""

import argparse
import json
import re
import time
from datetime import datetime, timezone
from uuid import uuid4

from pyspark import StorageLevel
from pyspark.sql import functions as F

from imdb_pipeline.config import CHECKPOINT_DIR, CLEAN_DIR, KAFKA_BOOTSTRAP_SERVERS, RESULTS_DIR, WIKIMEDIA_TOPIC
from imdb_pipeline.recent_changes import parse_changes
from imdb_pipeline.spark import create_spark
from imdb_pipeline.stream_matching import ArticleLookup, LOOKUP_SCHEMA, match_identifiers
from imdb_pipeline.tables import load_clean_tables
from jobs.preparation.prepare_titles import atomic_json


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default="wikipedia-live")
    parser.add_argument("--topic", default=WIKIMEDIA_TOPIC)
    parser.add_argument("--duration-seconds", type=int, default=900)
    parser.add_argument("--watermark", default="5 minutes")
    parser.add_argument("--available-now", action="store_true")
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9_-]+", args.run_id) or args.duration_seconds < 1:
        parser.error("Invalid run ID or duration")
    root = RESULTS_DIR / "streaming" / args.run_id
    checkpoint = CHECKPOINT_DIR / "streaming" / args.run_id / "spark"
    catalog = json.loads((CLEAN_DIR / "current.json").read_text())["run_id"]
    config = {"topic": args.topic, "brokers": KAFKA_BOOTSTRAP_SERVERS, "source_catalog": catalog, "watermark": args.watermark, "matching": "exact enwiki sitelink -> Wikidata P345 -> cleaned IMDb ID"}
    config_path = root / "configuration.json"
    if config_path.exists() and json.loads(config_path.read_text()) != config:
        raise ValueError("Checkpoint configuration changed: use a new --run-id")
    atomic_json(config_path, config)
    spark = create_spark("imdb-wikipedia-streaming")
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    query = keys = None
    lookup = ArticleLookup(CHECKPOINT_DIR / "streaming" / "article-lookup-cache-v2")
    session_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    stats = {"status": "initializing", "session_id": session_id, "run_id": args.run_id, "synthetic": False}
    started = None
    try:
        tables = load_clean_tables(spark, names=("title.basics", "name.basics"))
        keys = tables["title.basics"].select(F.col("tconst").alias("imdb_id")).withColumn("entity_kind", F.lit("title")).unionByName(
            tables["name.basics"].select(F.col("nconst").alias("imdb_id")).withColumn("entity_kind", F.lit("person"))
        ).persist(StorageLevel.DISK_ONLY)
        stats["imdb_keys"] = keys.count()
        raw = spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS).option(
            "subscribe", args.topic
        ).option("startingOffsets", "earliest").option("failOnDataLoss", "true").option("maxOffsetsPerTrigger", 500).load()
        events = parse_changes(raw).withWatermark("event_time", args.watermark).dropDuplicatesWithinWatermark(["event_key"])
        started = time.monotonic()
        test_start = datetime.now(timezone.utc)
        stats.update(status="running", started_at_utc=test_start.isoformat())

        def batch_output(batch, batch_id):
            marker = root / "batches" / f"{batch_id:012d}.json"
            if marker.exists():
                # foreachBatch may be replayed before Spark commits its offsets.
                batch.count()  # Fully consume stateful output even when the sink committed.
                return
            batch = batch.persist(StorageLevel.DISK_ONLY)
            matched = None
            attempt = root / "attempts" / f"{batch_id:012d}-{uuid4().hex[:8]}"
            try:
                count = batch.count()
                valid = batch.filter("reject_reason IS NULL")
                titles = [r.title for r in valid.select("title").distinct().collect()]
                mappings = lookup.resolve(titles, attempt / "lookups")
                frame = spark.createDataFrame(mappings, LOOKUP_SCHEMA)
                matched = match_identifiers(valid, frame, keys).persist(StorageLevel.DISK_ONLY)
                match_rows = matched.count()
                matched_events = matched.select("event_key").distinct().count()
                in_window = matched.filter(F.col("source_event_time") >= F.lit(test_start)).select("event_key").distinct().count()
                rejected = batch.filter("reject_reason IS NOT NULL")
                invalid = rejected.count()
                if count:
                    batch.write.parquet(str(attempt / "events"))
                if match_rows:
                    matched.write.parquet(str(attempt / "matches"))
                if invalid:
                    rejected.write.parquet(str(attempt / "quarantine"))
                metrics = {
                    "batch_id": batch_id, "session_id": session_id,
                    "rows_after_watermark_and_dedup": count, "invalid_rows": invalid,
                    "distinct_article_titles": len(titles), "matched_events": matched_events,
                    "matched_imdb_rows": match_rows, "matched_events_in_live_window": in_window,
                    "attempt_path": str(attempt.relative_to(root)),
                    "committed_at_utc": datetime.now(timezone.utc).isoformat(),
                }
                atomic_json(marker, metrics)
                print(json.dumps(metrics), flush=True)
            finally:
                batch.unpersist()
                if matched is not None:
                    matched.unpersist()

        writer = events.writeStream.foreachBatch(batch_output).option("checkpointLocation", str(checkpoint))
        writer = writer.trigger(availableNow=True) if args.available_now else writer.trigger(processingTime="30 seconds")
        query = writer.start()
        seen_batches = set()
        progress = []
        while query.isActive:
            query.awaitTermination(5)
            current = query.lastProgress
            if current and current["batchId"] not in seen_batches:
                progress.append(json.loads(current.json))
                seen_batches.add(current["batchId"])
                atomic_json(root / "progress" / f"{session_id}.json", progress)
            if time.monotonic() - started >= args.duration_seconds:
                query.stop()
                break
        stats["status"] = "stopped"
    except Exception as error:
        stats.update(status="failed", error=str(error))
        raise
    finally:
        if query is not None and query.isActive:
            query.stop()
        commits = [json.loads(path.read_text()) for path in (root / "batches").glob("*.json")]
        commits = [m for m in commits if m["session_id"] == session_id]
        stats.update({
            "elapsed_seconds": round(time.monotonic() - started, 2) if started else 0,
            "committed_batches": len(commits),
            "processed_rows": sum(m["rows_after_watermark_and_dedup"] for m in commits),
            "matched_events": sum(m["matched_events"] for m in commits),
            "matched_events_in_live_window": sum(m["matched_events_in_live_window"] for m in commits),
        })
        stats["live_test_passed"] = stats["status"] == "stopped" and not args.available_now and 900 <= stats["elapsed_seconds"] <= 1800 and stats["matched_events_in_live_window"] >= 2
        atomic_json(root / "sessions" / f"{session_id}.json", stats)
        print(json.dumps(stats), flush=True)
        if keys is not None:
            keys.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
