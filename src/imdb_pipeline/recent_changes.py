"""SSE framing and Spark parsing for real Wikimedia change events."""

import json

from pyspark.sql import functions as F, types as T

EVENTSTREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
STREAM_USER_AGENT = "ECE-IMDb-Streaming/1.0 (https://github.com/Chancelor2023/ece-big-data-processing-project)"


def sse_messages(lines):
    event_id, event_type, data = None, "message", []
    size = 0
    for line in lines:
        if not line:
            if data:
                yield event_id, event_type, "\n".join(data)
            event_id, event_type, data, size = None, "message", [], 0
            continue
        if line.startswith(":"):
            continue
        field, _, value = line.partition(":")
        value = value[1:] if value.startswith(" ") else value
        if field == "id" and "\x00" not in value:
            event_id = value
        elif field == "event":
            event_type = value
        elif field == "data":
            size += len(value)
            if size > 2_000_000:
                raise ValueError("Oversized SSE frame")
            data.append(value)


def eligible_change(event):
    return (
        event.get("wiki") == "enwiki" and event.get("namespace") == 0
        and event.get("type") in ("edit", "new")
        and event.get("meta", {}).get("domain") == "en.wikipedia.org"
        and bool(event.get("title"))
    )


CHANGE_SCHEMA = T.StructType([
    T.StructField("meta", T.StructType([
        T.StructField("id", T.StringType()), T.StructField("domain", T.StringType()),
        T.StructField("uri", T.StringType()),
    ])),
    T.StructField("id", T.LongType()), T.StructField("timestamp", T.LongType()),
    T.StructField("wiki", T.StringType()), T.StructField("namespace", T.LongType()),
    T.StructField("type", T.StringType()), T.StructField("title", T.StringType()),
    T.StructField("bot", T.BooleanType()),
    T.StructField("revision", T.StructType([
        T.StructField("old", T.LongType()), T.StructField("new", T.LongType()),
    ])),
])


def parse_changes(kafka):
    raw = kafka.select(
        F.col("value").cast("string").alias("raw_event"),
        "topic", "partition", "offset", F.col("timestamp").alias("ingested_at"),
    )
    frame = raw.withColumn("event", F.from_json("raw_event", CHANGE_SCHEMA))
    frame = frame.select(
        "raw_event", "topic", "partition", "offset", "ingested_at",
        F.col("event.meta.id").alias("source_event_id"),
        F.col("event.meta.uri").alias("article_url"),
        F.col("event.meta.domain").alias("domain"),
        F.col("event.id").alias("change_id"),
        F.timestamp_seconds("event.timestamp").alias("source_event_time"),
        F.col("event.wiki").alias("wiki"), F.col("event.namespace").alias("namespace"),
        F.col("event.type").alias("change_type"),
        F.regexp_replace(F.col("event.title"), "_", " ").alias("title"),
        F.col("event.bot").alias("bot"),
        F.col("event.revision.new").alias("revision_id"),
    )
    reason = (
        F.when(F.col("source_event_time").isNull(), "missing_event_time")
        .when(F.col("source_event_time") > F.col("ingested_at") + F.expr("INTERVAL 1 MINUTE"), "future_event_time")
        .when(F.col("source_event_id").isNull() | F.col("title").isNull(), "missing_event_identity")
        .when(~F.coalesce(
            (F.col("wiki") == "enwiki") & (F.col("namespace") == 0)
            & (F.col("domain") == "en.wikipedia.org") & F.col("change_type").isin("edit", "new"),
            F.lit(False),
        ), "outside_stream_scope")
    )
    return frame.withColumn("reject_reason", reason).withColumn(
        "event_time", F.when(reason.isNull(), F.col("source_event_time")).otherwise(F.col("ingested_at"))
    ).withColumn("event_key", F.when(reason.isNull(), F.col("source_event_id")).otherwise(
        F.concat_ws(":", "topic", F.col("partition"), F.col("offset"))
    ))
