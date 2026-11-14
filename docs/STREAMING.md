# Wikipedia → Kafka → Spark

The producer reads the real Wikimedia `recentchange` Server-Sent Events stream and publishes English Wikipedia article edits/new pages to Kafka. The Spark consumer deduplicates events with event-time state, resolves exact article sitelinks through Wikidata `P345`, and joins the declared IMDb IDs to the cleaned title/person keys. It is independent of completion of the text-enrichment collection.

The new streaming files can be reviewed separately from the uncommitted enrichment files. `PROJECT.md`, its user-written AI declaration and `REPORT.md` are unchanged. This guide contains execution details; the group writes its own methodological explanation in the report.

## Prerequisites

Start the existing Compose services and prepare the seven IMDb tables. Run commands from the project directory. Keep the existing enrichment collector running; no Compose recreation or image rebuild is needed for these Python modules.

```bash
docker compose exec -T spark python -m jobs.create_topics
docker compose exec -T spark python -m unittest tests.test_streaming -v
docker compose exec -T spark python -m jobs.streaming.verify_lookup
```

The topic defaults to `wikimedia.recentchange`, with three partitions and one local broker replica. Topic creation is repeatable and never deletes an existing topic. The external lookup check verifies a known film and person and rejects a city article as a title-name match. Unit tests use synthetic file-stream data; they are distinct from the real-event test.

## Start a real test

In terminal 1, start the real-event producer for 20 minutes:

```bash
docker compose exec -T spark python -m jobs.streaming.produce_recent_changes --duration-seconds 1200 --state-name wikipedia-producer
```

In terminal 2, start a 15-minute Spark consumer:

```bash
docker compose exec -T spark python -m jobs.streaming.consume_recent_changes --run-id wikipedia-live --duration-seconds 900
```

Producer duration `0` means continuous operation. Its state name selects its durable SSE cursor. Consumer run IDs select a durable Spark checkpoint and result set. Use only one producer per state name and one consumer per run ID at a time.

The producer includes bot edits and all English Wikipedia main-namespace articles. It excludes talk pages, non-English wikis and non-edit/new events. It does not require that an article already have a collected biography/synopsis. Only events whose exact English sitelink has a Wikidata IMDb ID present in the cleaned IMDb catalog appear in `matches`. One event may legitimately have several declared IMDb IDs; event counts and matched-row counts are separate.

## Matching and collection access

Article titles are normalized from underscores to spaces in Spark. English Wikidata sitelink names are queried with language-tagged literals (`@en`); names alone are not treated as evidence of an IMDb match. Mapping results retain the Wikidata entity URI, article URI, query URL and retrieval time. Raw lookup responses are saved in each committed batch's attempt directory. A five-minute per-article cache also retains the mapping rows and their lookup lineage.

The stream connection uses an identifying User-Agent. Lookup requests are serial, batched by at most 40 titles and spaced by at least one second; transient errors use bounded retries and server retry delays. A failed lookup aborts the microbatch, so its Kafka offsets are not marked complete. The Kafka backlog remains available subject to broker retention.

The static join covers the validated `title.basics` and `name.basics` keys. It is cached on disk to keep the driver heap bounded. The Kafka reader limits each microbatch to 500 records; only its bounded distinct title list and small mapping responses reach Python driver memory. Article parsing, event validation, ID validation, joins, deduplication and output construction run in Spark.

## Event time and duplicates

- Event time is Wikimedia's source `timestamp`; Kafka's timestamp records ingestion time.
- A five-minute watermark and `dropDuplicatesWithinWatermark(event_key)` bound duplicate-tracking state. The event key is Wikimedia's event UUID.
- Events older than the advancing watermark can be dropped by the stateful operator. Its `numRowsDroppedByWatermark` metric is retained in progress logs. The delay is relative to observed event time, not a promise of five minutes from the local clock.
- Invalid JSON, missing identity/time, out-of-scope records and timestamps more than one minute ahead of ingestion are marked for quarantine. For state handling only, invalid records use ingestion time and a Kafka topic/partition/offset key. Their original raw JSON is preserved.
- Duplicate suppression is bounded by the watermark horizon; it is not an unlimited global history of every event ID.

Change `--watermark` only with a new consumer run ID/checkpoint. The tests exercise duplicate suppression, checkpoint restarts and an event that becomes too old after the watermark advances. A live run with no naturally late events is not evidence that late data can never occur.

## Resume and output publication

The producer saves the SSE `Last-Event-ID` cursor only after Kafka delivery acknowledgement. A crash between acknowledgement and cursor persistence can replay an event; the Spark deduplicator handles duplicates within its supported horizon. This is not an end-to-end exactly-once claim about the external event source.

The consumer uses `startingOffsets=earliest` on a fresh checkpoint; a restart resumes its committed offsets instead. `failOnDataLoss=true` prevents silently skipping offsets lost to broker retention. Configuration guards reject changes to the topic, broker, source catalog or watermark for an existing run ID.

`foreachBatch` output uses a fresh attempt directory and an atomic commit marker per batch. A replayed committed batch is fully consumed for state recovery, but its sink is not published twice. Partial attempts remain for diagnosis. Read only attempts referenced by committed markers.

Outputs are under `results/streaming/<run_id>/`:

| Path | Content |
| --- | --- |
| `configuration.json` | Topic, catalog and watermark settings |
| `batches/<batch_id>.json` | Atomic commit marker, attempt path and counts |
| `attempts/<attempt>/events/` | Parquet events after watermark/deduplication, with Kafka offsets and raw payload |
| `attempts/<attempt>/matches/` | Parquet confirmed IMDb matches with article/entity/query lineage |
| `attempts/<attempt>/quarantine/` | Invalid records when present |
| `attempts/<attempt>/lookups/` | New raw Wikidata responses fetched for that batch |
| `progress/<session>.json` | Canonical Spark progress JSON, including offsets, rates and state metrics |
| `sessions/<session>.json` | Session duration, committed rows and confirmed matches |

Producer summaries are under `results/streaming/producer/`. Checkpoints and lookup cache are under `data/checkpoints/streaming/`. Keep checkpoints together with result markers when resuming.

`live_test_passed` requires a successful 15–30 minute consumer session with at least two distinct confirmed events whose source event times fall within that session. A backlog-only replay is not counted as this live test. `--available-now` drains the current backlog for diagnosis/resume and never claims to be a 15-minute live test:

```bash
docker compose exec -T spark python -m jobs.streaming.consume_recent_changes --run-id wikipedia-live --available-now
```

All generated events, lookup responses, results and checkpoints are ignored by Git. Do not commit them as if they were code. Preserve selected final metrics and outputs for the group's report.

## References

- [Wikimedia EventStreams HTTP service](https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams_HTTP_Service)
- [Spark 4.0.1 Structured Streaming](https://spark.apache.org/docs/4.0.1/streaming/apis-on-dataframes-and-datasets.html)
- [Spark 4.0.1 Kafka integration](https://spark.apache.org/docs/4.0.1/streaming/structured-streaming-kafka-integration.html)
