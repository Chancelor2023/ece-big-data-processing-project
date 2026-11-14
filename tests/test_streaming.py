"""SSE framing, exact IMDb matching and bounded event-time streaming state."""

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F

from imdb_pipeline.recent_changes import eligible_change, parse_changes, sse_messages
from imdb_pipeline.spark import create_spark
from imdb_pipeline.stream_matching import LOOKUP_SCHEMA, match_identifiers, title_query


class SourceTests(unittest.TestCase):
    def test_sse_comments_multiline_and_cursor(self):
        frames = list(sse_messages([
            ":heartbeat", "", "id: cursor-1", "event: message", "data: {",
            'data: "wiki":"enwiki"}', "", "data: incomplete",
        ]))
        self.assertEqual(frames, [("cursor-1", "message", '{\n"wiki":"enwiki"}')])

    def test_article_scope_includes_bots_not_other_namespaces(self):
        event = {"wiki": "enwiki", "namespace": 0, "type": "edit", "title": "Film", "bot": True, "meta": {"domain": "en.wikipedia.org"}}
        self.assertTrue(eligible_change(event))
        self.assertFalse(eligible_change({**event, "namespace": 1}))
        self.assertFalse(eligible_change({**event, "wiki": "frwiki"}))

    def test_title_literals_are_escaped(self):
        self.assertIn(json.dumps('A "quoted" title') + "@en", title_query(['A "quoted" title']))


class StreamingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark("streaming-tests")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_invalid_time_and_raw_json_are_auditable(self):
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        event = {"meta": {"id": "e1", "domain": "en.wikipedia.org"}, "wiki": "enwiki", "namespace": 0, "type": "edit", "title": "The_Film", "timestamp": int(now.replace(tzinfo=timezone.utc).timestamp())}
        raw = self.spark.createDataFrame([
            (json.dumps(event), "topic", 0, 1, now),
            ("not json", "topic", 0, 2, now),
            (json.dumps({**event, "timestamp": 4102444800}), "topic", 0, 3, now),
        ], "value string, topic string, partition int, offset long, timestamp timestamp")
        rows = {r.offset: r for r in parse_changes(raw).collect()}
        self.assertEqual(rows[1].title, "The Film")
        self.assertIsNone(rows[1].reject_reason)
        self.assertEqual(rows[2].raw_event, "not json")
        self.assertEqual(rows[2].reject_reason, "missing_event_time")
        self.assertEqual(rows[3].reject_reason, "future_event_time")
        self.assertEqual(rows[3].event_time, now)

    def test_only_declared_existing_imdb_ids_match(self):
        events = self.spark.createDataFrame([("Article", "e1")], "title string, event_key string")
        mapping = self.spark.createDataFrame([
            ("Article", "tt1", "Q1", "wiki", "query", "now"),
            ("Article", "nm2", "Q1", "wiki", "query", "now"),
            ("Article", "tt999", "Q1", "wiki", "query", "now"),
        ], LOOKUP_SCHEMA)
        keys = self.spark.createDataFrame([("tt1", "title"), ("nm2", "person")], "imdb_id string, entity_kind string")
        self.assertEqual({r.imdb_id for r in match_identifiers(events, mapping, keys).collect()}, {"tt1", "nm2"})

    def test_duplicate_suppression_watermark_and_checkpoint_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            incoming = root / "input"
            incoming.mkdir()
            received = []
            def execute(rows, number):
                (incoming / f"{number}.json").write_text("\n".join(json.dumps(r) for r in rows), encoding="utf-8")
                stream = self.spark.readStream.schema("event_key string, event_time timestamp").json(str(incoming))
                stream = stream.withWatermark("event_time", "5 minutes").dropDuplicatesWithinWatermark(["event_key"])
                def capture(batch, batch_id):
                    received.extend(r.event_key for r in batch.collect())
                query = stream.writeStream.foreachBatch(capture).option("checkpointLocation", str(root / "checkpoint")).trigger(availableNow=True).start()
                self.assertTrue(query.awaitTermination(120))
                return query.recentProgress
            execute([
                {"event_key": "same", "event_time": "2026-11-14T12:00:00Z"},
                {"event_key": "same", "event_time": "2026-11-14T12:00:30Z"},
            ], 1)
            execute([{"event_key": "advance", "event_time": "2026-11-14T12:20:00Z"}], 2)
            progress = execute([
                {"event_key": "late", "event_time": "2026-11-14T12:01:00Z"},
                {"event_key": "fresh", "event_time": "2026-11-14T12:20:01Z"},
            ], 3)
            self.assertEqual(sorted(received), ["advance", "fresh", "same"])
            self.assertGreaterEqual(sum(s.get("numRowsDroppedByWatermark", 0) for p in progress for s in p.get("stateOperators", [])), 1)
            # Spark 4 progress objects contain UUIDs; persist their canonical JSON.
            self.assertIsInstance(json.loads(progress[-1].json)["id"], str)


if __name__ == "__main__":
    unittest.main()
