"""Selection boundaries, text provenance and polite collection behavior."""

import json
import tempfile
import unittest
from unittest.mock import Mock, patch

from imdb_pipeline.enrichment import RAW_SCHEMA, clean_collected, read_collected, select_films, select_people
from imdb_pipeline.spark import create_spark
from imdb_pipeline.wikimedia import WikimediaClient, save_json
from pathlib import Path


class EnrichmentTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark("enrichment-tests")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_year_type_boundaries_multigenre_and_ties(self):
        basics = self.spark.createDataFrame([
            ("tt1", "movie", 1980, ["Drama", "Action"]),
            ("tt2", "movie", 1980, ["Drama"]),
            ("tt3", "short", 1980, ["Drama"]),
            ("tt4", "movie", 1979, ["Drama"]),
            ("tt5", "movie", 2025, ["Drama"]),
            ("tt6", "movie", 2026, ["Drama"]),
        ], "tconst string, titleType string, startYear int, genres array<string>")
        ratings = self.spark.createDataFrame([
            ("tt1", 8.0, 100), ("tt2", 8.0, 100), ("tt3", 9.0, 200),
            ("tt4", 9.0, 200), ("tt5", 7.0, 50), ("tt6", 9.0, 200),
        ], "tconst string, averageRating double, numVotes long")
        rows = select_films(basics, ratings, top_n=1).collect()
        self.assertEqual({(r.tconst, r.genre) for r in rows}, {
            ("tt1", "Drama"), ("tt1", "Action"), ("tt5", "Drama"),
        })

    def test_three_distinct_people_not_three_roles(self):
        credits = self.spark.createDataFrame([
            ("tt1", 1, "nm1", "actor"), ("tt1", 2, "nm1", "writer"),
            ("tt1", 3, "nm2", "actor"), ("tt1", 4, "nm3", "actor"),
            ("tt1", 5, "nm4", "director"), ("tt2", 1, "nm5", "actor"),
        ], "tconst string, ordering int, nconst string, category string")
        films = self.spark.createDataFrame([("tt1",)], ["tconst"])
        rows = select_people(credits, films).collect()
        self.assertEqual({r.nconst for r in rows}, {"nm1", "nm2", "nm3"})
        self.assertEqual(next(r for r in rows if r.nconst == "nm1").categories, ["actor", "writer"])

    def test_plot_lead_and_provenance_cleaned_in_spark(self):
        markup = '<table><tr><td><p>Infobox</p></td></tr></table><p>Jane &amp; Doe is an actor.<sup>[1]</sup></p><h2>Plot</h2><p>A human-written plot.</p><h3>Ending</h3><p>The ending.</p><h2>Cast</h2><p>Not a synopsis.</p>'
        payload = json.dumps({"parse": {"title": "Example", "pageid": 1, "revid": 42, "text": markup}})
        rows = [{"imdb_id": key, "kind": kind, "status": "collected", "raw_response": payload} for key, kind in [("tt1", "film"), ("nm1", "person")]]
        cleaned = {r.kind: r for r in clean_collected(self.spark.createDataFrame(rows, RAW_SCHEMA)).collect()}
        self.assertEqual(cleaned["film"].text, "A human-written plot.\n\nThe ending.")
        self.assertEqual(cleaned["person"].text, "Jane & Doe is an actor.")
        self.assertTrue(cleaned["film"].source_url.endswith("oldid=42"))
        self.assertEqual(cleaned["person"].status, "available")

    def test_missing_plot_not_replaced_with_article_lead(self):
        payload = json.dumps({"parse": {"revid": 7, "text": "<p>General article introduction.</p><h2>Cast</h2><p>Names.</p>"}})
        raw = self.spark.createDataFrame([{"imdb_id": "tt1", "kind": "film", "status": "collected", "raw_response": payload}], RAW_SCHEMA)
        row = clean_collected(raw).first()
        self.assertEqual(row.status, "missing_plot_section")
        self.assertIsNone(row.text)

    def test_incomplete_files_are_not_collected_inputs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            save_json(root / "tt1.json", {"imdb_id": "tt1", "kind": "film", "status": "not_found"})
            (root / "tt2.json.tmp").write_text('{"imdb_id": "tt2"}', encoding="utf-8")
            (root / ".tt3.json.tmp").write_text('unfinished', encoding="utf-8")
            self.assertEqual([r.imdb_id for r in read_collected(self.spark, root).collect()], ["tt1"])


class WikimediaClientTests(unittest.TestCase):
    def test_atomic_writer_recovers_from_transient_replace_failure(self):
        import os
        original_replace = os.replace
        attempts = []
        def replace_once(source, destination):
            attempts.append(source)
            if len(attempts) == 1:
                raise FileNotFoundError("transient filesystem failure")
            return original_replace(source, destination)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "nested" / "record.json"
            with patch("imdb_pipeline.wikimedia.os.replace", side_effect=replace_once):
                save_json(path, {"status": "collected"})
            self.assertEqual(json.loads(path.read_text()), {"status": "collected"})
            self.assertEqual(len(attempts), 2)
            self.assertEqual(list(path.parent.glob("*.tmp")), [])

    def test_cached_resume_reports_progress_without_rewriting_outputs(self):
        from jobs.enrichment import collect_wikipedia as collector
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            enrichment, results = root / "enrichment", root / "results"
            run = "20261114T142005Z"
            selection = enrichment / "runs" / run / "selection"
            save_json(selection / "requests" / "part-00000.json", {"imdb_id": "tt1", "kind": "film"})
            save_json(enrichment / "runs" / run / "selection_summary.json", {"unique_films": 1, "unique_people": 0, "pilot_requests": 1})
            record = {"imdb_id": "tt1", "kind": "film", "status": "not_found"}
            save_json(enrichment / "cache" / "records" / "tt1.json", record)
            output = enrichment / "runs" / run / "raw" / "tt1.json"
            save_json(output, record)
            original_mtime = output.stat().st_mtime_ns
            with patch.object(collector, "ENRICHMENT_DIR", enrichment), patch.object(collector, "RESULTS_DIR", results), patch.object(collector, "WikimediaClient") as client, patch("sys.argv", ["collector", "--scope", "full", "--run-id", run]), patch("builtins.print"):
                collector.main()
                client.return_value.get.assert_not_called()
            state = json.loads((results / "enrichment" / run / "collection_full.json").read_text())
            self.assertEqual(output.stat().st_mtime_ns, original_mtime)
            self.assertEqual(state["counts"], {"reused": 1})
            self.assertEqual(state["overall_status_counts"], {"not_found": 1})
            self.assertEqual(state["status"], "completed")
    def response(self, status=200):
        response = Mock(status_code=status, url="https://en.wikipedia.org/w/api.php?test=1")
        response.headers = {"Retry-After": "7"}
        response.json.return_value = {"parse": {"revid": 42}}
        response.text = json.dumps(response.json.return_value)
        return response

    @patch("imdb_pipeline.wikimedia.time.sleep")
    def test_cache_avoids_second_http_request(self, sleep):
        with tempfile.TemporaryDirectory() as directory:
            client = WikimediaClient(directory)
            client.session.get = Mock(return_value=self.response())
            first = client.get("https://en.wikipedia.org/w/api.php", {"test": 1})
            self.assertEqual(first, client.get("https://en.wikipedia.org/w/api.php", {"test": 1}))
            client.session.get.assert_called_once()

    @patch("imdb_pipeline.wikimedia.time.sleep")
    def test_server_retry_after_is_honored(self, sleep):
        with tempfile.TemporaryDirectory() as directory:
            client = WikimediaClient(directory)
            client.session.get = Mock(side_effect=[self.response(429), self.response()])
            client.get("https://en.wikipedia.org/w/api.php", {"test": 1})
            self.assertEqual(client.session.get.call_count, 2)
            sleep.assert_any_call(7.0)


if __name__ == "__main__":
    unittest.main()
