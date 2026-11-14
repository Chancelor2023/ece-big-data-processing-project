"""Selection boundaries, text provenance and polite collection behavior."""

import json
import tempfile
import unittest
from unittest.mock import Mock, patch

from imdb_pipeline.enrichment import RAW_SCHEMA, clean_collected, select_films, select_people
from imdb_pipeline.spark import create_spark
from imdb_pipeline.wikimedia import WikimediaClient


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


class WikimediaClientTests(unittest.TestCase):
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
