"""Small synthetic fixtures for edge cases before full-source execution."""

import gzip
import tempfile
import unittest
from pathlib import Path

from pyspark.sql import functions as F

from imdb_pipeline.preparation import (
    TITLE_COLUMNS, mark_rating_parents, prepare_rows, rank_records, read_tsv,
)
from imdb_pipeline.spark import create_spark


class PreparationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark("preparation-tests")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def fixture(self, directory, table, rows):
        path = Path(directory) / f"{table}.tsv.gz"
        text = "\t".join(TITLE_COLUMNS[table]) + "\n" + "\n".join(rows) + "\n"
        with gzip.open(path, "wt", encoding="utf-8") as target:
            target.write(text)
        return read_tsv(self.spark, path, table)

    def test_basics_normalization_and_literal_quotes(self):
        with tempfile.TemporaryDirectory() as directory:
            raw = self.fixture(directory, "title.basics", [
                'tt0000001\tmovie\t"Synthetic title"\tOriginal\t1\t2000\t1999\t0\tDrama, Comedy,Drama',
                'tt0000002\tshort\tOther\tOther\t\\N\t\\N\t\\N\t5\t\\N',
            ])
            rows = {r.tconst: r for r in prepare_rows(raw, "title.basics").collect()}
            first = rows["tt0000001"]
            self.assertEqual(first.primaryTitle, '"Synthetic title"')
            self.assertTrue(first.isAdult)
            self.assertIsNone(first.endYear)
            self.assertIsNone(first.runtimeMinutes)
            self.assertEqual(first.genres, ["Comedy", "Drama"])
            self.assertIn("endYear_before_startYear", first._issues)
            self.assertIsNone(rows["tt0000002"].isAdult)
            self.assertIsNone(rows["tt0000002"].genres)

    def test_bad_width_keys_and_deterministic_duplicates(self):
        with tempfile.TemporaryDirectory() as directory:
            raw = self.fixture(directory, "title.basics", [
                'tt0000001\tmovie\tSparse\t\\N\t0\t2000\t\\N\t90\tDrama',
                'tt0000001\tmovie\tComplete\tOriginal\t0\t2000\t\\N\t90\tDrama',
                'invalid\tmovie\tBad key\tOriginal\t0\t2000\t\\N\t90\tDrama',
                'tt0000003\tmovie\tToo few fields',
                'tt0000004\tmovie\tToo many\tOriginal\t0\t2000\t\\N\t90\tDrama\textra',
            ])
            prepared = prepare_rows(raw, "title.basics")
            reasons = [r._reject_reason for r in prepared.collect()]
            self.assertEqual(reasons.count("invalid_field_count"), 2)
            self.assertEqual(reasons.count("invalid_tconst"), 1)
            ranked = rank_records(prepared, "title.basics", 2)
            kept = ranked.filter(F.col("_key_rank") == 1).collect()
            self.assertEqual(len(kept), 1)
            self.assertEqual(kept[0].primaryTitle, "Complete")

    def test_ratings_invalid_numbers_and_missing_parents(self):
        with tempfile.TemporaryDirectory() as directory:
            raw = self.fixture(directory, "title.ratings", [
                'tt0000001\t8.1\t42',
                'tt0000002\tNaN\t-3',
                'tt0000003\t11\t999999999999999999999999',
                'tt0000004\t\\N\t0',
            ])
            prepared = prepare_rows(raw, "title.ratings")
            rows = {r.tconst: r for r in prepared.collect()}
            self.assertEqual(rows["tt0000001"].numVotes, 42)
            self.assertIsNone(rows["tt0000002"].averageRating)
            self.assertIsNone(rows["tt0000002"].numVotes)
            self.assertIsNone(rows["tt0000003"].numVotes)
            self.assertEqual(rows["tt0000004"].numVotes, 0)
            parents = self.spark.createDataFrame([("tt0000001",)], ["tconst"])
            marked = mark_rating_parents(
                prepared.select(*TITLE_COLUMNS["title.ratings"]), parents
            )
            self.assertEqual(marked.filter(F.col("_parent_exists")).count(), 1)
            self.assertEqual(marked.filter(F.col("_parent_exists").isNull()).count(), 3)


if __name__ == "__main__":
    unittest.main()
