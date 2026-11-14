"""Synthetic edge cases for the five remaining IMDb schemas and references."""

import tempfile
import unittest
from pathlib import Path

from pyspark.sql import functions as F

from imdb_pipeline.other_preparation import prepare_other_rows
from imdb_pipeline.preparation import rank_records
from imdb_pipeline.schemas import OTHER_COLUMNS
from imdb_pipeline.spark import create_spark
from jobs.preparation.prepare_remaining import clean_array_references, filter_scalar_references


class OtherPreparationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark("remaining-table-tests")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def raw(self, table, values):
        columns = OTHER_COLUMNS[table]
        rows = [tuple(row) + ("\t".join(row), len(row)) for row in values]
        return self.spark.createDataFrame(rows, columns + ["_raw_line", "_field_count"])

    def test_people_life_dates_and_tokens(self):
        raw = self.raw("name.basics", [[
            "nm0000001", " Test person ", "1950", "1949",
            "actor,writer,actor", "tt0000001,invalid,tt0000001",
        ]])
        row = prepare_other_rows(raw, "name.basics").first()
        self.assertEqual(row.primaryName, "Test person")
        self.assertIsNone(row.deathYear)
        self.assertIn("deathYear_before_birthYear", row._issues)
        self.assertEqual(row.primaryProfession, ["actor", "writer"])
        self.assertEqual(row.knownForTitles, ["tt0000001"])

    def test_episode_specials_and_self_parent(self):
        raw = self.raw("title.episode", [
            ["tt0000001", "tt0000002", "0", "0"],
            ["tt0000003", "tt0000003", "1", "1"],
            ["tt0000004", "tt0000002", "-1", "\\N"],
        ])
        rows = {r.tconst: r for r in prepare_other_rows(raw, "title.episode").collect()}
        self.assertEqual(rows["tt0000001"].seasonNumber, 0)
        self.assertEqual(rows["tt0000003"]._reject_reason, "self_parent")
        self.assertIsNone(rows["tt0000004"].seasonNumber)
        self.assertIsNone(rows["tt0000004"].episodeNumber)

    def test_alias_separator_language_and_composite_key(self):
        raw = self.raw("title.akas", [
            ["tt0000001", "1", "Alias", "FR", "fr", "original\x02imdbDisplay", "literal, translation\x02short title", "1"],
            ["tt0000001", "2", "Other", "US", "en", "\\N", "\\N", "0"],
            ["tt0000001", "1", "Alias", "FR", "fr", "original\x02imdbDisplay", "literal, translation\x02short title", "1"],
        ])
        prepared = prepare_other_rows(raw, "title.akas")
        rows = rank_records(prepared, "title.akas", 2).filter(F.col("_key_rank") == 1).collect()
        self.assertEqual(len(rows), 2)
        first = next(row for row in rows if row.ordering == 1)
        self.assertEqual(first.types, ["imdbDisplay", "original"])
        self.assertEqual(first.attributes, ["literal, translation", "short title"])
        self.assertTrue(first.isOriginalTitle)
        self.assertEqual(first.language, "fr")

    def test_credit_characters_json_and_required_person(self):
        raw = self.raw("title.principals", [
            ["tt0000001", "1", "nm0000001", "actor", "\\N", '["First, role","Second role"]'],
            ["tt0000001", "2", "nm0000002", "actor", "\\N", "broken JSON"],
            ["tt0000001", "3", "invalid", "actor", "\\N", "[]"],
        ])
        rows = prepare_other_rows(raw, "title.principals").orderBy("ordering").collect()
        self.assertEqual(rows[0].characters, ["First, role", "Second role"])
        self.assertIsNone(rows[1].characters)
        self.assertIn("invalid_characters_json", rows[1]._issues)
        self.assertEqual(rows[2]._reject_reason, "invalid_nconst")

    def test_scalar_and_array_orphan_handling(self):
        with tempfile.TemporaryDirectory() as directory:
            parents = {
                "title.basics": self.spark.createDataFrame([("tt0000001",)], ["tconst"]),
                "name.basics": self.spark.createDataFrame([("nm0000001",)], ["nconst"]),
            }
            raw = self.raw("title.crew", [
                ["tt0000001", "nm0000001,nm0000002", "nm0000002"],
                ["tt0000002", "nm0000001", "\\N"],
            ])
            prepared = prepare_other_rows(raw, "title.crew")
            valid, scalar = filter_scalar_references(
                prepared, "title.crew", parents, Path(directory)
            )
            cleaned, arrays = clean_array_references(
                valid, "title.crew", parents, Path(directory)
            )
            rows = cleaned.collect()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0].directors, ["nm0000001"])
            self.assertIsNone(rows[0].writers)
            self.assertEqual(scalar["tconst"]["rows_quarantined"], 1)
            self.assertEqual(arrays["references_removed_by_field"], {"directors": 1, "writers": 1})
            cleaned.unpersist()
            valid.unpersist()


if __name__ == "__main__":
    unittest.main()
