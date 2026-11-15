"""Meaningful edge cases for reuse of previously collected biographies."""
import unittest
from pyspark.sql import functions as F
from imdb_pipeline.spark import create_spark
from imdb_pipeline.supplements import biography_coverage, cached_biography_supplements


class CachedBiographySupplementTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark("cached-biography-supplement-tests")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def inputs(self):
        s = self.spark
        films = s.createDataFrame([(f"f{i}",) for i in range(1, 5)], "tconst string")
        links = s.createDataFrame([
            ("f1", "a"), ("f1", "x"), ("f1", "y"),
            ("f2", "a"), ("f2", "b"), ("f2", "c"), ("f3", "x"),
        ], "tconst string, nconst string")
        people = s.createDataFrame([
            (key, key.upper(), "available" if key in "abcd" else "no_english_article",
             "https://en.wikipedia.org/w/index.php?oldid=1" if key in "abcd" else None,
             1 if key in "abcd" else None)
            for key in ("a", "b", "c", "d", "x", "y")
        ], "nconst string, primaryName string, status string, source_url string, revision_id long")
        credits = s.createDataFrame([
            ("f1", "a", 1, "actor"), ("f1", "b", 4, "actor"),
            ("f1", "b", 6, "writer"), ("f1", "c", 4, "actor"),
            ("f1", "d", 5, "actor"), ("f1", "x", 2, "actor"),
            ("f2", "d", 1, "actor"), ("f3", "d", 3, "actor"),
            ("f3", "unknown", 1, "actor"),
        ], "tconst string, nconst string, ordering int, category string")
        return films, links, people, credits

    def test_coverage_counts_people_not_roles_and_keeps_zero_films(self):
        films, links, people, _ = self.inputs()
        repeated = links.unionByName(links.filter("tconst='f1' AND nconst='a'"))
        counts = {r.tconst: r.available_biographies for r in biography_coverage(films, repeated, people).collect()}
        self.assertEqual(counts, {"f1": 1, "f2": 3, "f3": 0, "f4": 0})

    def test_candidates_require_existing_biography_credit_and_deficit(self):
        films, links, people, credits = self.inputs()
        coverage = biography_coverage(films, links, people)
        rows = cached_biography_supplements(credits, links, people, coverage).collect()
        self.assertEqual({(r.tconst, r.nconst) for r in rows}, {("f1", "b"), ("f1", "c"), ("f3", "d")})
        b = next(r for r in rows if r.nconst == "b")
        self.assertEqual(b.first_ordering, 4)
        self.assertEqual(b.categories, ["actor", "writer"])
        self.assertEqual(b.supplement_rank, 1)
        self.assertEqual(next(r for r in rows if r.nconst == "c").supplement_rank, 2)

    def test_second_pass_adds_no_duplicates_or_replacements(self):
        films, links, people, credits = self.inputs()
        initial = biography_coverage(films, links, people)
        extra = cached_biography_supplements(credits, links, people, initial)
        combined = links.unionByName(extra.select("tconst", "nconst"))
        coverage = biography_coverage(films, combined, people)
        self.assertEqual(cached_biography_supplements(credits, combined, people, coverage).count(), 0)
        self.assertEqual(combined.join(links, ["tconst", "nconst"], "left_anti").count(), 3)
        self.assertEqual({r.tconst: r.available_biographies for r in coverage.collect()}, {"f1": 3, "f2": 3, "f3": 1, "f4": 0})


if __name__ == "__main__":
    unittest.main()
