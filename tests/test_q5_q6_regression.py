"""Synthetic regressions for Q5 specials handling and Q6 language inputs.

Run with: python -m unittest -v test_q5_q6_regression.py
Requires PySpark. No IMDb data is used.
"""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

try:
    from pyspark.sql import SparkSession
except ModuleNotFoundError:
    SparkSession = None


from jobs.exploration import analyses as A


@unittest.skipUnless(SparkSession is not None, "PySpark unavailable in this environment")
class Q5Q6RegressionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[1]")
                     .appName("q5-q6-regression").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    # ------------------------------------------------------------------ Q5
    def make_q5(self, season_rating_rows, min_seasons=3):
        # rows are (seasonNumber, rating, episodes_in_season); each season's
        # first episode has the specified rating, remaining ratings are 5.0.
        # seasonNumber may be None (null season).
        ep_rows, rating_rows = [], []
        n = 0
        for season, rating, count in season_rating_rows:
            for j in range(count):
                tconst = f"ep{n}"
                ep_rows.append(("series1", tconst, season))
                rating_rows.append((tconst, float(rating if j == 0 else 5.0), 10))
                n += 1
        eps = self.spark.createDataFrame(
            ep_rows, "parentTconst string, tconst string, seasonNumber int")
        ratings = self.spark.createDataFrame(
            rating_rows, "tconst string, averageRating double, numVotes long")
        basics = self.spark.createDataFrame(
            [("series1", "tvSeries", "Synthetic")],
            "tconst string, titleType string, primaryTitle string")
        return A.declining_series(basics, ratings, eps, min_seasons=min_seasons,
                                  min_episodes=2, min_votes=0).collect()

    def test_q5_specials_cannot_make_short_series_eligible_false_positive(self):
        # Season 0 plus only two qualifying ordinary seasons used to count as 3.
        out = self.make_q5([(0, 10, 2), (1, 8, 2), (2, 2, 2)])
        self.assertEqual(out, [])

    def test_q5_special_worst_episode_does_not_hide_ordinary_decline_false_negative(self):
        # Ordinary seasons satisfy eligibility; the worst ordinary episode is
        # in last season 3 (rating 1). A lower-rated special (rating 0) must
        # not replace it as the worst episode.
        out = self.make_q5([(0, 0, 2), (1, 10, 2), (2, 9, 2), (3, 1, 2)])
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0].worst_season, 3)
        self.assertEqual(out[0].best_season, 1)

    def test_q5_special_best_episode_does_not_become_best_season(self):
        # A top-rated special (rating 10) must not be selected as the best
        # episode; the best ordinary episode (rating 9, season 1) is.
        out = self.make_q5([(0, 10, 2), (1, 9, 2), (2, 7, 2), (3, 1, 2)])
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0].best_season, 1)
        self.assertEqual(out[0].best_rating, 9.0)
        self.assertEqual(out[0].worst_season, 3)

    def test_q5_null_season_numbers_are_ignored(self):
        # Episodes with a null season must not count as an ordinary season,
        # nor be candidates for best/worst.
        out = self.make_q5([(None, 0, 2), (1, 10, 2), (2, 9, 2), (3, 1, 2)])
        self.assertEqual(len(out), 1)
        self.assertEqual((out[0].worst_season, out[0].best_season), (3, 1))
        # Only two ordinary seasons plus a null season: not eligible.
        self.assertEqual(self.make_q5([(None, 10, 2), (1, 8, 2), (2, 2, 2)]), [])

    def test_q5_no_season_zero_unchanged(self):
        out = self.make_q5([(1, 10, 2), (2, 9, 2), (3, 1, 2)])
        self.assertEqual(len(out), 1)
        self.assertEqual((out[0].worst_season, out[0].best_season), (3, 1))

    def test_q5_gap_in_season_numbers_uses_two_lowest_present(self):
        # Seasons 1, 3, 4: the "first two" are the two lowest present (1 and 3),
        # so a best episode in season 3 qualifies; gaps are not filled.
        out = self.make_q5([(1, 6, 2), (3, 10, 2), (4, 1, 2)])
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0].best_season, 3)
        self.assertEqual(out[0].worst_season, 4)

    # ------------------------------------------------------------------ Q6
    def _q6_inputs(self):
        basics = self.spark.createDataFrame(
            [("m1", "movie", "One", "One", 2000),
             ("m2", "movie", "Two", "Two", 2001),
             ("m3", "movie", "Three", "Three", 2002)],
            "tconst string, titleType string, primaryTitle string, originalTitle string, startYear int")
        ratings = self.spark.createDataFrame(
            [("m1", 8.0, 1000), ("m2", 8.0, 1000), ("m3", 8.0, 1000)],
            "tconst string, averageRating double, numVotes long")
        principals = self.spark.createDataFrame(
            [("m1", "a1", "actor"), ("m2", "a1", "actress"), ("m3", "a1", "actor"),
             ("m1", "a2", "actor"), ("m2", "a2", "actor")],
            "tconst string, nconst string, category string")
        names = self.spark.createDataFrame(
            [("a1", "Actor One"), ("a2", "Actor Two")],
            "nconst string, primaryName string")
        return basics, ratings, principals, names

    def _langs(self, rows):
        return self.spark.createDataFrame(rows, "tconst string, language string")

    def test_q6_uses_p364_languages_and_restores_three_plus_output(self):
        basics, ratings, principals, names = self._q6_inputs()
        # Wikidata language QIDs; deliberately independent of title.akas.
        langs = self._langs([("m1", "Q1860"), ("m2", "Q150"), ("m3", "Q188")])
        q6 = A.multilingual_actors(basics, ratings, principals, langs, names,
                                   min_rating=7.0, min_votes=1000, min_languages=2)
        self.assertEqual({r.nconst for r in q6.collect()}, {"a1", "a2"})
        rows = A.q6_actors_3plus_languages(q6).collect()
        self.assertEqual([r.nconst for r in rows], ["a1"])
        self.assertEqual(rows[0].n_languages, 3)

    def test_q6_film_with_several_languages_counts_in_each(self):
        # m1 declares two languages: both are valid (not contradictory) and a
        # single film must count in each, without inflating the film count.
        basics, ratings, principals, names = self._q6_inputs()
        langs = self._langs([("m1", "Q1860"), ("m1", "Q150")])
        q6 = A.multilingual_actors(basics, ratings, principals, langs, names,
                                   min_rating=7.0, min_votes=1000, min_languages=2)
        rows = {r.nconst: r for r in q6.collect()}
        # a1 and a2 both act in m1 only (among films with a language).
        self.assertEqual(set(rows), {"a1", "a2"})
        self.assertEqual(rows["a1"].n_languages, 2)
        self.assertEqual(rows["a1"].languages, ["Q150", "Q1860"])
        self.assertEqual(rows["a1"].n_successful_movies, 1)

    def test_q6_duplicate_film_language_pairs_are_collapsed(self):
        basics, ratings, principals, names = self._q6_inputs()
        langs = self._langs([("m1", "Q1860"), ("m1", "Q1860"), ("m2", "Q150")])
        q6 = A.multilingual_actors(basics, ratings, principals, langs, names,
                                   min_rating=7.0, min_votes=1000, min_languages=2)
        rows = {r.nconst: r for r in q6.collect()}
        self.assertEqual(rows["a1"].n_languages, 2)
        self.assertEqual(rows["a1"].n_successful_movies, 2)

    def test_q6_films_without_language_are_ignored_no_fallback(self):
        # m3 has no row in the language table: it contributes no language, and
        # nothing is inferred from elsewhere (no title.akas fallback).
        basics, ratings, principals, names = self._q6_inputs()
        langs = self._langs([("m1", "Q1860"), ("m2", "Q150")])
        q6 = A.multilingual_actors(basics, ratings, principals, langs, names,
                                   min_rating=7.0, min_votes=1000, min_languages=2)
        rows = {r.nconst: r for r in q6.collect()}
        self.assertEqual(rows["a1"].n_languages, 2)
        self.assertEqual(rows["a1"].n_successful_movies, 2)
        self.assertEqual(A.q6_actors_3plus_languages(q6).count(), 0)

    def test_q6_blank_and_null_languages_are_dropped(self):
        basics, ratings, principals, names = self._q6_inputs()
        langs = self._langs([("m1", "Q1860"), ("m2", ""), ("m3", None)])
        q6 = A.multilingual_actors(basics, ratings, principals, langs, names,
                                   min_rating=7.0, min_votes=1000, min_languages=1)
        rows = {r.nconst: r for r in q6.collect()}
        self.assertEqual(rows["a1"].n_languages, 1)
        self.assertEqual(rows["a1"].n_successful_movies, 1)

    def test_q6_coverage_reports_not_collected_films(self):
        eligible = self.spark.createDataFrame(
            [("m1",), ("m2",), ("m3",), ("m4",)], "tconst string")
        status = self.spark.createDataFrame(
            [("m1", "declared"), ("m2", "no_p364"), ("m3", "declared")],
            "tconst string, status string")
        out = {r.status: r.n_films for r in A.q6_coverage(eligible, status).collect()}
        self.assertEqual(out, {"declared": 2, "no_p364": 1, "not_collected": 1})

    def test_q6_eligible_films_applies_rating_and_vote_thresholds(self):
        basics = self.spark.createDataFrame(
            [("m1", "movie", "A", "A", 2000), ("m2", "movie", "B", "B", 2000),
             ("m3", "movie", "C", "C", 2000), ("s1", "tvSeries", "D", "D", 2000)],
            "tconst string, titleType string, primaryTitle string, originalTitle string, startYear int")
        ratings = self.spark.createDataFrame(
            [("m1", 8.0, 1000), ("m2", 6.9, 5000), ("m3", 8.0, 999), ("s1", 9.0, 5000)],
            "tconst string, averageRating double, numVotes long")
        out = {r.tconst for r in A.q6_eligible_films(
            basics, ratings, min_rating=7.0, min_votes=1000).collect()}
        self.assertEqual(out, {"m1"})


if __name__ == "__main__":
    unittest.main(verbosity=2)