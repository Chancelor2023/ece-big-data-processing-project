import unittest

from pyspark.sql import SparkSession

from jobs.exploration.analyses import (
    declining_series, early_peak_actors, multi_talented,
    multilingual_actors, one_hit_directors)

BASICS_COLS = ["tconst", "titleType", "primaryTitle", "originalTitle", "startYear"]


class ExplorationQ4Q8Test(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[1]")
                     .config("spark.sql.shuffle.partitions", "1").getOrCreate())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def df(self, rows, cols):
        return self.spark.createDataFrame(rows, cols)

    def names(self, ids):
        return self.df([(i, i.upper()) for i in ids], ["nconst", "primaryName"])

    def test_q4_exactly_one_hit_and_low_others(self):
        ratings = {"m1": 8.5, "m2": 5.0, "m3": 6.0,           # d1: kept
                   "m4": 8.1, "m5": 8.2, "m6": 4.0, "m7": 4.0,  # d2: two hits
                   "m8": 9.0, "m9": 3.0}                        # d3: one other only
        basics = self.df([(t, "movie", t, t, 2000) for t in ratings], BASICS_COLS)
        rat = self.df([(t, r, 500) for t, r in ratings.items()],
                      ["tconst", "averageRating", "numVotes"])
        owner = {"m1": "d1", "m2": "d1", "m3": "d1", "m4": "d2", "m5": "d2",
                 "m6": "d2", "m7": "d2", "m8": "d3", "m9": "d3"}
        crew = self.df([(t, [d], []) for t, d in owner.items()],
                       "tconst string, directors array<string>, writers array<string>")
        out = one_hit_directors(basics, rat, crew, self.names(["d1", "d2", "d3"]))
        rows = out.collect()
        self.assertEqual([r.nconst for r in rows], ["d1"])
        self.assertEqual(rows[0].hit_title, "m1")
        self.assertAlmostEqual(rows[0].avg_other_rating, 5.5)

    def _series(self, sid, worst_season, best_season):
        eps, rat = [], []
        for season in (1, 2, 3):
            for i in range(10):
                t = f"{sid}s{season}e{i}"
                eps.append((t, sid, season, i + 1))
                r = 7.0
                if i == 0 and season == worst_season:
                    r = 2.0
                if i == 1 and season == best_season:
                    r = 9.5
                rat.append((t, r, 100))
        return eps, rat

    def test_q5_worst_last_best_early(self):
        e1, r1 = self._series("s1", worst_season=3, best_season=1)  # kept
        e2, r2 = self._series("s2", worst_season=1, best_season=1)  # worst too early
        basics = self.df([("s1", "tvSeries", "S1", "S1", 2000),
                          ("s2", "tvSeries", "S2", "S2", 2000)], BASICS_COLS)
        eps = self.df(e1 + e2, ["tconst", "parentTconst", "seasonNumber", "episodeNumber"])
        rat = self.df(r1 + r2, ["tconst", "averageRating", "numVotes"])
        out = declining_series(basics, rat, eps)
        self.assertEqual([r.parentTconst for r in out.collect()], ["s1"])

    def test_q6_languages_basic(self):
        basics = self.df([("m1", "movie", "A", "A", 2000), ("m2", "movie", "B", "B", 2000),
                          ("m3", "movie", "C", "C", 2000)], BASICS_COLS)
        rat = self.df([(t, 8.0, 5000) for t in ("m1", "m2", "m3")],
                      ["tconst", "averageRating", "numVotes"])
        lang = self.df([("m1", " fr "),   # trim
                        ("m2", "en"),
                        ("m2", "en"),     # duplicate row must not inflate counts
                        ("m3", "de")],
                       ["tconst", "language"])
        princ = self.df([("m1", "a1", "actor"), ("m2", "a1", "actor"), ("m3", "a1", "actor"),
                         ("m1", "a2", "actress"), ("m2", "a2", "actress"),
                         ("m1", "a3", "actor")], ["tconst", "nconst", "category"])
        out = {r.nconst: r.n_languages for r in multilingual_actors(
            basics, rat, princ, lang, self.names(["a1", "a2", "a3"])).collect()}
        self.assertEqual(out, {"a1": 3, "a2": 2})

    def test_q6_languages_from_languages_table(self):
        basics = self.df([("m1", "movie", "A", "A", 2000), ("m2", "movie", "B", "B", 2000),
                          ("m3", "movie", "C", "C", 2000)], BASICS_COLS)
        rat = self.df([(t, 8.0, 5000) for t in ("m1", "m2", "m3")],
                      ["tconst", "averageRating", "numVotes"])
        lang = self.df([("m1", " fr "),   # trim
                        ("m2", "en"),
                        ("m2", "en"),     # duplicate row must not inflate counts
                        ("m3", "de")],
                       ["tconst", "language"])
        princ = self.df([("m1", "a1", "actor"), ("m2", "a1", "actor"), ("m3", "a1", "actor"),
                         ("m1", "a2", "actress"), ("m2", "a2", "actress"),
                         ("m1", "a3", "actor")], ["tconst", "nconst", "category"])
        out = {r.nconst: r.n_languages for r in multilingual_actors(
            basics, rat, princ, lang, self.names(["a1", "a2", "a3"])).collect()}
        self.assertEqual(out, {"a1": 3, "a2": 2})

    def test_q6_ignores_null_and_empty_keeps_multilingual_films(self):
        titles = ["m1", "m2", "m3", "m4", "m5"]
        basics = self.df([(t, "movie", t, t, 2000) for t in titles], BASICS_COLS)
        rat = self.df([(t, 8.0, 5000) for t in titles],
                      ["tconst", "averageRating", "numVotes"])
        lang = self.df([("m1", "fr"),
                        ("m2", "en"), ("m2", "es"),   # multi-language film -> both kept
                        ("m3", None),                 # null -> ignored
                        ("m4", ""),                   # empty -> ignored
                        ("m5", "de")],
                       "tconst string, language string")
        princ = self.df([(t, "a1", "actor") for t in titles],
                        ["tconst", "nconst", "category"])
        rows = multilingual_actors(
            basics, rat, princ, lang, self.names(["a1"])).collect()
        out = {r.nconst: r.n_languages for r in rows}
        self.assertEqual(out, {"a1": 4})  # fr, en, es, de
        self.assertEqual(rows[0].n_successful_movies, 3)  # m1, m2, m5 (distinct)

    def test_q7_early_peak(self):
        years = range(2000, 2021)  # 21 movies, 20-year career
        basics, rat, princ = [], [], []
        for y in years:
            for actor, rating in (("a1", 9.0 - 0.2 * (y - 2000)),   # declining: kept
                                  ("a2", 5.0 + 0.2 * (y - 2000))):  # rising: excluded
                t = f"{actor}{y}"
                basics.append((t, "movie", t, t, y))
                rat.append((t, rating, 500))
                princ.append((t, actor, "actor"))
        out = early_peak_actors(self.df(basics, BASICS_COLS),
                                self.df(rat, ["tconst", "averageRating", "numVotes"]),
                                self.df(princ, ["tconst", "nconst", "category"]),
                                self.names(["a1", "a2"]))
        self.assertEqual([r.nconst for r in out.collect()], ["a1"])

    def test_q8_multi_vs_single_role(self):
        basics = self.df([("m1", "movie", "A", "A", 2000),
                          ("m2", "movie", "B", "B", 2001)], BASICS_COLS)
        rat = self.df([("m1", 8.0, 500), ("m2", 6.0, 500)],
                      ["tconst", "averageRating", "numVotes"])
        crew = self.df([("m1", ["p1"], []), ("m2", [], [])],
                       "tconst string, directors array<string>, writers array<string>")
        princ = self.df([("m1", "p1", "actor"), ("m2", "p1", "actor"),
                         ("m1", "p2", "actor")], ["tconst", "nconst", "category"])
        people, combos = multi_talented(basics, rat, crew, princ, self.names(["p1", "p2"]))
        rows = people.collect()
        self.assertEqual([r.nconst for r in rows], ["p1"])
        self.assertAlmostEqual(rows[0].rating_delta, 2.0)
        self.assertIn("actor+director", [r.combination for r in combos.collect()])


if __name__ == "__main__":
    unittest.main()