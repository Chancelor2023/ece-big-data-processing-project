import unittest
from pyspark.sql import SparkSession
from jobs.exploration import analyses as A
from jobs.exploration.params import Params

P = Params(min_votes=0, min_genre_size=3)
B = ("tconst string, titleType string, primaryTitle string, originalTitle string, "
     "startYear int, runtimeMinutes int, genres array<string>")
R = "tconst string, averageRating double, numVotes long"


def mv(i, rt=90, g=("Drama",), y=2000):
    return (f"t{i}", "movie", f"M{i}", f"M{i}", y, rt, list(g) if g else None)


class T(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.s = SparkSession.builder.master("local[1]").getOrCreate()

    def test_q1_nulls_ignored_outlier_found(self):
        rows = [mv(i, rt=90 + i % 3) for i in range(20)]
        rows += [mv(98, rt=300), mv(99, rt=None), mv(97, g=None)]
        out = A.compute_q1(self.s.createDataFrame(rows, B), P).collect()
        self.assertEqual([x.tconst for x in out], ["t98"])

    def test_q2_ties_kept_and_null_rating_excluded(self):
        b = self.s.createDataFrame([mv(1, g=("A",)), mv(2, g=("B",)),
                                    mv(3, g=("C",)), mv(4, g=("D",)), mv(5, g=("E",))], B)
        r = self.s.createDataFrame([("t1", 7.0, 50), ("t2", 7.0, 10), ("t3", 7.0, 10),
                                    ("t4", 7.0, 10), ("t5", None, 999)], R)
        out = A.compute_q2(A.genre_year_stats(b, r, P), P).collect()
        self.assertEqual(sorted(x.genre for x in out), ["A", "B", "C", "D"])
        self.assertTrue(all(x.is_tie for x in out if x.rank == 2))

    def test_multigenre_film_counted_once_per_genre(self):
        b = self.s.createDataFrame([mv(1, g=("A", "B"))], B)
        r = self.s.createDataFrame([("t1", 8.0, 100)], R)
        out = A.genre_year_stats(b, r, P).collect()
        self.assertEqual({(x.genre, x.n_movies) for x in out}, {("A", 1), ("B", 1)})


if __name__ == "__main__":
    unittest.main()