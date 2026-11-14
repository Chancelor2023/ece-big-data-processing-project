"""python -m jobs.exploration.run"""
import json
from pathlib import Path
from pyspark.sql import functions as F
from imdb_pipeline.config import CLEAN_DIR
from imdb_pipeline.spark import create_spark
from imdb_pipeline.tables import load_clean_tables
from jobs.exploration import analyses as A
from jobs.exploration.params import Params

OUT = Path("results/exploration")  # À VÉRIFIER : constante dans config ?


def run_id():
    m = json.loads((CLEAN_DIR / "current.json").read_text("utf-8"))
    return str(m.get("run_id") or m.get("run") or "unknown-run")  # À VÉRIFIER



def write(spark, df, path):
    df.write.mode("overwrite").parquet(str(path))
    print(f"wrote {path} ({spark.read.parquet(str(path)).count():,} rows)", flush=True)


def main():
    spark, p, rid = create_spark("exploration"), Params(), run_id()
    t = load_clean_tables(spark)
    b, r = t["title.basics"], t["title.ratings"]
    write(A.compute_q1(b, p), rid, "q1_runtime_outliers")
    stats = A.genre_year_stats(b, r, p).cache()
    # Cardinality check: one row per (year, genre)
    assert stats.groupBy("startYear", "genre").count().filter("count > 1").limit(1).count() == 0
    write(A.compute_q2(stats, p), rid, "q2_top_genres")
    write(A.compute_q3(stats, p), rid, "q3_top_vs_rest")
    basics, ratings = load("title.basics"), load("title.ratings")
    crew, episodes = load("title.crew"), load("title.episode")
    principals, akas, names = load("title.principals"), load("title.akas"), load("name.basics")

    write(spark, one_hit_directors(basics, ratings, crew, names, P.Q4_HIGH_RATING,
        P.Q4_LOW_AVG_RATING, P.Q4_MIN_OTHER_MOVIES, P.Q4_MIN_VOTES),
        out_dir / "q4_one_hit_directors")

    write(spark, declining_series(basics, ratings, episodes, P.Q5_MIN_FULL_SEASONS,
        P.Q5_MIN_EPISODES_PER_SEASON, P.Q5_MIN_EPISODE_VOTES),
        out_dir / "q5_declining_series")

    q6 = multilingual_actors(basics, ratings, principals, akas, names,
                            P.Q6_SUCCESS_RATING, P.Q6_SUCCESS_MIN_VOTES, P.Q6_MIN_LANGUAGES)
    write(spark, q6, out_dir / "q6_multilingual_actors")
    q6_written = spark.read.parquet(str(out_dir / "q6_multilingual_actors"))
    write(spark, q6_written.filter(f"n_languages >= {P.Q6_MIN_LANGUAGES_STRICT}"),
        out_dir / "q6_actors_3plus_languages")

    write(spark, early_peak_actors(basics, ratings, principals, names,
        P.Q7_MIN_CAREER_YEARS, P.Q7_MIN_MOVIES, P.Q7_MIN_VOTES),
        out_dir / "q7_early_peak_actors")

    q8_people, q8_combos = multi_talented(basics, ratings, crew, principals, names, P.Q8_MIN_VOTES)
    write(spark, q8_people, out_dir / "q8_multi_talented_people")
    write(spark, q8_combos, out_dir / "q8_role_combinations")
    spark.stop()


if __name__ == "__main__":
    main()