"""python -m jobs.exploration.run"""
import json
from pathlib import Path
from pyspark.sql import functions as F
from imdb_pipeline.config import CLEAN_DIR
from imdb_pipeline.spark import create_spark
from imdb_pipeline.tables import load_clean_tables
from jobs.exploration import analyses as A
from jobs.exploration.params import Params
from jobs.exploration import params as P

OUT = Path("results/exploration")


def run_id():
    m = json.loads((CLEAN_DIR / "current.json").read_text("utf-8"))
    return str(m.get("run_id") or m.get("run") or "unknown-run")


def write(spark, df, rid, name):
    path = OUT / rid / name
    df.write.mode("overwrite").parquet(str(path))
    print(f"wrote {path} ({spark.read.parquet(str(path)).count():,} rows)", flush=True)


def main():
    spark, p, rid = create_spark("exploration"), Params(), run_id()
    t = load_clean_tables(spark, (
    "title.basics", "title.ratings", "title.crew", "title.episode",
    "title.principals", "title.akas", "name.basics"))
    basics, ratings = t["title.basics"], t["title.ratings"]
    crew, episodes = t["title.crew"], t["title.episode"]
    principals, akas, names = t["title.principals"], t["title.akas"], t["name.basics"]

    write(spark, A.compute_q1(basics, p), rid, "q1_runtime_outliers")
    stats = A.genre_year_stats(basics, ratings, p).cache()
    assert stats.groupBy("startYear", "genre").count().filter("count > 1").limit(1).count() == 0
    write(spark, A.compute_q2(stats, p), rid, "q2_top_genres")
    write(spark, A.compute_q3(stats, p), rid, "q3_top_vs_rest")

    write(spark, A.one_hit_directors(
        basics, ratings, crew, names, P.Q4_HIGH_RATING, P.Q4_LOW_AVG_RATING,
        P.Q4_MIN_OTHER_MOVIES, P.Q4_MIN_VOTES), rid, "q4_one_hit_directors")

    write(spark, A.declining_series(
        basics, ratings, episodes, P.Q5_MIN_FULL_SEASONS,
        P.Q5_MIN_EPISODES_PER_SEASON, P.Q5_MIN_EPISODE_VOTES),
        rid, "q5_declining_series")

    write(spark, A.multilingual_actors(
        basics, ratings, principals, akas, names, P.Q6_SUCCESS_RATING,
        P.Q6_SUCCESS_MIN_VOTES, P.Q6_MIN_LANGUAGES),
        rid, "q6_multilingual_actors")

    write(spark, A.early_peak_actors(
        basics, ratings, principals, names, P.Q7_MIN_CAREER_YEARS,
        P.Q7_MIN_MOVIES, P.Q7_MIN_VOTES), rid, "q7_early_peak_actors")

    per_person, per_combo = A.multi_talented(
        basics, ratings, crew, principals, names, P.Q8_MIN_VOTES)
    write(spark, per_person, rid, "q8_multi_talented_people")
    write(spark, per_combo, rid, "q8_multi_talented_combinations")


if __name__ == "__main__":
    main()