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


def write(df, rid, name):
    out = OUT / rid / name
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(out))
    print(f"wrote {out}")


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
    spark.stop()


if __name__ == "__main__":
    main()