"""Select the full 1980–2025 target set and export a small collection pilot."""

import argparse
import json
from datetime import datetime, timezone

from pyspark import StorageLevel
from pyspark.sql import functions as F

from imdb_pipeline.config import CLEAN_DIR, ENRICHMENT_DIR, RESULTS_DIR
from imdb_pipeline.enrichment import select_films, select_people
from imdb_pipeline.spark import create_spark
from imdb_pipeline.tables import load_clean_tables
from jobs.preparation.prepare_titles import atomic_json


def requests_for(films, links, people):
    film_requests = films.select(
        F.col("tconst").alias("imdb_id"), F.lit("film").alias("kind"),
        F.col("primaryTitle").alias("label"),
    )
    names = people.join(links.select("nconst").distinct(), "nconst", "left_semi")
    return film_requests.unionByName(names.select(
        F.col("nconst").alias("imdb_id"), F.lit("person").alias("kind"),
        F.col("primaryName").alias("label"),
    ))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pilot-films", type=int, default=3)
    args = parser.parse_args()
    if args.pilot_films < 1:
        parser.error("--pilot-films must be positive")
    run = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    root = ENRICHMENT_DIR / "runs" / run
    selection = root / "selection"
    source = json.loads((CLEAN_DIR / "current.json").read_text(encoding="utf-8"))
    spark = create_spark("select-enrichment-targets")
    spark.conf.set("spark.sql.shuffle.partitions", "32")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
    try:
        tables = load_clean_tables(spark, names=(
            "title.basics", "title.ratings", "title.principals", "name.basics",
        ))
        if json.loads((CLEAN_DIR / "current.json").read_text())["run_id"] != source["run_id"]:
            raise ValueError("Source catalog changed during loading; retry selection")
        rankings = select_films(tables["title.basics"], tables["title.ratings"]).persist(StorageLevel.DISK_ONLY)
        rankings.write.parquet(str(selection / "film_rankings"))
        films = rankings.drop("genre", "genre_rank").dropDuplicates(["tconst"]).persist(StorageLevel.DISK_ONLY)
        film_count = films.count()
        if not film_count:
            raise ValueError("No enrichment targets selected")
        films.write.parquet(str(selection / "films"))
        links = select_people(tables["title.principals"], films).persist(StorageLevel.DISK_ONLY)
        people_keys = links.select("nconst").distinct()
        people = tables["name.basics"].join(F.broadcast(people_keys), "nconst", "left_semi").persist(StorageLevel.DISK_ONLY)
        links.write.parquet(str(selection / "film_people"))
        people.write.parquet(str(selection / "people"))
        requests_for(films, links, people).write.json(str(selection / "requests"))
        pilot = films.orderBy(F.col("numVotes").desc(), "tconst").limit(args.pilot_films)
        pilot_links = links.join(F.broadcast(pilot.select("tconst")), "tconst", "left_semi")
        requests_for(pilot, pilot_links, people).coalesce(1).write.json(str(selection / "pilot_requests"))
        counts = links.groupBy("tconst").count()
        deficient = films.select("tconst").join(counts, "tconst", "left").fillna(0, ["count"]).filter("count < 3")
        deficient.write.json(str(selection / "films_with_fewer_than_three_people"))
        summary = {
            "run_id": run, "source_catalog": source["run_id"],
            "selection": "titleType=movie; 1980<=startYear<=2025; numVotes>0; known genre",
            "ranking": "numVotes DESC, averageRating DESC NULLS LAST, tconst ASC",
            "per_genre_year": 10, "principals_per_film": 3,
            "people_order": "minimum IMDb ordering per distinct person, then nconst",
            "film_genre_year_rows": rankings.count(), "unique_films": film_count,
            "film_person_links": links.count(), "unique_people": people.count(),
            "films_with_fewer_than_three_people": deficient.count(),
            "pilot_films": args.pilot_films,
            "pilot_requests": spark.read.json(str(selection / "pilot_requests")).count(),
            "full_enrichment_complete": False,
        }
        atomic_json(root / "selection_summary.json", summary)
        atomic_json(ENRICHMENT_DIR / "current_selection.json", {"run_id": run})
        atomic_json(RESULTS_DIR / "enrichment" / run / "selection_summary.json", summary)
        print(json.dumps(summary), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
