"""Clean collected text with Spark and measure coverage against the full target set."""

import argparse
import json
import re
from datetime import datetime, timezone
from uuid import uuid4

from pyspark import StorageLevel
from pyspark.sql import functions as F

from imdb_pipeline.config import ENRICHMENT_DIR, RESULTS_DIR
from imdb_pipeline.enrichment import clean_collected, read_collected
from imdb_pipeline.spark import create_spark
from jobs.preparation.prepare_titles import atomic_json


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id")
    args = parser.parse_args()
    run = args.run_id or json.loads((ENRICHMENT_DIR / "current_selection.json").read_text())["run_id"]
    if not re.fullmatch(r"[0-9]{8}T[0-9]{6}Z", run):
        parser.error("Invalid run identifier")
    root = ENRICHMENT_DIR / "runs" / run
    if not list((root / "raw").glob("*.json")):
        raise ValueError("No collected records: run collect_wikipedia first")
    snapshot = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:6]
    output = root / "clean" / snapshot
    spark = create_spark("prepare-wikipedia-enrichment")
    try:
        raw = read_collected(spark, root / "raw")
        if raw.groupBy("imdb_id").count().filter("count > 1").limit(1).count():
            raise ValueError("Duplicate collected identifiers")
        content = clean_collected(raw).persist(StorageLevel.DISK_ONLY)
        requests = spark.read.json(str(root / "selection" / "requests"))
        expected = requests.join(content, ["imdb_id", "kind"], "left").withColumn(
            "status", F.coalesce(F.col("status"), F.lit("pending"))
        ).persist(StorageLevel.DISK_ONLY)
        if expected.filter("status = 'available' AND (source_url IS NULL OR length(text) = 0)").limit(1).count():
            raise ValueError("Available text must have content and revision provenance")
        films = spark.read.parquet(str(root / "selection" / "films"))
        people = spark.read.parquet(str(root / "selection" / "people"))
        film_content = expected.filter("kind = 'film'").drop("kind", "label").withColumnRenamed("imdb_id", "tconst").withColumnRenamed("text", "synopsis")
        person_content = expected.filter("kind = 'person'").drop("kind", "label").withColumnRenamed("imdb_id", "nconst").withColumnRenamed("text", "biography")
        films.join(film_content, "tconst", "left").write.parquet(str(output / "films"))
        people.join(person_content, "nconst", "left").write.parquet(str(output / "people"))
        links = spark.read.parquet(str(root / "selection" / "film_people"))
        coverage = links.join(person_content.select("nconst", "status"), "nconst", "left").groupBy("tconst").agg(
            F.countDistinct(F.when(F.col("status") == "available", F.col("nconst"))).alias("available_biographies")
        )
        films.select("tconst").join(coverage, "tconst", "left").fillna(0, ["available_biographies"]).write.parquet(str(output / "film_biography_coverage"))
        metrics = [{"kind": r.kind, "status": r.status, "count": r["count"]} for r in expected.groupBy("kind", "status").count().collect()]
        summary = json.loads((root / "selection_summary.json").read_text())
        films_with_three = coverage.filter("available_biographies >= 3").count()
        summary.update({
            "content_snapshot": snapshot, "coverage": metrics,
            "films_with_three_available_biographies": films_with_three,
            "collection_attempted_for_all_targets": not any(r["status"] in ("pending", "api_error") for r in metrics),
            "full_enrichment_complete": films_with_three == summary["unique_films"] and not any(
                r["status"] != "available" for r in metrics
            ),
        })
        for name, count in (("films", summary["unique_films"]), ("people", summary["unique_people"])):
            if spark.read.parquet(str(output / name)).count() != count:
                raise ValueError(f"Output count mismatch: {name}")
        atomic_json(RESULTS_DIR / "enrichment" / run / "coverage.json", summary)
        atomic_json(root / "current_content.json", {"snapshot": snapshot, "path": str(output.relative_to(root)), "coverage": str(RESULTS_DIR / "enrichment" / run / "coverage.json")})
        print(json.dumps(summary), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
