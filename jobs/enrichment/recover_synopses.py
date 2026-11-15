"""Recover missing synopses from cached articles; preserve existing enrichment."""

import argparse
import json
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from pyspark import StorageLevel
from pyspark.sql import functions as F, types as T

from imdb_pipeline.config import ENRICHMENT_DIR, RESULTS_DIR
from imdb_pipeline.enrichment import (
    RAW_SCHEMA, SYNOPSIS_EXTRACTION_VERSION, apply_synopsis_recoveries,
    clean_collected, same_wikipedia_article,
)
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
    result = RESULTS_DIR / "enrichment" / run
    publication_path = root / "current_content.json"
    publication_bytes = publication_path.read_bytes()
    publication = json.loads(publication_bytes)
    if publication.get("synopsis_extraction_version") == SYNOPSIS_EXTRACTION_VERSION:
        print(json.dumps({"status": "already_published", "snapshot": publication["snapshot"]}), flush=True)
        return
    previous = json.loads((result / "coverage.json").read_text())
    if previous["content_snapshot"] != publication["snapshot"]:
        raise ValueError("Coverage and publication disagree")
    selection_path = root / "selection_summary.json"
    selection_bytes = selection_path.read_bytes()
    base = (root / publication["path"]).resolve()
    if not base.is_relative_to(root.resolve()):
        raise ValueError("Invalid content path")
    snapshot = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:6]
    output = root / "clean" / snapshot
    evidence = result / "synopsis_recoveries" / snapshot
    spark = create_spark("recover-cached-synopses")
    spark.conf.set("spark.sql.shuffle.partitions", "16")
    checks = {}

    def check(name, condition):
        checks[name] = bool(condition)
        if not condition:
            raise ValueError("Validation failed: " + name)

    def same_rows(left, right):
        return (left.exceptAll(right).limit(1).count() == 0
                and right.exceptAll(left).limit(1).count() == 0)

    # File access runs on Spark workers for targeted keys, without collecting
    # a potentially large path list or rereading every raw article on Windows.
    raw_directory = str(root / "raw")

    def cached_record(identifier):
        if not re.fullmatch(r"tt[0-9]+", identifier):
            raise ValueError("Invalid film identifier")
        return (Path(raw_directory) / (identifier + ".json")).read_text(encoding="utf-8")

    try:
        films = spark.read.parquet(str(base / "films"))
        people = spark.read.parquet(str(base / "people"))
        coverage = spark.read.parquet(str(base / "film_biography_coverage"))
        links_path = (root / publication.get("film_people_path", "selection/film_people")).resolve()
        if not links_path.is_relative_to(root.resolve()):
            raise ValueError("Invalid effective association path")
        links = spark.read.parquet(str(links_path))
        targets = films.filter("status = 'missing_plot_section'")
        target_count = targets.count()
        parsed = (targets.select("tconst").repartition(16)
                  .withColumn("_record", F.from_json(
                      F.udf(cached_record, T.StringType())("tconst"), RAW_SCHEMA,
                      {"mode": "FAILFAST"}))
                  .persist(StorageLevel.DISK_ONLY))
        check("cached_records_match_targets", parsed.filter(
            F.col("_record.imdb_id").isNull()
            | (F.col("_record.imdb_id") != F.col("tconst"))
            | F.col("_record.kind").isNull() | F.col("_record.status").isNull()
            | (F.col("_record.kind") != "film")
            | (F.col("_record.status") != "collected")
        ).count() == 0)
        cleaned = clean_collected(parsed.select("_record.*")).withColumnRenamed("imdb_id", "tconst")
        decisions = (cleaned.withColumn("same_article", F.udf(
            same_wikipedia_article, T.BooleanType())("article_url", "article_title"))
            .withColumn("recovery_status", F.when(~F.col("same_article"), "redirect_requires_review")
                        .otherwise(F.col("status")))
            .withColumnRenamed("text", "synopsis")
            .persist(StorageLevel.DISK_ONLY))
        recovery = decisions.filter("recovery_status = 'available'")
        recovered_count = recovery.count()
        check("recovery_keys_unique", recovery.select("tconst").distinct().count() == recovered_count)
        check("recovery_same_revision_as_base", recovery.alias("new").join(
            targets.select("tconst", "source_url", "revision_id").alias("old"), "tconst"
        ).filter(~F.col("new.source_url").eqNullSafe(F.col("old.source_url"))
                 | ~F.col("new.revision_id").eqNullSafe(F.col("old.revision_id"))).count() == 0)
        check("recovery_text_and_provenance", recovery.filter(
            F.col("synopsis").isNull() | (F.length(F.trim("synopsis")) == 0)
            | F.col("source_url").isNull() | F.col("license_url").isNull()
        ).count() == 0)
        updated = apply_synopsis_recoveries(films, recovery).persist(StorageLevel.DISK_ONLY)
        check("film_keys_and_count_preserved", updated.count() == previous["unique_films"]
              and updated.select("tconst").distinct().count() == previous["unique_films"])
        check("existing_available_synopses_unchanged", same_rows(
            updated.join(films.filter("status = 'available'").select("tconst"), "tconst", "left_semi").select(*films.columns),
            films.filter("status = 'available'")))
        check("nonrecovered_films_unchanged", same_rows(
            updated.join(recovery.select("tconst"), "tconst", "left_anti"),
            films.join(recovery.select("tconst"), "tconst", "left_anti")))
        metadata_columns = [name for name in films.columns if name not in {"status", "synopsis"}]
        check("all_film_metadata_and_lineage_unchanged", same_rows(
            updated.select(*metadata_columns), films.select(*metadata_columns)))
        before_available = films.filter("status = 'available'").count()
        after_available = updated.filter("status = 'available'").count()
        check("synopsis_gain_matches_accepted_recoveries", after_available == before_available + recovered_count)
        before_both = films.filter("status = 'available'").join(
            coverage.filter("available_biographies >= 3"), "tconst").count()
        after_both = updated.filter("status = 'available'").join(
            coverage.filter("available_biographies >= 3"), "tconst").count()
        updated.write.parquet(str(output / "films"))
        people.write.parquet(str(output / "people"))
        coverage.write.parquet(str(output / "film_biography_coverage"))
        decision_export = decisions.select(
            "tconst", "article_title", "article_url", "source_url", "revision_id",
            "same_article", "recovery_status", F.length("synopsis").alias("candidate_characters"))
        decision_export.write.parquet(str(output / "synopsis_recovery_decisions"))
        decision_export.coalesce(1).write.option("header", True).option("escape", '"').csv(str(evidence / "decisions"))
        shutil.copyfile(next((evidence / "decisions").glob("part-*.csv")), evidence / "decisions.csv")
        check("published_films_match", same_rows(spark.read.parquet(str(output / "films")), updated))
        check("people_and_biographies_unchanged", same_rows(spark.read.parquet(str(output / "people")), people))
        check("biography_coverage_unchanged", same_rows(spark.read.parquet(str(output / "film_biography_coverage")), coverage))
        check("effective_links_unchanged", links.count() == previous["film_person_links"])
        check("selection_summary_unchanged", selection_path.read_bytes() == selection_bytes)
        check("base_publication_not_changed_concurrently", publication_path.read_bytes() == publication_bytes)
        metrics = ([{"kind": "film", "status": row.status, "count": row["count"]}
                    for row in updated.groupBy("status").count().collect()]
                   + [row for row in previous["coverage"] if row["kind"] == "person"])
        summary = dict(previous)
        summary.update({
            "stage": "synopsis_recovery", "content_snapshot": snapshot,
            "synopsis_base_snapshot": publication["snapshot"],
            "synopsis_extraction_version": SYNOPSIS_EXTRACTION_VERSION,
            "synopsis_targets_examined": target_count,
            "additional_synopses": recovered_count,
            "available_synopses": after_available,
            "coverage": metrics,
            "films_with_synopsis_and_three_available_biographies": after_both,
            "additional_films_with_both_from_synopsis_recovery": after_both - before_both,
            "recovery_decisions": [{"status": row.recovery_status, "count": row["count"]}
                                   for row in decisions.groupBy("recovery_status").count().collect()],
            "synopsis_validation": checks, "raw_collection_unchanged": True,
            "biographies_unchanged": True,
            "full_enrichment_complete": after_both == previous["unique_films"]
                and all(row["status"] == "available" for row in metrics),
        })
        atomic_json(evidence / "coverage.json", summary)
        atomic_json(evidence / "base_coverage.json", previous)
        atomic_json(evidence / "base_publication.json", publication)
        manifest = dict(publication)
        manifest.update({
            "snapshot": snapshot, "path": str(output.relative_to(root)),
            "coverage": str(evidence / "coverage.json"), "stage": "synopsis_recovery",
            "base_snapshot": publication["snapshot"],
            "synopsis_extraction_version": SYNOPSIS_EXTRACTION_VERSION,
        })
        # Existing film_people_path and supplemental_film_people_path deliberately
        # retain their immutable, already verified supplemental-association files.
        atomic_json(result / "coverage.json", summary)
        atomic_json(publication_path, manifest)
        print(json.dumps(summary), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
