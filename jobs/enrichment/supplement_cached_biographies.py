"""Publish supplementary IMDb principal links without collecting new text."""

import argparse
import hashlib
import json
import re
from datetime import datetime, timezone
from uuid import uuid4

from pyspark import StorageLevel
from pyspark.sql import functions as F

from imdb_pipeline.config import CLEAN_DIR, ENRICHMENT_DIR, RESULTS_DIR
from imdb_pipeline.spark import create_spark
from imdb_pipeline.supplements import biography_coverage, cached_biography_supplements
from imdb_pipeline.tables import load_clean_tables
from jobs.preparation.prepare_titles import atomic_json


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id")
    args = parser.parse_args()
    run = args.run_id or json.loads(
        (ENRICHMENT_DIR / "current_selection.json").read_text()
    )["run_id"]
    if not re.fullmatch(r"[0-9]{8}T[0-9]{6}Z", run):
        parser.error("Invalid run identifier")
    root = ENRICHMENT_DIR / "runs" / run
    result = RESULTS_DIR / "enrichment" / run
    publication_path = root / "current_content.json"
    publication_bytes = publication_path.read_bytes()
    publication = json.loads(publication_bytes)
    if publication.get("stage") == "cached_biography_supplement":
        print(json.dumps({"status": "already_published", "snapshot": publication["snapshot"]}), flush=True)
        return
    selection_bytes = (root / "selection_summary.json").read_bytes()
    selection = json.loads(selection_bytes)
    catalog_bytes = (CLEAN_DIR / "current.json").read_bytes()
    if json.loads(catalog_bytes)["run_id"] != selection["source_catalog"]:
        raise ValueError("IMDb catalog differs from the selected source catalog")
    base = (root / publication["path"]).resolve()
    if not base.is_relative_to(root.resolve()):
        raise ValueError("Content path outside selected run")
    previous = json.loads((result / "coverage.json").read_text())
    if previous["content_snapshot"] != publication["snapshot"]:
        raise ValueError("Coverage and content publication disagree")
    snapshot = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:6]
    output = root / "clean" / snapshot
    evidence = result / "supplements" / snapshot
    spark = create_spark("supplement-cached-biographies")
    spark.conf.set("spark.sql.shuffle.partitions", "16")
    checks = {}

    def check(name, condition):
        checks[name] = bool(condition)
        if not condition:
            raise ValueError("Validation failed: " + name)

    def same_rows(left, right):
        return (left.exceptAll(right).limit(1).count() == 0
                and right.exceptAll(left).limit(1).count() == 0)

    try:
        films = spark.read.parquet(str(base / "films"))
        people = spark.read.parquet(str(base / "people"))
        original = spark.read.parquet(str(root / "selection" / "film_people"))
        old_coverage = spark.read.parquet(str(base / "film_biography_coverage"))
        computed_old = biography_coverage(films, original, people)
        check("baseline_coverage_recomputed", same_rows(
            old_coverage.select("tconst", "available_biographies"), computed_old))
        principals = load_clean_tables(spark, names=("title.principals",))["title.principals"]
        additions = cached_biography_supplements(
            principals, original, people, old_coverage
        ).persist(StorageLevel.DISK_ONLY)
        addition_count = additions.count()
        print(f"Validated {addition_count} proposed film/person additions", flush=True)
        check("additions_unique", additions.select("tconst", "nconst").distinct().count() == addition_count)
        check("additions_not_original", additions.join(
            original.select("tconst", "nconst"), ["tconst", "nconst"], "inner").count() == 0)
        check("available_biography_provenance", additions.filter(
            F.col("biography_source_url").isNull() | F.col("biography_revision_id").isNull()
        ).count() == 0)
        available_lineage = people.filter("status = 'available'").select(
            "nconst", F.col("source_url").alias("biography_source_url"),
            F.col("revision_id").alias("biography_revision_id"))
        initial = (original.join(available_lineage, "nconst", "left")
                   .withColumn("association_source", F.lit("initial_selection"))
                   .withColumn("supplement_rank", F.lit(None).cast("int")))
        extra = (additions.drop("primaryName")
                 .withColumn("principal_rank", F.lit(None).cast("int"))
                 .withColumn("association_source", F.lit("cached_biography_supplement")))
        merged = (initial.unionByName(extra)
                  .withColumn("evidence_table", F.lit("title.principals"))
                  .withColumn("source_catalog", F.lit(selection["source_catalog"])))
        check("original_links_preserved", same_rows(
            merged.filter("association_source = 'initial_selection'").select(*original.columns), original))
        check("merged_links_unique", merged.select("tconst", "nconst").distinct().count() == original.count() + addition_count)
        updated = biography_coverage(films, merged, people).persist(StorageLevel.DISK_ONLY)
        check("no_biography_regression", updated.alias("new").join(
            old_coverage.alias("old"), "tconst").filter(
                F.col("new.available_biographies") < F.col("old.available_biographies")
            ).count() == 0)
        check("at_most_three_available_biographies", updated.filter("available_biographies > 3").count() == 0)
        before_three = old_coverage.filter("available_biographies >= 3").count()
        after_three = updated.filter("available_biographies >= 3").count()
        before_both = films.filter("status = 'available'").join(
            old_coverage.filter("available_biographies >= 3"), "tconst").count()
        after_both = films.filter("status = 'available'").join(
            updated.filter("available_biographies >= 3"), "tconst").count()
        prior_audit_path = result / "current_improvement_audit.json"
        if prior_audit_path.exists():
            prior = json.loads(prior_audit_path.read_text())
            audit = json.loads((result / prior["path"] / "summary.json").read_text())
            if audit["content_snapshot"] == publication["snapshot"]:
                check("matches_prior_audit_additions", addition_count == audit["proposed_film_person_links"])
                check("matches_prior_audit_three", after_three - before_three == audit["films_reaching_three_using_already_collected_biographies"])
                check("matches_prior_audit_both", after_both - before_both == audit["films_with_synopsis_reaching_three_using_already_collected_biographies"])
        films.write.parquet(str(output / "films"))
        people.write.parquet(str(output / "people"))
        merged.write.parquet(str(output / "film_people"))
        additions.write.parquet(str(output / "supplemental_film_people"))
        updated.write.parquet(str(output / "film_biography_coverage"))
        check("films_unchanged_on_readback", same_rows(spark.read.parquet(str(output / "films")), films))
        check("people_and_biographies_unchanged_on_readback", same_rows(spark.read.parquet(str(output / "people")), people))
        published_links = spark.read.parquet(str(output / "film_people"))
        published_coverage = spark.read.parquet(str(output / "film_biography_coverage"))
        check("coverage_recomputed_from_published_links", same_rows(
            biography_coverage(films, published_links, people), published_coverage))
        check("published_film_count", published_coverage.count() == selection["unique_films"])
        check("selection_summary_unchanged", (root / "selection_summary.json").read_bytes() == selection_bytes)
        check("source_catalog_unchanged", (CLEAN_DIR / "current.json").read_bytes() == catalog_bytes)
        check("base_publication_not_changed_concurrently", publication_path.read_bytes() == publication_bytes)
        summary = dict(previous)
        summary.update({
            "content_snapshot": snapshot, "stage": "cached_biography_supplement",
            "base_content_snapshot": publication["snapshot"],
            "selection_film_person_links": selection["film_person_links"],
            "film_person_links": published_links.count(),
            "supplemental_film_person_links": addition_count,
            "films_with_supplemental_links": additions.select("tconst").distinct().count(),
            "films_with_three_available_biographies": after_three,
            "films_with_synopsis_and_three_available_biographies": after_both,
            "additional_films_with_three_biographies": after_three - before_three,
            "additional_films_with_synopsis_and_three_biographies": after_both - before_both,
            "full_enrichment_complete": after_three == selection["unique_films"] and all(
                row["status"] == "available" for row in previous["coverage"]),
            "supplement_validation": checks,
            "selection_summary_sha256": hashlib.sha256(selection_bytes).hexdigest(),
            "text_collection_unchanged": True,
        })
        atomic_json(evidence / "coverage.json", summary)
        atomic_json(evidence / "base_publication.json", publication)
        atomic_json(evidence / "base_coverage.json", previous)
        # Publish only after all data and checks are complete; keep earlier snapshots.
        atomic_json(result / "coverage.json", summary)
        atomic_json(publication_path, {
            "snapshot": snapshot, "path": str(output.relative_to(root)),
            "coverage": str(evidence / "coverage.json"),
            "stage": "cached_biography_supplement",
            "base_snapshot": publication["snapshot"],
            "film_people_path": str((output / "film_people").relative_to(root)),
            "supplemental_film_people_path": str((output / "supplemental_film_people").relative_to(root)),
        })
        print(json.dumps(summary), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
