"""Read-only audit of films with fewer than three selected IMDb principals."""

import argparse
import json
import shutil
from datetime import datetime, timezone
from itertools import islice
from pathlib import Path
from uuid import uuid4

from pyspark import StorageLevel
from pyspark.sql import functions as F, types as T

from imdb_pipeline.config import CLEAN_DIR, ENRICHMENT_DIR, RESULTS_DIR
from imdb_pipeline.spark import create_spark
from imdb_pipeline.tables import load_clean_tables
from imdb_pipeline.wikimedia import WikimediaClient
from jobs.preparation.prepare_titles import atomic_json

CREDIT_PROPERTIES = ("P161", "P57", "P58", "P162", "P86", "P344", "P1040", "P725", "P175", "P3092")


def count_people(frame, column):
    return frame.groupBy("tconst").agg(F.countDistinct("nconst").alias(column))


def export_csv(frame, root, name):
    columns = [F.to_json(F.col(field.name)).alias(field.name)
               if isinstance(field.dataType, (T.ArrayType, T.StructType)) else F.col(field.name)
               for field in frame.schema]
    directory = root / f"{name}_parts"
    frame.select(*columns).coalesce(1).write.option("header", True).option("escape", '"').csv(str(directory))
    shutil.copyfile(next(directory.glob("part-*.csv")), root / f"{name}.csv")


def collect_claims(film_ids, output):
    client = WikimediaClient(ENRICHMENT_DIR / "cache" / "shortfall-audit-http")
    iterator = iter(film_ids)
    number = 0
    while group := list(islice(iterator, 20)):
        values = " ".join(json.dumps(identifier) for identifier in group)
        properties = " ".join("wdt:" + name for name in CREDIT_PROPERTIES)
        query = f'''SELECT ?filmId ?film ?role ?person ?personId ?article WHERE {{
            VALUES ?filmId {{ {values} }}
            ?film wdt:P345 ?filmId .
            OPTIONAL {{
                VALUES ?role {{ {properties} }}
                ?film ?role ?person . ?person wdt:P31 wd:Q5 .
                OPTIONAL {{ ?person wdt:P345 ?personId . }}
                OPTIONAL {{ ?article schema:about ?person;
                    schema:isPartOf <https://en.wikipedia.org/> . }}
            }}
        }}'''
        response = client.get("https://query.wikidata.org/sparql", {"query": query, "format": "json"})
        atomic_json(output / f"response-{number:04d}.json", response)
        number += 1


def read_claims(spark, output):
    term = T.StructType([T.StructField("value", T.StringType())])
    binding = T.StructType([T.StructField(name, term) for name in (
        "filmId", "film", "role", "person", "personId", "article",
    )])
    raw = spark.read.schema("url string, retrieved_at_utc string, raw_response string").option("multiLine", True).json(str(output))
    if raw.filter("raw_response IS NULL OR url IS NULL").limit(1).count():
        raise ValueError("Invalid saved Wikidata response envelope")
    parsed = raw.withColumn("bindings", F.from_json(
        F.get_json_object("raw_response", "$.results.bindings"), T.ArrayType(binding)
    ))
    if parsed.filter("bindings IS NULL").limit(1).count():
        raise ValueError("Wikidata response does not contain a readable bindings array")
    return parsed.withColumn("b", F.explode("bindings")).select(
        F.col("b.filmId.value").alias("tconst"), F.col("b.film.value").alias("film_entity_uri"),
        F.col("b.person.value").alias("person_entity_uri"), F.col("b.personId.value").alias("nconst"),
        F.col("b.article.value").alias("biography_article_url"),
        F.regexp_extract("b.role.value", r"/(P[0-9]+)$", 1).alias("role"),
        F.col("url").alias("lookup_url"), F.col("retrieved_at_utc").alias("lookup_at_utc"),
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wikidata", action="store_true", help="Gather supplementary credit evidence; does not change selection")
    args = parser.parse_args()
    run = json.loads((ENRICHMENT_DIR / "current_selection.json").read_text())["run_id"]
    root = ENRICHMENT_DIR / "runs" / run
    selection_info = json.loads((root / "selection_summary.json").read_text())
    catalog = json.loads((CLEAN_DIR / "current.json").read_text())
    if catalog["run_id"] != selection_info["source_catalog"]:
        raise ValueError("Selection/catalog mismatch: audit the original source catalog")
    audit_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:6]
    output = RESULTS_DIR / "enrichment" / run / "principal_audits" / audit_id
    output.mkdir(parents=True, exist_ok=False)
    spark = create_spark("audit-principal-shortfalls")
    spark.conf.set("spark.sql.shuffle.partitions", "16")
    try:
        films = spark.read.parquet(str(root / "selection" / "films"))
        selected = spark.read.parquet(str(root / "selection" / "film_people"))
        cases = films.join(count_people(selected, "selected_people"), "tconst", "left").fillna(0, ["selected_people"]).filter("selected_people < 3")
        case_count = cases.count()
        if case_count != selection_info["films_with_fewer_than_three_people"]:
            raise ValueError("Recorded shortfall count does not match selected links")
        if case_count == 0:
            atomic_json(output / "summary.json", {"case_count": 0, "selection_changed": False})
            return
        case_ids = cases.select("tconst")
        tables = load_clean_tables(spark, names=("title.principals", "title.crew", "name.basics"))
        principals = tables["title.principals"].join(F.broadcast(case_ids), "tconst", "left_semi").persist(StorageLevel.DISK_ONLY)
        principals.write.parquet(str(output / "original_valid_principals"))
        primary = principals.select("tconst", "nconst", F.col("category").alias("role")).withColumn("source", F.lit("title.principals"))
        crew = tables["title.crew"].join(F.broadcast(case_ids), "tconst", "left_semi")
        crew_edges = crew.select("tconst", F.explode("directors").alias("nconst")).withColumn("role", F.lit("director")).unionByName(
            crew.select("tconst", F.explode("writers").alias("nconst")).withColumn("role", F.lit("writer"))
        ).withColumn("source", F.lit("title.crew"))
        local_people = primary.unionByName(crew_edges).persist(StorageLevel.DISK_ONLY)
        quality = json.loads(Path(catalog["quality_report"]).read_text())
        principal_quality = quality["tables"]["title.principals"]
        if sum(principal_quality["rejected_records"].values()) or principal_quality["duplicate_rows_quarantined"]:
            raise ValueError("Raw reconstruction needs additional rejected/duplicate source records")
        quarantine = Path(catalog["quality_report"]).parent / "quarantine" / "title.principals"
        orphan_paths = [str(p) for p in quarantine.glob("missing_*") if p.is_dir()]
        orphans = spark.read.parquet(*orphan_paths).join(F.broadcast(case_ids), "tconst", "left_semi") if orphan_paths else principals.limit(0)
        orphans.write.parquet(str(output / "orphaned_principals"))
        raw_people = principals.select("tconst", "nconst").unionByName(orphans.select("tconst", "nconst"))
        cases = cases.join(count_people(principals, "valid_principals"), "tconst", "left").join(
            count_people(raw_people, "source_principals"), "tconst", "left"
        ).join(count_people(orphans, "orphaned_principals"), "tconst", "left").join(
            count_people(local_people, "known_people_with_crew"), "tconst", "left"
        ).fillna(0, ["valid_principals", "source_principals", "orphaned_principals", "known_people_with_crew"])
        cases = cases.withColumn("diagnosis", F.when(
            F.col("valid_principals") >= 3, "selection_discrepancy"
        ).when(F.col("source_principals") >= 3, "missing_person_parents").otherwise("source_principals_insufficient"))
        claims = None
        if args.wikidata:
            collect_claims((r.tconst for r in case_ids.toLocalIterator()), output / "wikidata_evidence")
            claims = read_claims(spark, output / "wikidata_evidence").persist(StorageLevel.DISK_ONLY)
            claims.write.parquet(str(output / "wikidata_claims"))
            export_csv(claims.orderBy("tconst", "person_entity_uri", "role"), output, "wikidata_claims")
            mappings = claims.groupBy("tconst").agg(F.countDistinct("film_entity_uri").alias("film_entity_matches"))
            # Do not combine credits from multiple Wikidata entities for one film ID.
            usable = claims.join(mappings.filter("film_entity_matches = 1"), "tconst").filter(
                F.col("person_entity_uri").isNotNull() & F.col("nconst").rlike(r"^nm[0-9]+$")
            )
            wikidata_people = usable.select("tconst", "nconst", "role").withColumn("source", F.lit("Wikidata"))
            all_people = local_people.unionByName(wikidata_people)
            cases = cases.join(mappings, "tconst", "left").fillna(0, ["film_entity_matches"])
        else:
            all_people = local_people
        # Unknown parent IDs from Wikidata remain evidence, not valid IMDb candidates.
        keys = all_people.select("nconst").distinct()
        names = tables["name.basics"].join(F.broadcast(keys), "nconst", "left_semi").select("nconst", "primaryName")
        candidates = all_people.join(F.broadcast(names), "nconst", "inner").groupBy("tconst", "nconst", "primaryName").agg(
            F.sort_array(F.collect_set("source")).alias("sources"),
            F.sort_array(F.collect_set("role")).alias("roles"),
        ).persist(StorageLevel.DISK_ONLY)
        candidates = candidates.withColumn("already_selected", F.array_contains("sources", "title.principals"))
        candidates.write.parquet(str(output / "candidate_people"))
        cases = cases.join(count_people(candidates, "known_people_with_wikidata"), "tconst", "left").fillna(0, ["known_people_with_wikidata"])
        cases = cases.withColumn("crew_can_reach_three", F.col("known_people_with_crew") >= 3).withColumn(
            "all_evidence_can_reach_three", F.col("known_people_with_wikidata") >= 3
        ).withColumn("additional_people_still_needed", F.greatest(F.lit(0), F.lit(3) - F.col("known_people_with_wikidata")))
        cases = cases.select(
            "tconst", "primaryTitle", "startYear", "genres", "numVotes", "averageRating",
            "selected_people", "valid_principals", "source_principals", "orphaned_principals",
            "known_people_with_crew", "known_people_with_wikidata", "diagnosis",
            "crew_can_reach_three", "all_evidence_can_reach_three", "additional_people_still_needed",
            *( ["film_entity_matches"] if args.wikidata else [] ),
        ).persist(StorageLevel.DISK_ONLY)
        cases.write.parquet(str(output / "film_cases"))
        export_csv(cases.orderBy("startYear", "primaryTitle"), output, "film_cases")
        export_csv(candidates.orderBy("tconst", "nconst"), output, "candidate_people")
        if cases.count() != case_count or cases.filter("valid_principals != selected_people").count():
            raise ValueError("Audit invariant failed: selected and available distinct principals differ")
        summary = {
            "status": "passed",
            "run_id": run, "audit_id": audit_id, "source_catalog": catalog["run_id"],
            "case_count": case_count, "selection_changed": False, "requests_changed": False,
            "wikidata_checked": args.wikidata,
            "wikidata_credit_properties": list(CREDIT_PROPERTIES) if args.wikidata else [],
            "wikidata_binding_rows": claims.count() if claims is not None else 0,
            "diagnoses": {r.diagnosis: r["count"] for r in cases.groupBy("diagnosis").count().collect()},
            "selected_people_distribution": {str(r.selected_people): r["count"] for r in cases.groupBy("selected_people").count().collect()},
            "films_affected_by_missing_parents": cases.filter("orphaned_principals > 0").count(),
            "crew_can_reach_three": cases.filter("crew_can_reach_three").count(),
            "all_evidence_can_reach_three": cases.filter("all_evidence_can_reach_three").count(),
            "still_below_three_with_all_evidence": cases.filter("NOT all_evidence_can_reach_three").count(),
            "candidate_count": candidates.count(),
            "supplementary_candidates_not_imdb_principals": candidates.filter("NOT already_selected").count(),
            "biographies_not_collected_by_this_audit": True,
        }
        atomic_json(output / "summary.json", summary)
        atomic_json(RESULTS_DIR / "enrichment" / run / "current_principal_audit.json", {"audit_id": audit_id, "path": str(output)})
        print(json.dumps(summary), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
