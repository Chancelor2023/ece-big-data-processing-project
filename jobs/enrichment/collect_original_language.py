"""Collect Wikidata original language (P364) for every Q6-eligible film.

Eligible = movies with a rating, numVotes >= Q6_SUCCESS_MIN_VOTES and
averageRating >= Q6_SUCCESS_RATING (same constants as the Q6 analysis).
Matching is by exact IMDb id (P345). Output next to the Q6 input:

  data/clean/film_original_language               Parquet (tconst, language, label, iso639_1)
  data/clean/film_original_language_status.json   read by jobs.exploration.run
  data/clean/film_original_language_provenance.json

Validation: accuracy against a hand-verified sample
(data/enrichment/ground_truth_language.csv, columns tconst,iso639_1).
The akas overlap is published as an indicator only: it measures translation
coverage, not the accuracy of Wikidata.

Usage: python -m jobs.enrichment.collect_original_language [--limit N] [--refresh]
"""

import argparse
import csv
import hashlib
import json
import re
from collections import Counter
from datetime import datetime, timezone
from itertools import islice

from imdb_pipeline.config import CLEAN_DIR, ENRICHMENT_DIR
from imdb_pipeline.wikimedia import WikimediaClient, save_json
from jobs.exploration import params as P

QUERY_ENDPOINT = "https://query.wikidata.org/sparql"
BATCH_SIZE = 300
TRUTH_FILE = ENRICHMENT_DIR / "ground_truth_language.csv"  # tconst,iso639_1
TRUTH_MIN_SAMPLE = 25
TRUTH_ACCURACY_FLOOR = 0.95
OUTPUT = CLEAN_DIR / "film_original_language"
STATUS = CLEAN_DIR / "film_original_language_status.json"
PROVENANCE = CLEAN_DIR / "film_original_language_provenance.json"
CACHE = ENRICHMENT_DIR / "cache" / "p364"
QID = re.compile(r"/(Q[0-9]+)$")


def lookup_query(ids):
    if not all(re.fullmatch(r"tt[0-9]+", value) for value in ids):
        raise ValueError("Invalid IMDb identifier in collection request")
    values = " ".join(json.dumps(value) for value in ids)
    return f'''SELECT ?imdb ?item ?lang ?langLabel ?iso WHERE {{
        VALUES ?imdb {{ {values} }}
        ?item wdt:P345 ?imdb .
        OPTIONAL {{ ?item wdt:P364 ?lang .
                    OPTIONAL {{ ?lang wdt:P218 ?iso . }} }}
        SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}'''


def classify(identifier, bindings):
    """Turn the SPARQL rows of one film into a record (pure, no I/O)."""
    rows = [r for r in bindings if r["imdb"]["value"] == identifier]
    entities = {r["item"]["value"] for r in rows}
    record = {"tconst": identifier, "languages": []}
    if not rows:
        record["status"] = "not_found"
    elif len(entities) != 1:
        record["status"] = "ambiguous"
    else:
        seen = {}
        for r in rows:
            if "lang" not in r:
                continue
            match = QID.search(r["lang"]["value"])
            if match:
                seen[match.group(1)] = {
                    "language": match.group(1),
                    "label": r.get("langLabel", {}).get("value"),
                    "iso639_1": r.get("iso", {}).get("value"),
                }
        record["languages"] = [seen[k] for k in sorted(seen)]
        record["status"] = "ok" if seen else "no_p364"
    return record


def eligible_films(spark):
    """Q6-eligible tconsts, using the same constants as the analysis."""
    from jobs.exploration import analyses as A
    from imdb_pipeline.tables import load_clean_tables
    tables = load_clean_tables(spark, names=("title.basics", "title.ratings", "title.akas"))
    films = A.rated_movies(tables["title.basics"], tables["title.ratings"],
                           P.Q6_SUCCESS_MIN_VOTES)
    films = films.filter(films.averageRating >= P.Q6_SUCCESS_RATING)
    return sorted(r.tconst for r in films.select("tconst").collect()), tables


def akas_title_language_overlap(spark, tables, records):
    """Share of films whose Wikidata language appears among the languages of their
    translated titles in title.akas.

    Indicative only: title.akas carries no language on the original-title row, so
    this measures translation coverage, not the accuracy of Wikidata."""
    from pyspark.sql import functions as F
    expected = {r["tconst"]: {l["iso639_1"] for l in r["languages"] if l["iso639_1"]}
                for r in records if r["status"] == "ok"}
    expected = {t: s for t, s in expected.items() if s}
    if not expected:
        return 0, None
    akas = (tables["title.akas"]
            .filter(F.col("titleId").isin(list(expected)) & F.col("language").isNotNull())
            .select(F.col("titleId").alias("tconst"), "language").distinct())
    seen = {}
    for r in akas.collect():
        seen.setdefault(r.tconst, set()).add(r.language)
    sample = [t for t in expected if t in seen]
    if not sample:
        return 0, None
    overlap = sum(1 for t in sample if expected[t] & seen[t])
    return len(sample), overlap / len(sample)


def ground_truth_accuracy(records):
    """Accuracy of Wikidata P364 against a hand-verified sample (one ISO 639-1
    code per film, checked on an independent source)."""
    if not TRUTH_FILE.exists():
        return 0, None
    with TRUTH_FILE.open(encoding="utf-8", newline="") as handle:
        truth = {row["tconst"]: row["iso639_1"].strip()
                 for row in csv.DictReader(handle)
                 if row.get("iso639_1", "").strip()}
    got = {r["tconst"]: {l["iso639_1"] for l in r["languages"] if l["iso639_1"]}
           for r in records if r["status"] == "ok"}
    checked = [t for t in truth if t in got]
    if not checked:
        return 0, None
    correct = sum(1 for t in checked if truth[t] in got[t])
    return len(checked), correct / len(checked)


def collect(targets, client, refresh):
    records, counts = [], Counter()
    todo = []
    for identifier in targets:
        cache = CACHE / f"{identifier}.json"
        if not refresh and cache.exists():
            cached = json.loads(cache.read_text(encoding="utf-8"))
            if cached["status"] != "api_error":
                records.append(cached)
                counts["reused"] += 1
                continue
        todo.append(identifier)
    iterator = iter(todo)
    while batch := list(islice(iterator, BATCH_SIZE)):
        try:
            lookup = client.get(QUERY_ENDPOINT, {"query": lookup_query(batch),
                                                 "format": "json"})
            bindings = json.loads(lookup["raw_response"])["results"]["bindings"]
            error = None
        except (RuntimeError, ValueError, KeyError) as exc:
            bindings, error = [], str(exc)
        for identifier in batch:
            record = ({"tconst": identifier, "languages": [], "status": "api_error",
                       "error": error} if error else classify(identifier, bindings))
            record["retrieved_at_utc"] = datetime.now(timezone.utc).isoformat()
            save_json(CACHE / f"{identifier}.json", record)
            records.append(record)
        print(f"{len(records)}/{len(targets)} films", flush=True)
    for record in records:
        counts[record["status"]] += 1
    return records, counts


def write_outputs(spark, tables, targets, records, counts):
    rows = [(r["tconst"], l["language"], l["label"], l["iso639_1"])
            for r in records for l in r["languages"]]
    frame = spark.createDataFrame(rows, "tconst string, language string, "
                                        "label string, iso639_1 string")
    frame.coalesce(1).write.mode("overwrite").parquet(str(OUTPUT))
    overlap_n, overlap = akas_title_language_overlap(spark, tables, records)
    truth_n, truth_rate = ground_truth_accuracy(records)
    no_error = counts["api_error"] == 0
    all_covered = len(records) == len(targets) == len({r["tconst"] for r in records})
    in_scope = {r["tconst"] for r in records} <= set(targets)
    pairs = [(a, b) for a, b, _, _ in rows]
    no_dupes = len(pairs) == len(set(pairs))
    truth_ok = truth_n >= TRUTH_MIN_SAMPLE and truth_rate >= TRUTH_ACCURACY_FLOOR
    status = {
        "complete": bool(no_error and all_covered),
        "controls_ok": bool(in_scope and no_dupes and truth_ok),
        "eligible_films": len(targets),
        "covered_films": counts["ok"],
        "no_p364": counts["no_p364"],
        "not_found": counts["not_found"],
        "ambiguous": counts["ambiguous"],
        "api_errors": counts["api_error"],
        "language_rows": len(rows),
        # indicatif seulement: couverture des titres traduits, pas une exactitude
        "akas_title_language_overlap_sample": overlap_n,
        "akas_title_language_overlap": None if overlap is None else round(overlap, 4),
        # validation: exactitude mesurée sur un échantillon vérifié à la main
        "ground_truth_sample": truth_n,
        "ground_truth_accuracy": None if truth_rate is None else round(truth_rate, 4),
        "ground_truth_floor": TRUTH_ACCURACY_FLOOR,
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    save_json(STATUS, status)
    save_json(PROVENANCE, {
        "source": "Wikidata SPARQL P364 matched on exact IMDb id P345",
        "endpoint": QUERY_ENDPOINT, "batch_size": BATCH_SIZE,
        "run_id": json.loads((CLEAN_DIR / "current.json").read_text())["run_id"],
        "targets_sha256": hashlib.sha256("\n".join(targets).encode()).hexdigest(),
        "criteria": {"rating": P.Q6_SUCCESS_RATING,
                     "min_votes": P.Q6_SUCCESS_MIN_VOTES},
        "generated_at_utc": status["checked_at_utc"],
    })
    return status


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int,
                        help="smoke test on the first N films; never final")
    parser.add_argument("--refresh", action="store_true")
    args = parser.parse_args()
    from imdb_pipeline.spark import create_spark
    spark = create_spark("collect-original-language")
    targets, tables = eligible_films(spark)
    if args.limit:
        targets = targets[:args.limit]
    client = WikimediaClient(ENRICHMENT_DIR / "cache" / "http", refresh=args.refresh)
    records, counts = collect(targets, client, args.refresh)
    status = write_outputs(spark, tables, targets, records, counts)
    if args.limit:
        status["complete"] = False  # a sample can never validate Q6
        save_json(STATUS, status)
    print(json.dumps(status, indent=2))


if __name__ == "__main__":
    main()