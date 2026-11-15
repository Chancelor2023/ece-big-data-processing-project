"""Package existing validated results and source indexes for the course hand-in."""
import argparse
import csv
import hashlib
import json
import shutil
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F
from imdb_pipeline.config import PROJECT_ROOT
from imdb_pipeline.spark import create_spark


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8-sig"))


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def write_csv(path, fields, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default="report-assets/evidence-20261115")
    parser.add_argument("--stream-run", default="live-validation-20261114-v2")
    parser.add_argument("--stream-session", default="20261114T150245Z")
    args = parser.parse_args()
    root = PROJECT_ROOT.resolve()
    output = (root / args.output).resolve()
    if not output.is_relative_to(root) or output == root:
        parser.error("Output must be a new directory inside the repository")
    if output.exists():
        parser.error("Output exists; choose a new output name to preserve prior evidence")
    protected = {name: sha256(root / name) for name in ("PROJECT.md", "REPORT.md")}
    catalog_path = root / "data/clean/current.json"
    catalog_bytes = catalog_path.read_bytes()
    catalog = read_json(catalog_path)
    quality_root = root / "results/quality" / catalog["run_id"]
    quality_path = quality_root / "quality_report.json"
    quality = read_json(quality_path)
    run = read_json(root / "data/enrichment/current_selection.json")["run_id"]
    content_root = root / "data/enrichment/runs" / run
    publication_path = content_root / "current_content.json"
    publication_bytes = publication_path.read_bytes()
    publication = read_json(publication_path)
    enrichment_root = root / "results/enrichment" / run
    coverage = read_json(enrichment_root / "coverage.json")
    check_pointer = read_json(enrichment_root / "current_coverage_check.json")
    check_root = enrichment_root / check_pointer["path"]
    verification = read_json(check_root / "verification.json")
    collection = read_json(enrichment_root / "collection_full.json")
    streaming_root = root / "results/streaming" / args.stream_run
    session_path = streaming_root / "sessions" / (args.stream_session + ".json")
    session = read_json(session_path)
    configuration = read_json(streaming_root / "configuration.json")
    progress = read_json(streaming_root / "progress" / (args.stream_session + ".json"))
    markers = [read_json(p) for p in sorted((streaming_root / "batches").glob("*.json"))]
    markers = [m for m in markers if m["session_id"] == args.stream_session]
    checks = {}

    def check(name, condition):
        checks[name] = bool(condition)
        if not condition:
            raise ValueError("Evidence check failed: " + name)

    check("quality_passed", quality["status"] == "passed" and quality["full_imdb_dataset_profiled"])
    check("catalog_and_quality_match", quality["run_id"] == catalog["run_id"] == coverage["source_catalog"])
    check("enrichment_snapshot_matches", coverage["content_snapshot"] == publication["snapshot"] == verification["content_snapshot"])
    check("enrichment_verification_passed", verification["status"] == "passed" and all(verification["checks"].values()))
    check("collection_complete", collection["status"] == "completed" and collection["processed"] == collection["total_targets"])
    check("live_test_passed", not session["synthetic"] and session["live_test_passed"] and 900 <= session["elapsed_seconds"] <= 1800)
    check("stream_catalog_matches", configuration["source_catalog"] == catalog["run_id"])
    check("committed_batch_count_matches", len(markers) == session["committed_batches"])
    for field, target in (("rows_after_watermark_and_dedup", "processed_rows"),
                          ("matched_events", "matched_events"),
                          ("matched_events_in_live_window", "matched_events_in_live_window")):
        check("stream_" + field + "_matches_session", sum(m[field] for m in markers) == session[target])

    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".evidence-", dir=output.parent) as temporary:
        package = Path(temporary) / "bundle"
        package.mkdir()
        sources = []

        def source_copy(source, destination, section, description):
            destination_path = package / destination
            destination_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, destination_path)
            if sha256(destination_path) != sha256(source):
                raise ValueError("Copied evidence differs from source: " + str(source))
            sources.append({"section": section, "description": description,
                            "original_path": source.relative_to(root).as_posix(),
                            "packaged_path": destination, "source_url": "",
                            "sha256": sha256(source)})

        def json_file(name, value):
            path = package / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

        source_copy(quality_path, "01-quality/quality_report.original.json", "quality", "Full seven-table quality report")
        source_copy(catalog_path, "01-quality/clean_catalog.original.json", "quality", "Published schemas, row counts and paths")
        source_copy(quality_root / "column_profile.csv", "01-quality/column_profile.csv", "quality", "All 39 columns, raw and clean profiles")
        with (quality_root / "column_profile.csv").open(encoding="utf-8-sig", newline="") as stream:
            profiles = list(csv.DictReader(stream))
        check("all_78_column_profiles", len(profiles) == 78 and {p["stage"] for p in profiles} == {"raw", "clean"})
        table_rows, issue_rows = [], []
        for name, stats in quality["tables"].items():
            check("catalog_rows_" + name, stats["clean_rows"] == catalog["tables"][name]["rows"])
            table_rows.append({"table": name, "source_rows": stats["source_rows"], "clean_rows": stats["clean_rows"],
                               "rows_removed": stats["source_rows"] - stats["clean_rows"],
                               "rejected_rows": sum(stats.get("rejected_records", {}).values()),
                               "duplicate_rows_quarantined": stats.get("duplicate_rows_quarantined", 0),
                               "read_back_validated": stats["read_back_validated"],
                               "key_validated_recorded": stats.get("key_validated", "not_separately_recorded")})
            for field, count in stats.get("field_issue_counts", {}).items():
                issue_rows.append({"table": name, "category": "field_correction", "field_or_reason": field, "count": count, "unit": "field_occurrences"})
            for field, info in stats.get("scalar_references", {}).items():
                if info["rows_quarantined"]:
                    issue_rows.append({"table": name, "category": "orphan_quarantine", "field_or_reason": field,
                                       "count": info["rows_quarantined"], "unit": "rows_per_reference_check"})
            for field, count in stats.get("array_references", {}).get("references_removed_by_field", {}).items():
                issue_rows.append({"table": name, "category": "array_reference_removed", "field_or_reason": field, "count": count, "unit": "reference_occurrences"})
        check("seven_tables", len(table_rows) == 7)
        write_csv(package / "01-quality/table_counts.csv", list(table_rows[0]), table_rows)
        write_csv(package / "01-quality/corrections_and_quarantines.csv", ["table", "category", "field_or_reason", "count", "unit"], issue_rows)
        write_csv(package / "01-quality/top_five_missing_raw.csv", ["table", "stage", "column", "row_count", "missing_count", "missing_percent"], quality["top_missing_columns_global"])
        write_csv(package / "01-quality/imdb_source_fingerprints.csv", ["table", "url", "file", "bytes", "sha256", "downloaded_at_utc", "last_modified"], [dict(info, table=name) for name, info in quality["sources"].items()])
        for name, info in quality["sources"].items():
            sources.append({"section": "IMDb source", "description": name, "original_path": "data/raw/" + info["file"],
                            "packaged_path": "", "source_url": info["url"], "sha256": info["sha256"]})

        source_copy(publication_path, "02-enrichment/content_publication.original.json", "enrichment", "Current content and effective association paths")
        source_copy(enrichment_root / "coverage.json", "02-enrichment/coverage.original.json", "enrichment", "Current enrichment coverage")
        source_copy(check_root / "verification.json", "02-enrichment/verification.original.json", "enrichment", "Independent current-snapshot checks")
        source_copy(enrichment_root / "collection_full.json", "02-enrichment/collection.original.json", "enrichment", "Completed HTTP traversal and raw statuses")
        source_copy(content_root / "selection_summary.json", "02-enrichment/selection.original.json", "enrichment", "Selection scope and original counts")
        for name in ("films_without_synopsis.csv", "people_without_biography.csv", "films_with_fewer_than_three_biographies.csv", "genre_year_coverage.csv"):
            source_copy(check_root / name, "02-enrichment/" + name, "enrichment", "Current " + name)
        recovery_root = enrichment_root / "synopsis_recoveries" / publication["snapshot"]
        if recovery_root.is_dir():
            source_copy(recovery_root / "decisions.csv", "02-enrichment/synopsis_recovery_decisions.csv", "enrichment", "Cached synopsis acceptance/review decisions")
        status_rows = [dict(row, denominator=coverage["unique_films"] if row["kind"] == "film" else coverage["unique_people"]) for row in coverage["coverage"]]
        write_csv(package / "02-enrichment/status_counts.csv", ["kind", "status", "count", "denominator"], status_rows)
        write_csv(package / "02-enrichment/biographies_per_film.csv", ["available_biographies", "films"], [{"available_biographies": key, "films": value} for key, value in sorted(verification["film_biography_distribution"].items())])
        history = []
        snapshots = [coverage.get("base_content_snapshot"), coverage.get("synopsis_base_snapshot"), coverage["content_snapshot"]]
        for snapshot in dict.fromkeys(s for s in snapshots if s):
            matches = sorted((enrichment_root / "coverage_checks").glob(snapshot + "-*/verification.json"))
            if not matches:
                raise ValueError("Missing verified historical coverage: " + snapshot)
            record = read_json(matches[-1])
            check("historical_verification_" + snapshot, record["status"] == "passed")
            history.append({"snapshot": snapshot, "synopses": record["available_synopses"], "synopsis_percent": record["synopsis_percent"],
                            "unique_biographies": record["available_biographies"], "biography_percent": record["biography_percent"],
                            "films_with_three_biographies": record["films_with_three_available_biographies"], "films_with_three_percent": record["films_with_three_percent"],
                            "films_with_both": record["films_with_synopsis_and_three_biographies"], "both_percent": record["both_percent"]})
            source_copy(matches[-1], "02-enrichment/history/" + snapshot + ".json", "enrichment", "Verified coverage at historical snapshot")
        write_csv(package / "02-enrichment/coverage_progression.csv", list(history[0]), history)

        spark = create_spark("export-evidence-source-index")
        try:
            content = content_root / publication["path"]
            films = spark.read.parquet(str(content / "films"))
            people = spark.read.parquet(str(content / "people"))
            def lineage(frame, kind, key, label):
                return frame.filter("status = 'available'").select(F.lit(kind).alias("kind"), F.col(key).alias("imdb_id"),
                    F.col(label).alias("title_or_name"), "entity_uri", "article_url", "source_url", "revision_id",
                    F.date_format("retrieved_at_utc", "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX").alias("retrieved_at_utc"), "license_url")
            records = lineage(films, "film", "tconst", "primaryTitle").unionByName(lineage(people, "person", "nconst", "primaryName"))
            expected = verification["available_synopses"] + verification["available_biographies"]
            check("available_source_index_rows", records.count() == expected)
            with tempfile.TemporaryDirectory(prefix="evidence-source-index-") as export:
                export_path = Path(export) / "csv"
                records.orderBy("kind", "imdb_id").coalesce(1).write.option("header", True).option("escape", '"').csv(str(export_path))
                shutil.copyfile(next(export_path.glob("part-*.csv")), package / "02-enrichment/available_text_sources.csv")
        finally:
            spark.stop()

        source_copy(session_path, "03-streaming/session.original.json", "streaming", "Real 15-minute validation session")
        source_copy(streaming_root / "configuration.json", "03-streaming/configuration.original.json", "streaming", "Topic, source catalog, watermark and matching")
        source_copy(streaming_root / "progress" / (args.stream_session + ".json"), "03-streaming/progress.original.json", "streaming", "Original Spark microbatch metrics")
        batch_rows = []
        for marker in markers:
            batch_rows.append({key: marker.get(key) for key in ("batch_id", "rows_after_watermark_and_dedup", "invalid_rows", "distinct_article_titles", "matched_events", "matched_imdb_rows", "matched_events_in_live_window", "attempt_path", "committed_at_utc")})
            source_copy(streaming_root / "batches" / (f"{marker['batch_id']:012d}.json"),
                        "03-streaming/batch_markers/" + f"{marker['batch_id']:012d}.json", "streaming", "Committed batch marker")
        write_csv(package / "03-streaming/committed_batches.csv", list(batch_rows[0]), batch_rows)
        progress_rows = []
        for row in progress:
            operators = row.get("stateOperators", [])
            sources_info = row.get("sources", [])
            progress_rows.append({"batch_id": row["batchId"], "timestamp": row["timestamp"], "input_rows": row["numInputRows"],
                "batch_duration_ms": row.get("batchDuration"), "processed_rows_per_second": row.get("processedRowsPerSecond"),
                "watermark": row.get("eventTime", {}).get("watermark"),
                "rows_dropped_by_watermark": sum(o.get("numRowsDroppedByWatermark", 0) for o in operators),
                "duplicates_dropped": sum(o.get("customMetrics", {}).get("numDroppedDuplicateRows", 0) for o in operators),
                "start_offsets": json.dumps([s.get("startOffset") for s in sources_info]),
                "end_offsets": json.dumps([s.get("endOffset") for s in sources_info])})
        write_csv(package / "03-streaming/microbatch_progress.csv", list(progress_rows[0]), progress_rows)
        stream_metrics = dict(session)
        stream_metrics.update({"watermark": configuration["watermark"], "observed_rows_dropped_by_watermark": sum(r["rows_dropped_by_watermark"] for r in progress_rows),
                               "observed_duplicates_dropped": sum(r["duplicates_dropped"] for r in progress_rows),
                               "progress_input_rows": sum(r["input_rows"] for r in progress_rows)})
        json_file("03-streaming/metrics.json", stream_metrics)
        write_csv(package / "03-streaming/metrics.csv", ["metric", "value"], [{"metric": key, "value": value} for key, value in stream_metrics.items()])

        external = [("Wikimedia collection", "English Wikipedia parse API", "https://en.wikipedia.org/w/api.php"),
                    ("Identity matching", "Wikidata query service, exact IMDb P345", "https://query.wikidata.org/sparql"),
                    ("Live events", "Wikimedia recentchange EventStreams", "https://stream.wikimedia.org/v2/stream/recentchange"),
                    ("Text license", "Wikipedia attribution license retained in records", "https://creativecommons.org/licenses/by-sa/4.0/")]
        sources.extend({"section": section, "description": description, "original_path": "", "packaged_path": "", "source_url": url, "sha256": ""} for section, description, url in external)
        write_csv(package / "SOURCES.csv", ["section", "description", "original_path", "packaged_path", "source_url", "sha256"], sources)
        index = f'''# Verified project evidence

Generated: {datetime.now(timezone.utc).isoformat()}. Source catalog: `{catalog['run_id']}`. Enrichment snapshot: `{publication['snapshot']}`. Streaming session: `{args.stream_run}` / `{args.stream_session}`.

This folder contains factual exports and copies of existing results. It contains no generated methodological explanation for REPORT.md. Contributors add their own reasoning, alternatives and interpretation in that report. Peer-reviewed exploration and open-question results are separate deliverables.

| Evidence | File |
| --- | --- |
| Seven-table row counts | [table_counts.csv](01-quality/table_counts.csv) |
| All missing-value profiles | [column_profile.csv](01-quality/column_profile.csv) |
| Global five highest raw missing percentages | [top_five_missing_raw.csv](01-quality/top_five_missing_raw.csv) |
| Corrections and quarantines | [corrections_and_quarantines.csv](01-quality/corrections_and_quarantines.csv) |
| IMDb source URLs, dates and hashes | [imdb_source_fingerprints.csv](01-quality/imdb_source_fingerprints.csv) |
| Enrichment coverage progression | [coverage_progression.csv](02-enrichment/coverage_progression.csv) |
| Current enrichment statuses | [status_counts.csv](02-enrichment/status_counts.csv) |
| Number of available biographies per film | [biographies_per_film.csv](02-enrichment/biographies_per_film.csv) |
| Per-record Wikipedia revision and license index (metadata only) | [available_text_sources.csv](02-enrichment/available_text_sources.csv) |
| Remaining films without synopsis | [films_without_synopsis.csv](02-enrichment/films_without_synopsis.csv) |
| Remaining people without biography | [people_without_biography.csv](02-enrichment/people_without_biography.csv) |
| Films below three biographies | [films_with_fewer_than_three_biographies.csv](02-enrichment/films_with_fewer_than_three_biographies.csv) |
| Coverage by genre/year | [genre_year_coverage.csv](02-enrichment/genre_year_coverage.csv) |
| Live streaming summary | [metrics.csv](03-streaming/metrics.csv) |
| Batch counts and matches | [committed_batches.csv](03-streaming/committed_batches.csv) |
| Watermark, durations, rates and offsets | [microbatch_progress.csv](03-streaming/microbatch_progress.csv) |
| Original sources and packaged copies | [SOURCES.csv](SOURCES.csv) |

## Facts to preserve when quoting the tables

- Current enrichment: {verification['available_synopses']:,} synopses / {coverage['unique_films']:,} films; {verification['available_biographies']:,} unique biographies / {coverage['unique_people']:,} people; {verification['films_with_synopsis_and_three_biographies']:,} films with synopsis and at least three biographies. Missing and ambiguous records remain explicit; full coverage is not claimed.
- Source corrections can overlap across columns and checks; do not sum them as a count of deleted rows. Table row differences and reference occurrences have separate units.
- A multigenre film can appear in several genre/year groups. The genre/year table is not a unique-film total.
- Streaming: {session['elapsed_seconds']} seconds, {session['processed_rows']:,} processed rows, {session['matched_events_in_live_window']} relevant events in the live window. The {session['matched_events']} matched events over committed batches include the historical backlog.
- Observed natural late drops: {stream_metrics['observed_rows_dropped_by_watermark']}; observed duplicate drops: {stream_metrics['observed_duplicates_dropped']}. These observations do not replace the separate synthetic late-data/duplicate tests.
- Only metadata, small factual exports and original report JSONs are packaged. Raw IMDb archives, Wikipedia article bodies, Parquet datasets, Kafka data, credentials and checkpoints stay outside this folder.

## Integrity and reproduction

[MANIFEST.json](MANIFEST.json) lists packaged file hashes. [VALIDATION.json](VALIDATION.json) records the consistency checks. [SHA256SUMS.txt](SHA256SUMS.txt) lists payload hashes. SOURCES.csv records original repository-relative paths and hashes for copied evidence. Those original data/results paths are local and may not exist in a fresh clone.

After producing the validated outputs, reproduce this bundle with `docker compose exec -T spark python -m jobs.reporting.prepare_evidence --output report-assets/NEW-EVIDENCE-DIRECTORY`. Existing evidence directories are never overwritten.
'''
        (package / "INDEX.md").write_text(index, encoding="utf-8")
        check("catalog_not_changed_concurrently", catalog_path.read_bytes() == catalog_bytes)
        check("enrichment_not_changed_concurrently", publication_path.read_bytes() == publication_bytes)
        for name, digest in protected.items():
            check(name + "_unchanged", sha256(root / name) == digest)
        json_file("VALIDATION.json", {"status": "passed", "checks": checks,
                  "available_source_records": expected, "protected_file_sha256": protected})
        payload = sorted(p for p in package.rglob("*") if p.is_file())
        (package / "SHA256SUMS.txt").write_text("\n".join(sha256(p) + "  " + p.relative_to(package).as_posix() for p in payload) + "\n", encoding="utf-8")
        payload = sorted(p for p in package.rglob("*") if p.is_file())
        try:
            commit = subprocess.check_output(["git", "-c", "safe.directory=" + str(root), "-C", str(root), "rev-parse", "HEAD"], text=True, stderr=subprocess.DEVNULL).strip()
        except (OSError, subprocess.CalledProcessError):
            commit = None
        manifest = {"generated_at_utc": datetime.now(timezone.utc).isoformat(), "code_commit": commit,
                    "exporter_sha256": sha256(Path(__file__).resolve()),
                    "source_catalog": catalog["run_id"], "enrichment_run": run, "content_snapshot": publication["snapshot"],
                    "streaming_run": args.stream_run, "streaming_session": args.stream_session,
                    "files": [{"path": p.relative_to(package).as_posix(), "bytes": p.stat().st_size, "sha256": sha256(p)} for p in payload]}
        json_file("MANIFEST.json", manifest)
        for record in manifest["files"]:
            check_path = package / record["path"]
            if sha256(check_path) != record["sha256"] or check_path.stat().st_size != record["bytes"]:
                raise ValueError("Packaged file changed: " + record["path"])
        package.rename(output)
    print(json.dumps({"status": "passed", "output": str(output), "files": len(manifest["files"]) + 1,
                      "bytes": sum(r["bytes"] for r in manifest["files"]) + (output / "MANIFEST.json").stat().st_size,
                      "checks": len(checks), "available_source_records": expected}), flush=True)


if __name__ == "__main__":
    main()
