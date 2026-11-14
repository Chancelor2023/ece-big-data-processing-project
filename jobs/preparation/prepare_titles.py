"""Prepare full title.basics and title.ratings snapshots with Spark."""

import argparse
import csv
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from pyspark import StorageLevel
from pyspark.sql import functions as F

from imdb_pipeline.config import CLEAN_DIR, RAW_DIR, RESULTS_DIR
from imdb_pipeline.preparation import (
    TITLE_COLUMNS, mark_rating_parents, prepare_rows, profile_columns,
    rank_records, read_tsv,
)
from imdb_pipeline.spark import create_spark
from jobs.preparation.download_titles import fingerprint


def atomic_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def prepare_table(spark, table, raw_path, snapshot, quality, partitions, parents):
    print(f"Preparing {table} from the complete source", flush=True)
    columns = TITLE_COLUMNS[table]
    raw = read_tsv(spark, raw_path, table).persist(StorageLevel.DISK_ONLY)
    prepared = ranked = marked = clean = None
    try:
        raw_count, raw_profile = profile_columns(raw, table, "raw")
        if raw_count == 0:
            raise ValueError(f"Empty source: {table}")
        print(f"{table}: {raw_count:,} source records", flush=True)
        prepared = prepare_rows(raw, table).persist(StorageLevel.DISK_ONLY)
        prepared.count()
        raw.unpersist()

        reasons = {
            row._reject_reason: row["count"]
            for row in prepared.filter(F.col("_reject_reason").isNotNull())
            .groupBy("_reject_reason").count().collect()
        }
        issue_counts = {
            row.issue: row["count"]
            for row in prepared.select(F.explode("_issues").alias("issue"))
            .groupBy("issue").count().collect()
        }
        rejection_root = quality / "quarantine" / table
        prepared.filter(F.col("_reject_reason").isNotNull()).write.parquet(
            str(rejection_root / "invalid_records")
        )

        ranked = rank_records(prepared, table, partitions).persist(
            StorageLevel.DISK_ONLY
        )
        duplicates = ranked.filter(F.col("_key_rank") > 1)
        duplicate_count = duplicates.count()
        duplicates.withColumn("_reject_reason", F.lit("duplicate_tconst"))\
            .write.parquet(str(rejection_root / "duplicates"))
        kept = ranked.filter(F.col("_key_rank") == 1).select(*columns)
        orphan_count = 0
        if parents is not None:
            marked = mark_rating_parents(kept, parents).persist(
                StorageLevel.DISK_ONLY
            )
            orphans = marked.filter(F.col("_parent_exists").isNull())
            orphan_count = orphans.count()
            orphans.drop("_parent_exists").withColumn(
                "_reject_reason", F.lit("missing_basics_parent")
            ).write.parquet(str(rejection_root / "missing_parents"))
            kept = marked.filter(F.col("_parent_exists")).select(*columns)

        clean = kept.persist(StorageLevel.DISK_ONLY)
        clean_count, clean_profile = profile_columns(clean, table, "clean")
        expected = raw_count - sum(reasons.values()) - duplicate_count - orphan_count
        if clean_count != expected:
            raise AssertionError(f"Record accounting failed for {table}")
        output = snapshot / table
        clean.write.mode("errorifexists").parquet(str(output))
        saved = spark.read.parquet(str(output))
        if saved.count() != clean_count:
            raise AssertionError(f"Parquet read-back failed for {table}")
        duplicate_keys = saved.groupBy("tconst").count().filter("count > 1")
        if duplicate_keys.limit(1).count():
            raise AssertionError(f"Duplicate cleaned keys in {table}")
        if saved.filter(F.col("tconst").isNull()).limit(1).count():
            raise AssertionError(f"Null cleaned keys in {table}")
        metrics = {
            "source_rows": raw_count, "clean_rows": clean_count,
            "rejected_records": reasons,
            "duplicate_rows_quarantined": duplicate_count,
            "orphan_ratings_quarantined": orphan_count,
            "field_issue_counts": issue_counts,
            "read_back_validated": True,
        }
        print(f"{table}: {json.dumps(metrics)}", flush=True)
        return metrics, raw_profile + clean_profile, saved.schema.jsonValue()
    finally:
        for frame in (raw, prepared, ranked, marked, clean):
            if frame is not None:
                frame.unpersist()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-dir", type=Path, default=RAW_DIR)
    parser.add_argument("--clean-dir", type=Path, default=CLEAN_DIR)
    parser.add_argument("--quality-dir", type=Path, default=RESULTS_DIR / "quality")
    parser.add_argument("--partitions", type=int, default=8)
    args = parser.parse_args()
    if args.partitions < 1:
        parser.error("--partitions must be positive")
    sources = {}
    for table in TITLE_COLUMNS:
        path = args.raw_dir / f"{table}.tsv.gz"
        sources[table] = {"file": path.name, **fingerprint(path, table)}
    download_manifest = args.raw_dir / "title_downloads.json"
    if download_manifest.exists():
        metadata = json.loads(download_manifest.read_text(encoding="utf-8"))
        for table, source in sources.items():
            previous = metadata.get(table, {})
            if previous.get("sha256") != source["sha256"]:
                raise ValueError(f"Download manifest does not match {table}")
            sources[table] = {**previous, **source}

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
    snapshot = args.clean_dir / "runs" / run_id
    quality = args.quality_dir / run_id
    quality.mkdir(parents=True, exist_ok=False)
    report = {
        "status": "running", "run_id": run_id,
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "scope": list(TITLE_COLUMNS), "full_imdb_dataset_profiled": False,
        "sources": sources, "tables": {},
    }
    atomic_json(quality / "quality_report.json", report)
    spark = create_spark("imdb-prepare-title-tables")
    spark.conf.set("spark.sql.shuffle.partitions", str(args.partitions))
    profiles = []
    tables = {}
    try:
        for table in TITLE_COLUMNS:
            parents = (
                spark.read.parquet(str(snapshot / "title.basics"))
                if table == "title.ratings" else None
            )
            metrics, profile, schema = prepare_table(
                spark, table, args.raw_dir / f"{table}.tsv.gz",
                snapshot, quality, args.partitions, parents,
            )
            report["tables"][table] = metrics
            profiles.extend(profile)
            tables[table] = {
                "path": str(Path("runs") / run_id / table),
                "rows": metrics["clean_rows"], "schema": schema,
                "key": ["tconst"],
            }
            atomic_json(quality / "quality_report.json", report)
        basics = spark.read.parquet(str(snapshot / "title.basics"))
        ratings = spark.read.parquet(str(snapshot / "title.ratings"))
        if ratings.join(basics.select("tconst"), "tconst", "left_anti").limit(1).count():
            raise AssertionError("Cleaned ratings contain a missing parent")
        joined_rows = basics.join(ratings, "tconst", "inner").count()
        if joined_rows != tables["title.ratings"]["rows"]:
            raise AssertionError("Cleaned join is not one-to-zero-or-one")
        with (quality / "column_profile.csv").open(
            "w", newline="", encoding="utf-8"
        ) as target:
            writer = csv.DictWriter(target, fieldnames=list(profiles[0]))
            writer.writeheader()
            writer.writerows(profiles)
        report.update({
            "status": "passed", "spark_version": spark.version,
            "finished_at_utc": datetime.now(timezone.utc).isoformat(),
            "join_rows": joined_rows,
            "top_missing_columns_in_this_two_table_subset": sorted(
                [p for p in profiles if p["stage"] == "raw"],
                key=lambda p: (-p["missing_percent"], p["table"], p["column"]),
            )[:5],
        })
        atomic_json(quality / "quality_report.json", report)
        publication = {
            "contract_version": 1, "run_id": run_id,
            "published_at_utc": report["finished_at_utc"],
            "tables": tables, "sources": sources,
            "quality_report": str((quality / "quality_report.json").resolve()),
        }
        # Publish the pair only after both snapshots and their join pass validation.
        atomic_json(args.clean_dir / "current.json", publication)
        print(f"PASS: published {args.clean_dir / 'current.json'}", flush=True)
        print(f"Quality report: {quality / 'quality_report.json'}", flush=True)
    except Exception as exc:
        report.update({"status": "failed", "error": str(exc)})
        atomic_json(quality / "quality_report.json", report)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
