"""Prepare the remaining five tables and publish a validated seven-table catalog."""

import argparse
import csv
import gzip
import hashlib
import json
import os
from datetime import datetime, timezone
from functools import reduce
from pathlib import Path
from uuid import uuid4

from pyspark import StorageLevel
from pyspark.sql import functions as F

from imdb_pipeline.config import CLEAN_DIR, RAW_DIR, RESULTS_DIR
from imdb_pipeline.other_preparation import (
    ARRAY_REFERENCES, SCALAR_REFERENCES, prepare_other_rows,
)
from imdb_pipeline.preparation import profile_columns, rank_records, read_tsv
from imdb_pipeline.schemas import ALL_COLUMNS, OTHER_COLUMNS, TABLE_KEYS
from imdb_pipeline.spark import create_spark
from jobs.preparation.download_titles import fingerprint
from jobs.preparation.prepare_titles import atomic_json


def expand_source(raw_dir, table, source_hash):
    """Uncompressed TSVs let Spark split large archives across several tasks."""
    destination = raw_dir / "expanded" / f"{table}.tsv"
    metadata_path = destination.with_suffix(".tsv.json")
    if destination.exists() and metadata_path.exists():
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        if (metadata["source_sha256"] == source_hash
                and metadata["bytes"] == destination.stat().st_size):
            print(f"Reusing expanded source {table}", flush=True)
            return destination
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(".tsv.part")
    sha = hashlib.sha256()
    size = 0
    with gzip.open(raw_dir / f"{table}.tsv.gz", "rb") as source:
        with temporary.open("wb") as target:
            for block in iter(lambda: source.read(4 * 1024 * 1024), b""):
                target.write(block)
                sha.update(block)
                size += len(block)
    os.replace(temporary, destination)
    atomic_json(metadata_path, {
        "source_sha256": source_hash, "sha256": sha.hexdigest(), "bytes": size,
    })
    print(f"Expanded {table}: {size / 1e9:.2f} GB", flush=True)
    return destination


def write_quarantine(frame, path, reason):
    frame.withColumn("_reject_reason", F.lit(reason)).write.parquet(str(path))


def filter_scalar_references(frame, table, parents, quality):
    metrics = {}
    current = frame
    for field, parent, parent_key in SCALAR_REFERENCES[table]:
        references = current.select(F.col(field).alias("_ref")).distinct()
        missing = references.join(
            parents[parent].select(F.col(parent_key).alias("_ref")),
            "_ref", "left_anti",
        ).persist(StorageLevel.DISK_ONLY)
        missing_keys = missing.count()
        removed = 0
        if missing_keys:
            keys = missing.withColumnRenamed("_ref", field)
            if missing_keys <= 100000:
                keys = F.broadcast(keys)
            rejected = current.join(keys, field, "left_semi")
            removed = rejected.count()
            write_quarantine(
                rejected, quality / f"missing_{field}",
                f"missing_parent_{field}",
            )
            previous = current
            current = current.join(keys, field, "left_anti")
            # Resolve the missing-key side while its cache is still available.
            current = current.persist(StorageLevel.DISK_ONLY)
            current.count()
            if previous is not frame:
                previous.unpersist()
        metrics[field] = {
            "parent": parent, "missing_distinct_keys": missing_keys,
            "rows_quarantined": removed,
        }
        missing.unpersist()
        print(f"{table}: reference {field}: {metrics[field]}", flush=True)
    return current, metrics


def clean_array_references(frame, table, parents, quality):
    if table not in ARRAY_REFERENCES:
        return frame, {}
    fields, parent, parent_key = ARRAY_REFERENCES[table]
    keys = TABLE_KEYS[table]
    edges = reduce(DataFrameUnion, [
        frame.select(
            *keys, F.lit(field).alias("_field"),
            F.explode(F.col(field)).alias("_ref"),
        ) for field in fields
    ])
    missing = edges.select("_ref").distinct().join(
        parents[parent].select(F.col(parent_key).alias("_ref")),
        "_ref", "left_anti",
    ).persist(StorageLevel.DISK_ONLY)
    missing_count = missing.count()
    metrics = {"parent": parent, "missing_distinct_keys": missing_count}
    if missing_count == 0:
        missing.unpersist()
        return frame, metrics
    side = F.broadcast(missing) if missing_count <= 100000 else missing
    invalid = edges.join(side, "_ref", "left_semi").persist(StorageLevel.DISK_ONLY)
    metrics["references_removed_by_field"] = {
        row._field: row["count"]
        for row in invalid.groupBy("_field").count().collect()
    }
    invalid.write.parquet(str(quality / "missing_array_references"))
    invalid_by_owner = invalid.groupBy(*keys).agg(
        F.collect_set("_ref").alias("_missing_refs")
    )
    current = frame.join(invalid_by_owner, keys, "left")
    for field in fields:
        valid = F.array_except(
            F.col(field),
            F.coalesce(F.col("_missing_refs"), F.array().cast("array<string>")),
        )
        current = current.withColumn(
            field,
            F.when(F.size(valid) > 0, valid)
            .otherwise(F.lit(None).cast("array<string>")),
        )
    current = current.drop("_missing_refs").persist(StorageLevel.DISK_ONLY)
    current.count()
    invalid.unpersist()
    missing.unpersist()
    print(f"{table}: array references {metrics}", flush=True)
    return current, metrics


def DataFrameUnion(left, right):
    return left.unionByName(right)


def prepare_table(spark, table, expanded, output, quality, parents, partitions):
    columns, keys = ALL_COLUMNS[table], TABLE_KEYS[table]
    raw = read_tsv(spark, expanded, table).persist(StorageLevel.DISK_ONLY)
    prepared = candidates = duplicate_keys = clean = linked = linked_arrays = None
    try:
        source_count, before = profile_columns(raw, table, "raw")
        if not source_count:
            raise ValueError(f"Empty IMDb source: {table}")
        print(f"{table}: {source_count:,} raw rows", flush=True)
        prepared = prepare_other_rows(raw, table).persist(StorageLevel.DISK_ONLY)
        prepared.count()
        raw.unpersist(blocking=True)
        reasons = {
            row._reject_reason: row["count"]
            for row in prepared.filter(F.col("_reject_reason").isNotNull())
            .groupBy("_reject_reason").count().collect()
        }
        issues = {
            row.issue: row["count"]
            for row in prepared.select(F.explode("_issues").alias("issue"))
            .groupBy("issue").count().collect()
        }
        if reasons:
            prepared.filter(F.col("_reject_reason").isNotNull()).write.parquet(
                str(quality / "invalid_records")
            )
        if issues:
            prepared.filter(F.size("_issues") > 0).write.parquet(
                str(quality / "field_corrections")
            )
        candidates = prepared.filter(F.col("_reject_reason").isNull())
        duplicate_keys = candidates.groupBy(*keys).count().filter(
            F.col("count") > 1
        ).persist(StorageLevel.DISK_ONLY)
        duplicate_groups = duplicate_keys.count()
        duplicate_rows = 0
        if duplicate_groups:
            duplicates = candidates.join(duplicate_keys.select(*keys), keys, "left_semi")
            ranked = rank_records(duplicates, table, partitions)
            discarded = ranked.filter(F.col("_key_rank") > 1)
            duplicate_rows = discarded.count()
            write_quarantine(discarded, quality / "duplicates", "duplicate_key")
            unique = candidates.join(duplicate_keys.select(*keys), keys, "left_anti")
            candidates = unique.unionByName(
                ranked.filter(F.col("_key_rank") == 1).drop("_key_rank")
            )
        print(f"{table}: key checks complete; {duplicate_rows} duplicate rows", flush=True)
        linked, scalar_metrics = filter_scalar_references(candidates, table, parents, quality)
        linked_arrays, array_metrics = clean_array_references(linked, table, parents, quality)
        clean = linked_arrays.select(*columns).persist(StorageLevel.DISK_ONLY)
        count, after = profile_columns(clean, table, "clean")
        orphan_rows = sum(m["rows_quarantined"] for m in scalar_metrics.values())
        expected = source_count - sum(reasons.values()) - duplicate_rows - orphan_rows
        if count != expected:
            raise AssertionError(f"Record accounting failed: {table}: {count} != {expected}")
        clean.write.option("maxRecordsPerFile", 1500000).parquet(str(output))
        saved = spark.read.parquet(str(output))
        if saved.count() != count:
            raise AssertionError(f"Parquet read-back failed: {table}")
        # Rows are only removed/array fields corrected after the exact key check.
        # No later operation may multiply keys: join sides are unique/grouped.
        metrics = {
            "source_rows": source_count, "clean_rows": count,
            "rejected_records": reasons,
            "duplicate_groups": duplicate_groups,
            "duplicate_rows_quarantined": duplicate_rows,
            "field_issue_counts": issues,
            "scalar_references": scalar_metrics,
            "array_references": array_metrics,
            "key_validated": True, "read_back_validated": True,
        }
        print(f"{table}: {json.dumps(metrics)}", flush=True)
        return metrics, before + after, saved.schema.jsonValue()
    finally:
        for frame in (raw, prepared, duplicate_keys, clean, linked, linked_arrays):
            if frame is not None:
                frame.unpersist()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--partitions", type=int, default=32)
    parser.add_argument("--resume-run")
    args = parser.parse_args()
    if args.partitions < 1:
        parser.error("--partitions must be positive")
    current_path = CLEAN_DIR / "current.json"
    base = json.loads(current_path.read_text(encoding="utf-8"))
    run_id = args.resume_run or (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
    )
    if Path(run_id).name != run_id or ".." in run_id:
        parser.error("Invalid run identifier")
    output_root = CLEAN_DIR / "runs" / run_id
    quality = RESULTS_DIR / "quality" / run_id
    quality.mkdir(parents=True, exist_ok=True)
    report_path = quality / "quality_report.json"
    downloaded = json.loads((RAW_DIR / "imdb_downloads.json").read_text(encoding="utf-8"))
    sources = dict(base["sources"])
    for table in OTHER_COLUMNS:
        path = RAW_DIR / f"{table}.tsv.gz"
        actual = fingerprint(path, table)
        if downloaded[table]["sha256"] != actual["sha256"]:
            raise ValueError(f"Source fingerprint mismatch: {table}")
        sources[table] = {**downloaded[table], **actual}
    if (not args.resume_run and set(base["tables"]) == set(ALL_COLUMNS)
            and all(base["sources"][n]["sha256"] == s["sha256"] for n, s in sources.items())):
        print(f"All seven tables already published for these sources: {base['run_id']}")
        return
    if args.resume_run:
        report = json.loads(report_path.read_text(encoding="utf-8"))
        if any(report["sources"][n]["sha256"] != s["sha256"] for n, s in sources.items()):
            raise ValueError("Sources changed: start a new run instead of resuming")
        if report.get("status") == "passed" and base["run_id"] == run_id:
            print(f"Run already published: {run_id}")
            return
        if base["run_id"] != report["base_run_id"]:
            raise ValueError("Published catalog changed since this run started")
    else:
        base_report = json.loads(Path(base["quality_report"]).read_text(encoding="utf-8"))
        base_profiles = list(csv.DictReader(
            (Path(base["quality_report"]).parent / "column_profile.csv").open(encoding="utf-8")
        ))
        base_profiles = [row for row in base_profiles if row["table"] not in OTHER_COLUMNS]
        # Restore CSV numeric types for the final global ranking.
        for row in base_profiles:
            for field in ("row_count", "missing_count"):
                row[field] = int(row[field])
            row["missing_percent"] = float(row["missing_percent"]) if row["missing_percent"] else None
        report = {
            "status": "running", "run_id": run_id, "base_run_id": base["run_id"],
            "started_at_utc": datetime.now(timezone.utc).isoformat(),
            "scope": list(ALL_COLUMNS), "sources": sources,
            "full_imdb_dataset_profiled": False,
            "tables": {n: m for n, m in base_report["tables"].items() if n not in OTHER_COLUMNS},
            "outputs": {n: m for n, m in base["tables"].items() if n not in OTHER_COLUMNS},
            "profiles": base_profiles,
            "completed_other_tables": [],
        }
    report["status"] = "running"
    report.pop("error", None)
    atomic_json(report_path, report)
    spark = create_spark("imdb-prepare-remaining-tables")
    spark.conf.set("spark.sql.shuffle.partitions", str(args.partitions))
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
    parents = {}
    try:
        for name in ("title.basics", "name.basics"):
            if name in report["outputs"]:
                column = "tconst" if name == "title.basics" else "nconst"
                parents[name] = spark.read.parquet(str(
                    CLEAN_DIR / report["outputs"][name]["path"]
                )).select(column).persist(StorageLevel.DISK_ONLY)
        for table in OTHER_COLUMNS:
            if table in report["completed_other_tables"]:
                print(f"Reusing completed table {table}", flush=True)
                continue
            expanded = expand_source(RAW_DIR, table, sources[table]["sha256"])
            attempt = table
            if (output_root / attempt).exists() or (quality / "quarantine" / attempt).exists():
                attempt += "-retry-" + uuid4().hex[:8]
            output = output_root / attempt
            metrics, profiles, schema = prepare_table(
                spark, table, expanded, output,
                quality / "quarantine" / attempt, parents, args.partitions,
            )
            report["tables"][table] = metrics
            report["profiles"].extend(profiles)
            report["outputs"][table] = {
                "path": str(output.relative_to(CLEAN_DIR)),
                "rows": metrics["clean_rows"], "schema": schema,
                "key": TABLE_KEYS[table],
            }
            report["completed_other_tables"].append(table)
            atomic_json(report_path, report)
            if table == "name.basics":
                parents[table] = spark.read.parquet(str(output)).select("nconst").persist(StorageLevel.DISK_ONLY)
        profiles = report["profiles"]
        if len([r for r in profiles if r["stage"] == "raw"]) != 39:
            raise AssertionError("The global profile must cover 39 source columns")
        with (quality / "column_profile.csv").open("w", newline="", encoding="utf-8") as target:
            writer = csv.DictWriter(target, fieldnames=list(profiles[0]))
            writer.writeheader()
            writer.writerows(profiles)
        report.update({
            "status": "passed", "full_imdb_dataset_profiled": True,
            "finished_at_utc": datetime.now(timezone.utc).isoformat(),
            "spark_version": spark.version,
            "top_missing_columns_global": sorted(
                [r for r in profiles if r["stage"] == "raw"],
                key=lambda r: (-r["missing_percent"], r["table"], r["column"]),
            )[:5],
        })
        atomic_json(report_path, report)
        if json.loads(current_path.read_text())["run_id"] != report["base_run_id"]:
            raise RuntimeError("Another job changed the catalog; refusing to replace it")
        atomic_json(current_path, {
            "contract_version": 1, "run_id": run_id,
            "base_run_id": report["base_run_id"],
            "published_at_utc": report["finished_at_utc"],
            "tables": report["outputs"], "sources": sources,
            "quality_report": str(report_path.resolve()),
        })
        print(f"PASS: all seven tables published in {current_path}", flush=True)
    except Exception as exc:
        report.update({"status": "failed", "error": str(exc)})
        atomic_json(report_path, report)
        raise
    finally:
        for frame in parents.values():
            frame.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
