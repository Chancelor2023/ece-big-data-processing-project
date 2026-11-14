"""Download the two official IMDb TSV archives with integrity metadata."""

import argparse
import gzip
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from imdb_pipeline.config import RAW_DIR

TABLES = {
    "title.basics": [
        "tconst", "titleType", "primaryTitle", "originalTitle", "isAdult",
        "startYear", "endYear", "runtimeMinutes", "genres",
    ],
    "title.ratings": ["tconst", "averageRating", "numVotes"],
}
SOURCE_ROOT = "https://datasets.imdbws.com"


def fingerprint(path: Path, table: str) -> dict:
    """Check the full gzip CRC and exact header without retaining data in RAM."""
    sha = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            sha.update(block)
    with gzip.open(path, "rb") as source:
        header = source.readline().decode("utf-8").rstrip("\r\n")
        if header.split("\t") != TABLES[table]:
            raise ValueError(f"Unexpected IMDb header for {table}: {header!r}")
        for _ in iter(lambda: source.read(1024 * 1024), b""):
            pass
    return {"bytes": path.stat().st_size, "sha256": sha.hexdigest()}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-dir", type=Path, default=RAW_DIR)
    parser.add_argument("--refresh", action="store_true")
    args = parser.parse_args()
    args.raw_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = args.raw_dir / "title_downloads.json"
    previous = (
        json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest_path.exists() else {}
    )
    session = requests.Session()
    session.headers.update({
        "User-Agent": "ECE-IMDb-course-project/1.0 (educational download)",
        "Accept-Encoding": "identity",
    })
    session.mount("https://", HTTPAdapter(max_retries=Retry(
        total=4, backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"], respect_retry_after_header=True,
    )))
    for table in TABLES:
        destination = args.raw_dir / f"{table}.tsv.gz"
        url = f"{SOURCE_ROOT}/{destination.name}"
        if destination.exists() and not args.refresh:
            details = fingerprint(destination, table)
            old = previous.get(table, {})
            if old.get("sha256") and old["sha256"] != details["sha256"]:
                raise ValueError(f"Source checksum changed locally: {table}")
            previous[table] = {
                **old, **details, "url": url, "file": destination.name,
                "verified_at_utc": datetime.now(timezone.utc).isoformat(),
            }
            print(f"Verified existing source: {destination.name}", flush=True)
        else:
            temporary = destination.with_suffix(destination.suffix + ".part")
            with session.get(url, stream=True, timeout=(15, 120)) as response:
                response.raise_for_status()
                total = int(response.headers.get("Content-Length", 0))
                downloaded = 0
                last_notice = 0
                with temporary.open("wb") as target:
                    for block in response.iter_content(1024 * 1024):
                        target.write(block)
                        downloaded += len(block)
                        if downloaded - last_notice >= 32 * 1024 * 1024:
                            print(
                                f"{table}: {downloaded / 1e6:.1f} MB"
                                f" / {total / 1e6:.1f} MB", flush=True,
                            )
                            last_notice = downloaded
                if total and downloaded != total:
                    raise ValueError(f"Incomplete download: {table}")
                details = fingerprint(temporary, table)
                previous[table] = {
                    **details, "url": url, "file": destination.name,
                    "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
                    "last_modified": response.headers.get("Last-Modified"),
                    "etag": response.headers.get("ETag"),
                }
            os.replace(temporary, destination)
            print(f"Downloaded and checked: {table}", flush=True)
        manifest_temp = manifest_path.with_suffix(".json.tmp")
        manifest_temp.write_text(
            json.dumps(previous, indent=2) + "\n", encoding="utf-8"
        )
        os.replace(manifest_temp, manifest_path)
    print(f"Source manifest: {manifest_path}")


if __name__ == "__main__":
    main()
