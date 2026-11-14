# Cleaned IMDb title tables

Contract version 1 covers `title.basics` and `title.ratings`. The preparation preserves all source title types and years; analytical scope filters belong to downstream jobs. Every published table has one row per non-null `tconst`.

## Commands

From the project directory, after starting Compose:

```bash
docker compose exec -T spark python -m jobs.preparation.download_titles
docker compose exec -T spark python -m unittest tests.test_preparation -v
docker compose exec -T spark python -m jobs.preparation.prepare_titles
```

All transformations, profiling and joins run with Spark. Python handles streamed file downloads, gzip validation and small metadata files. Downloads are from [IMDb's official dataset host](https://datasets.imdbws.com/), whose [schema documentation](https://developer.imdb.com/non-commercial-datasets/) describes the source fields and missing-value marker.

## Snapshot publication and loading

`data/clean/current.json` contains a run identifier, source fingerprints, relative Parquet paths, row counts and Spark schemas. It is replaced only after both output tables and their join have passed validation. Parquet directories are at `data/clean/runs/<run_id>/title.basics/` and `data/clean/runs/<run_id>/title.ratings/`.

```python
from imdb_pipeline.spark import create_spark
from imdb_pipeline.tables import load_clean_tables
from pyspark.sql import functions as F

spark = create_spark("exploration")
tables = load_clean_tables(spark)
basics, ratings = tables["title.basics"], tables["title.ratings"]
movies = basics.filter(F.col("titleType") == "movie")
with_ratings = movies.join(ratings, "tconst", "left")
by_genre = with_ratings.withColumn("genre", F.explode("genres"))
```

The helper reads the publication once, so related tables come from the same run. Keep the run identifier with analytical outputs. IMDb refreshes its sources daily; separate downloads may have different row counts and ratings even with identical code.

## Title basics schema

| Column | Spark type | Values after preparation |
| --- | --- | --- |
| `tconst` | string | Non-null, unique, matching `tt` followed by one or more digits |
| `titleType` | string | Trimmed source value; unknown or missing values are not guessed |
| `primaryTitle` | string | Trimmed title, preserving internal punctuation and literal quotes |
| `originalTitle` | string | Trimmed original title |
| `isAdult` | boolean | Source `0` becomes false and `1` becomes true; missing/invalid becomes null |
| `startYear` | integer | Parsed year between 1 and 9999, otherwise null |
| `endYear` | integer | Same range; a value before a known start year becomes null |
| `runtimeMinutes` | integer | Strictly positive parsed minutes; missing, invalid or non-positive becomes null |
| `genres` | array of strings | Trimmed, distinct, sorted genre tokens; unknown/no usable tokens becomes null |

No field is filled with an invented title, year, duration, genre or adulthood value. Do not replace unknown durations with zero in downstream averages. Future release years are preserved if valid calendar years.

## Title ratings schema

| Column | Spark type | Values after preparation |
| --- | --- | --- |
| `tconst` | string | Non-null, unique; exists in published `title.basics` |
| `averageRating` | double | Finite value between 1 and 10 inclusive; missing/invalid becomes null |
| `numVotes` | long | Parsed non-negative count; missing, invalid, negative or overflowing becomes null |

Zero votes are not rewritten as missing. Invalid ratings and vote counts are nulled and counted as anomalies rather than causing the whole title record to disappear.

## Missing values and row handling

- Trim outer spaces in fields. Empty/space-only fields and the literal `\N` become null. Missing genres remain null, not an invented category.
- Split physical TSV lines on literal tabs, including trailing empty fields. Treat double quotes as title characters. Reject wrong field counts instead of silently shifting columns or dropping extra tokens.
- Quarantine malformed rows and missing/invalid keys. Retain the original physical line with its rejection reason.
- For duplicate valid keys, retain the record with the most non-null cleaned fields. Break ties by the lexical JSON representation of cleaned fields and then the original line. Quarantine the others. Deterministic selection does not establish which conflicting value is factually correct.
- Quarantine rating records without a corresponding cleaned title. Published ratings therefore join to basics without unmatched parents or duplicate-key multiplication.
- A basics record need not have a rating. Use a left join to retain unrated titles; an inner join restricts the analysis to titles with a ratings record.

Parquet may report columns as nullable even for validated keys. Non-null/unique keys are checked on the data, not inferred from nullable metadata.

## Quality outputs

`results/quality/<run_id>/quality_report.json` records source identities, raw/clean counts, corrections, duplicate and orphan counts, read-back checks and join counts. `column_profile.csv` lists missing counts and percentages for every column before and after preparation. The denominator is the relevant raw or clean row count.

Quarantines are grouped by table and reason under the quality-run directory. They preserve diagnostic records and are not analytical inputs by default. Field-anomaly counts can overlap and do not represent rows removed.

The five most missing columns in the report are ranked only across these two source tables. The project's global ranking and full-data assessment must be extended when the other IMDb tables are prepared.

## Collaboration boundary

Jerry owns these source and cleaned-table jobs. Brett consumes the publication through the loader. Agree on contract changes before renaming columns, changing types or reusing output directories. Generated sources, snapshots, quality reports and quarantines stay outside Git; peers reproduce them locally from the scripts.
