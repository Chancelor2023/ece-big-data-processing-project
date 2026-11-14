# Preparing all seven IMDb tables

This stage adds `name.basics`, `title.crew`, `title.episode`, `title.principals` and `title.akas` to the existing validated title-table delivery. It retains the published `title.basics` and `title.ratings` snapshots and extends the catalog only when the five additional tables pass their checks.

## Execution

Run from the project directory with Compose services started:

```bash
docker compose exec -T spark python -m jobs.preparation.download_remaining
docker compose exec -T spark python -m unittest tests.test_preparation tests.test_other_preparation -v
docker compose exec -T spark python -m jobs.preparation.prepare_remaining
```

The download command verifies and reuses existing archives. Use `--refresh` only to request new source downloads. Metadata for all downloaded files is recorded in `data/raw/imdb_downloads.json`; the first two tables retain their original source provenance.

The larger sources are also expanded to `data/raw/expanded/` so Spark can split their physical TSV lines into parallel tasks. The compressed originals remain intact. Expanded-file metadata records the source fingerprint and uncompressed bytes/checksum. Large archives, expanded files, intermediate disk caches and Parquet snapshots require substantial disk space.

The processing defaults to 32 shuffle partitions with adaptive coalescing disabled for this job. This avoids concentrating millions of keys into one small number of tasks in the local 2 GB Spark driver heap. Adjust the partition count if needed:

```bash
docker compose exec -T spark python -m jobs.preparation.prepare_remaining --partitions 64
```

## Restart after a failure

Each run has its own `results/quality/<run_id>/quality_report.json`. Successfully completed tables and their output paths are recorded after every table. A failed run does not replace `data/clean/current.json`.

Use the exact failed run identifier to reuse completed tables:

```bash
docker compose exec -T spark python -m jobs.preparation.prepare_remaining --resume-run RUN_ID
```

Replace `RUN_ID` with the identifier in the report. Resuming refuses changed input fingerprints or a changed base publication. An unfinished table uses a fresh attempt directory when earlier files exist, preserving the failed attempt for diagnosis.

After publication, rerunning the command with unchanged source fingerprints reuses the existing complete catalog instead of processing all five tables again.

## Schema and reference checks

Read [DATA_CONTRACT.md](DATA_CONTRACT.md) for the column types and join keys. The pipeline checks physical field counts and identifiers before using a record. It profiles every source column, normalizes missing values, safely casts numeric and Boolean fields, and records field corrections separately from rejected rows.

Key checks use the table's natural key. Only duplicate-key groups require deterministic row ranking; unique source records avoid a full-row window sort. The composite key of aliases and credits includes `ordering`, so distinct aliases or roles are not collapsed into one title row.

Scalar references are checked using distinct identifiers against unique parent keys. Missing parent rows are quarantined with a reason. In arrays such as `directors`, `writers` and `knownForTitles`, missing references are recorded as invalid edges and removed while preserving the owning title/person and its remaining valid references.

The pipeline preserves season/episode zero for specials, records and nulls a death year before a known birth year, parses `characters` as a JSON string array, and uses ASCII STX to split alias `types` and `attributes`. It retains unknown category/type values and does not infer a film's original language from an alternative-title language.

## Quality outputs and publication

The final run report covers all seven tables. `column_profile.csv` has 78 rows: 39 source columns, each profiled before and after cleaning. The five columns with the largest raw missing percentages are ranked globally across those 39 columns.

For cleaned arrays, null and empty arrays count as missing in that profile. An explicit source JSON `[]` is not a raw missing marker, but becomes an empty array in the cleaned profile; raw and clean percentages can therefore differ without a record being lost.

The report distinguishes invalid source rows, duplicate rows, scalar-orphan rows, field corrections and removed array references. Corrections can overlap; their counts must not be summed as a number of deleted rows. Record-count accounting and Parquet read-back must pass for every new table.

Publication uses one atomic catalog update. That catalog can point to earlier validated title-table files and newly created files for the other tables. It still provides one consistent set of paths and source fingerprints for downstream jobs.

## Loading

```python
from imdb_pipeline.spark import create_spark
from imdb_pipeline.tables import load_clean_tables

spark = create_spark("exploration")
tables = load_clean_tables(spark, names=(
    "title.basics", "title.ratings", "name.basics", "title.crew",
    "title.episode", "title.principals", "title.akas",
))
```

The helper defaults to the first two tables for backward compatibility. Request the additional tables by name after the full publication exists. All files remain local and excluded from Git; peers reproduce them using these commands.
