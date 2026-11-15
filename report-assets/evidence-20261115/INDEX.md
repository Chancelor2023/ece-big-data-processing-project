# Verified project evidence

Generated: 2026-11-15T13:26:26.840814+00:00. Source catalog: `20261114T115800Z-6c04bdd1`. Enrichment snapshot: `20261115T105744Z-c082eb`. Streaming session: `live-validation-20261114-v2` / `20261114T150245Z`.

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

- Current enrichment: 5,983 synopses / 7,263 films; 8,055 unique biographies / 11,443 people; 5,329 films with synopsis and at least three biographies. Missing and ambiguous records remain explicit; full coverage is not claimed.
- Source corrections can overlap across columns and checks; do not sum them as a count of deleted rows. Table row differences and reference occurrences have separate units.
- A multigenre film can appear in several genre/year groups. The genre/year table is not a unique-film total.
- Streaming: 900.63 seconds, 1,776 processed rows, 130 relevant events in the live window. The 203 matched events over committed batches include the historical backlog.
- Observed natural late drops: 0; observed duplicate drops: 0. These observations do not replace the separate synthetic late-data/duplicate tests.
- Only metadata, small factual exports and original report JSONs are packaged. Raw IMDb archives, Wikipedia article bodies, Parquet datasets, Kafka data, credentials and checkpoints stay outside this folder.

## Integrity and reproduction

[MANIFEST.json](MANIFEST.json) lists packaged file hashes. [VALIDATION.json](VALIDATION.json) records the consistency checks. [SHA256SUMS.txt](SHA256SUMS.txt) lists payload hashes. SOURCES.csv records original repository-relative paths and hashes for copied evidence. Those original data/results paths are local and may not exist in a fresh clone.

After producing the validated outputs, reproduce this bundle with `docker compose exec -T spark python -m jobs.reporting.prepare_evidence --output report-assets/NEW-EVIDENCE-DIRECTORY`. Existing evidence directories are never overwritten.
