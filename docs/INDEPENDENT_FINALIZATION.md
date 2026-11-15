# Independent fallback and comparison

This guide documents the completion integrated on release/finalize-project. Q1–Q8 functions, parameters and the original three analysis test files originate from Brett273's 3ad08a7 commit (Q1–Q3 from 69da2b4). His PR commits are retained in the integration history. The tested runner, P364 pipeline, output guards, extra tests and actor–director question complete that work. The earlier experimental collect_original_language entry point is superseded by the select_language_targets → collect_languages → prepare_languages sequence below.

PROJECT.md's AI Assistance Declaration is preserved byte-for-byte. The completed English report describes the recorded runs and their limits. The group reviews the complete code, report and evidence together before merging.

## Prerequisites

Use the existing Compose setup and the published seven-table IMDb catalog.
Run one Spark application at a time. The serial HTTP language collector does
not start Spark and may run alongside one Spark job. Raw responses, Parquet,
cache and complete results remain under ignored data/results/logs directories.
Never change branches while jobs execute against the mounted checkout.

## Q6 language collection

Select **all** films meeting Q6's success thresholds; no top-ten, year or
runtime restriction from the text-enrichment selection is added:

```bash
docker compose exec -T spark python -m jobs.exploration.select_language_targets
docker compose exec -T spark python -m jobs.exploration.collect_languages
docker compose exec -T spark python -m jobs.exploration.prepare_languages
```

Selection defaults are rating >=7.0 and votes >=1000. The selector supports
`--min-rating` and `--min-votes`; default runner thresholds are recorded in
jobs/exploration/params.py. The language publication must match those thresholds
and the same IMDb catalog before final Q6 execution.

The first command creates a new run. Do not rerun selection to resume a
collector. Both collection and preparation support `--run-id RUN_ID`.
Collection uses batches of up to 40 exact IMDb P345 IDs, looks up P364, retains
original HTTP JSON/provenance and supports cache/reuse after interruption.
Use `--max-batches 1` for a collection pilot; it remains explicitly partial.
Use the original batch size when resuming; a configuration guard rejects changes.
Run one collector per language run. Interrupt its own terminal with Ctrl+C and
resume the same run ID; do not stop unrelated processes.

The collector makes serial requests with an identifying User-Agent, a minimum
one-second interval and bounded retry/backoff honoring Retry-After. Follow
[WDQS service limits](https://www.mediawiki.org/wiki/Wikidata_query_service/Implementation)
and server throttling. This is gathered Wikidata data, not generated language
labels. Spark parses the responses, validates identities/IRIs, assigns statuses,
deduplicates film-language pairs and performs publication checks.

Paths under data/languages:

| Path | Content |
| --- | --- |
| current_selection.json | Run to collect/prepare |
| runs/RUN_ID/selection.json | Catalog, thresholds, targets and request fingerprint |
| runs/RUN_ID/targets | Eligible films, Parquet |
| runs/RUN_ID/raw | Original response and query URL per request batch |
| runs/RUN_ID/collection.json | Progress/status; completed does not mean every film has languages |
| runs/RUN_ID/clean/SNAPSHOT/languages | Distinct (tconst, language QID) with lineage |
| runs/RUN_ID/clean/SNAPSHOT/collection_status | Every eligible film and its status |
| runs/RUN_ID/clean/SNAPSHOT/coverage.json | Final denominator, counts and coverage |
| current.json | Successfully validated publication pointer |

Multiple P364 languages for one film are accepted. Multiple film entities for
one exact IMDb ID are ambiguous and excluded. Missing matches/languages remain
documented, with no title.akas fallback. No arbitrary percentage threshold is
imposed; zero coverage, incomplete collection, malformed responses, mismatched
thresholds/catalogs or invalid counts prevent final publication.

## Exploration runner

```bash
docker compose exec -T spark python -m jobs.exploration.run
```

The default is final mode for Q1–Q8, requiring a validated language publication.
To advance the other analyses before languages are ready:

```bash
docker compose exec -T spark python -m jobs.exploration.run --q6-mode pending
```

The summary remains partial with Q6 pending; it never claims all eight questions
complete. Use `--questions Q1,Q2,Q3` or another explicit list for targeted runs.
Every run gets its own results/independent_exploration/RUN_ID directory; an
existing run ID is refused, avoiding silent overwrites of Brett's outputs.
run.json records the source catalog, parameters, provenance, row counts,
status and paths. Q4–Q8 receive the declared parameter constants explicitly.

Each output has typed Parquet and a headered CSV directory. Array/struct/map
columns are JSON-encoded for CSV; Parquet retains their actual types. The Q6
output includes the main >=2-language list and q6_actors_3plus_languages.

## Open question selected by Jerry

Question: how do ratings in the first observed year of an actor–director
collaboration compare with later collaboration years?

```bash
docker compose exec -T spark python -m jobs.open_question.run
```

Defaults: titleType movie, 1980–2025, non-null rating and >=1000 votes. Join
basics, ratings, crew/directors, acting principals and people names. A pair/movie
is unique even when credits are duplicated. All films in the pair's first
observed year form the initial group; no order within a year is invented.
Only pairs having films in later years enter the paired comparison.

Outputs under results/independent_open_question/RUN_ID:
pair_comparison, pair_movie_observations, paired_summary and sample_coverage,
each with Parquet and CSV. The summary compares pair-level mean ratings;
movies can appear in multiple pairs. The first year is observed within the
eligible sample, not necessarily the first collaboration in a full career.
The outputs do not establish causality. Jerry writes the motivation,
methodological justification and interpretation personally in REPORT.md.

## Tests and integration

```bash
docker compose exec -T spark python -m unittest tests.test_independent_finalization -v
docker compose exec -T spark python -m unittest discover -s tests -v
```

The new tests cover raw P364 identity/language distinctions, publication guards,
array export, explicit pending mode and collaboration-year/credit edge cases.
Do not confuse a synthetic test result with full-source execution. Record the
real run summaries and compare the two implementations on the same source
catalog and declared parameters before deciding what to integrate.

## Compare deliveries

When both implementations have produced outputs:

```bash
docker compose exec -T spark python -m jobs.exploration.compare --reference results/exploration/BRETT_RUN --candidate results/independent_exploration/OUR_RUN --reference-catalog CATALOG_ID
```

The reference can be legacy CSV directories or Parquet; the candidate has the
new run.json. Supply `--reference-parameters FILE.json` for an undocumented
legacy parameter set. Without documented matching parameters, the tool reports
differences but does not confirm semantic equivalence. Catalog mismatches are
rejected. Missing outputs and different column sets remain explicit. The report
uses outer key comparisons and a 1e-6 numerical tolerance; array-language CSV
exports are parsed against the typed array when available. Results go to a new
results/exploration_comparisons/RUN_ID directory; neither delivery is modified.

## Observed completed runs

- Language selection/collection: 20261115T172917Z-0ad8a6; validated snapshot
  20261115T174630Z-b9b01c. 12,862/14,190 films declared; 812 no P364,
  505 no Wikidata match, 11 ambiguous matches; 14,814 distinct pairs.
- Full exploration: 20261115T174842Z-15e20a, final mode, all eight questions passed.
- Open question: 20261115T173528Z-ee767f, four outputs passed. Actor/director
  role IDs may belong to the same person; no distinct-person filter is imposed.
- Isolated full suite: 68 tests passed; candidate code tree
  4386f019bef1edfb0566228f1c6542ac1bf30358. Later metadata packaging is separately
  verified by the bundle integrity checks.
- Comparison: 20261115T181405Z-17cf45; Q1–Q3 equal on all keys/shared values,
  same catalog and parameters. Brett's later 52b7886 changed README only,
  so the compared analysis/runner code is unchanged from 69da2b4.

Final CSV exports were checked with Spark and Python's standard csv reader;
embedded quotes use standard double-quote escaping. Authoritative Parquet keeps
arrays typed. The formatting job preserves the earlier CSV directories as local
backups; it does not change the numerical results or other run directories.

The complete factual outputs are in report-assets/independent-final-20261115,
with 28 read-back/consistency checks. Test/comparison evidence is separately
packaged in report-assets/independent-validation-20261115. Both bundles have
source-copy hashes, SHA256SUMS and manifests; run verify_evidence on each after
checkout. Keep raw Parquet, responses, caches, checkpoints and complete logs out
of commits. These two deliberately selected factual bundles are suitable for review.
