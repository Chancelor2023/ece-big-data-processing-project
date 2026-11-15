# ECE Big Data Processing project

IMDb data engineering project for ECE Big Data Processing, fall 2026.

Repository: [Chancelor2023/ece-big-data-processing-project](https://github.com/Chancelor2023/ece-big-data-processing-project).

| Group information | Value |
| --- | --- |
| Class group | Ing5 Gr3 |
| Contributor 1 / GitHub username | Jerry CHENG / Chancelor2023 |
| Contributor 2 / GitHub username | Brett FAJGENBAUM / Brett273 |

Start with [PROJECT.md](PROJECT.md) for contributors, setup, processing order and operations. Complete the actual email addresses in its contributor table before submission. [REPORT.md](REPORT.md) remains the group’s own results and methodological write-up; its current template must be completed by the authors.

## Delivery status

Checked on 15 November 2026. Preparation, enrichment, streaming and evidence are merged into main; exploration and the open question remain incomplete.

| Component | Current status |
| --- | --- |
| Environment and preparation | Seven complete IMDb tables, 78 before/after column profiles, quality and read-back checks validated |
| Enrichment | Collection completed; cached-biography supplementation and synopsis recovery merged via #10/#11 |
| Streaming | Real Wikipedia → Kafka → Spark test validated over fifteen minutes; 130 relevant events in the live window |
| Evidence | Versioned bundle merged; #13 preserves exact bytes and adds an integrity verifier; published and fresh-clone checks passed |
| Exploration Q1–Q3 | [PR #5](https://github.com/Chancelor2023/ece-big-data-processing-project/pull/5) open; technical checks passed, documentation and approval pending |
| Exploration Q4–Q8 | [PR #7](https://github.com/Chancelor2023/ece-big-data-processing-project/pull/7) open; Q5/Q6 corrections and final validation pending |
| Open question | Assigned to Brett; no delivered implementation or validated command |
| Final report and reproduction | Personal write-up, contributor emails and complete integrated clean-clone execution pending |

Historical enrichment coverage: 5,983/7,263 synopses, 8,055/11,443 unique biographies, 5,825 films with at least three biographies, and 5,329 with both synopsis and at least three biographies. These measurements are not a claim of complete coverage or guaranteed counts on newer sources.

## Evidence and execution guides

- [Evidence index](report-assets/evidence-20261115/INDEX.md): quality tables, coverage, missing cases, streaming metrics and source metadata.
- [Sources](report-assets/evidence-20261115/SOURCES.csv), [manifest](report-assets/evidence-20261115/MANIFEST.json) and [evidence verification guide](docs/EVIDENCE_PACKAGE.md).
- [Seven-table contract](docs/DATA_CONTRACT.md) and [remaining-table preparation](docs/REMAINING_TABLES.md).
- [Enrichment](docs/ENRICHMENT.md), [cached biographies](docs/CACHED_BIOGRAPHIES.md) and [synopsis recovery](docs/SYNOPSIS_RECOVERY.md).
- [Streaming execution and outputs](docs/STREAMING.md).

After starting the environment as documented in PROJECT.md, verify the packaged evidence without Spark or network access:

```bash
docker compose exec -T spark python -m jobs.reporting.verify_evidence
docker compose exec -T spark python -m jobs.exploration.run
docker compose exec -T spark python -m unittest tests.test_exploration -v

```

Expected bundle counts: 64 manifest entries, 63 checksum entries, 48 source copies and 18 CSVs, with `status: passed`. This check does not reproduce the processing pipeline. For a full new run, follow PROJECT.md in order; use your own selection/run IDs and run one Spark application at a time. After base text preparation, apply biography supplementation before synopsis recovery.

## Layout

```text
compose.yaml               Local Kafka and Spark/Jupyter environment
docker/spark/              Image and Spark configuration
src/imdb_pipeline/         Shared configuration, data contracts and transformations
jobs/create_topics.py      Repeatable topic creation
jobs/smoke_test.py         Synthetic environment verification
jobs/preparation/          Download, profile and clean seven IMDb tables
jobs/enrichment/           Select, collect, prepare and improve external text
jobs/streaming/            Live Wikipedia producer and Spark consumer
jobs/reporting/            Evidence export and exact-byte verification
jobs/exploration/          Q1–Q8 modules remain in PRs #5/#7
jobs/open_question/        Placeholder; additional analysis pending
docs/                      Execution and data-contract guides
report-assets/             Deliberately versioned factual evidence
notebooks/                 Interactive environment
data/                      Ignored inputs, Parquet, cache and checkpoints
results/                   Ignored complete runtime outputs
logs/                      Ignored execution logs
```

Raw archives, article bodies, Parquet, caches, credentials and checkpoints are excluded from Git. The selected factual evidence in report-assets is intentionally versioned; Git attributes preserve its exact source bytes and line endings. Keep the historical bundle unchanged when producing a newer snapshot.

## Collaboration and submission

Use one feature branch per task and obtain a peer review and formal approval before merging its PR. No direct commits to main. Comments such as “All good” and a merge action are not formal GitHub approvals. Historical review-trace gaps remain to be clarified; do not describe later reviews as having preceded a merge.

