# ECE Big Data Processing project

IMDb data engineering project for ECE Big Data Processing, fall 2026.

Repository: [Chancelor2023/ece-big-data-processing-project](https://github.com/Chancelor2023/ece-big-data-processing-project).

| Group information | Value |
| --- | --- |
| Class group | Ing5 Gr3 |
| Contributor 1 / GitHub username | Jerry CHENG / Chancelor2023 |
| Contributor 2 / GitHub username | Brett FAJGENBAUM / Brett273 |

Start with [PROJECT.md](PROJECT.md) for contributors, setup, processing order and operations. Complete the actual email addresses in its contributor table before submission. [REPORT.md](REPORT.md) remains the group’s own results and methodological write-up; its current template must be completed by the authors.

## Team

Group: Ing5 Gr3.

| Member | GitHub username | Email |
| --- | --- | --- |
| Jerry CHENG | Chancelor2023 | jerry.cheng@edu.ece.fr |
| Brett FAJGENBAUM | Brett273 | brett.fajgenbaum@edu.ece.fr |

## Delivery status

This integration includes the seven-table preparation, Wikipedia enrichment, live Kafka/Spark pipeline, final Q1–Q8 runner, Wikidata P364 language pipeline, actor–director open question and complete English report. Peer review and approval are required before merging this branch into main.

The exploration functions retain Brett's authorship. The integration fixes the runner and adds the tested collection/preparation and open-question components; original PR history is preserved.

- [Execution and operations](PROJECT.md).
- [Report and findings](REPORT.md).
- [Exploration/P364/open-question reproduction](docs/INDEPENDENT_FINALIZATION.md).
- [Complete query outputs](report-assets/independent-final-20261115/INDEX.md).
- [Test and comparison evidence](report-assets/independent-validation-20261115/INDEX.md).

Recorded source scope: 14,190 Q6-eligible films, 90.64% declared-language coverage, and 18,764 actor–director pairs in the paired comparison. Enrichment coverage remains partial and is quantified in the report.

## Evidence and execution guides

- [Evidence index](report-assets/evidence-20261115/INDEX.md): quality tables, coverage, missing cases, streaming metrics and source metadata.
- [Sources](report-assets/evidence-20261115/SOURCES.csv), [manifest](report-assets/evidence-20261115/MANIFEST.json) and [evidence verification guide](docs/EVIDENCE_PACKAGE.md).
- [Seven-table contract](docs/DATA_CONTRACT.md) and [remaining-table preparation](docs/REMAINING_TABLES.md).
- [Enrichment](docs/ENRICHMENT.md), [cached biographies](docs/CACHED_BIOGRAPHIES.md) and [synopsis recovery](docs/SYNOPSIS_RECOVERY.md).
- [Streaming execution and outputs](docs/STREAMING.md).

After starting the environment as documented in PROJECT.md, verify the packaged evidence without Spark or network access:

```bash
docker compose exec -T spark python -m jobs.reporting.verify_evidence
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
jobs/exploration/          Final Q1–Q8 runner and P364 collection/preparation
jobs/open_question/        Actor–director collaboration analysis
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

Write the explanations and reasoning in REPORT.md personally. Verify final outputs and repository links after integrating the corrected exploration and open question. Submit the repository according to the course README once the report and integrated reproduction are ready.
