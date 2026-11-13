# ECE Big Data Processing project

IMDb data engineering project for ECE Big Data Processing, fall 2026.

Repository: [Chancelor2023/ece-big-data-processing-project](https://github.com/Chancelor2023/ece-big-data-processing-project).

| Group information | Value |
| --- | --- |
| Class group | Ing5 Gr3 |
| Contributor 1 / GitHub username | Jerry CHENG / Chancelor2023 |
| Contributor 2 / GitHub username | Brett FAJGENBAUM / Brett273 |

Start with [PROJECT.md](PROJECT.md) for installation, commands and environment details.
[REPORT.md](REPORT.md) is a heading-only template for the group's own results and reasoning.

## Layout

```text
compose.yaml               Kafka and a local Spark/Jupyter environment
docker/spark/              Reproducible Python/Spark image and configuration
src/imdb_pipeline/         Shared settings and Spark session factory
jobs/create_topics.py      Explicit, repeatable project-topic creation
jobs/smoke_test.py         Synthetic end-to-end environment verification
jobs/preparation/          Future IMDb loading and quality jobs
jobs/exploration/          Future answers to the eight exploration questions
jobs/enrichment/           Future collection and Spark enrichment jobs
jobs/streaming/            Future live Wikimedia processing
jobs/open_question/        Future additional analysis
notebooks/                Interactive development
data/                     Local inputs, cleaned tables and checkpoints
results/                  Local outputs grouped by project section
logs/                     Local execution logs
```

Data and generated outputs are ignored by Git. Include selected final outputs in the report or a deliberately versioned report-assets directory when preparing the submission.

## Collaboration

Use one feature branch per task. Each pull request requires review and approval by the other group member before merging. No direct commits to `main`.

The initial environment is developed on `feature/bootstrap-environment` and submitted for review against `main`. The repository's starting point is an empty history-initialization commit: all project files enter `main` through the reviewed pull request.
