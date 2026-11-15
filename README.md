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
jobs/preparation/          Download, profile and prepare the seven IMDb tables
docs/REMAINING_TABLES.md   Prepare the five remaining IMDb tables and global profile
docs/DATA_CONTRACT.md      Published schemas and loading interface
jobs/exploration/          Future answers to the eight exploration questions
jobs/enrichment/           IMDb target selection, Wikipedia collection and Spark enrichment
jobs/streaming/            Future live Wikimedia processing
jobs/open_question/        Future additional analysis
notebooks/                Interactive development
data/                     Local inputs, cleaned tables and checkpoints
results/                  Local outputs grouped by project section
logs/                     Local execution logs
```

Data and generated outputs are ignored by Git. Include selected final outputs in the report or a deliberately versioned report-assets directory when preparing the submission.

## First IMDb data delivery

Run the three preparation commands in [PROJECT.md](PROJECT.md#prepare-the-first-two-imdb-tables) to download, test and prepare the first two tables. [The data contract](docs/DATA_CONTRACT.md) documents types, missing values, row handling and how both tables are loaded from one validated snapshot.

For people, crews, episodes, credits and aliases, follow [the remaining-table guide](docs/REMAINING_TABLES.md). It describes the five-table preparation, resumable runs and the final seven-table catalog.

For the film/person text pilot and full collection commands, follow [the enrichment guide](docs/ENRICHMENT.md).

## Collaboration

Use one feature branch per task. Each pull request requires review and approval by the other group member before merging. No direct commits to `main`.

The initial environment is developed on `feature/bootstrap-environment` and submitted for review against `main`. The repository's starting point is an empty history-initialization commit: all project files enter `main` through the reviewed pull request.

## How to change

Parameters are not exposed on the command line. To change a value, edit
`jobs/exploration/params.py` (a `Params` dataclass instantiated with its
defaults by the runner) and rerun.

## Validation

    python -m jobs.exploration.run
    python -m pytest tests/test_exploration.py

Both commands run inside the project container (Python, Spark and Java are
not installed on the host). <prefix with your container command>

## Catalogue

Catalog `20261114T174336Z-d9d2e91f` (`data/clean/current.json`). It is a
composite catalog: `title.basics` (12,963,111 rows) and `title.ratings`
(1,732,169 rows) come from base run `20261114T171444Z-8c0fefb2`, built from
IMDb dumps downloaded on 2026-11-14. Output directories are named after the
catalog ID, not the base run ID.

## Output

The output directory name is the `run_id` field of `data/clean/current.json`
(`unknown-run` if the field is absent). The exploration job does not
generate its own ID: rerunning against the same catalog overwrites the
three output directories.
