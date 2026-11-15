# Project execution and operations guide

## Contributors

| Contributor | GitHub username | Email |
| --- | --- | --- |
| Jerry CHENG | Chancelor2023 | jerry.cheng@edu.ece.fr |
| Brett FAJGENBAUM | Brett273 | brett.fajgenbaum@edu.ece.fr |

Class group: Ing5 Gr3. Contributor email addresses supplied by Jerry for publication.

## Environment scope

The project provides Kafka, Spark batch processing, Spark Structured Streaming, JupyterLab and persistent local data. All seven IMDb tables have download, profiling, cleaning, quarantine and publication jobs. Film/person enrichment and live Wikipedia ingestion are implemented.

This integration branch combines the existing preparation, enrichment and streaming with the validated exploration runner, Wikidata P364 pipeline and actor–director open question. It requires Brett's peer review and approval before merging into main.

| Component | Status on this integration branch |
| --- | --- |
| Environment and seven-table preparation | Existing main implementation; full-source outputs and read-back checks available |
| Wikipedia text enrichment | Collection finished; supplementary biography links and synopsis recovery validated; coverage remains partial |
| Wikipedia → Kafka → Spark | Existing main implementation; versioned real 15-minute test and separate isolated reproduction passed |
| Exploration Q1–Q8 | Tested final-mode runner and complete output tables included |
| Q6 original languages | Reproducible P345/P364 collection, Spark preparation, coverage and final/pending controls included |
| Open question | Actor–director first-observed-year versus later-year comparison implemented and executed |
| Report and contacts | English report and contributor emails included; final group review remains required |

Spark runs in `local[2]` mode inside one container, as in a local lab. This is not a multi-node Spark cluster. Kafka uses one KRaft broker/controller with persistent storage and no ZooKeeper.

| Component | Version |
| --- | --- |
| Python base image | `python:3.12.12-slim-bookworm` |
| Java | OpenJDK 17, Debian package |
| Spark / PySpark | 4.0.1 |
| Scala binary version | 2.13 |
| Spark Kafka connector | `org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1` |
| Kafka broker | `apache/kafka:4.1.1` |
| Python Kafka client | confluent-kafka 2.12.0 |
| JupyterLab | 4.4.10 |

Direct Python dependencies and application versions are pinned. Base-image tags, Debian packages and transitive Python dependencies are not a complete immutable dependency lock. The Kafka connector and its transitive JAR dependencies are resolved during image construction and cached in the image.

## Prerequisites

- Docker Desktop running with Linux containers and Docker Compose v2.
- Internet access for the initial image build (image registries, Debian, PyPI and Maven).
- As an initial local budget, allocate around 8 GB RAM to Docker and allow at least 15 GB free disk for images and development data; full IMDb downloads and outputs require additional space.
- No host-side Python, Java, Spark or Kafka installation is required.
- Native amd64 and arm64 Linux base images are used; no architecture emulation is forced.

Run every command below from the repository directory (called `imdb-big-data` on Jerry's machine). For a new checkout:

```bash
git clone https://github.com/Chancelor2023/ece-big-data-processing-project.git
cd ece-big-data-processing-project
```

The repository name does not affect the commands; the working directory must contain `compose.yaml`. Generated inputs, outputs and `.env` are local and must be recreated on a new machine. Full compressed sources, expanded TSV files, Parquet snapshots and temporary Spark files need substantially more space than the environment-only disk budget above.

## First startup

Create `.env` only on first setup; preserve an existing local configuration. PowerShell:

```powershell
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
```

Linux / macOS / Git Bash:

```bash
test -f .env || cp .env.example .env
```

Choose your own `JUPYTER_TOKEN` in `.env`. For a generated token using Docker:

```bash
docker run --rm python:3.12.12-slim-bookworm python -c "import secrets; print(secrets.token_urlsafe(32))"
```

Then validate, build and start:

```bash
docker compose config --quiet
docker compose up -d --build --wait --wait-timeout 240
docker compose ps
docker compose exec -T spark python -m jobs.create_topics
docker compose exec -T spark python -m jobs.smoke_test
```

The first build downloads Spark and connector dependencies and can take several minutes. `--wait` checks Kafka readiness and the authenticated Jupyter API; the separate smoke test checks actual Spark calculations and Kafka-to-Spark streaming.

The expected smoke-test result is `status: passed`, `parquet_rows: 5` and `streamed_messages: [0, 1, 2]`. Its timestamped verification JSON is written under `results/environment/<run_id>/verification.json`. A failed invocation exits with a nonzero code. Messages are synthetic and do not count as IMDb results or the required live-streaming demonstration.

## Access

- JupyterLab: [localhost:8888](http://localhost:8888), using the token from `.env`.
- Spark UI: [localhost:4040](http://localhost:4040), only while a Spark application is running.
- Kafka from inside Compose: `kafka:19092`.
- Kafka from host-side tools: `localhost:9092`.

Host ports can be changed in `.env`. The external Kafka advertised port follows `KAFKA_PORT`. Never use `localhost:9092` from the Spark container to reach Kafka.

The three host ports are bound to loopback. The development Kafka listeners use plaintext and are intended for this local environment.

## Working in notebooks and scripts

Open `notebooks/00_environment.ipynb` in JupyterLab.

```python
from imdb_pipeline.spark import create_spark
from imdb_pipeline.config import RAW_DIR, CLEAN_DIR

spark = create_spark("imdb-preparation")
spark.range(10).show()
# Your job code here.
spark.stop()
```

Shared code lives in `src/imdb_pipeline`, which is on the container's Python path. Runnable modules are grouped under `jobs/preparation`, `jobs/enrichment` and, on the integrated branch, `jobs/streaming` and `jobs/exploration`. Use the shared Spark factory for consistent master, shuffle partitions and UTC handling.

Run only one Spark application at a time within the default 4 GB container / 2 GB driver budget. Stop unused notebook Spark sessions before batch jobs or the streaming consumer. The HTTP enrichment collector does not start a Spark JVM and can run alongside one Spark application. Its progress viewer only reads a small JSON file.

## Persistence and inputs

### Prepare the first two IMDb tables

```bash
docker compose exec -T spark python -m jobs.preparation.download_titles
docker compose exec -T spark python -m unittest tests.test_preparation -v
docker compose exec -T spark python -m jobs.preparation.prepare_titles
```

The download job saves the official compressed files in `data/raw`, checks the exact headers and complete gzip integrity, and records source URLs, HTTP metadata and SHA-256 fingerprints in `data/raw/title_downloads.json`. Existing files are verified and reused. `--refresh` explicitly requests a new download; the sources are updated daily by IMDb.

The preparation job processes both complete source files, without filtering years or title types. It writes a new immutable snapshot under `data/clean/runs/<run_id>/`, column profiles and a machine-readable quality report under `results/quality/<run_id>/`, and quarantined rows under that report directory. It does not overwrite previous snapshots.

After both tables pass read-back, key and join checks, `data/clean/current.json` atomically publishes the pair. A failed preparation does not replace the previous publication. The per-run report explicitly covers only these two tables, not the complete IMDb dataset.

Read [docs/DATA_CONTRACT.md](docs/DATA_CONTRACT.md) before implementing analyses. Load both tables from one publication:

```python
from imdb_pipeline.spark import create_spark
from imdb_pipeline.tables import load_clean_tables

spark = create_spark("my-analysis")
tables = load_clean_tables(spark)
basics = tables["title.basics"]
ratings = tables["title.ratings"]
```

The cleaned `genres` field is an array of strings, not a comma-separated string. Downloaded raw files are preserved. A peer can reproduce the tables using the same commands; source fingerprints identify the snapshot used for a result.

Use `--partitions N` on `prepare_titles` to adjust the default eight shuffle/output partitions. The job uses disk-backed persistence for intermediate DataFrames instead of attempting to hold the complete title dataset in memory.

The project directory is mounted at `/workspace`. Files written under `/workspace/data`, `/workspace/results` and `/workspace/notebooks` remain on the host after container recreation. Kafka logs use the named `kafka-data` volume.

Use `data/raw` for downloaded IMDb source files, `data/clean` for the shared cleaned-table contract, `data/enrichment` for collected/enriched data and `data/checkpoints` for separate streaming query checkpoints. Do not reuse one checkpoint directory for unrelated streaming queries.

`jobs.create_topics` creates `wikimedia.recentchange` with three partitions and replication factor one. It can be rerun without deleting the topic or resetting its offsets.

## Prepare the five remaining IMDb tables

Run after the first two tables have been published:

```bash
docker compose exec -T spark python -m jobs.preparation.download_remaining
docker compose exec -T spark python -m unittest tests.test_preparation tests.test_other_preparation -v
docker compose exec -T spark python -m jobs.preparation.prepare_remaining
```

This adds `name.basics`, `title.crew`, `title.episode`, `title.principals` and `title.akas`. The final `data/clean/current.json` references all seven validated tables. Its quality report under `results/quality/<run_id>/` includes 39 source columns profiled before/after cleaning (78 profile rows), the global top-five missing columns and traceable corrections/quarantines.

Read [the seven-table guide](docs/REMAINING_TABLES.md) for restart with `--resume-run RUN_ID`, source checks and partition tuning. The loader defaults to basics/ratings; explicitly request additional tables:

```python
tables = load_clean_tables(spark, names=(
    "title.basics", "title.ratings", "name.basics", "title.crew",
    "title.episode", "title.principals", "title.akas",
))
```

The local full catalog validated on 14 November is `20261114T115800Z-6c04bdd1`. A new reproduction creates its own run IDs and source fingerprints.

## Film and person enrichment

Read [the enrichment guide](docs/ENRICHMENT.md) for selection rules, provenance and outputs. On a new reproduction, after seven-table preparation:

```bash
docker compose exec -T spark python -m unittest tests.test_enrichment -v
docker compose exec -T spark python -m jobs.enrichment.select_targets
docker compose exec -T spark python -m jobs.enrichment.collect_wikipedia
docker compose exec -T spark python -m jobs.enrichment.prepare_wikipedia
```

Selection covers 1980–2025 and up to ten films per genre/year, ordered by vote count with deterministic tie-breaking. Collection initially covers a pilot of three films and their selected people. Text comes from existing English Wikipedia content; Spark performs schema enforcement, text extraction/cleaning and joins. Records retain revision URLs, retrieval times and source provenance.

To continue that same selection on the full scope:

```bash
docker compose exec -T spark python -m jobs.enrichment.collect_wikipedia --scope full
```

Do not rerun `select_targets` to resume collection: it creates a new run. Jerry’s completed selection is `20261114T142005Z`, containing 7,263 films and 11,443 unique people (18,706 requests). Its full collection is finished and does not need restarting. The following commands illustrate resuming that run after an interruption; use your own run ID for a new reproduction:

```bash
docker compose up -d
docker compose exec -T spark python -m jobs.enrichment.collect_wikipedia --scope full --run-id 20261114T142005Z
```

Use only one collector per run/cache. Saved successes and missing/ambiguous matches are reused; API failures are retried. `--refresh` deliberately bypasses that reuse and is not a normal resume option. The resume improvements and viewer are included in `main` through #8.

### View collection progress on Windows

Double-click `scripts/Ouvrir-Suivi-Collecte.cmd`, or run from a second PowerShell window:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\Watch-Enrichment.ps1
```

Add `-Once` for a single snapshot or `-RunId 20261114T142005Z` to select this existing run explicitly. The viewer refreshes every ten seconds and reads `results/enrichment/<run_id>/collection_full.json`. It does not control the collector. Closing the viewer does not stop collection; counters are checkpointed by batches of fifty, so short plateaus are normal. The timestamp and stale-checkpoint warning help distinguish a recent checkpoint from an old report; `running` in a file alone does not prove process liveness.

### Stop and resume collection

In the terminal actually running the collector, interrupt with Ctrl+C and wait for it to return. If it was started elsewhere, first identify its PID from a second terminal:

```bash
docker compose exec -T spark ps -eo pid,etime,stat,args
```

Send `kill -INT PID` inside the Spark container only to the identified collector process, replacing `PID` with its current numeric identifier. Wait for it to stop before sleeping the laptop or stopping Docker. Resume with the same scope/run ID afterward. Sleep suspends network access; Docker restart does not automatically restart an exec-launched collector. Avoid source changes or branch switching underneath running jobs.

### Publish cleaned text and coverage after collection

```bash
docker compose exec -T spark python -m jobs.enrichment.prepare_wikipedia --run-id 20261114T142005Z
```

For a new reproduction, use its own run ID, or omit `--run-id` to use its current selection. Inspect `results/enrichment/<run_id>/coverage.json`; `data/enrichment/runs/<run_id>/current_content.json` points to the cleaned `films`, `people` and `film_biography_coverage` Parquet outputs. Finishing the request list does not imply that all synopses/biographies exist. Inspect API failures, missing pages, absent plot sections and per-film biography coverage.

Fifty-three selected films have fewer than three distinct IMDb principals. The [principal-shortfall audit](docs/PRINCIPAL_SHORTFALLS.md) is read-only and preserves active inputs:

```bash
docker compose exec -T spark python -m jobs.enrichment.audit_principal_shortfalls --wikidata
```

Supplemental credit proposals are separate from the active collection and do not prove that three biographies are available. Preserve and report these limitations explicitly.

### Apply the cached-text improvements

Run after the full collection and its base `prepare_wikipedia` publication, in this order. Omit `--run-id` to use the current selection in your own reproduction:

```bash
docker compose exec -T spark python -m unittest tests.test_cached_biography_supplements tests.test_enrichment tests.test_synopsis_recovery -v
docker compose exec -T spark python -m jobs.enrichment.supplement_cached_biographies
docker compose exec -T spark python -m jobs.enrichment.recover_synopses
```

For an explicitly chosen existing run, both jobs accept `--run-id 20261114T142005Z`. They reuse cached text without HTTP requests. The original selection and existing text are preserved; effective associations include the cached-biography supplements. Rerunning an already applied improvement returns `already_published` without creating another snapshot. Running base `prepare_wikipedia` again creates an unsupplemented publication, so apply the two improvements afterward if that is intended.

Read [cached biographies](docs/CACHED_BIOGRAPHIES.md) and [synopsis recovery](docs/SYNOPSIS_RECOVERY.md) for output paths, guards and provenance. Downstream readers must use `current_content.json`'s `film_people_path` when present, with the initial selection as fallback. The synopsis snapshot retains associations from the supplement snapshot.

Measured coverage for completed selection `20261114T142005Z`, final content snapshot `20261115T105744Z-c082eb`:

| Metric | Observed result |
| --- | --- |
| Films / unique people targeted | 7,263 / 11,443 |
| Available synopses | 5,983 / 7,263 (82.38%) |
| Available unique biographies | 8,055 / 11,443 (70.39%) |
| Films with at least three biographies | 5,825 / 7,263 (80.20%) |
| Films with synopsis and at least three biographies | 5,329 / 7,263 (73.37%) |
| Supplementary associations / recovered synopses | 916 / 102 |

These are historical measurements, not guaranteed counts for a later IMDb/Wikipedia snapshot. Missing text and insufficient-person cases remain explicit. See [coverage progression](report-assets/evidence-20261115/02-enrichment/coverage_progression.csv), [status counts](report-assets/evidence-20261115/02-enrichment/status_counts.csv) and [remaining cases](report-assets/evidence-20261115/INDEX.md). Reusing a biography increases associations, not the number of unique biography texts.

## Evidence and integrity verification

Start with the [evidence index](report-assets/evidence-20261115/INDEX.md). It links all seven table counts, 78 column profiles, enrichment coverage and missing cases, streaming metrics and source metadata. [SOURCES.csv](report-assets/evidence-20261115/SOURCES.csv) records original files and URLs; [MANIFEST.json](report-assets/evidence-20261115/MANIFEST.json) and [SHA256SUMS.txt](report-assets/evidence-20261115/SHA256SUMS.txt) record exact bytes.

Verify the versioned bundle without Spark, collection or network access:

```bash
docker compose exec -T spark python -m jobs.reporting.verify_evidence
```

With host-side Python available, `python -m jobs.reporting.verify_evidence` also works from the repository root. Expected results for this bundle: `status: passed`, 64 manifest entries, 63 checksum entries, 48 source copies and 18 CSVs. These checks passed on the published fix commit `ca83520` and a fresh clone; after #13 merged into `main` (`1ff95ba`), the evidence subtree, Git attributes and verifier were confirmed identical to that commit. This is an evidence-integrity check, not the complete clean-clone execution of all processing stages.

Git preserves exact evidence bytes with `report-assets/** -text`. Keep original CRLF or LF record terminators as recorded by the hashes. Raw archives, article bodies, Parquet datasets, caches, credentials and checkpoints remain excluded; selected factual report assets are deliberately versioned.

To create a new evidence snapshot from existing validated local outputs:

```bash
docker compose exec -T spark python -m jobs.reporting.prepare_evidence --output report-assets/NEW-EVIDENCE-DIRECTORY
```

Choose a new destination; the exporter refuses to overwrite an existing bundle. See [the evidence guide](docs/EVIDENCE_PACKAGE.md). Keep the existing historical bundle unchanged when reproducing results on newer sources.

## Live Wikipedia → Kafka → Spark

This component is merged on `main`. See [the streaming execution guide](docs/STREAMING.md) for outputs, matching, checkpoints and late-data behavior.

After table preparation and service startup:

```bash
docker compose exec -T spark python -m jobs.create_topics
docker compose exec -T spark python -m unittest tests.test_streaming -v
docker compose exec -T spark python -m jobs.streaming.verify_lookup
```

In terminal 1, start the producer for 20 minutes:

```bash
docker compose exec -T spark python -m jobs.streaming.produce_recent_changes --duration-seconds 1200 --state-name wikipedia-producer
```

In terminal 2, start the consumer for 15 minutes:

```bash
docker compose exec -T spark python -m jobs.streaming.consume_recent_changes --run-id wikipedia-live --duration-seconds 900
```

Use one producer per state name and one consumer per run ID. A repeated run ID resumes its checkpoint; use a new ID for an independent test or changed configuration. Keep checkpoint files under `data/checkpoints/streaming/` with their result markers under `results/streaming/<run_id>/`. Read only committed attempts referenced by `batches/` markers. Session summaries are under `sessions/`; Spark progress metrics are under `progress/`.

The validated session `live-validation-20261114-v2` / `20261114T150245Z` lasted about 900.6 seconds and captured 130 distinct relevant live events in its window. This is a prior measured run, not a promise of identical future traffic. A fresh reproduction must inspect its own metrics. Matching uses exact English sitelinks → Wikidata IMDb IDs → cleaned title/person keys. The implementation uses a five-minute watermark and bounded deduplication; methodological discussion belongs to the members' own report.

## Exploration, P364 and open-question execution

Q1–Q8 functions and original tests retain Brett273's authorship (3ad08a7, with Q1–Q3 from 69da2b4). The integration history also retains his PR branches. Jerry's completion adds the tested runner, P364 collection/preparation, publication guards, output handling, extra tests and open question. See [the implementation and reproduction guide](docs/INDEPENDENT_FINALIZATION.md).

After preparing the seven IMDb tables and starting Compose, use this sequence
on this integration branch:

```bash
docker compose exec -T spark python -m jobs.exploration.select_language_targets
docker compose exec -T spark python -m jobs.exploration.collect_languages
docker compose exec -T spark python -m jobs.exploration.prepare_languages
docker compose exec -T spark python -m jobs.exploration.run --q6-mode final
docker compose exec -T spark python -m jobs.open_question.run
```

Use `collect_languages --run-id RUN_ID` to resume an existing selection; do not
reselect it. Collection is serial, cached and resumable. Spark validates and
publishes distinct `(tconst, language)` pairs with P345/P364 provenance, statuses
and coverage. Q6 rejects incomplete or invalid publications, without an arbitrary
minimum percentage. `jobs.exploration.run --q6-mode pending` can run the other
questions while explicitly marking Q6 and the complete delivery partial.

Every independent query run has a unique output directory and refuses an
existing run ID. Parameters and the source catalog are recorded in run.json.
Outputs have typed Parquet and standard UTF-8 CSV; nested values are JSON strings
in CSV. No results from Brett’s own directories are overwritten.

Recorded full-data results use catalog `20261114T115800Z-6c04bdd1`:

- P364: 14,190 eligible films, 12,862 with declared languages (90.64%),
  14,814 film-language pairs; missing/ambiguous cases remain explicit.
- Q1–Q8 completed in final mode, including 11,815 actors/actresses with at least
  two languages and 4,478 with at least three.
- Open question selected by Jerry: first observed versus later years of
  actor–director collaborations; 18,764 pairs in the paired comparison.
- 68 automated tests passed in an isolated checkout/container. Q1–Q3 have zero
  differences from Brett’s unchanged reference on the same catalog/parameters.
  This does not establish equivalence of Q4–Q8 with his unfinished runner.

[Final query tables and coverage](report-assets/independent-final-20261115/INDEX.md)
and [test/comparison evidence](report-assets/independent-validation-20261115/INDEX.md)
are deliberately versioned factual assets. Verify them without Spark:

```bash
docker compose exec -T spark python -m jobs.reporting.verify_evidence --bundle report-assets/independent-final-20261115
docker compose exec -T spark python -m jobs.reporting.verify_evidence --bundle report-assets/independent-validation-20261115
```

New reproductions use their own run IDs and can package outputs with
`jobs.reporting.export_final_outputs --exploration-run RUN --open-run RUN --output report-assets/NEW-DIRECTORY`.
Read the fallback guide for comparison commands and source/cohort limitations.
The historical preparation/enrichment/streaming proofs remain unchanged.
The report is populated. Declaration review and peer approval before main integration remain the group’s responsibility.

## Final reproduction and collaboration

Follow environment startup, both IMDb preparation stages, enrichment selection and collection, base text preparation, cached-biography supplementation, synopsis recovery, P364 selection/collection/preparation, final exploration, live-streaming validation and the open question. Run one Spark application at a time.

```bash
docker compose exec -T spark python -m unittest discover -s tests -v
docker compose exec -T spark python -m jobs.reporting.verify_evidence
docker compose exec -T spark python -m jobs.reporting.verify_evidence --bundle report-assets/independent-final-20261115
docker compose exec -T spark python -m jobs.reporting.verify_evidence --bundle report-assets/independent-validation-20261115
```

The isolated simulation passed 68 tests, full-catalog exploration and open-question reproduction, 28 output checks, and a new real streaming session of 902.84 seconds with 140 relevant live-window events. It reused prepared datasets and collected sources read-only; it did not download and recollect every source from zero. REPORT.md cites the separately versioned 14 November streaming session of 900.63 seconds and 130 live-window matches. Do not mix the two sessions.

Brett reviews the complete integration PR before approval and merge. Earlier comments or merges are not retroactive formal approvals. Follow the course README for the submission email after checking the final shared commit, report and evidence links.

## Stop and restart

First stop active collection/streaming jobs gracefully. Service restart alone does not resume exec-launched Python processes.

```bash
docker compose stop
docker compose start
```

To remove project containers and the project network while retaining Kafka's named data volume and host files:

```bash
docker compose down
docker compose up -d --wait
```

Adding `--volumes` to `down` deletes Kafka's stored data. It is not part of normal shutdown.

## Diagnostics

```bash
docker version
docker compose ps
docker compose logs --tail 100 kafka
docker compose logs --tail 100 spark
docker compose exec -T spark python -m jobs.smoke_test
```

- Missing Docker engine/pipe: start Docker Desktop and wait for its Linux engine.
- Host port already in use: change the relevant port in `.env`, then recreate the services.
- Image build fails downloading Python packages or JARs: check network access and Docker proxy settings, then retry the build. Existing layers are cached.
- `JAVA_GATEWAY_EXITED` or an out-of-memory container: inspect logs and Docker's memory budget. The default Spark driver heap is 2 GB inside a container limited to 4 GB.
- On Linux, host directories must be writable by container UID 1000; adjust ownership or permissions for your local account as needed. On Windows, keep the project in a Docker-accessible local directory.
- Change `requirements.txt`, Dockerfile or `spark-defaults.conf`: rebuild the Spark image. Normal source edits are immediately visible through the workspace mount.

## AI assistance declaration

OpenAI Codex was used to create the initial project structure and configuration, and tasks repartition between members. No analytical results or methodological reasoning for `REPORT.md` have been generated in this setup.

## Official references

- [Spark 4.0.1 overview and runtime requirements](https://spark.apache.org/docs/4.0.1/)
- [Spark 4.0.1 Kafka integration](https://spark.apache.org/docs/4.0.1/streaming/structured-streaming-kafka-integration.html)
- [Apache Kafka 4.1 Docker documentation](https://kafka.apache.org/41/getting-started/docker/)
