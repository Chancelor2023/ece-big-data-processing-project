# Project execution and operations guide

## Contributors

| Contributor | GitHub username |
| --- | --- | --- |
| Jerry | Chancelor2023 |
| Brett | Brett273 |

## Environment scope

This initial setup provides Kafka, Spark batch processing, Spark Structured Streaming, JupyterLab, persistent local files, explicit topic creation and a synthetic integration test. IMDb preparation, analytical answers, enrichment and live Wikimedia ingestion are not implemented yet.

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

Run every command below from the `imdb-big-data` directory.

## First startup

PowerShell:

```powershell
Copy-Item .env.example .env
```

Linux / macOS / Git Bash:

```bash
cp .env.example .env
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

Shared code lives in `src/imdb_pipeline`, which is on the container's Python path. New runnable jobs can be placed in the corresponding `jobs` section and invoked as modules, for example:

```bash
docker compose exec -T spark python -m jobs.preparation.your_module
```

That example is a naming convention for a future module, not a currently implemented command. Use the shared Spark factory so that master, shuffle partitions and UTC handling remain consistent. Plain `SparkSession.builder` also loads the connector through `spark-defaults.conf`, but does not read the project's `SPARK_MASTER` override unless you set it yourself.

Only one Spark application binds to UI port 4040 at a time. Stop an unused notebook Spark session before launching another job if you need its UI at that address. Additional Spark UIs can use other container ports that are not published in this initial setup.

## Persistence and inputs

The project directory is mounted at `/workspace`. Files written under `/workspace/data`, `/workspace/results` and `/workspace/notebooks` remain on the host after container recreation. Kafka logs use the named `kafka-data` volume.

Use `data/raw` for downloaded IMDb source files, `data/clean` for the shared cleaned-table contract, `data/enrichment` for collected/enriched data and `data/checkpoints` for separate streaming query checkpoints. Do not reuse one checkpoint directory for unrelated streaming queries.

`jobs.create_topics` creates `wikimedia.recentchange` with three partitions and replication factor one. It can be rerun without deleting the topic or resetting its offsets.

## Stop and restart

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
