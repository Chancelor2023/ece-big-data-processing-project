FROM python:3.12.12-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYSPARK_PYTHON=python \
    SPARK_LOCAL_IP=127.0.0.1 \
    SPARK_CONF_DIR=/opt/spark-conf \
    PYTHONPATH=/workspace/src:/workspace \
    HOME=/home/app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       openjdk-17-jre-headless ca-certificates procps tini \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 1000 --shell /bin/bash app

COPY requirements.txt /opt/bootstrap/requirements.txt
RUN pip install --no-cache-dir -r /opt/bootstrap/requirements.txt

COPY docker/spark/spark-defaults.conf /opt/spark-conf/spark-defaults.conf
COPY docker/spark/check_jupyter.py docker/spark/start_jupyter.py \
     docker/spark/resolve_connector.py /opt/bootstrap/

RUN mkdir -p /workspace /home/app/.ivy2 \
    && chown -R app:app /workspace /home/app
USER app

# Resolve and cache the Kafka connector during the build, not the first lab run.
# This also checks that the JVM, PySpark and connector can start together.
RUN python /opt/bootstrap/resolve_connector.py

WORKDIR /workspace
EXPOSE 8888 4040
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "/opt/bootstrap/start_jupyter.py"]
