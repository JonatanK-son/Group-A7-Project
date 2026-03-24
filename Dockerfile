
# Stage 1: Get Spark binaries from the official image
FROM apache/spark:3.5.0 AS spark-dist

# Stage 2: Build the actual image on a modern Python base (to support Dask 2024+)
FROM python:3.12-slim

USER root
WORKDIR /app

# Install Java 21 and essential tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jre-headless \
    procps \
    tini \
    curl \
    gosu \
    && groupadd -r spark --gid=185 \
    && useradd -r -g spark --uid=185 spark \
    && rm -rf /var/lib/apt/lists/*

# Install Official Apache Spark 3.5.0 from the spark-dist stage
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

COPY --from=spark-dist /opt/spark /opt/spark
COPY --from=spark-dist /opt/entrypoint.sh /opt/entrypoint.sh
RUN chmod +x /opt/entrypoint.sh

# Add python -> python3 symlink
RUN ln -sf /usr/bin/python3 /usr/bin/python

# Install python dependencies via uv
COPY pyproject.toml uv.lock ./
RUN pip3 install uv && \
    uv sync --frozen --no-dev

# Use the virtual environment for all subsequent commands
ENV PATH="/app/.venv/bin:$PATH"

# Copy application sources
COPY src/       src/
COPY pipeline/  pipeline/

# Ensure output and log directories exist and are writable
RUN mkdir -p /output/parquet/validated /app/output/logs /app/output/results /app/output/results_spark && \
    chmod -R 777 /app/output /output && \
    chown -R spark:spark /app/output /output

ENV PYTHONPATH=/app

# Default command for the runner
CMD ["python3", "pipeline/run_pipeline.py"]

# Official Spark entrypoint for Spark-on-Kubernetes roles (driver/executor)
ENTRYPOINT ["/opt/entrypoint.sh"]
