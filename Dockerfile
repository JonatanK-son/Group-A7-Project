
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
    && rm -rf /var/lib/apt/lists/*

# Create spark user/group that the official entrypoint expects
RUN groupadd -g 1000 spark && \
    useradd -u 1000 -g spark -d /app -s /bin/bash spark && \
    chown -R spark:spark /app

# Install Official Apache Spark 3.5.0
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Copy Spark from the official image instead of downloading it slowly
COPY --from=spark-dist /opt/spark /opt/spark

# Use the official entrypoint script included in the Spark distribution itself
# This ensures perfect compatibility and avoids Windows/Linux line ending issues.
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
