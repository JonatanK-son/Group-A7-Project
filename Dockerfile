
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

# Install Official Apache Spark 3.5.0
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

RUN curl -sL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    | tar -xz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME}

# Use the official entrypoint script included in the Spark distribution itself
# This ensures perfect compatibility and avoids Windows/Linux line ending issues.
RUN cp ${SPARK_HOME}/kubernetes/dockerfiles/spark/entrypoint.sh /opt/entrypoint.sh && \
    chmod +x /opt/entrypoint.sh

# Add python -> python3 symlink
RUN ln -sf /usr/bin/python3 /usr/bin/python

# Install requirements (pyspark 3.5.0 and dask 2026.x)
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy application sources
COPY src/       src/
COPY pipeline/  pipeline/

# Ensure output and log directories exist and are writable
RUN mkdir -p /output/parquet/validated /app/output/logs /app/output/results /app/output/results_spark && \
    chmod -R 777 /app/output /output

ENV PYTHONPATH=/app

# Default command for the runner
CMD ["python3", "pipeline/run_pipeline.py"]

# Official Spark entrypoint for Spark-on-Kubernetes roles (driver/executor)
ENTRYPOINT ["/opt/entrypoint.sh"]
