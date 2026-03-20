# Stage 1: Get Spark binaries from the official image
FROM apache/spark:3.5.0 AS spark-dist

# Stage 2: Build the actual image on a modern Python base (to support Dask 2024+)
FROM python:3.10-slim

USER root
WORKDIR /app

# Install Java and other tools needed for Spark & Dask
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jre-headless \
    procps \
    tini \
    curl \
    gosu \
    && rm -rf /var/lib/apt/lists/*

# Copy Spark from the first stage
COPY --from=spark-dist /opt/spark /opt/spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Ensure we have the same entrypoint if needed
COPY entrypoint.sh /opt/entrypoint.sh
RUN chmod +x /opt/entrypoint.sh

# Add python -> python3 symlink for compatibility
RUN ln -sf /usr/bin/python3 /usr/bin/python

# Install requirements (pyspark 3.5.0 and dask 2026.x match local)
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY src/       src/
COPY pipeline/  pipeline/

# Ensure results and logs directories are writable
RUN mkdir -p /output/parquet/validated /app/output/logs /app/output/results /app/output/results_spark && \
    chmod -R 777 /app/output /output

ENV PYTHONPATH=/app

# Use the same command as original but allow for entrypoint if needed
CMD ["python3", "pipeline/run_pipeline.py"]
ENTRYPOINT ["/opt/entrypoint.sh"]
