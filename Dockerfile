
# Stage 1: Get Spark binaries from the official image
FROM apache/spark:3.5.0 AS spark-dist

# Stage 2: Build the actual image on a modern Python base (to support Dask 2024+)
FROM python:3.12-slim

USER root
WORKDIR /app

# Install Java and other tools needed for Spark & Dask
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jre-headless \
    procps \
    tini \
    curl \
    gosu \
    dos2unix \
    && rm -rf /var/lib/apt/lists/*
    
# Add spark group/user (standard UID 185 for Spark)
RUN groupadd -g 185 spark && \
    useradd -u 185 -g 185 -d /app -s /bin/bash spark

# Copy Spark from the first stage
COPY --from=spark-dist /opt/spark /opt/spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Ensure we have the same entrypoint if needed
COPY entrypoint.sh /opt/entrypoint.sh
RUN dos2unix /opt/entrypoint.sh && chmod +x /opt/entrypoint.sh

# Add python -> python3 symlink for compatibility
RUN ln -sf /usr/bin/python3 /usr/bin/python

# Install requirements (pyspark 3.5.0 and dask 2026.x match local)
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY src/       src/
COPY pipeline/  pipeline/

# Ensure results and logs directories are writable and owned by spark
RUN mkdir -p /output/parquet/validated /app/output/logs /app/output/results /app/output/results_spark && \
    chown -R spark:spark /app /output && \
    chmod -R 777 /app/output /output

USER spark
ENV PYTHONPATH=/app

# Use the same command as original but allow for entrypoint if needed
CMD ["python3", "pipeline/run_pipeline.py"]
ENTRYPOINT ["/opt/entrypoint.sh"]
