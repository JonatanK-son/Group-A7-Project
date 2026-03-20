#!/usr/bin/env python3
"""
Standalone Spark pipeline runner mirroring the Dask implementation.
Uses the same Phase 1 (Local Ingest) data and runs Phase 2 computations.
"""
import os
import sys
import time
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pyspark.sql import SparkSession
from src.config import PARQUET_VALIDATED, RESULTS_DIR, OUTPUT_DIR
from src.logger import StructuredLogger
from src.transformations_spark import (
    compute_revenue_by_category,
    compute_conversion_funnel,
    compute_hourly_activity,
    compute_session_stats,
    compute_top_brands,
)

log = StructuredLogger("run_spark_pipeline")


def get_spark_session(remote: bool = False) -> SparkSession:
    """Configures Spark for local or Minikube (K8s) execution."""
    builder = SparkSession.builder.appName("EComPipelineSpark")
    
    # Common performance settings
    builder = (
        builder
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.executor.memoryOverhead", "384")
        .config("spark.executor.instances", "1")
        # Standard Parquet timestamp behavior (now that we use microseconds in validation.py)
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
        .config("spark.hadoop.fs.permissions.umask-mode", "000")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.ui.enabled", "true")
        .config("spark.ui.port", "4040")
        .config("spark.driver.bindAddress", "0.0.0.0")
    )
    
    if remote:
        # Detect if we are already inside K8s (Cluster/Job mode)
        in_k8s = os.getenv("KUBERNETES_SERVICE_HOST") is not None
        # Use the standard Service Shortcut (kubernetes.default.svc) for the master URL
        # instead of the fully qualified .cluster.local which can be flaky on some setups.
        k8s_master = "k8s://https://kubernetes.default.svc" if in_k8s else "k8s://https://192.168.49.2:8443"
        log.info("configuring_spark_k8s", master=k8s_master, in_k8s=in_k8s)
        
        builder = (
            builder
            .master(k8s_master)
            .config("spark.kubernetes.container.image", "ecom-pipeline:latest")
            .config("spark.kubernetes.container.image.pullPolicy", "IfNotPresent")
            .config("spark.kubernetes.authenticate.driver.serviceAccountName", "spark")
            .config("spark.kubernetes.namespace", "default")
            .config("spark.kubernetes.executor.deleteOnTermination", "false")
            
            # This allows the executor pods to see the mounted ./output folder in Minikube
            .config("spark.kubernetes.executor.volumes.hostPath.output.mount.path", "/app/output")
            .config("spark.kubernetes.executor.volumes.hostPath.output.options.path", "/output")
            .config("spark.kubernetes.executor.volumes.hostPath.output.mount.readOnly", "false")
            
            # Python env in the container
            .config("spark.kubernetes.pyspark.pythonVersion", "3")
            .config("spark.pyspark.python", "python3")
            .config("spark.pyspark.driver.python", "python3")
            .config("spark.executorEnv.PYTHONPATH", "/app")
            # Ensure executors can connect back to the driver by using its IP
            .config("spark.driver.host", os.getenv("SPARK_DRIVER_BIND_ADDRESS", "localhost"))
        )
        
        # If running from host, we need to allow executors to call back locally (Client mode)
        if not in_k8s:
             # Most likely IP for Minikube host
             builder = builder.config("spark.driver.host", "192.168.49.1")
             builder = builder.config("spark.driver.bindAddress", "0.0.0.0")
    else:
        builder = builder.master("local[*]")
        
    return builder.getOrCreate()


def run_analysis_spark(spark: SparkSession, validated_path: str):
    """Executes the mirrored analyses in Spark and saves to ./output/results_spark/."""
    results_spark = RESULTS_DIR.parent / "results_spark"
    results_spark.mkdir(parents=True, exist_ok=True)
    
    log.info("spark_reading_input", path=validated_path)
    df = spark.read.parquet(validated_path)
    
    # ── Debugging: verify what Spark sees ─────────────────────────────────────
    try:
        row_count = df.count()
        schema = df.schema.simpleString()
        log.info("spark_input_verified", rows=row_count, schema=schema)
        
        # High-level check for columns
        cols = df.columns
        if "event_type" not in cols:
            log.warning("spark_missing_partition_col", columns=cols)
            # If partitioning wasn't auto-detected, we might need to cast or re-read
        
        if row_count == 0:
            log.error("spark_input_empty", path=validated_path)
    except Exception as e:
        log.error("spark_input_check_failed", error=str(e))
        raise e

    # ── Analysis Execution ────────────────────────────────────────────────────
    # Write directly to the shared mount so executors and driver see the same files.
    # We use a subfolder in the results directory.
    
    log.info("spark_revenue_by_category")
    revenue = compute_revenue_by_category(df)
    revenue.write.parquet(str(results_spark / "revenue_by_category.parquet"), mode="overwrite")
    
    log.info("spark_conversion_funnel")
    funnel = compute_conversion_funnel(df)
    funnel.write.parquet(str(results_spark / "conversion_funnel.parquet"), mode="overwrite")
    
    log.info("spark_hourly_activity")
    hourly = compute_hourly_activity(df)
    hourly.write.parquet(str(results_spark / "hourly_activity.parquet"), mode="overwrite")
    
    log.info("spark_session_stats")
    sessions = compute_session_stats(df)
    sessions.write.parquet(str(results_spark / "session_stats.parquet"), mode="overwrite")
    
    log.info("spark_top_brands")
    brands = compute_top_brands(df)
    brands.write.parquet(str(results_spark / "top_brands.parquet"), mode="overwrite")

    log.info("spark_analysis_complete")


def main():
    log.info("spark_pipeline_started")
    pipeline_start = time.time()
    
    # Detect if we are already inside K8s (Cluster/Job mode)
    in_k8s = os.getenv("KUBERNETES_SERVICE_HOST") is not None
    
    # Use remote if in k8s, otherwise local for host debugging
    REMOTE = in_k8s 
    spark = get_spark_session(remote=REMOTE)
    
    try:
        # If in K8s (REMOTE), executors see the path as /app/output/parquet/validated (Linux path in pod)
        validated_path = "/app/output/parquet/validated" if REMOTE else str(PARQUET_VALIDATED)
        
        run_analysis_spark(spark, validated_path)
        
        # ── Display results summary (mirroring Dask) ──────────────────────────
        results_spark = RESULTS_DIR.parent / "results_spark"
        print("\n" + "=" * 80)
        print(" SPARK PIPELINE RESULTS SUMMARY ".center(80, "="))
        print("=" * 80)

        # Analysis names and matching titles from the Dask pipeline
        summaries = [
            ("revenue_by_category", "Analysis 1: Revenue by Category (Top 10)"),
            ("top_brands",          "Analysis 2: Top Brands by Revenue (Top 10)"),
            ("conversion_funnel",   "Analysis 3: Conversion Funnel (Top 10 Categories)"),
            ("session_stats",       "Analysis 4: Session Statistics (Sample)")
        ]

        for parquet_name, title in summaries:
            path = results_spark / f"{parquet_name}.parquet"
            try:
                # Load back with Spark and sample to Pandas for pretty display
                # This is much more robust than pd.read_parquet on Spark-managed directories
                res_df = spark.read.parquet(str(path))
                pdf = res_df.limit(10).toPandas()
                
                print(f"\n[ {title} ]")
                if not pdf.empty:
                    print(pdf.to_string(index=False))
                else:
                    print("Empty DataFrame. Verify that upstream data and transformations are working.")
                sys.stdout.flush()
            except Exception as e:
                log.warning("summary_load_failed", analysis=parquet_name, error=str(e), path=str(path))

        total = round(time.time() - pipeline_start, 1)
        log.info("spark_pipeline_completed", total_s=total)
        print(f"\nSpark Pipeline completed in {total}s")
        print("Spark run completed.")
        sys.stdout.flush()
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
