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
        .config("spark.sql.shuffle.partitions", "32")
        .config("spark.driver.memory", "1024m")
        .config("spark.executor.memory", "1536m")
        .config("spark.kubernetes.memoryOverheadFactor", "0.2")
        .config("spark.executor.instances", "2")
        .config("spark.driver.port", "7077")
        .config("spark.blockManager.port", "7078")
        .config("spark.sql.parquet.nanosAsLong", "true")
        .config("spark.sql.legacy.parquet.nanosAsLong", "true")
        .config("spark.sql.parquet.enableVectorizedReader", "true")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
        .config("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
        .config("spark.sql.parquet.mergeSchema", "false")
        .config("spark.hadoop.fs.permissions.umask-mode", "000")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    )
    
    if remote:
        # Detect if we are already inside K8s (Cluster/Job mode)
        in_k8s = os.getenv("KUBERNETES_SERVICE_HOST") is not None
        k8s_master = "k8s://https://kubernetes.default.svc.cluster.local" if in_k8s else "k8s://https://192.168.49.2:8443"
        log.info("configuring_spark_k8s", master=k8s_master, in_k8s=in_k8s)
        
        builder = (
            builder
            .master(k8s_master)
            .config("spark.kubernetes.container.image", "ecom-pipeline:dev-v6")
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
            .config("spark.pyspark.python", "/usr/local/bin/python3")
            .config("spark.pyspark.driver.python", "/usr/local/bin/python3")
            .config("spark.executorEnv.PYTHONPATH", "/app")
            .config("spark.executorEnv.PYSPARK_PYTHON", "/usr/local/bin/python3")
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
    
    df = spark.read.parquet(validated_path)
    
    # Global correction: Convert nanoseconds BigInt to Spark Timestamp
    from pyspark.sql import functions as F
    df = df.withColumn("event_time", (F.col("event_time") / 1_000_000_000).cast("timestamp"))
    
    # ── 1. Revenue by Category ────────────────────────────────────────────────
    # Use a local directory for writing to avoid chmod issues on mounted volumes
    temp_results = Path("/tmp/results_spark")
    temp_results.mkdir(parents=True, exist_ok=True)
    
    log.info("spark_revenue_by_category")
    revenue = compute_revenue_by_category(df)
    revenue.write.parquet(str(temp_results / "revenue_by_category.parquet"), mode="overwrite")
    
    log.info("spark_conversion_funnel")
    funnel = compute_conversion_funnel(df)
    funnel.write.parquet(str(temp_results / "conversion_funnel.parquet"), mode="overwrite")
    
    log.info("spark_hourly_activity")
    hourly = compute_hourly_activity(df)
    hourly.write.parquet(str(temp_results / "hourly_activity.parquet"), mode="overwrite")
    
    log.info("spark_session_stats")
    sessions = compute_session_stats(df)
    sessions.write.parquet(str(temp_results / "session_stats.parquet"), mode="overwrite")
    
    log.info("spark_top_brands")
    brands = compute_top_brands(df)
    brands.write.parquet(str(temp_results / "top_brands.parquet"), mode="overwrite")

    # Final move to the actual output directory
    import shutil
    log.info("spark_persisting_results", src=str(temp_results), dst=str(results_spark))
    
    # On some filesystems (like Minikube host mounts), rmtree fails with locks.
    # We use dirs_exist_ok=True (Python 3.8+) to copy over existing files.
    try:
        shutil.copytree(temp_results, results_spark, dirs_exist_ok=True)
        log.info("spark_results_persisted", path=str(results_spark))
    except Exception as e:
        log.warning("spark_copy_partial_fail", error=str(e))
        # Often occurs on Windows mounts even if files were copied
        if (results_spark / "session_stats.parquet").exists():
            log.info("spark_results_verified_at_destination")
        else:
            raise e
    
    log.info("spark_analysis_complete")


def main():
    log.info("spark_pipeline_started")
    pipeline_start = time.time()
    
    # Increase log level temporarily for diagnostics
    # sc.setLogLevel is only available after context is started, using conf instead
    pass
    
    # Detect if we are already inside K8s (Cluster/Job mode)
    in_k8s = os.getenv("KUBERNETES_SERVICE_HOST") is not None
    
    # Use remote if in k8s, otherwise local for host debugging
    REMOTE = in_k8s 
    spark = get_spark_session(remote=REMOTE)
    
    try:
        # If in K8s (REMOTE), executors see the path as /app/output/parquet/validated (Linux path in pod)
        validated_path = "/app/output/parquet/validated" if REMOTE else str(PARQUET_VALIDATED)
        
        run_analysis_spark(spark, validated_path)
        
        # Display sample for verification
        results_spark = RESULTS_DIR.parent / "results_spark"
        print("\n" + "=" * 80)
        print(" SPARK PIPELINE RESULTS SUMMARY ".center(80, "="))
        print("=" * 80)
        
        sessions_sample = pd.read_parquet(results_spark / "session_stats.parquet").head(10)
        print("\n[ Session Statistics (Sample) ]")
        print(sessions_sample.to_string(index=False))
        
        total = round(time.time() - pipeline_start, 1)
        log.info("spark_pipeline_completed", total_s=total)
        print(f"\nSpark Pipeline completed in {total}s")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
