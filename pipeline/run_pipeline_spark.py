#!/usr/bin/env python3
"""
Standalone Spark pipeline runner.
Reads from CSV files mounted at /data (or the local data/ folder).
"""
import os
import sys
import time
from pathlib import Path
import shutil

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.config import RAW_OCT, RAW_NOV, OUTPUT_DIR, LOGS_DIR, RESULTS_DIR
from src.logger import StructuredLogger

from src.transformations_spark import (
    get_spark_session, get_spark_dashboard_url, load_csv_spark,
    compute_revenue_by_category_spark, compute_conversion_funnel_spark,
    compute_window_rank_spark, compute_hourly_activity_spark
)

log = StructuredLogger("run_pipeline_spark")

def run_full_pipeline_spark(sample_mode):
    spark = get_spark_session()
    dashboard = get_spark_dashboard_url(spark)
    log.info("spark_session_connected", dashboard=dashboard)
    
    # 1. Ingest
    paths = [str(RAW_OCT), str(RAW_NOV)]
    df = load_csv_spark(spark, paths)
    if sample_mode:
        df = df.sample(fraction=0.01) # Approx sample

    # 3. Analyze    
    revenue_df = compute_revenue_by_category_spark(df)
    funnel_df = compute_conversion_funnel_spark(df)
    window_df = compute_window_rank_spark(df)
    hourly_df = compute_hourly_activity_spark(df)

    # 4. Save results (Spark writes Parquet natively)
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    
    def save_spark(sdf, name):
        p = str(RESULTS_DIR / name)
        if Path(p).exists():
            shutil.rmtree(p)
        sdf.write.parquet(p)

    save_spark(revenue_df, "revenue_by_category.parquet")
    save_spark(funnel_df, "conversion_funnel.parquet")
    save_spark(window_df, "window_rank.parquet")
    save_spark(hourly_df, "hourly_activity.parquet")
    
    results = {
        "revenue": revenue_df.limit(10).toPandas(),
        "funnel": funnel_df.limit(10).toPandas(),
        "window": window_df.limit(10).toPandas(),
        "hourly": hourly_df.limit(10).toPandas()
    }
    
    spark.stop()
    return results

def main():
    log.info("pipeline_started", engine="spark")
    pipeline_start = time.time()
    SAMPLE_MODE = True  # Set to False to run on the full dataset

    for d in [OUTPUT_DIR, LOGS_DIR, RESULTS_DIR]:
        Path(d).mkdir(parents=True, exist_ok=True)

    results = run_full_pipeline_spark(SAMPLE_MODE)
    
    print("\n" + "="*80)
    print(" PIPELINE RESULTS SUMMARY (SPARK) ".center(80, "="))
    print("="*80)

    print("\n[ Analysis 1: Revenue by Category (Top 10) ]")
    print(results["revenue"].to_string(index=False))

    print("\n[ Analysis 2: Conversion Funnel (Top 10 Categories) ]")
    print(results["funnel"].to_string(index=False))

    print("\n[ Analysis 3: Window Rank (Top 10) ]")
    print(results["window"].to_string(index=False))

    print("\n[ Analysis 4: Hourly Activity (Sample) ]")
    print(results["hourly"].to_string(index=False))

    total = round(time.time() - pipeline_start, 1)
    log.info("pipeline_completed", total_s=total)
    print("\n" + "="*80)
    print(f"Pipeline completed in {total}s")
    print("="*80)

if __name__ == "__main__":
    main()
