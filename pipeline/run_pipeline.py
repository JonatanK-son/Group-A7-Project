#!/usr/bin/env python3
"""
Standalone pipeline runner — used by the Kubernetes Job and for local testing.
Reads from CSV files mounted at /data (or the local data/ folder).
"""
import os
import sys
import time
from pathlib import Path
import shutil # Added for consistent handling of directory removal

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import platform
import socket
import dask.dataframe as dd
from dask.distributed import Client

from src.config import (
    RAW_OCT, RAW_NOV, get_available_data_paths,
    OUTPUT_DIR, LOGS_DIR,
    PARQUET_VALIDATED, RESULTS_DIR,
)
from src.logger import StructuredLogger
from src.validation import validate_schema, clean_data
from src.ingestion import load_csvs
from src.transformations_dask import (
    compute_revenue_by_category,
    compute_conversion_funnel,
    compute_hourly_activity,
    compute_session_stats,
    compute_top_brands,
)
from src.storage import save_parquet_dask, save_parquet_pandas, load_parquet

log = StructuredLogger("run_pipeline")

def get_scheduler_address():
    """Determine the Dask scheduler address. Defaults to Minikube port-forward if available."""
    addr = os.getenv("DASK_SCHEDULER_ADDRESS")
    if addr:
        return addr
    # Check if we can reach the default port-forwarded address
    try:
        with socket.create_connection(("127.0.0.1", 8786), timeout=0.1):
            return "tcp://127.0.0.1:8786"
    except:
        return None


def run_full_pipeline_remote(sample_mode):
    """This function runs ON THE WORKER to avoid path/os mismatches."""
    from src.config import RAW_OCT, RAW_NOV, PARQUET_VALIDATED, RESULTS_DIR
    from src.ingestion import load_csvs
    from src.validation import clean_data
    from src.storage import save_parquet_dask, save_parquet_pandas
    from src.transformations_dask import (
        compute_revenue_by_category, compute_conversion_funnel,
        compute_hourly_activity, compute_session_stats, compute_top_brands
    )
    import dask.dataframe as dd
    import shutil

    # 1. Ingest
    ddf = load_csvs([RAW_OCT, RAW_NOV])
    if sample_mode:
        ddf = ddf.partitions[:1]
    
    # 2. Clean & Save
    clean_ddf = clean_data(ddf)
    save_parquet_dask(clean_ddf, PARQUET_VALIDATED, partition_on=["event_type"], overwrite=True)
    
    # 3. Analyze (Re-read to ensure clean graph)
    ddf_final = dd.read_parquet(str(PARQUET_VALIDATED))
    results = {
        "revenue":  compute_revenue_by_category(ddf_final),
        "funnel":   compute_conversion_funnel(ddf_final),
        "hourly":   compute_hourly_activity(ddf_final),
        "sessions": compute_session_stats(ddf_final),
        "brands":   compute_top_brands(ddf_final),
    }

    # 4. Save results to mounted output folder
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    save_parquet_pandas(results["revenue"], RESULTS_DIR / "revenue_by_category.parquet")
    save_parquet_pandas(results["funnel"],  RESULTS_DIR / "conversion_funnel.parquet")
    save_parquet_pandas(results["hourly"],  RESULTS_DIR / "hourly_activity.parquet")
    save_parquet_pandas(results["brands"],  RESULTS_DIR / "top_brands.parquet")
    
    session_path = RESULTS_DIR / "session_stats"
    if session_path.exists():
        shutil.rmtree(session_path)
    results["sessions"].to_parquet(str(session_path), write_index=False)

    # Return summary data to client
    return {
        "revenue": results["revenue"],
        "brands":  results["brands"],
        "funnel":  results["funnel"],
        "sessions_sample": results["sessions"].head(10)
    }


def main():
    log.info("pipeline_started")
    pipeline_start = time.time()
    SAMPLE_MODE = True  # Set to False to run on the full dataset

    # Ensure local output directories exist
    for d in [OUTPUT_DIR, LOGS_DIR, PARQUET_VALIDATED, RESULTS_DIR]:
        Path(d).mkdir(parents=True, exist_ok=True)

    scheduler_addr = get_scheduler_address()
    
    if scheduler_addr:
        log.info("remote_execution_mode", scheduler=scheduler_addr)
        with Client(scheduler_addr) as client:
            log.info("dask_client_connected", dashboard=client.dashboard_link)
            results_pkg = client.submit(run_full_pipeline_remote, SAMPLE_MODE).result()
            
            # Unpack results for display logic
            results = {
                "revenue":  results_pkg["revenue"],
                "brands":   results_pkg["brands"],
                "funnel":   results_pkg["funnel"],
                "sessions": results_pkg["sessions_sample"]  # We only bring the sample back
            }
    else:
        log.info("local_execution_mode")
        # Stage 1: Ingest
        with log.timer("ingestion") as t:
            raw_ddf = load_csvs([RAW_OCT, RAW_NOV])
            if SAMPLE_MODE:
                raw_ddf = raw_ddf.partitions[:1]
        
        # Stage 2: Clean & Save
        clean_ddf = clean_data(raw_ddf)
        save_parquet_dask(clean_ddf, PARQUET_VALIDATED, partition_on=["event_type"], overwrite=True)
        
        # Stage 3: Analyze
        ddf = dd.read_parquet(str(PARQUET_VALIDATED))
        results = {
            "revenue":  compute_revenue_by_category(ddf),
            "funnel":   compute_conversion_funnel(ddf),
            "hourly":   compute_hourly_activity(ddf),
            "sessions": compute_session_stats(ddf),
            "brands":   compute_top_brands(ddf),
        }
        
        # Stage 4: Local Save
        save_parquet_pandas(results["revenue"], RESULTS_DIR / "revenue_by_category.parquet")
        save_parquet_pandas(results["funnel"],  RESULTS_DIR / "conversion_funnel.parquet")
        save_parquet_pandas(results["hourly"],  RESULTS_DIR / "hourly_activity.parquet")
        save_parquet_pandas(results["brands"],  RESULTS_DIR / "top_brands.parquet")
        
        session_path = RESULTS_DIR / "session_stats"
        import shutil
        if session_path.exists():
            shutil.rmtree(session_path)
        results["sessions"].to_parquet(str(session_path), write_index=False)
        
        # For display consistency
        results["sessions"] = results["sessions"].head(10)

    # Display results summary
    print("\n" + "="*80)
    print(" PIPELINE RESULTS SUMMARY ".center(80, "="))
    print("="*80)

    print("\n[ Analysis 1: Revenue by Category (Top 10) ]")
    print(results["revenue"].head(10).to_string(index=False))

    print("\n[ Analysis 2: Top Brands by Revenue (Top 10) ]")
    print(results["brands"].head(10).to_string(index=False))

    print("\n[ Analysis 3: Conversion Funnel (Top 10 Categories) ]")
    print(results["funnel"].head(10).to_string(index=False))

    print("\n[ Analysis 4: Session Statistics (Sample) ]")
    print(results["sessions"].to_string(index=False))

    total = round(time.time() - pipeline_start, 1)
    log.info("pipeline_completed", total_s=total)
    print("\n" + "="*80)
    print(f"Pipeline completed in {total}s")
    print("="*80)


if __name__ == "__main__":
    main()
