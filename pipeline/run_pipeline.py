#!/usr/bin/env python3
"""
Standalone pipeline runner — used by the Kubernetes Job and for local testing.
Reads from CSV files mounted at /data (or the local data/ folder).
"""
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dask.distributed import Client

from src.config import (
    RAW_OCT, RAW_NOV, OUTPUT_DIR, LOGS_DIR,
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

SCHEDULER_ADDRESS = os.getenv("DASK_SCHEDULER_ADDRESS", None)

<<<<<<< Updated upstream
for d in [OUTPUT_DIR, LOGS_DIR, PARQUET_VALIDATED, RESULTS_DIR]:
    Path(d).mkdir(parents=True, exist_ok=True)
=======

def run_full_pipeline_remote(sample_mode):
    """
    This function runs ON THE WORKER to avoid path/os mismatches.
    
    ⚠️ OPTIMIZED FOR CLUSTER STABILITY:
    - Persists cleaned data to avoid recomputation
    - Rechunks before heavy shuffle operations (like session stats)
    - Runs analyses sequentially (not all at once) to manage memory
    - Computes expensive results in order of memory impact
    """
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
    from src.logger import StructuredLogger
    
    log = StructuredLogger("run_pipeline_remote")

    # 1. Ingest
    log.info("stage_ingest_start")
    ddf = load_csvs([RAW_OCT, RAW_NOV])
    if sample_mode:
        ddf = ddf.partitions[:1]
    
    # 2. Clean & Save
    log.info("stage_clean_start")
    clean_ddf = clean_data(ddf)
    save_parquet_dask(clean_ddf, PARQUET_VALIDATED, partition_on=["event_type"], overwrite=True)
    
    # 3. Analyze — Re-read to ensure clean graph
    log.info("stage_analysis_start", strategy="sequential_with_persist")
    ddf_final = dd.read_parquet(str(PARQUET_VALIDATED))
    
    # CRITICAL: Persist the base dataset to avoid recomputation across analyses
    # This caches the in-memory partitions on all workers, making downstream
    # operations much faster and more memory-efficient.
    log.info("persisting_base_dataset", npartitions=ddf_final.npartitions)
    ddf_final = ddf_final.persist()
    
    # Run analyses sequentially, smallest-first, to avoid peak memory spikes.
    # Each compute() will use cached partitions from persist() above.
    results = {}
    
    try:
        log.info("analysis_1_revenue_start")
        results["revenue"] = compute_revenue_by_category(ddf_final)
        log.info("analysis_1_revenue_done", rows=len(results["revenue"]))
        
        log.info("analysis_2_brands_start")
        results["brands"] = compute_top_brands(ddf_final)
        log.info("analysis_2_brands_done", rows=len(results["brands"]))
        
        log.info("analysis_3_funnel_start")
        results["funnel"] = compute_conversion_funnel(ddf_final)
        log.info("analysis_3_funnel_done", rows=len(results["funnel"]))
        
        log.info("analysis_4_hourly_start")
        results["hourly"] = compute_hourly_activity(ddf_final)
        log.info("analysis_4_hourly_done", rows=len(results["hourly"]))
        
        # Session stats is the heaviest operation (crosses all partitions).
        # Run it last, after lighter operations so memory is freed up.
        log.info("analysis_5_sessions_start")
        results["sessions"] = compute_session_stats(ddf_final)
        log.info("analysis_5_sessions_done", rows=len(results["sessions"]))
        
    except Exception as e:
        log.error("analysis_failed", error=str(e))
        raise

    # 4. Save results to mounted output folder
    log.info("saving_results_start")
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    save_parquet_pandas(results["revenue"], RESULTS_DIR / "revenue_by_category.parquet")
    save_parquet_pandas(results["funnel"],  RESULTS_DIR / "conversion_funnel.parquet")
    save_parquet_pandas(results["hourly"],  RESULTS_DIR / "hourly_activity.parquet")
    save_parquet_pandas(results["brands"],  RESULTS_DIR / "top_brands.parquet")
    
    session_path = RESULTS_DIR / "session_stats"
    if session_path.exists():
        shutil.rmtree(session_path)
    results["sessions"].to_parquet(str(session_path), index=False)
    log.info("saving_results_done")

    # Return summary data to client
    return {
        "revenue": results["revenue"],
        "brands":  results["brands"],
        "funnel":  results["funnel"],
        "sessions_sample": results["sessions"].head(10)
    }
>>>>>>> Stashed changes


def main():
    log.info("pipeline_started")
    timings: dict[str, float] = {}
    pipeline_start = time.time()

    # ── Stage 1: Ingest ───────────────────────────────────────────────────────
    with log.timer("ingestion") as t:
        raw_ddf = load_csvs([RAW_OCT, RAW_NOV])
    timings["ingestion"] = t.elapsed

<<<<<<< Updated upstream
    schema = validate_schema(raw_ddf)
    if not schema["valid"]:
        log.error("schema_invalid", missing=schema["missing"])
        sys.exit(1)

    with log.timer("cleaning") as t:
        clean_ddf = clean_data(raw_ddf)
    timings["cleaning"] = t.elapsed
=======
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
        log.info("stage_clean_start")
        clean_ddf = clean_data(raw_ddf)
        save_parquet_dask(clean_ddf, PARQUET_VALIDATED, partition_on=["event_type"], overwrite=True)
        
        # Stage 3: Analyze with memory optimization
        log.info("stage_analysis_start", strategy="sequential_with_persist")
        ddf = dd.read_parquet(str(PARQUET_VALIDATED))
        
        # CRITICAL: Persist the base dataset to avoid recomputation
        log.info("persisting_base_dataset", npartitions=ddf.npartitions)
        ddf = ddf.persist()
        
        # Run analyses sequentially, smallest-first
        results = {}
        try:
            log.info("analysis_1_revenue_start")
            results["revenue"] = compute_revenue_by_category(ddf)
            log.info("analysis_1_revenue_done", rows=len(results["revenue"]))
            
            log.info("analysis_2_brands_start")
            results["brands"] = compute_top_brands(ddf)
            log.info("analysis_2_brands_done", rows=len(results["brands"]))
            
            log.info("analysis_3_funnel_start")
            results["funnel"] = compute_conversion_funnel(ddf)
            log.info("analysis_3_funnel_done", rows=len(results["funnel"]))
            
            log.info("analysis_4_hourly_start")
            results["hourly"] = compute_hourly_activity(ddf)
            log.info("analysis_4_hourly_done", rows=len(results["hourly"]))
            
            log.info("analysis_5_sessions_start")
            results["sessions"] = compute_session_stats(ddf)
            log.info("analysis_5_sessions_done", rows=len(results["sessions"]))
            
        except Exception as e:
            log.error("analysis_failed", error=str(e))
            raise
        
        # Stage 4: Local Save
        log.info("saving_results_start")
        save_parquet_pandas(results["revenue"], RESULTS_DIR / "revenue_by_category.parquet")
        save_parquet_pandas(results["funnel"],  RESULTS_DIR / "conversion_funnel.parquet")
        save_parquet_pandas(results["hourly"],  RESULTS_DIR / "hourly_activity.parquet")
        save_parquet_pandas(results["brands"],  RESULTS_DIR / "top_brands.parquet")
        
        session_path = RESULTS_DIR / "session_stats"
        import shutil
        if session_path.exists():
            shutil.rmtree(session_path)
        results["sessions"].to_parquet(str(session_path), index=False)
        log.info("saving_results_done")
        
        # For display consistency
        results["sessions"] = results["sessions"].head(10)
>>>>>>> Stashed changes

    # ── Stage 2: Save validated Parquet ───────────────────────────────────────
    with log.timer("save_validated") as t:
        save_parquet_dask(clean_ddf, PARQUET_VALIDATED, partition_on=["event_type"])
    timings["save_validated"] = t.elapsed

    validated_ddf = load_parquet(PARQUET_VALIDATED)

    # ── Stage 3: Dask analyses ────────────────────────────────────────────────
    client_kwargs = {"address": SCHEDULER_ADDRESS} if SCHEDULER_ADDRESS else {}

    with Client(**client_kwargs) as client:
        log.info("dask_client_connected", dashboard=getattr(client, "dashboard_link", "N/A"))

        analyses = [
            ("revenue",  compute_revenue_by_category, [validated_ddf]),
            ("funnel",   compute_conversion_funnel,   [validated_ddf]),
            ("hourly",   compute_hourly_activity,     [validated_ddf]),
            ("sessions", compute_session_stats,       [validated_ddf]),
            ("brands",   compute_top_brands,          [validated_ddf]),
        ]
        results = {}
        for name, fn, args in analyses:
            with log.timer(f"dask_{name}") as t:
                results[name] = fn(*args)
            timings[f"dask_{name}"] = t.elapsed

    # ── Stage 4: Persist results ──────────────────────────────────────────────
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    save_parquet_pandas(results["revenue"],                RESULTS_DIR / "revenue_by_category.parquet")
    save_parquet_pandas(results["funnel"],                 RESULTS_DIR / "conversion_funnel.parquet")
    save_parquet_pandas(results["hourly"],                 RESULTS_DIR / "hourly_activity.parquet")
    save_parquet_pandas(results["brands"],                 RESULTS_DIR / "top_brands.parquet")
    save_parquet_pandas(results["sessions"].head(500_000), RESULTS_DIR / "session_stats.parquet")

    total = round(time.time() - pipeline_start, 1)
    log.info("pipeline_completed", total_s=total, stage_times=timings)
    print(f"\nPipeline completed in {total}s")
    for stage, s in timings.items():
        print(f"  {stage:<20} {s:.1f}s")


if __name__ == "__main__":
    main()
