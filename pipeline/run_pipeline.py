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

SCHEDULER_ADDRESS = os.getenv("DASK_SCHEDULER_ADDRESS", None)

for d in [OUTPUT_DIR, LOGS_DIR, PARQUET_VALIDATED, RESULTS_DIR]:
    Path(d).mkdir(parents=True, exist_ok=True)


def main():
    log.info("pipeline_started")
    timings: dict[str, float] = {}
    pipeline_start = time.time()

    # Stage 1: Ingest ───────────────────────────────────────────────────────
    SAMPLE_MODE = os.getenv("SAMPLE_MODE", "True").lower() == "true"

    available_paths = get_available_data_paths()
    if not available_paths:
        log.error("no_data_found", message="No CSV files found in data/ directory. Did you run 'just data-sample'?")
        sys.exit(1)

    with log.timer("ingestion") as t:
        raw_ddf = load_csvs(available_paths)
        if SAMPLE_MODE:
            # If the data is already small (e.g. 100MB sample), Dask might have only 1 partition
            num_available = raw_ddf.npartitions
            log.info("sampling_enabled", available_partitions=num_available)
            raw_ddf = raw_ddf.partitions[:1]
    timings["ingestion"] = t.elapsed

    schema = validate_schema(raw_ddf)
    if not schema["valid"]:
        log.error("schema_invalid", missing=schema["missing"])
        sys.exit(1)

    with log.timer("cleaning") as t:
        clean_ddf = clean_data(raw_ddf)
    timings["cleaning"] = t.elapsed

    # ── Stage 2: Save validated Parquet ───────────────────────────────────────
    with log.timer("save_validated") as t:
        save_parquet_dask(clean_ddf, PARQUET_VALIDATED, partition_on=["event_type"], overwrite=True)
    timings["save_validated"] = t.elapsed

    # ── Stage 3: Dask analyses ────────────────────────────────────────────────
    client_kwargs = {"address": SCHEDULER_ADDRESS} if SCHEDULER_ADDRESS else {}

    with Client(**client_kwargs) as client:
        log.info("dask_client_connected", dashboard=getattr(client, "dashboard_link", "N/A"))

        def run_all_analyses():
            """This function runs entirely on the cluster to avoid path mismatches."""
            import dask.dataframe as dd
            from src.config import PARQUET_VALIDATED
            from src.transformations_dask import (
                compute_revenue_by_category,
                compute_conversion_funnel,
                compute_hourly_activity,
                compute_session_stats,
                compute_top_brands,
            )
            
            # Discovery happens on the cluster using Linux paths
            ddf = dd.read_parquet(str(PARQUET_VALIDATED))
            
            res = {}
            res["revenue"]  = compute_revenue_by_category(ddf)
            res["funnel"]   = compute_conversion_funnel(ddf)
            res["hourly"]   = compute_hourly_activity(ddf)
            res["sessions"] = compute_session_stats(ddf)
            res["brands"]   = compute_top_brands(ddf)
            return res

        if SCHEDULER_ADDRESS:
            log.info("remote_analysis_started")
            results = client.submit(run_all_analyses).result()
            log.info("remote_analysis_done")
        else:
            # Local Mode: just call the function directly
            results = run_all_analyses()


    # ── Stage 4: Persist & Display results ────────────────────────────────────
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    save_parquet_pandas(results["revenue"],                RESULTS_DIR / "revenue_by_category.parquet")
    save_parquet_pandas(results["funnel"],                 RESULTS_DIR / "conversion_funnel.parquet")
    save_parquet_pandas(results["hourly"],                 RESULTS_DIR / "hourly_activity.parquet")
    save_parquet_pandas(results["brands"],                 RESULTS_DIR / "top_brands.parquet")
    save_parquet_pandas(results["sessions"].head(500_000), RESULTS_DIR / "session_stats.parquet")

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
    print(results["sessions"].head(5).to_string(index=False))

    total = round(time.time() - pipeline_start, 1)
    log.info("pipeline_completed", total_s=total, stage_times=timings)
    print("\n" + "="*80)
    print(f"Pipeline completed in {total}s")
    for stage, s in timings.items():
        print(f"  {stage:<20} {s:.1f}s")
    print("="*80)


if __name__ == "__main__":
    main()
