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

SCHEDULER_ADDRESS = os.getenv("DASK_SCHEDULER_ADDRESS", "tcp://localhost:8786")

for d in [OUTPUT_DIR, LOGS_DIR, PARQUET_VALIDATED, RESULTS_DIR]:
    Path(d).mkdir(parents=True, exist_ok=True)


def main():
    log.info("pipeline_started")
    timings: dict[str, float] = {}
    pipeline_start = time.time()

    # Stage 1: Ingest ───────────────────────────────────────────────────────
    SAMPLE_MODE = False  # Set to False to run on the full dataset

    with log.timer("ingestion") as t:
        raw_ddf = load_csvs([RAW_OCT, RAW_NOV])
        if SAMPLE_MODE:
            log.info("sampling_enabled", message="Processing only the first 128MB partition")
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

        # Discovery task to resolve paths on the cluster
        def get_remote_ddf():
            import dask.dataframe as dd
            from src.config import PARQUET_VALIDATED
            # We use the version of PARQUET_VALIDATED defined on the cluster (Linux)
            return dd.read_parquet(str(PARQUET_VALIDATED))

        if SCHEDULER_ADDRESS:
            log.info("remote_analysis_started")
            # 1. Resolve DDF on the cluster. Keep it as a future to avoid Windows serialization issues.
            ddf_future = client.submit(get_remote_ddf)
            
            # 2. Submit each analysis separately, passing the future as an argument
            log.info("submitting_individual_analyses")
            results = {
                "revenue":  client.submit(compute_revenue_by_category, ddf_future).result(),
                "funnel":   client.submit(compute_conversion_funnel,   ddf_future).result(),
                "hourly":   client.submit(compute_hourly_activity,    ddf_future).result(),
                "sessions": client.submit(compute_session_stats,      ddf_future).result(),
                "brands":   client.submit(compute_top_brands,         ddf_future).result(),
            }
            log.info("remote_analysis_done")
        else:
            # Local Mode execution
            ddf = load_parquet(PARQUET_VALIDATED)
            results = {
                "revenue":  compute_revenue_by_category(ddf),
                "funnel":   compute_conversion_funnel(ddf),
                "hourly":   compute_hourly_activity(ddf),
                "sessions": compute_session_stats(ddf),
                "brands":   compute_top_brands(ddf),
            }


    # ── Stage 4: Persist & Display results ────────────────────────────────────
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    save_parquet_pandas(results["revenue"], RESULTS_DIR / "revenue_by_category.parquet")
    save_parquet_pandas(results["funnel"],  RESULTS_DIR / "conversion_funnel.parquet")
    save_parquet_pandas(results["hourly"],  RESULTS_DIR / "hourly_activity.parquet")
    save_parquet_pandas(results["brands"],  RESULTS_DIR / "top_brands.parquet")

    # Session stats are large (millions of rows). We save them as Dask Parquet (partitioned)
    # instead of pulling everything to the driver.
    session_parquet_path = RESULTS_DIR / "session_stats"
    if hasattr(results["sessions"], "to_parquet"):
        log.info("saving_sessions_dask", path=str(session_parquet_path))
        import shutil
        if session_parquet_path.exists():
            shutil.rmtree(session_parquet_path)
        results["sessions"].to_parquet(str(session_parquet_path), write_index=False)
        # Compute just a small sample for the display summary
        session_sample = results["sessions"].head(10)
    else:
        save_parquet_pandas(results["sessions"], session_parquet_path.with_suffix(".parquet"))
        session_sample = results["sessions"].head(10)

    # Display results summary
    print("\n" + "="*80)
    print(" PIPELINE RESULTS SUMMARY ".center(80, "="))
    print("="*80)

    print("\n[ Analysis 1: Revenue by Category (Top 10) ]")
    print(results["revenue"].head(10).to_string(index=False))

    print("\n[ Analysis 2: Top Brands by Revenue (Top 10) ]")
    print(results["brands"].head(10).to_string(index=False))

    print("\n[ Analysis 3: Conversion Funnel (Top 10 Categories - Sorted by View Volume) ]")
    print(results["funnel"].head(10).to_string(index=False))

    print("\n[ Analysis 4: Session Statistics (Sample) ]")
    print(session_sample.to_string(index=False))

    total = round(time.time() - pipeline_start, 1)
    log.info("pipeline_completed", total_s=total, stage_times=timings)
    print("\n" + "="*80)
    print(f"Pipeline completed in {total}s")
    for stage, s in timings.items():
        print(f"  {stage:<20} {s:.1f}s")
    print("="*80)


if __name__ == "__main__":
    main()
