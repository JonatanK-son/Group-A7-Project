#!/usr/bin/env python3
"""
Standalone pipeline runner — two-phase architecture:

  Phase 1 — LOCAL  : Ingest raw CSVs, clean, and write validated Parquet to
                     ./output/parquet/validated/ on the local machine.
                     Raw CSVs never need to be mounted into Minikube.

  Phase 2 — REMOTE : Dask workers read the validated Parquet from /app/output
                     (mounted from ./output) and run the five analyses.
                     Only the ./output folder needs to be mounted into Minikube.
"""
import os
import sys
import time
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import socket
import dask
import dask.dataframe as dd
from dask.distributed import Client
import pandas as pd

from src.config import (
    RAW_OCT, RAW_NOV,
    OUTPUT_DIR, LOGS_DIR,
    PARQUET_VALIDATED, RESULTS_DIR,
)
from src.logger import StructuredLogger
from src.validation import clean_data
from src.ingestion import load_csvs
from src.transformations_dask import (
    compute_revenue_by_category,
    compute_conversion_funnel,
    compute_hourly_activity,
    compute_session_stats,
    compute_top_brands,
    SESSION_META,
)
from src.storage import save_parquet_dask, save_parquet_pandas

log = StructuredLogger("run_pipeline")


def get_scheduler_address():
    """Determine the Dask scheduler address. Defaults to Minikube port-forward if available."""
    addr = os.getenv("DASK_SCHEDULER_ADDRESS")
    if addr:
        return addr
    try:
        with socket.create_connection(("127.0.0.1", 8786), timeout=0.1):
            return "tcp://127.0.0.1:8786"
    except Exception:
        return None


# ── Phase 1: Local ingestion ──────────────────────────────────────────────────

def ingest_locally(sample_mode: bool):
    """
    Runs on the LOCAL machine (Windows).

    Reads raw CSVs, cleans the data, and writes validated Parquet to
    OUTPUT_DIR/parquet/validated/.  If validated Parquet already exists AND
    matches the requested sample_mode, the step is skipped.

    Pass sample_mode=True to limit ingestion to the first 2 CSV partitions
    for fast iteration during development.
    """
    marker_file = Path(PARQUET_VALIDATED) / ".sample_mode"
    
    if Path(PARQUET_VALIDATED).exists():
        # Check if current mode matches previous mode recorded in the marker
        if marker_file.exists():
            try:
                was_sample = marker_file.read_text().strip() == "True"
                if was_sample == sample_mode:
                    existing = list(Path(PARQUET_VALIDATED).rglob("*.parquet"))
                    if existing:
                        log.info("ingest_skipped_mode_match",
                                 path=str(PARQUET_VALIDATED), sample_mode=sample_mode, files=len(existing))
                        return
            except Exception:
                pass # Corrupt marker, proceed to re-ingest

    log.info("ingest_started", sample_mode=sample_mode)
    with log.timer("ingestion"):
        raw_ddf = load_csvs([RAW_OCT, RAW_NOV])
        if sample_mode:
            raw_ddf = raw_ddf.partitions[:2]

    with log.timer("cleaning_and_save"):
        clean_ddf = clean_data(raw_ddf)
        # overwrite=True clears the directory, including our marker
        save_parquet_dask(clean_ddf, PARQUET_VALIDATED,
                          partition_on=["event_type"], overwrite=True)

    # Write fresh marker after successful save
    marker_file.write_text(str(sample_mode))
    log.info("ingest_done", path=str(PARQUET_VALIDATED), sample_mode=sample_mode)


# ── Phase 2: Remote analysis (runs on Minikube workers) ──────────────────────

def run_analysis_remote():
    """
    Runs ON THE DASK WORKER inside Minikube.

    Reads validated Parquet from /app/output/parquet/validated/ (mounted from
    the local ./output folder) and runs all five analyses sequentially,
    computing and saving one analysis at a time to bound peak memory usage.
    Raw CSVs are never accessed — only the ./output mount is required.
    """
    from src.config import PARQUET_VALIDATED, RESULTS_DIR
    from src.storage import save_parquet_pandas
    from src.transformations_dask import (
        compute_revenue_by_category, compute_conversion_funnel,
        compute_hourly_activity, compute_session_stats, compute_top_brands,
    )
    import dask.dataframe as dd
    import gc

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    # ── 1. Revenue by category ────────────────────────────────────────────────
    ddf_a = dd.read_parquet(str(PARQUET_VALIDATED))
    revenue = compute_revenue_by_category(ddf_a)
    save_parquet_pandas(revenue, RESULTS_DIR / "revenue_by_category.parquet")
    del ddf_a, revenue
    gc.collect()

    # ── 2. Conversion funnel ──────────────────────────────────────────────────
    ddf_a = dd.read_parquet(str(PARQUET_VALIDATED))
    funnel = compute_conversion_funnel(ddf_a)
    save_parquet_pandas(funnel, RESULTS_DIR / "conversion_funnel.parquet")
    del ddf_a, funnel
    gc.collect()

    # ── 3. Hourly activity ────────────────────────────────────────────────────
    ddf_a = dd.read_parquet(str(PARQUET_VALIDATED))
    hourly = compute_hourly_activity(ddf_a)
    save_parquet_pandas(hourly, RESULTS_DIR / "hourly_activity.parquet")
    del ddf_a, hourly
    gc.collect()

    # ── 4. Session statistics (Checkpoint Pattern) ────────────────────────────
    # Step 4a: Phase 1 partial reduction saved to disk to clear memory
    ddf_a = dd.read_parquet(str(PARQUET_VALIDATED))
    # We only use the Phase 1 part of the lazy graph
    def _phase1_only(df):
        if df.empty: return SESSION_META
        return df.groupby("user_session", sort=False).agg(
            session_start=("event_time", "min"), session_end=("event_time", "max"),
            num_events=("product_id", "count"), total_spend=("price", "sum")
        ).reset_index()

    partial_ddf = ddf_a.map_partitions(_phase1_only, meta=SESSION_META)
    checkpoint_path = RESULTS_DIR / "session_partials.parquet"
    partial_ddf.to_parquet(str(checkpoint_path), overwrite=True)
    del ddf_a, partial_ddf
    gc.collect()

    dask.config.set({"dataframe.query-planning": False})  # Fallback to legacy engine for better memory stability on small clusters
    
    # Step 4b: Read back and finalize global reduction
    partial_ddf = dd.read_parquet(str(checkpoint_path))
    sessions_lazy = (
        partial_ddf.groupby("user_session")
        .agg({"session_start":"min", "session_end":"max", "num_events":"sum", "total_spend":"sum"}, split_out=32)
        .reset_index()
    )
    sessions_lazy["session_duration_min"] = (sessions_lazy["session_end"] - sessions_lazy["session_start"]).dt.total_seconds() / 60
    
    # Save directly to disk from workers; NEVER .compute() back to a single worker's memory
    final_path = RESULTS_DIR / "session_stats.parquet"
    if final_path.exists():
        if final_path.is_file(): final_path.unlink()
        else: shutil.rmtree(final_path)
    sessions_lazy.to_parquet(str(final_path))
    del partial_ddf, sessions_lazy
    gc.collect()

    # ── 5. Top brands ─────────────────────────────────────────────────────────
    ddf_a = dd.read_parquet(str(PARQUET_VALIDATED))
    brands = compute_top_brands(ddf_a)
    save_parquet_pandas(brands, RESULTS_DIR / "top_brands.parquet")
    del ddf_a, brands
    gc.collect()

    return "Remote analysis completed successfully and results saved to disk."


def _run_analysis_local():
    """
    Fallback: runs the five analyses locally (no Minikube) using the same
    sequential compute-save-free pattern as the remote version.
    Reads Parquet produced by ingest_locally().
    """
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    ddf_a = dd.read_parquet(str(PARQUET_VALIDATED))
    revenue = compute_revenue_by_category(ddf_a)
    save_parquet_pandas(revenue, RESULTS_DIR / "revenue_by_category.parquet")
    del ddf_a

    ddf_a = dd.read_parquet(str(PARQUET_VALIDATED))
    funnel = compute_conversion_funnel(ddf_a)
    save_parquet_pandas(funnel, RESULTS_DIR / "conversion_funnel.parquet")
    del ddf_a

    # ── 3. Hourly activity ────────────────────────────────────────────────────
    ddf_a = dd.read_parquet(str(PARQUET_VALIDATED))
    hourly = compute_hourly_activity(ddf_a)
    save_parquet_pandas(hourly, RESULTS_DIR / "hourly_activity.parquet")
    del ddf_a 

    # ── 4. Session statistics ─────────────────────────────────────────────────
    ddf_a = dd.read_parquet(str(PARQUET_VALIDATED))
    # Local fallback also uses checkpointing for consistency and memory safety
    def _phase1_only(df):
        if df.empty: return SESSION_META
        return df.groupby("user_session", sort=False).agg(
            session_start=("event_time", "min"), session_end=("event_time", "max"),
            num_events=("product_id", "count"), total_spend=("price", "sum")
        ).reset_index()

    partial_ddf = ddf_a.map_partitions(_phase1_only, meta=SESSION_META)
    checkpoint_path = RESULTS_DIR / "session_partials.parquet"
    partial_ddf.to_parquet(str(checkpoint_path), overwrite=True)
    del ddf_a, partial_ddf

    partial_ddf = dd.read_parquet(str(checkpoint_path))
    sessions_lazy = (
        partial_ddf.groupby("user_session")
        .agg({"session_start":"min", "session_end":"max", "num_events":"sum", "total_spend":"sum"})
        .reset_index()
    )
    sessions_lazy["session_duration_min"] = (sessions_lazy["session_end"] - sessions_lazy["session_start"]).dt.total_seconds() / 60
    
    final_path = RESULTS_DIR / "session_stats.parquet"
    if final_path.exists():
        if final_path.is_file(): final_path.unlink()
        else: shutil.rmtree(final_path)
    sessions_lazy.to_parquet(str(final_path))
    del partial_ddf, sessions_lazy

    ddf_a = dd.read_parquet(str(PARQUET_VALIDATED))
    brands = compute_top_brands(ddf_a)
    save_parquet_pandas(brands, RESULTS_DIR / "top_brands.parquet")
    del ddf_a

    return {
        "revenue":  revenue,
        "funnel":   funnel,
        "brands":   brands,
        "sessions": pd.read_parquet(RESULTS_DIR / "session_stats.parquet").head(10),
    }


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    log.info("pipeline_started")
    pipeline_start = time.time()
    SAMPLE_MODE = False  # Set to True to ingest only 2 CSV partitions for quick dev runs

    # Ensure local output directories exist
    for d in [OUTPUT_DIR, LOGS_DIR, PARQUET_VALIDATED, RESULTS_DIR]:
        Path(d).mkdir(parents=True, exist_ok=True)

    # ── Phase 1: Local ingestion ───────────────────────────────────────────────
    # Always runs on the local (Windows) machine.  Produces validated Parquet
    # in ./output which Minikube workers read via the ./output:/output mount.
    # Skipped automatically if the Parquet already exists.
    log.info("phase1_local_ingest")
    ingest_locally(SAMPLE_MODE)

    # ── Phase 2: Analysis ──────────────────────────────────────────────────────
    scheduler_addr = get_scheduler_address()

    if scheduler_addr:
        log.info("remote_execution_mode", scheduler=scheduler_addr)
        with Client(scheduler_addr) as client:
            log.info("dask_client_connected", dashboard=client.dashboard_link)
            # Submit the sequential analysis; it saves results to the mounted ./output folder
            status = client.submit(run_analysis_remote).result()
            log.info("remote_analysis_done", status=status)
            
        results = {
            "revenue":  pd.read_parquet(RESULTS_DIR / "revenue_by_category.parquet"),
            "brands":   pd.read_parquet(RESULTS_DIR / "top_brands.parquet"),
            "funnel":   pd.read_parquet(RESULTS_DIR / "conversion_funnel.parquet"),
            "sessions": pd.read_parquet(RESULTS_DIR / "session_stats.parquet").head(10),
        }
    else:
        log.info("local_execution_mode")
        results = _run_analysis_local()

    # ── Display results summary ────────────────────────────────────────────────
    print("\n" + "=" * 80)
    print(" PIPELINE RESULTS SUMMARY ".center(80, "="))
    print("=" * 80)

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
    print("\n" + "=" * 80)
    print(f"Pipeline completed in {total}s")
    print("=" * 80)


if __name__ == "__main__":
    main()
