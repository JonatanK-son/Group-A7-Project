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

for d in [OUTPUT_DIR, LOGS_DIR, PARQUET_VALIDATED, RESULTS_DIR]:
    Path(d).mkdir(parents=True, exist_ok=True)


def main():
    log.info("pipeline_started")
    timings: dict[str, float] = {}
    pipeline_start = time.time()

    # ── Stage 1: Ingest ───────────────────────────────────────────────────────
    with log.timer("ingestion") as t:
        raw_ddf = load_csvs([RAW_OCT, RAW_NOV])
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
