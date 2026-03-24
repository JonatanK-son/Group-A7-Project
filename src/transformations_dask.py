from __future__ import annotations
"""All Dask-based transformations: 5 distinct operations."""

import dask
import dask.dataframe as dd
import pandas as pd
import gc
import shutil
from pathlib import Path

from src.logger import StructuredLogger

log = StructuredLogger("dask_transforms")


def _top_category(ddf: dd.DataFrame) -> dd.DataFrame:
    return ddf.assign(
        top_category=lambda x: x["category_code"].str.split(".").str[0].fillna("unknown")
    )


# ── Analysis 1: Revenue by category (filter + aggregate) ─────────────────────

def compute_revenue_by_category(ddf: dd.DataFrame) -> pd.DataFrame:
    """Purchase revenue and volume grouped by top-level product category."""
    purchases = ddf[ddf["event_type"] == "purchase"]
    purchases = _top_category(purchases)
    result = (
        purchases
        .groupby("top_category")["price"]
        .agg(["sum", "count", "mean"])
        .compute()
        .sort_values("sum", ascending=False)
        .reset_index()
    )
    result.columns = ["top_category", "total_revenue", "num_purchases", "avg_price"]
    log.info("revenue_by_category_done", categories=len(result))
    return result


# ── Analysis 2: Conversion funnel (per-event-type filter + single-key groupby) ─

def compute_conversion_funnel(ddf: dd.DataFrame) -> pd.DataFrame:
    """
    View → Cart → Purchase rates per top-level category.

    Uses 4 separate single-key groupbys instead of one multi-key groupby.
    This avoids the partition shuffle that causes OOM on large datasets,
    and takes advantage of the Parquet already being partitioned by event_type.
    """
    ddf = _top_category(ddf)
    event_types = ["view", "cart", "purchase", "remove_from_cart"]
    series: list[pd.Series] = []
    for etype in event_types:
        s = (
            ddf[ddf["event_type"] == etype]
            .groupby("top_category")
            .size()
            .compute()
            .rename(etype)
        )
        series.append(s)
    pivot = pd.concat(series, axis=1).fillna(0).reset_index()
    pivot.columns.name = None
    for col in event_types:
        if col not in pivot.columns:
            pivot[col] = 0
    pivot["cart_rate"]     = pivot["cart"]     / pivot["view"].replace(0, float("nan"))
    pivot["purchase_rate"] = pivot["purchase"] / pivot["view"].replace(0, float("nan"))
    
    # Sort by view volume so the most important categories are at the top
    pivot = pivot.sort_values("view", ascending=False).reset_index(drop=True)
    
    log.info("conversion_funnel_done", categories=len(pivot))
    return pivot


# ── Analysis 3: Hourly activity volume (per-event-type single-key groupby) ────

def compute_hourly_activity(ddf: dd.DataFrame) -> pd.DataFrame:
    """
    Event count broken down by hour-of-day and event type.

    Uses per-event-type filter + single-key groupby to avoid a multi-key
    shuffle across the full dataset.
    """
    event_types = ["view", "cart", "purchase", "remove_from_cart"]
    frames: list[pd.DataFrame] = []
    for etype in event_types:
        subset = ddf[ddf["event_type"] == etype]
        # Use value_counts on the Series directly to avoid .assign() index alignment issues
        counts = (
            subset["event_time"].dt.hour
            .value_counts()
            .compute()
            .rename_axis("hour")
            .rename("count")
            .reset_index()
        )
        counts["event_type"] = etype
        frames.append(counts)
    hourly = (
        pd.concat(frames, ignore_index=True)
        .sort_values(["hour", "event_type"])
        .reset_index(drop=True)
    )
    log.info("hourly_activity_done")
    return hourly


# ── Analysis 4: Session statistics (map-reduce, shuffle-free) ────────────────

SESSION_META = pd.DataFrame({
    "user_session":  pd.Series(dtype="object"),
    "session_start": pd.Series(dtype="datetime64[ns]"),
    "session_end":   pd.Series(dtype="datetime64[ns]"),
    "num_events":    pd.Series(dtype="int64"),
    "total_spend":   pd.Series(dtype="float64"),
})

def compute_session_stats(ddf: dd.DataFrame, checkpoint_path: str, final_path: str) -> None:
    """
    Per-session aggregation using a two-phase map-reduce pattern with disk checkpointing.
    
    This implementation saves partial results to disk halfway through the shuffle
    to bound peak memory usage, which is critical for stability on small clusters.
    
    Args:
        ddf: Input validated Dask DataFrame.
        checkpoint_path: Path where intermediate partition-level aggregates are saved.
        final_path: Final destination for the session statistics Parquet file.
    """
    # Step 4a: Phase 1 partial reduction saved to disk to clear memory
    def _phase1_only(df):
        if df.empty: return SESSION_META
        return df.groupby("user_session", sort=False).agg(
            session_start=("event_time", "min"), session_end=("event_time", "max"),
            num_events=("product_id", "count"), total_spend=("price", "sum")
        ).reset_index()

    partial_ddf = ddf.map_partitions(_phase1_only, meta=SESSION_META)
    
    log.info("session_stats_phase1_started", path=str(checkpoint_path))
    partial_ddf.to_parquet(
        str(checkpoint_path), 
        overwrite=True, 
        engine="pyarrow", 
        coerce_timestamps="us", 
        allow_truncated_timestamps=True
    )
    
    del partial_ddf
    gc.collect()

    # Disable query planning for better memory stability during the global reduction phase
    dask.config.set({"dataframe.query-planning": False})
    
    # Step 4b: Read back and finalize global reduction
    log.info("session_stats_phase2_started", path=str(final_path))
    gc.collect()  # Flush any remaining memory from Phase 1
    partial_ddf = dd.read_parquet(str(checkpoint_path))
    sessions_lazy = (
        partial_ddf.groupby("user_session")
        .agg({"session_start":"min", "session_end":"max", "num_events":"sum", "total_spend":"sum"}, split_out=128)
        .reset_index()
    )
    sessions_lazy["session_duration_min"] = (sessions_lazy["session_end"] - sessions_lazy["session_start"]).dt.total_seconds() / 60
    
    # Clean up target directory if it exists as a folder or file
    p = Path(final_path)
    if p.exists():
        if p.is_file(): p.unlink()
        else: shutil.rmtree(p)
        
    sessions_lazy.to_parquet(
        str(final_path), 
        engine="pyarrow", 
        coerce_timestamps="us", 
        allow_truncated_timestamps=True
    )
    
    del partial_ddf, sessions_lazy
    gc.collect()
    log.info("session_stats_done", final_path=str(final_path))


# ── Analysis 5: Top brands by purchase revenue (filter + aggregate) ───────────

def compute_top_brands(ddf: dd.DataFrame, top_n: int = 20) -> pd.DataFrame:
    """Rank brands by total purchase revenue."""
    result = (
        ddf[ddf["event_type"] == "purchase"]
        .dropna(subset=["brand"])
        .groupby("brand")["price"]
        .agg(["sum", "count"])
        .nlargest(top_n, "sum")
        .compute()
        .reset_index()
    )
    result.columns = ["brand", "total_revenue", "num_purchases"]
    log.info("top_brands_done", brands=len(result))
    return result
