"""All Dask-based transformations: 5 distinct operations."""
import dask.dataframe as dd
import pandas as pd

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

def compute_session_stats(ddf: dd.DataFrame) -> pd.DataFrame:
    """
    Per-session aggregation using a two-phase map-reduce pattern that avoids
    any cross-partition shuffle (which OOMs on millions of unique session UUIDs).

    Phase 1 — map   : groupby within each partition independently (no data movement)
    Phase 2 — reduce: re-aggregate the partial results entirely in pandas

    Derived columns:
      session_start / session_end    min/max event_time
      num_events                     total event count across all partitions
      total_spend                    sum of all prices in the session
      session_duration_min           wall-clock session length in minutes
    """
    def _per_partition(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame(
                columns=["user_session", "session_start", "session_end",
                         "num_events", "total_spend"]
            )
        return (
            df.groupby("user_session", sort=False)
            .agg(
                session_start=("event_time", "min"),
                session_end=("event_time", "max"),
                num_events=("product_id", "count"),
                total_spend=("price", "sum"),
            )
            .reset_index()
        )

    # Phase 1: each partition processed independently, no shuffle.
    # scheduler='threads' bypasses the distributed scheduler and its automatic
    # repartition(npartitions=1) step, which would OOM a worker with the full result.
    # The threads scheduler concatenates partitions directly in the driver process.
    log.info("session_stats_phase1_started")
    partial = ddf.map_partitions(_per_partition).compute(scheduler='threads')
    log.info("session_stats_phase1_done", partial_rows=len(partial))

    # Phase 2: re-aggregate partial results in pandas (small relative to raw data)
    agg = (
        partial
        .groupby("user_session", sort=False)
        .agg(
            session_start=("session_start", "min"),
            session_end=("session_end", "max"),
            num_events=("num_events", "sum"),
            total_spend=("total_spend", "sum"),
        )
        .reset_index()
    )
    agg["session_duration_min"] = (
        (agg["session_end"] - agg["session_start"]).dt.total_seconds() / 60
    )
    log.info("session_stats_done", sessions=len(agg))
    return agg


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
