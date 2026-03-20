"""Spark mirroring of the 5 Dask-based transformations."""
from pyspark.sql import SparkSession, DataFrame, functions as F
from src.logger import StructuredLogger

log = StructuredLogger("spark_transforms")


def _top_category(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "top_category",
        F.coalesce(F.split(F.col("category_code"), r"\.").getItem(0), F.lit("unknown"))
    )


# ── Analysis 1: Revenue by category ──────────────────────────────────────────

def compute_revenue_by_category(df: DataFrame) -> DataFrame:
    """Purchase revenue and volume grouped by top-level product category."""
    purchases = df.filter(F.col("event_type") == "purchase")
    with_cat = _top_category(purchases)

    result = (
        with_cat.groupBy("top_category")
        .agg(
            F.sum("price").alias("total_revenue"),
            F.count("*").alias("num_purchases"),
            F.mean("price").alias("avg_price")
        )
        .orderBy(F.desc("total_revenue"))
    )
    return result


# ── Analysis 2: Conversion funnel ─────────────────────────────────────────────

def compute_conversion_funnel(df: DataFrame) -> DataFrame:
    """View → Cart → Purchase rates per top-level category."""
    with_cat = _top_category(df)
    
    # Spark is very efficient at pivots
    pivoted = (
        with_cat.groupBy("top_category")
        .pivot("event_type", ["view", "cart", "purchase", "remove_from_cart"])
        .count()
        .fillna(0)
    )
    
    result = pivoted.withColumn(
        "cart_rate", F.col("cart") / F.expr("nullif(view, 0)")
    ).withColumn(
        "purchase_rate", F.col("purchase") / F.expr("nullif(view, 0)")
    ).orderBy(F.desc("view"))
    
    return result


# ── Analysis 3: Hourly activity ───────────────────────────────────────────────

def compute_hourly_activity(df: DataFrame) -> DataFrame:
    """Event count by hour and event type."""
    hour_df = df.withColumn("hour", F.hour(F.col("event_time")))
    
    result = (
        hour_df.groupBy("hour", "event_type")
        .count()
        .orderBy("hour", "event_type")
    )
    return result


# ── Analysis 4: Session statistics ────────────────────────────────────────────

def compute_session_stats(df: DataFrame) -> DataFrame:
    """High-cardinality session aggregation (equivalent to Dask map-reduce)."""
    sessions = (
        df.groupBy("user_session")
        .agg(
            F.min("event_time").alias("session_start"),
            F.max("event_time").alias("session_end"),
            F.count("*").alias("num_events"),
            F.sum("price").alias("total_spend")
        )
    )
    
    # Calculate duration
    result = sessions.withColumn(
        "session_duration_min", 
        (F.unix_timestamp("session_end") - F.unix_timestamp("session_start")) / 60
    )
    return result


# ── Analysis 5: Top brands ────────────────────────────────────────────────────

def compute_top_brands(df: DataFrame, top_n: int = 20) -> DataFrame:
    """Rank brands by purchase revenue."""
    purchases = df.filter(F.col("event_type") == "purchase").where(F.col("brand").isNotNull())
    
    result = (
        purchases.groupBy("brand")
        .agg(
            F.sum("price").alias("total_revenue"),
            F.count("*").alias("num_purchases")
        )
        .orderBy(F.desc("total_revenue"))
        .limit(top_n)
    )
    return result
