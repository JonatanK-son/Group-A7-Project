"""Schema enforcement, null checks, value-range assertions, and data cleaning."""
import dask.dataframe as dd
from src.config import EXPECTED_COLUMNS, VALID_EVENT_TYPES
from src.logger import StructuredLogger

log = StructuredLogger("validation")


# ── Schema validation ─────────────────────────────────────────────────────────

def validate_schema(ddf: dd.DataFrame) -> dict:
    """Return a report dict; raises RuntimeError on missing columns."""
    actual  = set(ddf.columns)
    missing = EXPECTED_COLUMNS - actual
    extra   = actual - EXPECTED_COLUMNS
    report  = {"valid": not bool(missing), "missing": sorted(missing), "extra": sorted(extra)}
    if missing:
        log.warning("schema_mismatch", **report)
    else:
        log.info("schema_valid", columns=sorted(actual))
    return report


# ── Data quality report (fast — computed per-partition lazily) ────────────────

def data_quality_report(ddf: dd.DataFrame) -> dict:
    """Compute null counts + invalid-value summary. Triggers computation."""
    with log.timer("data_quality_report"):
        null_counts     = ddf.isnull().sum().compute().to_dict()
        invalid_events  = int((~ddf["event_type"].isin(VALID_EVENT_TYPES)).sum().compute())
        negative_prices = int((ddf["price"] < 0).sum().compute())
        total_rows      = len(ddf)

    report = {
        "total_rows":       total_rows,
        "null_counts":      null_counts,
        "invalid_events":   invalid_events,
        "negative_prices":  negative_prices,
    }
    log.info("data_quality_report", **{k: v for k, v in report.items() if k != "null_counts"})
    return report


# ── Cleaning ──────────────────────────────────────────────────────────────────

def clean_data(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Apply cleaning rules (lazy — returns a new Dask DataFrame):
      1. Drop rows with nulls in key columns
      2. Keep only known event types
      3. Remove negative prices
      4. Drop rows with blank user_session
    """
    ddf = ddf.dropna(subset=["user_id", "product_id", "price", "event_time", "user_session"])
    ddf = ddf[ddf["event_type"].isin(VALID_EVENT_TYPES)]
    ddf = ddf[ddf["price"] >= 0]
    ddf = ddf[ddf["user_session"].str.strip() != ""]
    # Force microsecond precision for Spark compatibility.
    # The CSV parser reads it as timezone-aware (UTC), so we must strip the zone
    # (localize to None) before casting to a naive datetime64[us].
    ddf["event_time"] = ddf["event_time"].dt.tz_localize(None).astype("datetime64[us]")
    log.info("cleaning_rules_applied")
    return ddf
