from __future__ import annotations
from pathlib import Path


import os
import platform
PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).resolve().parent.parent))

# When running in Linux containers (Docker/K8s), ensure we use /app as the base
# We check for a common container file to be sure
IS_CONTAINER = os.path.exists("/.dockerenv") or os.path.exists("/run/.containerenv")
if platform.system() == "Linux" and (IS_CONTAINER or os.path.exists("/app/src")):
    PROJECT_ROOT = Path("/app")

DATA_DIR     = PROJECT_ROOT / "data"
OUTPUT_DIR   = PROJECT_ROOT / "output"
LOGS_DIR     = PROJECT_ROOT / "logs"

# ── Input paths ───────────────────────────────────────────────────────────────
RAW_OCT = DATA_DIR / "2019-Oct.csv"
RAW_NOV = DATA_DIR / "2019-Nov.csv"

def get_available_data_paths() -> list[Path]:
    """Discover all existing e-commerce CSVs in the data directory."""
    paths = []
    for p in [RAW_OCT, RAW_NOV]:
        if p.exists():
            paths.append(p)
    return paths

# ── Storage paths ─────────────────────────────────────────────────────────────
PARQUET_VALIDATED = OUTPUT_DIR / "parquet" / "validated"
RESULTS_DIR       = OUTPUT_DIR / "results"

# ── Schema ────────────────────────────────────────────────────────────────────
EXPECTED_COLUMNS = {
    "event_time", "event_type", "product_id",
    "category_id", "category_code", "brand",
    "price", "user_id", "user_session",
}

VALID_EVENT_TYPES = {"view", "cart", "remove_from_cart", "purchase"}

DTYPES = {
    "event_type":    "category",
    "product_id":    "int64",
    "category_id":   "int64",
    "category_code": "object",
    "brand":         "object",
    "price":         "float64",
    "user_id":       "int64",
    "user_session":  "object",
}

BLOCKSIZE = "64MB"
