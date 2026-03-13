"""Parquet read/write helpers for Dask and pandas DataFrames."""
from pathlib import Path

import dask.dataframe as dd
import pandas as pd

from src.config import TEST_MODE

from src.logger import StructuredLogger

log = StructuredLogger("storage")


def save_parquet_dask(
    ddf: dd.DataFrame,
    path: str | Path,
    partition_on: list | None = None,
    overwrite: bool = False,
) -> None:
    """Write a Dask DataFrame to a Parquet dataset directory."""
    path = Path(path)
    if overwrite and path.exists():
        log.info("clearing_existing_directory", path=str(path))
        import shutil
        shutil.rmtree(path)
    
    path.mkdir(parents=True, exist_ok=True)
    log.info("saving_parquet_dask", path=str(path))
    kwargs = {"partition_on": partition_on} if partition_on else {}
    ddf.to_parquet(str(path), write_index=False, **kwargs)
    log.info("saved_parquet_dask", path=str(path))


def save_parquet_pandas(df: pd.DataFrame, path: str | Path) -> None:
    """Write a pandas DataFrame to a single Parquet file."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(str(path), index=False)
    log.info("saved_parquet_pandas", path=str(path), rows=len(df))


def load_parquet(path: str | Path) -> dd.DataFrame:
    """Load a Parquet dataset as a lazy Dask DataFrame."""
    ddf = dd.read_parquet(str(path))
    if TEST_MODE:
        log.info("test_mode_enabled", action="limiting_dask_load")
        ddf = ddf.partitions[:1]
    return ddf
