"""CSV ingestion with retry logic and bad-line handling."""
import time
from pathlib import Path

import dask.dataframe as dd

from src.config import DTYPES, BLOCKSIZE, TEST_MODE
from src.logger import StructuredLogger

log = StructuredLogger("ingestion")


def load_csv(path: str | Path, max_retries: int = 3) -> dd.DataFrame:
    """
    Load a single e-commerce CSV with retry logic for transient I/O failures.
    Malformed rows are silently skipped via on_bad_lines='skip'.
    """
    path = str(path)
    for attempt in range(1, max_retries + 1):
        try:
            log.info("load_csv_attempt", path=path, attempt=attempt)
            ddf = dd.read_csv(
                path,
                dtype=DTYPES,
                parse_dates=["event_time"],
                blocksize=BLOCKSIZE,
                on_bad_lines="skip",
            )
            log.info("load_csv_ok", path=path, partitions=ddf.npartitions)
            return ddf
        except FileNotFoundError:
            log.error("file_not_found", path=path)
            raise
        except Exception as exc:
            log.warning("load_csv_error", path=path, attempt=attempt, error=str(exc))
            if attempt == max_retries:
                log.error("load_csv_failed", path=path, error=str(exc))
                raise
            time.sleep(1)


def load_csvs(paths: list) -> dd.DataFrame:
    """
    Load multiple CSV files into a single Dask DataFrame in one read pass.
    Dask handles the list natively — no concat overhead.
    """
    str_paths = [str(p) for p in paths]
    if TEST_MODE:
        log.info("test_mode_enabled", action="limiting_dask_input")
        # In test mode, only load the first file and only take a few partitions
        str_paths = str_paths[:1]

    log.info("load_csvs_started", files=str_paths)
    ddf = dd.read_csv(
        str_paths,
        dtype=DTYPES,
        parse_dates=["event_time"],
        blocksize=BLOCKSIZE,
        on_bad_lines="skip",
    )
    
    if TEST_MODE:
        # Limit to first partition for speed
        ddf = ddf.partitions[:1]

    log.info("load_csvs_ok", files=len(str_paths), partitions=ddf.npartitions)
    return ddf
