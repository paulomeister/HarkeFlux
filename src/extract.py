"""Extract stage helpers for the ETL pipeline."""

import logging
from itertools import chain
from pathlib import Path
from typing import Iterator, Optional, Union

try:
    import pandas as pd
except Exception as e:
    raise ImportError(
        "pandas es requerido para la extracción. Instálalo con: pip install pandas"
    ) from e

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _file_metadata(path: Path) -> dict:
    stat = path.stat()
    return {
        "file": str(path),
        "size_bytes": stat.st_size,
        "size_mb": round(stat.st_size / (1024 * 1024), 3),
        "last_modified": stat.st_mtime,
    }


def _log_df_profile(df: pd.DataFrame, sample_rows: int) -> None:
    logger.info("Rows=%d Columns=%d", len(df), len(df.columns))
    logger.info("Columns=%s", list(df.columns))
    logger.info("Duplicate rows=%d", int(df.duplicated().sum()))
    logger.info("Sample:\n%s", df.head(sample_rows))


def _stream_chunks_with_metrics(
    first_chunk: pd.DataFrame,
    iterator: Iterator[pd.DataFrame],
    sample_rows: int,
) -> Iterator[pd.DataFrame]:
    seen_hashes = set()
    total_rows = 0
    chunk_count = 0
    duplicate_rows = 0

    for chunk in chain([first_chunk], iterator):
        chunk_count += 1
        total_rows += len(chunk)

        # Hash per row lets us count duplicates across chunk boundaries.
        row_hashes = pd.util.hash_pandas_object(chunk, index=False)
        for row_hash in row_hashes.to_numpy():
            value = int(row_hash)
            if value in seen_hashes:
                duplicate_rows += 1
            else:
                seen_hashes.add(value)

        if chunk_count == 1:
            logger.info("First chunk preview")
            _log_df_profile(chunk, sample_rows)

        logger.info(
            "Chunk %d processed | rows=%d | duplicate_rows_so_far=%d",
            chunk_count,
            len(chunk),
            duplicate_rows,
        )
        yield chunk

    logger.info(
        "Chunk mode summary | chunks=%d | rows=%d | duplicate_rows=%d",
        chunk_count,
        total_rows,
        duplicate_rows,
    )


def extract_data(
    file_path: Union[str, Path],
    chunksize: Optional[int] = None,
    sample_rows: int = 5,
    sep: str = ",",
    encoding: str = "utf-8",
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Read CSV data for ETL in full mode or chunk mode."""

    p = Path(file_path)
    logger.info("Starting extraction: %s", p)

    if not p.exists():
        logger.error("File not found: %s", p)
        raise FileNotFoundError(f"File not found: {p}")
    if p.stat().st_size == 0:
        logger.error("Empty file: %s", p)
        raise ValueError(f"Empty file: {p}")

    metadata = _file_metadata(p)
    logger.info("Source metadata: %s", metadata)

    read_csv_kwargs = {
        "sep": sep,
        "encoding": encoding,
        "quotechar": '"',
        "low_memory": False,
        "keep_default_na": False,
    }

    try:
        if chunksize and chunksize > 0:
            logger.info("Reading in chunk mode: chunksize=%d", chunksize)
            iterator = pd.read_csv(p, chunksize=chunksize, **read_csv_kwargs)

            try:
                first_chunk = next(iterator)
            except StopIteration:
                logger.warning("No data rows in file: %s", p)
                return iter(())

            return _stream_chunks_with_metrics(first_chunk, iterator, sample_rows)

        logger.info("Reading full file in memory")
        df = pd.read_csv(p, **read_csv_kwargs)
        _log_df_profile(df, sample_rows)
        return df

    except pd.errors.ParserError as pe:
        logger.exception("CSV parser error. Check delimiter/encoding: %s", pe)
        raise
    except Exception as exc:
        logger.exception("Unexpected extraction error: %s", exc)
        raise


if __name__ == "__main__":
    repo_root = Path(__file__).resolve().parents[1]
    default_path = repo_root / "data" / "raw" / "Delitos_Informaticos_V1_20260328.csv"

    try:
        df_or_iter = extract_data(default_path)
        if hasattr(df_or_iter, "head"):
            logger.info("Extraction check OK. Rows=%d", len(df_or_iter))
        else:
            count = 0
            for chunk in df_or_iter:
                count += 1
                logger.info("Chunk %d rows=%d", count, len(chunk))
                if count >= 3:
                    break
    except FileNotFoundError:
        logger.error("Example CSV not found at: %s", default_path)
