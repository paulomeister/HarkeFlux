"""ETL pipeline orchestration (extract -> transform -> load)."""

import logging
import time
from pathlib import Path

import pandas as pd

from extract import extract_data
from load import load_data
from transform import save_processed_csv, transform_data

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main(
    source_path: Path,
    processed_output_path: Path,
    table_name: str = "delitos_processed",
    if_exists: str = "replace",
) -> None:
    pipeline_start = time.perf_counter()
    logger.info("Pipeline started")

    extract_start = time.perf_counter()
    extracted = extract_data(source_path)
    if not isinstance(extracted, pd.DataFrame):
        raise RuntimeError("Pipeline expects full DataFrame mode. Use chunksize=None.")
    logger.info("Extract finished in %.2fs", time.perf_counter() - extract_start)

    transform_start = time.perf_counter()
    transformed = transform_data(extracted)
    save_processed_csv(transformed, processed_output_path)
    logger.info("Transform+save finished in %.2fs", time.perf_counter() - transform_start)

    load_start = time.perf_counter()
    loaded_rows = load_data(transformed, table_name=table_name, if_exists=if_exists)
    logger.info("Load finished in %.2fs", time.perf_counter() - load_start)

    logger.info(
        "Pipeline OK | rows_transformed=%d | rows_loaded=%d | total_time=%.2fs",
        len(transformed),
        loaded_rows,
        time.perf_counter() - pipeline_start,
    )


if __name__ == "__main__":
    repo_root = Path(__file__).resolve().parents[1]
    source = repo_root / "data" / "raw" / "Delitos_Informaticos_V1_20260328.csv"
    output = repo_root / "data" / "processed" / "Delitos_Informaticos_V1_20260328_stage3.csv"
    main(source, output, table_name="delitos_processed", if_exists="replace")
