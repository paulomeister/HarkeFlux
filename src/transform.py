"""Transform stages 2 and 3 for ETL business rules."""

import logging
import re
import unicodedata
from pathlib import Path

try:
    import pandas as pd
except Exception as e:
    raise ImportError(
        "pandas es requerido para la transformacion. Instálalo con: pip install pandas"
    ) from e


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


_COLUMN_ALIASES = {
    "AO_DENUNCIA": "ANO_DENUNCIA",
    "AO_ENTRADA": "ANO_ENTRADA",
    "AO_HECHOS": "ANO_HECHOS",
    "PAS_HECHO": "PAIS_HECHO",
    "ES_PRECLUSIN": "ES_PRECLUSION",
}

_NUMERIC_BASE_COLUMNS = [
    "ANO_DENUNCIA",
    "ANO_ENTRADA",
    "ANO_HECHOS",
    "TOTAL_PROCESOS",
]


def _normalize_column_name(name: str) -> str:
    clean = unicodedata.normalize("NFKD", str(name))
    clean = "".join(ch for ch in clean if not unicodedata.combining(ch))
    clean = clean.upper().strip()
    clean = re.sub(r"[^A-Z0-9]+", "_", clean)
    clean = re.sub(r"_+", "_", clean).strip("_")
    return _COLUMN_ALIASES.get(clean, clean)


def _canonicalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    renamed = df.rename(columns={col: _normalize_column_name(col) for col in df.columns})
    if renamed.columns.duplicated().any():
        duplicated = renamed.columns[renamed.columns.duplicated()].tolist()
        raise ValueError(f"Column collision after normalization: {duplicated}")
    return renamed


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """Apply stage-2 and stage-3 transformations and return a cleaned DataFrame."""
    if not isinstance(df, pd.DataFrame):
        raise TypeError("transform_data expects a pandas DataFrame")

    transformed = _canonicalize_columns(df.copy())
    duplicate_rows = int(transformed.duplicated().sum())
    logger.info("Duplicate rows (reported, not removed): %d", duplicate_rows)

    drop_cols = ["CONSUMADO", "GRUPO_DELITO"]
    cols_to_drop = [col for col in drop_cols if col in transformed.columns]
    transformed = transformed.drop(columns=cols_to_drop)

    # Flag records that contain SIN DATO before adding numeric helper columns.
    transformed["ES_INCOMPLETO"] = (
        transformed.astype("string").eq("SIN DATO").any(axis=1).map({True: "SI", False: "NO"})
    )

    for col in _NUMERIC_BASE_COLUMNS:
        if col not in transformed.columns:
            raise KeyError(f"Missing expected numeric source column: {col}")

        numeric_col = f"{col}_NUM"
        source = transformed[col].astype("string").str.strip()
        transformed[numeric_col] = pd.to_numeric(
            source.where(source != "SIN DATO", pd.NA),
            errors="coerce",
        ).astype("Int64")

    if "DELITO" not in transformed.columns:
        raise KeyError("Missing expected text column: DELITO")

    transformed["ES_AGRAVADO"] = transformed["DELITO"].astype("string").str.contains(
        "AGRAVADO", case=False, na=False
    ).map({True: "SI", False: "NO"})

    transformed["TIEMPO_DENUNCIA"] = transformed["ANO_DENUNCIA_NUM"] - transformed["ANO_HECHOS_NUM"]

    logger.info("Stage 2 output rows=%d columns=%d", len(transformed), len(transformed.columns))
    logger.info("Dropped columns=%s", cols_to_drop)
    numeric_nulls = {
        f"{col}_NUM": int(transformed[f"{col}_NUM"].isna().sum())
        for col in _NUMERIC_BASE_COLUMNS
    }
    logger.info(
        "Stage 3 nulls in *_NUM=%s",
        numeric_nulls,
    )
    return transformed


def save_processed_csv(df: pd.DataFrame, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8")
    logger.info("Processed CSV saved at: %s", output_path)
    return output_path


if __name__ == "__main__":
    from extract import extract_data

    repo_root = Path(__file__).resolve().parents[1]
    source_path = repo_root / "data" / "raw" / "Delitos_Informaticos_V1_20260328.csv"
    output_path = repo_root / "data" / "processed" / "Delitos_Informaticos_V1_20260328_stage3.csv"

    extracted = extract_data(source_path)
    if not isinstance(extracted, pd.DataFrame):
        raise RuntimeError("Chunk mode is not supported in transform.py standalone run")

    stage3_df = transform_data(extracted)
    save_processed_csv(stage3_df, output_path)
