"""Load stage: dynamic PostgreSQL load using SQLAlchemy and .env."""

import logging
import os
import re
import unicodedata
from pathlib import Path
from typing import Dict, Optional
from urllib.parse import quote_plus

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import BigInteger, Boolean, DateTime, Float, String, create_engine, text

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_identifier(name: str, kind: str) -> str:
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid {kind}: {name}")
    return name


def _resolve_env_file(dotenv_path: Optional[Path]) -> Path:
    if dotenv_path:
        return dotenv_path

    repo_root = Path(__file__).resolve().parents[1]
    root_env = repo_root / ".env"

    if not root_env.exists():
        raise FileNotFoundError(
            f"Root .env not found: {root_env}. Create it at project root."
        )
    return root_env


def _getenv_any(*keys: str, default: str = "") -> str:
    for key in keys:
        value = os.getenv(key)
        if value is not None and value.strip() != "":
            return value.strip()
    return default


def _build_db_config(dotenv_path: Optional[Path] = None) -> Dict[str, str]:
    env_file = _resolve_env_file(dotenv_path)
    load_dotenv(env_file)

    config = {
        "host": _getenv_any("DB_HOST", default="127.0.0.1"),
        "port": _getenv_any("DB_PORT", default="5432"),
        "name": _getenv_any("DB_NAME", "POSTGRES_DB"),
        "user": _getenv_any("DB_USER", "POSTGRES_USER"),
        "password": _getenv_any("DB_PASSWORD", "POSTGRES_PASSWORD"),
        "schema": _getenv_any("DB_SCHEMA", default=""),
    }

    missing = [
        key
        for key in ("name", "user", "password")
        if not config.get(key)
    ]
    if missing:
        raise ValueError(
            "Missing DB configuration in .env. Required: "
            "DB_NAME/POSTGRES_DB, DB_USER/POSTGRES_USER, DB_PASSWORD/POSTGRES_PASSWORD"
        )
    return config


def _build_sqlalchemy_url(config: Dict[str, str]) -> str:
    user = quote_plus(config["user"])
    password = quote_plus(config["password"])
    host = config["host"]
    port = config["port"]
    name = config["name"]
    return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{name}"


def _map_sqlalchemy_types(df: pd.DataFrame) -> Dict[str, object]:
    dtype_map: Dict[str, object] = {}
    for col in df.columns:
        series = df[col]
        if pd.api.types.is_integer_dtype(series):
            dtype_map[col] = BigInteger()
        elif pd.api.types.is_float_dtype(series):
            dtype_map[col] = Float()
        elif pd.api.types.is_bool_dtype(series):
            dtype_map[col] = Boolean()
        elif pd.api.types.is_datetime64_any_dtype(series):
            dtype_map[col] = DateTime()
        else:
            max_len = int(series.astype("string").str.len().max() or 0)
            dtype_map[col] = String(length=max(max_len, 32))
    return dtype_map


def _normalize_db_column_name(name: str) -> str:
    clean = unicodedata.normalize("NFKD", str(name))
    clean = "".join(ch for ch in clean if not unicodedata.combining(ch))
    clean = clean.lower().strip()
    clean = re.sub(r"[^a-z0-9]+", "_", clean)
    clean = re.sub(r"_+", "_", clean).strip("_")
    if not clean:
        clean = "col"
    if clean[0].isdigit():
        clean = f"col_{clean}"
    return _validate_identifier(clean, "column name")


def _prepare_dataframe_for_db(df: pd.DataFrame) -> pd.DataFrame:
    mapping = {col: _normalize_db_column_name(col) for col in df.columns}
    prepared = df.rename(columns=mapping)

    if prepared.columns.duplicated().any():
        duplicated = prepared.columns[prepared.columns.duplicated()].tolist()
        raise ValueError(f"Column collision after DB normalization: {duplicated}")

    changed = {src: dst for src, dst in mapping.items() if src != dst}
    if changed:
        logger.info("DB column normalization applied: %s", changed)

    return prepared


def load_data(
    df: pd.DataFrame,
    table_name: str = "delitos_processed",
    schema: Optional[str] = None,
    if_exists: str = "replace",
    chunk_size: int = 2000,
    dotenv_path: Optional[Path] = None,
) -> int:
    """Load a DataFrame into PostgreSQL creating table columns dynamically."""
    if not isinstance(df, pd.DataFrame):
        raise TypeError("load_data expects a pandas DataFrame")
    if df.empty:
        raise ValueError("Cannot load an empty DataFrame")
    if if_exists not in {"replace", "append", "fail"}:
        raise ValueError("if_exists must be one of: replace, append, fail")

    table_name = _validate_identifier(table_name, "table name")
    if schema:
        schema = _validate_identifier(schema, "schema name")

    config = _build_db_config(dotenv_path)
    if not schema and config.get("schema"):
        schema = _validate_identifier(config["schema"], "schema name")

    url = _build_sqlalchemy_url(config)
    df_for_db = _prepare_dataframe_for_db(df)
    dtype_map = _map_sqlalchemy_types(df_for_db)

    engine = create_engine(url, pool_pre_ping=True)

    logger.info(
        "Starting load | table=%s | schema=%s | rows=%d | if_exists=%s",
        table_name,
        schema or "public",
        len(df_for_db),
        if_exists,
    )

    with engine.begin() as conn:
        df_for_db.to_sql(
            name=table_name,
            con=conn,
            schema=schema,
            if_exists=if_exists,
            index=False,
            chunksize=chunk_size,
            method="multi",
            dtype=dtype_map,
        )

        qualified_name = f'"{table_name}"'
        if schema:
            qualified_name = f'"{schema}"."{table_name}"'

        inserted_rows = conn.execute(text(f"SELECT COUNT(*) FROM {qualified_name}")).scalar_one()

    logger.info("Load finished | table_rows=%d", inserted_rows)
    return int(inserted_rows)


if __name__ == "__main__":
    repo_root = Path(__file__).resolve().parents[1]
    default_processed = (
        repo_root / "data" / "processed" / "Delitos_Informaticos_V1_20260328_stage3.csv"
    )

    if not default_processed.exists():
        raise FileNotFoundError(f"Processed file not found: {default_processed}")

    df_processed = pd.read_csv(default_processed, keep_default_na=False)
    load_data(df_processed)
