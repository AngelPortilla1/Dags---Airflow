"""Módulo de carga transaccional e idempotente hacia PostgreSQL (`crypto_dw`)."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from crypto_etl.config import get_dw_dsn, get_run_paths

logger = logging.getLogger(__name__)

UPSERT_DIM_SQL = """
INSERT INTO dim_cryptocurrencies (coin_id, symbol, name, updated_at)
VALUES %s
ON CONFLICT (coin_id) DO UPDATE SET
    symbol = EXCLUDED.symbol,
    name = EXCLUDED.name,
    updated_at = NOW();
"""

UPSERT_FACT_SQL = """
INSERT INTO fact_crypto_market_snapshots (
    coin_id,
    snapshot_date,
    current_price,
    market_cap,
    total_volume,
    price_change_pct_24h,
    market_cap_rank,
    high_24h,
    low_24h,
    circulating_supply,
    extracted_at
)
VALUES %s
ON CONFLICT (coin_id, snapshot_date) DO UPDATE SET
    current_price = EXCLUDED.current_price,
    market_cap = EXCLUDED.market_cap,
    total_volume = EXCLUDED.total_volume,
    price_change_pct_24h = EXCLUDED.price_change_pct_24h,
    market_cap_rank = EXCLUDED.market_cap_rank,
    high_24h = EXCLUDED.high_24h,
    low_24h = EXCLUDED.low_24h,
    circulating_supply = EXCLUDED.circulating_supply,
    extracted_at = NOW();
"""


def _to_sql_value(value: Any) -> Any:
    """Convierte valores NaN/pd.NA de pandas a None (NULL en PostgreSQL)."""
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        return value.item()
    return value


def prepare_dim_rows(df: pd.DataFrame) -> list[tuple[Any, ...]]:
    """Prepara tuplas para UPSERT en `dim_cryptocurrencies`."""
    return [
        (
            _to_sql_value(row["coin_id"]),
            _to_sql_value(row["symbol"]),
            _to_sql_value(row["name"]),
        )
        for _, row in df[["coin_id", "symbol", "name"]].iterrows()
    ]


def prepare_fact_rows(df: pd.DataFrame, snapshot_date: str) -> list[tuple[Any, ...]]:
    """Prepara tuplas para UPSERT en `fact_crypto_market_snapshots`."""
    return [
        (
            _to_sql_value(row["coin_id"]),
            snapshot_date,
            _to_sql_value(row["current_price"]),
            _to_sql_value(row["market_cap"]),
            _to_sql_value(row["total_volume"]),
            _to_sql_value(row["price_change_pct_24h"]),
            _to_sql_value(row["market_cap_rank"]),
            _to_sql_value(row["high_24h"]),
            _to_sql_value(row["low_24h"]),
            _to_sql_value(row["circulating_supply"]),
        )
        for _, row in df.iterrows()
    ]


def load_market_snapshot(
    df: pd.DataFrame,
    snapshot_date: str,
    dsn: str | None = None,
) -> int:
    """Carga un snapshot diario en `crypto_dw` dentro de una única transacción atómica."""
    if df.empty:
        raise ValueError("No se puede cargar un DataFrame vacío al Data Warehouse.")

    connection_dsn = dsn or get_dw_dsn()
    dim_rows = prepare_dim_rows(df)
    fact_rows = prepare_fact_rows(df, snapshot_date=snapshot_date)

    conn = psycopg2.connect(connection_dsn)
    try:
        with conn:
            with conn.cursor() as cursor:
                execute_values(
                    cursor,
                    UPSERT_DIM_SQL,
                    dim_rows,
                    template="(%s, %s, %s, NOW())",
                )
                execute_values(
                    cursor,
                    UPSERT_FACT_SQL,
                    fact_rows,
                    template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())",
                )
        logger.info(
            "Carga SQL completada para snapshot_date=%s: %d registros en dim/fact.",
            snapshot_date,
            len(fact_rows),
        )
        return len(fact_rows)
    finally:
        conn.close()


def load_csv_to_dw(
    ds: str,
    processed_csv_path: Path | None = None,
    dsn: str | None = None,
) -> int:
    """Lee el CSV procesado de la fecha lógica `ds` y lo carga de forma idempotente en Postgres."""
    source = (
        processed_csv_path if processed_csv_path is not None else get_run_paths(ds).processed_csv
    )
    if not source.exists():
        raise FileNotFoundError(f"Archivo CSV procesado no encontrado para carga: {source}")

    df = pd.read_csv(source, encoding="utf-8")
    return load_market_snapshot(df=df, snapshot_date=ds, dsn=dsn)
