"""Configuración centralizada y gestión de rutas particionadas del pipeline ETL."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

COINGECKO_MARKETS_URL = "https://api.coingecko.com/api/v3/coins/markets"
DEFAULT_VS_CURRENCY = "usd"
DEFAULT_PER_PAGE = 20
HTTP_TIMEOUT: tuple[int, int] = (5, 30)  # (connect_timeout, read_timeout) en segundos


@dataclass(frozen=True)
class RunPaths:
    """Rutas de artefactos particionadas por fecha lógica (ds = YYYY-MM-DD)."""

    raw_json: Path
    processed_csv: Path
    summary_json: Path
    charts_dir: Path


def get_data_base_dir() -> Path:
    """Retorna el directorio raíz de datos fuera de `dags/`."""
    return Path(os.getenv("CRYPTO_DATA_DIR", "/opt/airflow/data"))


def get_dw_dsn() -> str:
    """Retorna la cadena de conexión PostgreSQL para el Data Warehouse `crypto_dw`."""
    dsn = os.getenv("CRYPTO_DW_DSN") or os.getenv("AIRFLOW_CONN_CRYPTO_DW")
    if not dsn:
        raise ValueError(
            "No se encontró la variable de entorno CRYPTO_DW_DSN ni AIRFLOW_CONN_CRYPTO_DW."
        )
    return dsn


def get_run_paths(ds: str, base_dir: Path | None = None) -> RunPaths:
    """Construye rutas idempotentes particionadas por fecha lógica `dt=<ds>`."""
    root = base_dir if base_dir is not None else get_data_base_dir()
    partition = f"dt={ds}"
    return RunPaths(
        raw_json=root / "raw" / partition / "crypto_full.json",
        processed_csv=root / "processed" / partition / "crypto_processed.csv",
        summary_json=root / "processed" / partition / "analysis_summary.json",
        charts_dir=root / "charts" / partition,
    )
