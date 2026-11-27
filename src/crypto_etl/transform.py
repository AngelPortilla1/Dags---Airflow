"""Módulo de transformación, limpieza y análisis estadístico de datos de mercado."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

import pandas as pd

from crypto_etl.config import get_run_paths

logger = logging.getLogger(__name__)

OUTPUT_COLUMNS: list[str] = [
    "coin_id",
    "symbol",
    "name",
    "snapshot_date",
    "current_price",
    "market_cap",
    "total_volume",
    "price_change_pct_24h",
    "market_cap_rank",
    "high_24h",
    "low_24h",
    "circulating_supply",
]

NUMERIC_COLUMNS: list[str] = [
    "current_price",
    "market_cap",
    "total_volume",
    "price_change_pct_24h",
    "market_cap_rank",
    "high_24h",
    "low_24h",
    "circulating_supply",
]


def transform_market_records(
    records: list[dict[str, Any]],
    snapshot_date: str,
) -> pd.DataFrame:
    """Normaliza registros crudos de CoinGecko preservando nulos reales y deduplicando por coin_id.

    A diferencia de convertir `None` a `0`, mantener `NaN`/`NULL` evita sesgar promedios,
    mínimos y escalas logarítmicas.
    """
    if not records:
        raise ValueError("No hay registros crudos para transformar.")

    normalized_rows: list[dict[str, Any]] = []
    for coin in records:
        if not isinstance(coin, dict):
            continue
        raw_id = coin.get("id")
        coin_id = str(raw_id).strip().lower() if raw_id is not None else ""
        if not coin_id:
            logger.warning("Registro omitido por carecer de identificador 'id': %s", coin)
            continue

        raw_symbol = coin.get("symbol")
        raw_name = coin.get("name")

        normalized_rows.append(
            {
                "coin_id": coin_id,
                "symbol": str(raw_symbol).strip().upper() if raw_symbol is not None else None,
                "name": str(raw_name).strip() if raw_name is not None else None,
                "snapshot_date": snapshot_date,
                "current_price": coin.get("current_price"),
                "market_cap": coin.get("market_cap"),
                "total_volume": coin.get("total_volume"),
                "price_change_pct_24h": coin.get("price_change_percentage_24h"),
                "market_cap_rank": coin.get("market_cap_rank"),
                "high_24h": coin.get("high_24h"),
                "low_24h": coin.get("low_24h"),
                "circulating_supply": coin.get("circulating_supply"),
            }
        )

    if not normalized_rows:
        raise ValueError("Ningún registro crudo contenía un 'coin_id' válido.")

    df = pd.DataFrame(normalized_rows, columns=OUTPUT_COLUMNS)

    for col in NUMERIC_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["market_cap_rank"] = df["market_cap_rank"].astype("Int64")

    before_dedup = len(df)
    df = df.drop_duplicates(subset=["coin_id"], keep="first").reset_index(drop=True)
    duplicates_removed = before_dedup - len(df)
    if duplicates_removed > 0:
        logger.warning("Se eliminaron %d registros duplicados por coin_id.", duplicates_removed)

    logger.info(
        "Transformación completada: %d registros recibidos -> %d registros limpios.",
        len(records),
        len(df),
    )
    return df


def compute_market_summary(df: pd.DataFrame, snapshot_date: str) -> dict[str, Any]:
    """Calcula estadísticas resumen del mercado usando la fecha lógica `snapshot_date`."""
    if df.empty:
        raise ValueError("No se puede calcular resumen estadístico sobre un DataFrame vacío.")

    valid_prices = df.dropna(subset=["current_price"])
    if valid_prices.empty:
        max_price_coin = None
        max_price = None
    else:
        max_idx = valid_prices["current_price"].idxmax()
        max_price_coin = str(valid_prices.loc[max_idx, "name"])
        max_price = float(valid_prices.loc[max_idx, "current_price"])

    total_market_cap = float(df["market_cap"].dropna().sum())

    valid_changes = df.dropna(subset=["price_change_pct_24h"])
    if valid_changes.empty:
        avg_change_24h = None
        top_gainer = None
        top_gainer_change = None
        top_loser = None
        top_loser_change = None
    else:
        avg_change_24h = float(valid_changes["price_change_pct_24h"].mean())
        gainer_idx = valid_changes["price_change_pct_24h"].idxmax()
        loser_idx = valid_changes["price_change_pct_24h"].idxmin()
        top_gainer = str(valid_changes.loc[gainer_idx, "name"])
        top_gainer_change = float(valid_changes.loc[gainer_idx, "price_change_pct_24h"])
        top_loser = str(valid_changes.loc[loser_idx, "name"])
        top_loser_change = float(valid_changes.loc[loser_idx, "price_change_pct_24h"])

    return {
        "snapshot_date": snapshot_date,
        "total_cryptos": int(len(df)),
        "max_price_coin": max_price_coin,
        "max_price": max_price,
        "total_market_cap": total_market_cap,
        "avg_change_24h": avg_change_24h,
        "top_gainer": top_gainer,
        "top_gainer_change": top_gainer_change,
        "top_loser": top_loser,
        "top_loser_change": top_loser_change,
    }


def transform_file(
    ds: str,
    raw_json_path: Path | None = None,
    processed_csv_path: Path | None = None,
) -> Path:
    """Lee el archivo JSON crudo, lo transforma y escribe el CSV limpio de forma atómica."""
    paths = get_run_paths(ds)
    source = raw_json_path if raw_json_path is not None else paths.raw_json
    target = processed_csv_path if processed_csv_path is not None else paths.processed_csv

    if not source.exists():
        raise FileNotFoundError(f"Archivo JSON crudo no encontrado: {source}")

    with source.open("r", encoding="utf-8") as handle:
        records = json.load(handle)

    df = transform_market_records(records=records, snapshot_date=ds)

    target.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = target.with_suffix(".csv.tmp")
    df.to_csv(tmp_path, index=False, encoding="utf-8")
    os.replace(tmp_path, target)

    logger.info("CSV procesado guardado en %s (%d filas).", target, len(df))
    return target


def analyze_file(
    ds: str,
    processed_csv_path: Path | None = None,
    summary_json_path: Path | None = None,
) -> Path:
    """Calcula métricas resumen a partir del CSV procesado y las guarda en JSON."""
    paths = get_run_paths(ds)
    source = processed_csv_path if processed_csv_path is not None else paths.processed_csv
    target = summary_json_path if summary_json_path is not None else paths.summary_json

    if not source.exists():
        raise FileNotFoundError(f"Archivo CSV procesado no encontrado: {source}")

    df = pd.read_csv(source, encoding="utf-8")
    summary = compute_market_summary(df=df, snapshot_date=ds)

    target.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = target.with_suffix(".json.tmp")
    with tmp_path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, ensure_ascii=False)
    os.replace(tmp_path, target)

    logger.info(
        "Resumen [%s]: total=%s, max=%s (%s), market_cap=%.0f, avg_24h=%s",
        ds,
        summary["total_cryptos"],
        summary["max_price_coin"],
        summary["max_price"],
        summary["total_market_cap"],
        summary["avg_change_24h"],
    )
    return target
