"""Módulo de extracción desde la API pública de CoinGecko."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from crypto_etl.config import (
    COINGECKO_MARKETS_URL,
    DEFAULT_PER_PAGE,
    DEFAULT_VS_CURRENCY,
    HTTP_TIMEOUT,
    get_run_paths,
)

logger = logging.getLogger(__name__)


def build_http_session(max_retries: int = 3, backoff_factor: float = 1.5) -> requests.Session:
    """Crea una sesión HTTP con reintentos exponenciales ante HTTP 429 y errores 5xx."""
    retry_strategy = Retry(
        total=max_retries,
        connect=max_retries,
        read=max_retries,
        status=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"Accept": "application/json", "User-Agent": "crypto-market-etl/0.1"})
    return session


def fetch_market_data(
    vs_currency: str = DEFAULT_VS_CURRENCY,
    per_page: int = DEFAULT_PER_PAGE,
    page: int = 1,
    timeout: tuple[int, int] = HTTP_TIMEOUT,
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    """Consulta el endpoint `/coins/markets` de CoinGecko y valida la respuesta JSON."""
    params = {
        "vs_currency": vs_currency,
        "order": "market_cap_desc",
        "per_page": per_page,
        "page": page,
        "sparkline": "false",
    }

    own_session = session is None
    client = session or build_http_session()
    try:
        logger.info(
            "Consultando CoinGecko API (vs_currency=%s, per_page=%s, page=%s)",
            vs_currency,
            per_page,
            page,
        )
        response = client.get(COINGECKO_MARKETS_URL, params=params, timeout=timeout)
        response.raise_for_status()
        payload = response.json()
    except requests.exceptions.RequestException as exc:
        logger.error("Error HTTP consultando CoinGecko API: %s", exc)
        raise
    finally:
        if own_session:
            client.close()

    if not isinstance(payload, list) or len(payload) == 0:
        raise ValueError(
            "La respuesta de CoinGecko API no es una lista JSON válida o llegó vacía."
        )

    logger.info("Extracción completada: %d registros recibidos.", len(payload))
    return payload


def extract_to_file(
    ds: str,
    output_path: Path | None = None,
    per_page: int = DEFAULT_PER_PAGE,
    session: requests.Session | None = None,
) -> Path:
    """Extrae datos de CoinGecko y los persiste de forma atómica en JSON UTF-8."""
    target = output_path if output_path is not None else get_run_paths(ds).raw_json
    target.parent.mkdir(parents=True, exist_ok=True)

    records = fetch_market_data(per_page=per_page, session=session)

    tmp_path = target.with_suffix(".json.tmp")
    with tmp_path.open("w", encoding="utf-8") as handle:
        json.dump(records, handle, ensure_ascii=False, indent=2)
    os.replace(tmp_path, target)

    logger.info("Archivo raw guardado en %s (%d registros).", target, len(records))
    return target
