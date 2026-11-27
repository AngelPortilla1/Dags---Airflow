"""Módulo de visualización headless (Matplotlib Agg) sin duplicación de lógica."""

from __future__ import annotations

import logging
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.axes import Axes

from crypto_etl.config import get_run_paths

logger = logging.getLogger(__name__)


def _plot_top_prices(ax: Axes, df: pd.DataFrame, *, show_labels: bool = True) -> None:
    """Grafica el Top 10 real ordenado explícitamente por `current_price` descendente."""
    top_10 = df.dropna(subset=["current_price"]).nlargest(10, "current_price")
    colors = plt.cm.viridis(np.linspace(0, 1, max(len(top_10), 1)))

    bars = ax.bar(range(len(top_10)), top_10["current_price"], color=colors)
    ax.set_title("Top 10 Criptomonedas por Precio Actual (USD)", fontsize=14, fontweight="bold")
    ax.set_xlabel("Criptomonedas")
    ax.set_ylabel("Precio (USD)")
    ax.set_xticks(range(len(top_10)))
    ax.set_xticklabels(top_10["symbol"], rotation=45)
    ax.grid(True, alpha=0.3)

    if show_labels:
        for bar in bars:
            height = bar.get_height()
            ax.text(
                bar.get_x() + bar.get_width() / 2.0,
                height + height * 0.01,
                f"${height:,.2f}",
                ha="center",
                va="bottom",
                fontsize=8,
            )


def _plot_market_cap_distribution(ax: Axes, df: pd.DataFrame) -> None:
    """Grafica la distribución de Market Cap (Top 10 por market_cap + Otros)."""
    valid = df.dropna(subset=["market_cap"])
    valid = valid[valid["market_cap"] > 0].sort_values("market_cap", ascending=False)
    top_10 = valid.head(10)
    others = float(valid.iloc[10:]["market_cap"].sum()) if len(valid) > 10 else 0.0

    labels = list(top_10["name"])
    sizes = list(top_10["market_cap"])
    if others > 0:
        labels.append("Otros")
        sizes.append(others)

    if not sizes:
        ax.set_title("Sin datos positivos de Market Cap")
        return

    colors = plt.cm.Set3(np.linspace(0, 1, len(labels)))
    ax.pie(
        sizes,
        labels=labels,
        autopct="%1.1f%%",
        startangle=90,
        colors=colors,
        textprops={"fontsize": 9},
    )
    ax.set_title("Distribución de Market Cap - Top 10", fontsize=14, fontweight="bold")


def _plot_price_vs_volume(ax: Axes, df: pd.DataFrame, *, annotate_top: bool = True) -> None:
    """Grafica dispersión log-log de Precio vs Volumen filtrando valores estrictamente positivos."""
    valid = df.dropna(subset=["total_volume", "current_price"])
    valid = valid[(valid["total_volume"] > 0) & (valid["current_price"] > 0)]

    if valid.empty:
        ax.set_title("Sin datos positivos para escala logarítmica")
        return

    color_values = valid["price_change_pct_24h"].fillna(0)
    scatter = ax.scatter(
        valid["total_volume"],
        valid["current_price"],
        c=color_values,
        cmap="RdYlGn",
        alpha=0.7,
        s=70,
        edgecolors="black",
        linewidth=0.5,
    )
    ax.set_title("Precio vs Volumen (Color = Cambio 24h)", fontsize=14, fontweight="bold")
    ax.set_xlabel("Volumen Total (USD)")
    ax.set_ylabel("Precio (USD)")
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.grid(True, alpha=0.3)

    if annotate_top:
        for _, row in valid.head(5).iterrows():
            ax.annotate(
                row["symbol"],
                (row["total_volume"], row["current_price"]),
                xytext=(5, 5),
                textcoords="offset points",
                fontsize=8,
                bbox={"boxstyle": "round,pad=0.3", "facecolor": "yellow", "alpha": 0.7},
            )

    ax.figure.colorbar(scatter, ax=ax, label="Cambio 24h (%)")


def _plot_24h_changes(ax: Axes, df: pd.DataFrame, *, show_labels: bool = True) -> None:
    """Grafica cambios porcentuales de 24h ordenados ascendentemente."""
    valid = df.dropna(subset=["price_change_pct_24h"]).sort_values(
        "price_change_pct_24h", ascending=True
    )
    colors = ["red" if val < 0 else "green" for val in valid["price_change_pct_24h"]]
    bars = ax.barh(
        range(len(valid)),
        valid["price_change_pct_24h"],
        color=colors,
        alpha=0.7,
        edgecolor="black",
        linewidth=0.5,
    )
    ax.set_title("Cambio Porcentual 24h", fontsize=14, fontweight="bold")
    ax.set_xlabel("Cambio (%)")
    ax.set_ylabel("Criptomonedas")
    ax.set_yticks(range(len(valid)))
    ax.set_yticklabels(valid["symbol"], fontsize=8)
    ax.grid(True, alpha=0.3, axis="x")
    ax.axvline(x=0, color="black", linestyle="-", linewidth=1)

    if show_labels:
        for bar in bars:
            width = bar.get_width()
            ax.text(
                width + (0.2 if width >= 0 else -0.2),
                bar.get_y() + bar.get_height() / 2.0,
                f"{width:.1f}%",
                ha="left" if width >= 0 else "right",
                va="center",
                fontsize=8,
            )


def generate_all_charts(
    ds: str,
    processed_csv_path: Path | None = None,
    charts_dir: Path | None = None,
) -> list[Path]:
    """Genera los 4 gráficos individuales y el dashboard consolidado en `charts/dt=<ds>/`."""
    paths = get_run_paths(ds)
    source = processed_csv_path if processed_csv_path is not None else paths.processed_csv
    output_dir = charts_dir if charts_dir is not None else paths.charts_dir

    if not source.exists():
        raise FileNotFoundError(f"Archivo CSV procesado no encontrado para graficar: {source}")

    df = pd.read_csv(source, encoding="utf-8")
    if df.empty:
        raise ValueError("DataFrame vacío; no se pueden generar gráficos.")

    output_dir.mkdir(parents=True, exist_ok=True)
    generated_files: list[Path] = []

    single_specs = [
        ("01_precios_actuales.png", (12, 8), lambda ax: _plot_top_prices(ax, df)),
        (
            "02_market_cap_distribucion.png",
            (12, 8),
            lambda ax: _plot_market_cap_distribution(ax, df),
        ),
        ("03_precio_vs_volumen.png", (12, 8), lambda ax: _plot_price_vs_volume(ax, df)),
        ("04_cambios_24h.png", (12, 10), lambda ax: _plot_24h_changes(ax, df)),
    ]

    for filename, figsize, draw_fn in single_specs:
        fig, ax = plt.subplots(figsize=figsize)
        try:
            draw_fn(ax)
            fig.tight_layout()
            out_path = output_dir / filename
            fig.savefig(out_path, dpi=200, bbox_inches="tight")
            generated_files.append(out_path)
        finally:
            plt.close(fig)

    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(20, 16))
    try:
        fig.suptitle(f"Dashboard de Criptomonedas ({ds})", fontsize=18, fontweight="bold")
        _plot_top_prices(ax1, df, show_labels=False)
        _plot_market_cap_distribution(ax2, df)
        _plot_price_vs_volume(ax3, df, annotate_top=False)
        _plot_24h_changes(ax4, df, show_labels=False)
        fig.tight_layout()
        dashboard_path = output_dir / "00_dashboard_completo.png"
        fig.savefig(dashboard_path, dpi=200, bbox_inches="tight")
        generated_files.append(dashboard_path)
    finally:
        plt.close(fig)

    logger.info("Gráficos generados en %s (%d archivos).", output_dir, len(generated_files))
    return generated_files
