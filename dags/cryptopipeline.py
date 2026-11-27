"""DAG de orquestación del pipeline diario de mercado de criptomonedas.

Mantiene la definición del DAG ligera (sin imports pesados en top-level) delegando
toda la lógica de negocio a `src/crypto_etl`.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task

DEFAULT_ARGS = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=5),
}


@dag(
    dag_id="crypto_dashboard_completo",
    default_args=DEFAULT_ARGS,
    description="Pipeline ETL diario e idempotente de criptomonedas (CoinGecko -> Postgres DW)",
    schedule="@daily",
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    tags=["crypto", "etl", "postgres", "analytics"],
)
def crypto_market_etl_dag():
    """Define el flujo Extract -> Transform -> Load (Postgres) -> Analyze & Visualize."""

    @task(task_id="descargar_datos_coingecko")
    def extract_task(ds: str | None = None) -> str:
        from crypto_etl.extract import extract_to_file

        if ds is None:
            raise ValueError("Airflow no proporcionó la fecha lógica 'ds'.")
        raw_path = extract_to_file(ds=ds)
        return str(raw_path)

    @task(task_id="procesar_datos_csv")
    def transform_task(raw_json_path: str, ds: str | None = None) -> str:
        from crypto_etl.transform import transform_file

        if ds is None:
            raise ValueError("Airflow no proporcionó la fecha lógica 'ds'.")
        processed_path = transform_file(ds=ds, raw_json_path=Path(raw_json_path))
        return str(processed_path)

    @task(task_id="cargar_datos_postgres")
    def load_task(processed_csv_path: str, ds: str | None = None) -> int:
        from crypto_etl.load import load_csv_to_dw

        if ds is None:
            raise ValueError("Airflow no proporcionó la fecha lógica 'ds'.")
        return load_csv_to_dw(ds=ds, processed_csv_path=Path(processed_csv_path))

    @task(task_id="analizar_estadisticas")
    def analyze_task(processed_csv_path: str, ds: str | None = None) -> str:
        from crypto_etl.transform import analyze_file

        if ds is None:
            raise ValueError("Airflow no proporcionó la fecha lógica 'ds'.")
        summary_path = analyze_file(ds=ds, processed_csv_path=Path(processed_csv_path))
        return str(summary_path)

    @task(task_id="generar_visualizaciones")
    def visualize_task(processed_csv_path: str, ds: str | None = None) -> list[str]:
        from crypto_etl.visualize import generate_all_charts

        if ds is None:
            raise ValueError("Airflow no proporcionó la fecha lógica 'ds'.")
        chart_paths = generate_all_charts(ds=ds, processed_csv_path=Path(processed_csv_path))
        return [str(path) for path in chart_paths]

    raw_file = extract_task()
    processed_file = transform_task(raw_file)
    loaded_rows = load_task(processed_file)

    # Los artefactos analíticos y gráficos se generan solo tras confirmar la carga en Postgres DW
    loaded_rows >> [analyze_task(processed_file), visualize_task(processed_file)]


crypto_market_etl_dag()
