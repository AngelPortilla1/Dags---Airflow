from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import csv
import json
import os

DAGS_FOLDER = os.path.dirname(os.path.realpath(__file__))

def descargar_datos():
    url = "https://restcountries.com/v3.1/all"
    response = requests.get(url)
    response.raise_for_status()
    with open(os.path.join(DAGS_FOLDER, "paises.json"), "w", encoding="utf-8") as f:
        f.write(response.text)

def procesar_datos():
    with open(os.path.join(DAGS_FOLDER, "paises.json"), "r", encoding="utf-8") as f:
        data = json.load(f)
    with open(os.path.join(DAGS_FOLDER, "paises.csv"), "w", newline="", encoding="utf-8") as csvfile:
        fieldnames = ['name', 'region', 'population']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for country in data:
            writer.writerow({
                'name': country.get('name', {}).get('common', 'N/A'),
                'region': country.get('region', 'N/A'),
                'population': country.get('population', 0)
            })

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG(
    dag_id='proyecto_api_paises',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Descarga y procesa datos de una API pública de países',
) as dag:
    tarea_descarga = PythonOperator(
        task_id='descargar_datos_paises',
        python_callable=descargar_datos
    )
    tarea_procesar = PythonOperator(
        task_id='procesar_y_guardar_csv',
        python_callable=procesar_datos
    )
    tarea_descarga >> tarea_procesar
