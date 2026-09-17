from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import csv
import json
import os

# Carpeta donde están los dags, dentro del contenedor
DAGS_FOLDER = os.path.dirname(os.path.realpath(__file__))

def descargar_datos():
    url = "https://jsonplaceholder.typicode.com/users"
    response = requests.get(url)
    response.raise_for_status()
    with open(os.path.join(DAGS_FOLDER, "datos.json"), "w") as f:
        f.write(response.text)

def procesar_datos():
    with open(os.path.join(DAGS_FOLDER, "datos.json"), "r") as f:
        data = json.load(f)
    with open(os.path.join(DAGS_FOLDER, "usuarios.csv"), "w", newline="") as csvfile:
        fieldnames = ['id', 'name', 'email']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for user in data:
            writer.writerow({
                'id': user['id'],
                'name': user['name'],
                'email': user['email']
            })

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG(
    dag_id='mini_proyecto_api',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Descarga y procesa datos de una API',
) as dag:
    tarea_descarga = PythonOperator(
        task_id='descargar_datos_api',
        python_callable=descargar_datos
    )
    tarea_procesar = PythonOperator(
        task_id='procesar_y_guardar_csv',
        python_callable=procesar_datos
    )
    tarea_descarga >> tarea_procesar
