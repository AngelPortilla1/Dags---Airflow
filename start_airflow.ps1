# Navega a la carpeta de trabajo
cd Apache Airflow

# Levanta los contenedores
docker compose up -d

Start-Sleep -Seconds 10

# Inicializa la base de datos de Airflow
docker compose exec airflow airflow db init

# Crea un usuario administrador
docker compose exec airflow airflow users create `
    --username admin `
    --password admin `
    --firstname Admin `
    --lastname User `
    --role Admin `
    --email admin@example.com

Write-Host ✔ Apache Airflow está listo en httplocalhost8080
