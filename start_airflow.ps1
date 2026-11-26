# Navega a la carpeta del script
Set-Location -Path $PSScriptRoot

# Construye y levanta los contenedores
docker compose up -d --build

Write-Host "Esperando a que PostgreSQL y Airflow inicien..."
Start-Sleep -Seconds 10

# Inicializa / migra la base de datos de Airflow
docker compose exec airflow-webserver airflow db migrate

# Crea un usuario administrador
docker compose exec airflow-webserver airflow users create `
    --username admin `
    --password admin `
    --firstname Admin `
    --lastname User `
    --role Admin `
    --email admin@example.com

Write-Host "✔ Apache Airflow está listo en http://localhost:8080 (usuario: admin / contraseña: admin)" -ForegroundColor Green

