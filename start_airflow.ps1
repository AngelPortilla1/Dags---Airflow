# Navega a la carpeta raíz del proyecto
Set-Location -Path $PSScriptRoot

if (-not (Test-Path ".env")) {
    Write-Host "ERROR: No se encontró el archivo .env. Copia .env.example a .env y configura tus variables." -ForegroundColor Red
    exit 1
}

Write-Host "Construyendo e iniciando servicios de Apache Airflow y PostgreSQL..." -ForegroundColor Cyan
docker compose up -d --build

if ($LASTEXITCODE -eq 0) {
    Write-Host "Servicios iniciados. Verifica el estado con: docker compose ps" -ForegroundColor Green
    Write-Host "UI de Airflow disponible en http://localhost:8080 una vez que airflow-init y webserver estén saludables." -ForegroundColor Green
}
