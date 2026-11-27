#!/usr/bin/env bash
set -euo pipefail

DW_DB_NAME="${DW_DB:-crypto_dw}"

echo "Inicializando base de datos analítica: ${DW_DB_NAME}..."

psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${POSTGRES_DB}" <<-EOSQL
    SELECT 'CREATE DATABASE ${DW_DB_NAME}'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${DW_DB_NAME}')\gexec
EOSQL

psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${DW_DB_NAME}" -f /sql/schema.sql

echo "Base de datos analítica ${DW_DB_NAME} lista."
