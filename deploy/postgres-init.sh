#!/bin/sh
# Se ejecuta solo en la primera inicialización del volumen de PostgreSQL.
# Crea una base de datos y un rol propietario por microservicio (database per service).
set -eu

for entry in pedidos:PEDIDOS saga:SAGA inventario:INVENTARIO pagos:PAGOS envios:ENVIOS notificaciones:NOTIFICACIONES; do
  nombre="${entry%%:*}"
  var="PG_PASS_${entry##*:}"
  eval "pass=\${$var}"
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<SQL
CREATE ROLE ${nombre}_app LOGIN PASSWORD '${pass}';
CREATE DATABASE ${nombre}_db OWNER ${nombre}_app;
REVOKE ALL ON DATABASE ${nombre}_db FROM PUBLIC;
SQL
done
