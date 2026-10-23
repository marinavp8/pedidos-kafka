# pedidos-kafka

Sistema de pedidos con microservicios, Kafka (saga orquestada + transactional outbox) y PostgreSQL (una base de datos por servicio).
Documentación de diseño en `docs/`: SRS, arquitectura, modelo de datos y [bitácora de la sesión](docs/bitacora-sesion.html) (HTML autónomo).

## Estructura

```
libs/common/          Librería compartida: outbox + relay, inbox idempotente, migraciones, kafka, bootstrap
services/
  gateway/            REST + JWT (HS256); único servicio expuesto
  pedidos/            API de pedidos · pedidos_db
  orquestador/        Máquina de estados de la saga + API de operación · saga_db
  inventario/         Reservas de stock · inventario_db
  pagos/              Cobros/reembolsos (pasarela simulada) · pagos_db
  envios/             Envíos y transportistas · envios_db
  notificaciones/     Notificaciones al cliente (canal de demostración: log) · notificaciones_db
  */migrations/       SQL versionado (se aplica al arrancar, con advisory lock)
  */seeds/            Datos de demo (solo si SEED_DEMO_DATA=true)
deploy/postgres-init.sh   Crea una BD y un rol por servicio
Dockerfile            Una imagen parametrizada: --build-arg SERVICE=<servicio>
compose.dev.yml       Entorno local (1 broker, 1 PostgreSQL)
ansible/              Despliegue en un VPS con Docker (3 brokers Kafka, Traefik, TLS)
scripts/              token.mjs (JWT) y smoke.mjs (pruebas extremo a extremo)
```

## Desarrollo local

```bash
docker compose -f compose.dev.yml up -d --build      # gateway en http://localhost:18080
node scripts/smoke.mjs                               # AC-01…AC-04, idempotencia y seguridad
docker compose -f compose.dev.yml down -v
```

Compilar sin Docker: `npm install && npm run build`.

## Web

El gateway sirve una interfaz en `/` (`services/gateway/public/index.html`, sin dependencias ni build):
- **Nuevo pedido**: formulario con escenarios predefinidos (camino feliz, stock insuficiente, pago rechazado, sin transportista) y seguimiento en directo.
- **Pedidos**: listado, detalle e historial; cancelación de pedidos pendientes.
- **Operación** (rol operador): sagas por estado, paso a paso con compensaciones, y reintento de las `FALLIDA_MANUAL`.

En local abre http://localhost:18080 y pulsa "Entrar como cliente/operador" (login de demostración, `DEV_LOGIN=true`, activo solo en `compose.dev.yml`). En un servidor, `DEV_LOGIN` está apagado (`demo_login: false` en Ansible) y se entra pegando un token JWT. `demo_login: true` permite que cualquiera obtenga un token de operador: solo para demos.

## API (vía gateway, cabecera `Authorization: Bearer <jwt>`)

| Método | Ruta | Descripción |
|---|---|---|
| GET | `/api/pedidos` | Lista tus pedidos (operador: todos, últimos 50) |
| POST | `/api/pedidos` | Crea un pedido (`Idempotency-Key` opcional). Responde `202 {pedidoId, estado:"PENDIENTE"}` |
| GET | `/api/pedidos/:id` | Pedido, líneas e historial |
| POST | `/api/pedidos/:id/cancelar` | Solicita cancelación (compensa lo hecho) |
| GET | `/api/sagas?estado=` | **operador**: lista de sagas |
| GET | `/api/sagas/:pedidoId` | **operador**: saga y traza de pasos |
| POST | `/api/sagas/:sagaId/reintentar` | **operador**: reanuda una saga en `FALLIDA_MANUAL` |

```bash
TOKEN=$(JWT_SECRET=dev-secret-no-usar-en-produccion node scripts/token.mjs)   # cliente nuevo
curl -s -X POST localhost:18080/api/pedidos -H "authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"lineas":[{"productoId":"11111111-1111-4111-8111-111111111111","cantidad":1,"precioUnitario":900}],
       "direccionEnvio":{"calle":"Gran Vía 1","ciudad":"Madrid","pais":"ES"}}'
```

Datos de demo: productos `1111…` (100 uds), `2222…` (500), `3333…` (2 uds → fuerza "stock insuficiente"); país `JP` no tiene transportista; importes > 5000 los rechaza la pasarela simulada.

## Variables de entorno principales

| Variable | Servicios | Descripción |
|---|---|---|
| `DATABASE_URL` | todos menos gateway | Conexión a su base de datos |
| `KAFKA_BROKERS`, `KAFKA_RF`, `KAFKA_MIN_ISR`, `KAFKA_PARTITIONS` | todos menos gateway | Clúster y configuración de topics |
| `JWT_SECRET` | gateway | Clave HS256 |
| `SEED_DEMO_DATA` | inventario, envios | `true` carga datos de demo |
| `SAGA_STEP_TIMEOUT_MS`, `SAGA_MAX_INTENTOS` | orquestador | Timeout por paso (con backoff exponencial) y reintentos |
| `PAGO_LIMITE` | pagos | Umbral de rechazo de la pasarela simulada |

## Qué está implementado y qué es simulado

Implementado: saga orquestada con compensaciones en orden inverso, outbox transaccional con relay, consumidores idempotentes (`processed_message` + únicos de negocio), timeouts con reintentos y paso a `FALLIDA_MANUAL`/DLQ, cancelación por el cliente, optimistic locking, orquestador con 2 réplicas, RF=3/`min.insync.replicas=2`.

Simulado o pendiente: pasarela de pago (adaptador en `services/pagos`), canal de notificaciones (log), TLS/SASL en Kafka (red interna Docker sin exponer puertos), Schema Registry (mensajes JSON), métricas/trazas OpenTelemetry.
