# SRS — Plataforma de pedidos con microservicios, Kafka y PostgreSQL

| Campo | Valor |
|---|---|
| Versión | 1.0 (borrador) |
| Fecha | 2026-10-22 |
| Fuente | `kafka-en-microservicios.html` (guía interactiva de Kafka) |
| Documentos relacionados | `02-arquitectura.md`, `03-modelo-de-datos.md` |

## 1. Introducción

### 1.1 Propósito
Especificar los requisitos de un sistema de procesamiento de pedidos construido como microservicios que se comunican mediante Apache Kafka, donde cada servicio tiene su propia base de datos PostgreSQL. El sistema debe coordinar una operación de negocio distribuida (reservar stock, cobrar, enviar) **sin transacciones distribuidas**, usando una saga orquestada con compensaciones.

### 1.2 Problema que se resuelve
Una compra afecta a varios servicios con datos propios. No existe una transacción ACID que los abarque, y las llamadas síncronas encadenadas acoplan servicios y propagan fallos. Hay que garantizar:

1. Consistencia eventual: el pedido termina `CONFIRMADO` o `CANCELADO`, nunca a medias.
2. Que guardar un cambio y publicar su evento sea atómico (problema de la doble escritura).
3. Tolerancia a duplicados (Kafka entrega *at-least-once*) y a caídas de cualquier componente.
4. Visibilidad del estado de cada pedido en un único lugar.

### 1.3 Alcance
**Incluido:** creación y consulta de pedidos, saga de confirmación (inventario → pagos → envíos), compensaciones, notificaciones al cliente, observabilidad, operación del clúster Kafka.
**Excluido:** catálogo de productos, carrito, autenticación de usuarios finales (se delega en el API Gateway/IdP), pasarela de pago real (se integra vía adaptador), UI.

### 1.4 Definiciones
| Término | Significado |
|---|---|
| Saga | Secuencia de transacciones locales con compensaciones que sustituye a la transacción distribuida |
| Orquestador | Servicio que conoce el flujo completo, emite comandos y reacciona a respuestas |
| Compensación | Acción de negocio que anula el efecto de un paso ya confirmado (no es un rollback) |
| Outbox | Tabla en la BD del servicio donde se inserta el evento en la misma transacción que el cambio |
| Relay | Proceso que lee el outbox y publica en Kafka (poller o Debezium CDC) |
| ISR | In-Sync Replicas: réplicas sincronizadas con el líder de una partición |
| Lag | Offset último escrito − offset último leído por un grupo de consumidores |
| DLQ | Cola de mensajes muertos para lo irrecuperable |

## 2. Descripción general

### 2.1 Actores
- **Cliente** (web/móvil): crea y consulta pedidos.
- **Operador**: supervisa sagas, gestiona la DLQ, reintenta o cancela manualmente.
- **Pasarela de pago** y **transportista** (sistemas externos).

### 2.2 Servicios
| Servicio | Responsabilidad | BD propia (PostgreSQL) |
|---|---|---|
| API Gateway | Entrada REST, autenticación, enrutado | — |
| Pedidos | Ciclo de vida del pedido, outbox de `PedidoCreado` | `pedidos_db` |
| Orquestador | Máquina de estados de la saga | `saga_db` |
| Inventario | Stock y reservas | `inventario_db` |
| Pagos | Cobros y reembolsos | `pagos_db` |
| Envíos | Envíos y asignación de transportista | `envios_db` |
| Notificaciones | Email/push | `notificaciones_db` |

### 2.3 Flujo funcional
**Camino feliz:** `PedidoCreado` → reservar stock → cobrar → crear envío → pedido `CONFIRMADO` → notificar.

**Fallos y compensación (orden inverso, solo lo completado):**
| Fallo | Compensaciones |
|---|---|
| Stock insuficiente | Ninguna; pedido → `CANCELADO` |
| Pago rechazado | Liberar stock |
| Sin transportista | Reembolsar cobro, liberar stock |

El pedido nace en `PENDIENTE` y solo pasa a `CONFIRMADO` al final de la saga (sin aislamiento ACID entre pasos).

### 2.4 Suposiciones y restricciones
- Kafka ≥ 4.0 en modo KRaft (sin ZooKeeper).
- PostgreSQL ≥ 16 con `wal_level=logical` (necesario para Debezium).
- Un servicio nunca accede a la BD de otro; solo se comunican por Kafka (y REST síncrono para consultas del Gateway).
- Entrega *at-least-once*: todos los consumidores son idempotentes.

## 3. Requisitos funcionales

### 3.1 Pedidos
| ID | Requisito |
|---|---|
| RF-01 | El sistema permitirá crear un pedido con cliente, líneas (producto, cantidad, precio) y dirección de envío. |
| RF-02 | Al crear un pedido, este se persistirá en estado `PENDIENTE` y se insertará el evento `PedidoCreado` en la tabla outbox **en la misma transacción**. |
| RF-03 | El sistema permitirá consultar un pedido y su estado, incluyendo el historial de transiciones. |
| RF-04 | La creación aceptará una `Idempotency-Key`; repetirla devolverá el mismo pedido sin duplicarlo. |
| RF-05 | El pedido pasará a `CONFIRMADO` o `CANCELADO` solo al recibir el resultado final de la saga. |
| RF-06 | Un cliente podrá solicitar la cancelación de un pedido `PENDIENTE`; se tratará como fallo de la saga y se compensará. |

### 3.2 Orquestación
| ID | Requisito |
|---|---|
| RF-10 | El orquestador iniciará una saga por cada `PedidoCreado`, identificada por `sagaId` (clave = `pedidoId`). |
| RF-11 | Ejecutará los pasos en orden: reservar stock, cobrar, crear envío, enviando un comando por paso. |
| RF-12 | Persistirá cada transición de estado **antes** de enviar el siguiente comando. |
| RF-13 | Ante un fallo, ejecutará las compensaciones de los pasos completados, del último al primero. |
| RF-14 | Vigilará timeouts por paso y reintentará con espera creciente (*backoff*). |
| RF-15 | Agotados los reintentos, enviará el caso a la DLQ y generará una alerta para el operador. |
| RF-16 | Tras un reinicio, retomará cada saga desde su último estado conocido. |
| RF-17 | Ignorará respuestas duplicadas o fuera de orden para un paso ya resuelto. |

### 3.3 Inventario
| ID | Requisito |
|---|---|
| RF-20 | Reservará stock por línea de pedido de forma atómica (todo o nada); responderá `StockReservado` o `StockInsuficiente`. |
| RF-21 | Liberará una reserva al recibir `LiberarStock`; la liberación será idempotente. |
| RF-22 | Una reserva repetida para el mismo `(sagaId, paso)` no descontará stock dos veces. |

### 3.4 Pagos
| ID | Requisito |
|---|---|
| RF-30 | Cobrará el importe del pedido contra la pasarela; responderá `PagoCompletado` o `PagoRechazado`. |
| RF-31 | Reembolsará un cobro al recibir `ReembolsarPago`; cobro y reembolso quedarán ambos registrados. |
| RF-32 | Enviará a la pasarela una clave de idempotencia derivada de `sagaId` para evitar dobles cobros. |

### 3.5 Envíos
| ID | Requisito |
|---|---|
| RF-40 | Creará un envío asignando transportista; responderá `EnvioCreado` o `SinTransportista`. |
| RF-41 | Cancelará un envío al recibir `CancelarEnvio` (idempotente). |

### 3.6 Notificaciones
| ID | Requisito |
|---|---|
| RF-50 | Escuchará `pedidos.eventos` y notificará al cliente la confirmación o cancelación (email/push). |
| RF-51 | No enviará notificaciones duplicadas por el mismo evento. |

### 3.7 Operación
| ID | Requisito |
|---|---|
| RF-60 | Habrá un endpoint/consola para listar sagas por estado, ver su traza y reintentar o cancelar las bloqueadas. |
| RF-61 | La DLQ será consultable y los mensajes reprocesables. |

## 4. Reglas de diseño (requisitos de arquitectura obligatorios)

| ID | Regla |
|---|---|
| RA-01 | **Outbox:** todo evento/respuesta se inserta en la tabla `outbox` de la BD del servicio en la misma transacción que el cambio de estado. Ningún servicio publica en Kafka directamente desde la lógica de negocio. |
| RA-02 | **Idempotencia:** cada consumidor registra `(message_id)` o `(saga_id, paso)` procesado en la misma transacción que su efecto y descarta repetidos. |
| RA-03 | **Clave por saga:** todos los mensajes de un pedido usan `pedidoId` como clave Kafka para conservar su orden dentro de la partición. |
| RA-04 | **Esquemas versionados:** Avro o Protobuf con Schema Registry; compatibilidad `BACKWARD` como mínimo. |
| RA-05 | **Database per service:** sin joins ni FKs entre bases de datos; referencias por identificador. |
| RA-06 | **Compensaciones idempotentes**, reintentables con *backoff*, y a DLQ si se agotan. |
| RA-07 | **Orden de pasos:** primero los reversibles (stock), luego el pivote (cobro), al final los reintentables (envío). |

## 5. Requisitos no funcionales

### 5.1 Fiabilidad y consistencia
| ID | Requisito |
|---|---|
| RNF-01 | Productores con `acks=all`, `enable.idempotence=true`. |
| RNF-02 | Topics con `replication.factor=3` y `min.insync.replicas=2`; el clúster tolera la caída de 1 broker sin perder escrituras ni mensajes confirmados. |
| RNF-03 | Ningún evento confirmado se pierde ante la caída de un servicio: lo pendiente permanece en el outbox y se publica al arrancar. |
| RNF-04 | `enable.auto.commit=false`; el offset se confirma tras persistir el efecto. |
| RNF-05 | El orquestador se ejecutará con ≥ 2 réplicas (sin punto único de fallo); la concurrencia por saga se controla con bloqueo optimista (`version`) en `saga_state`. |

### 5.2 Rendimiento
| ID | Requisito |
|---|---|
| RNF-10 | Objetivo inicial: 200 pedidos/s sostenidos; P95 de saga completa (camino feliz) < 2 s. |
| RNF-11 | Lag de cualquier grupo de consumidores < 1000 mensajes en régimen normal; alerta si crece de forma sostenida. |
| RNF-12 | Topics de comandos/eventos con 6 particiones iniciales (paralelismo máximo por grupo = nº de particiones). |

### 5.3 Seguridad
| ID | Requisito |
|---|---|
| RNF-20 | Autenticación JWT en el Gateway; autorización por rol (cliente, operador). |
| RNF-21 | Kafka con TLS y SASL (SCRAM o mTLS) y ACLs por servicio: cada servicio solo produce/consume sus topics. |
| RNF-22 | PostgreSQL con un rol por servicio sin privilegios sobre otras BD; TLS en conexiones. |
| RNF-23 | Secretos fuera del repositorio (gestor de secretos / `.env` local del host); datos de tarjeta nunca almacenados (tokenización en la pasarela). |
| RNF-24 | Datos personales (cliente, dirección) minimizados en los eventos; solo identificadores cuando sea posible. |

### 5.4 Observabilidad
| ID | Requisito |
|---|---|
| RNF-30 | Trazas distribuidas (OpenTelemetry) con `traceId` propagado en cabeceras Kafka y `sagaId` en logs. |
| RNF-31 | Métricas: lag por grupo, ISR/under-replicated partitions, tamaño y antigüedad del outbox, sagas por estado y duración, tamaño de DLQ. |
| RNF-32 | Alertas: lag creciente, outbox con mensajes > 30 s sin publicar, saga bloqueada > timeout, DLQ > 0. |

### 5.5 Mantenibilidad y despliegue
| ID | Requisito |
|---|---|
| RNF-40 | Cada servicio es desplegable de forma independiente (contenedor) y gestiona sus migraciones (Flyway/Liquibase). |
| RNF-41 | Despliegue con Docker Compose por servidor (`~/docker-stack/`), Traefik como proxy; en producción, orquestador de contenedores a elección. |
| RNF-42 | Evolución de esquemas de eventos sin romper consumidores (RA-04). |

### 5.6 Retención y datos
| ID | Requisito |
|---|---|
| RNF-50 | Retención de topics de eventos: 7 días (`retention.ms`); permite a un servicio nuevo releer la historia reciente. |
| RNF-51 | Las filas del outbox publicadas se purgan tras 7 días; los registros de mensajes procesados se purgan tras un periodo mayor que la retención del topic. |

## 6. Escenarios de aceptación

| ID | Escenario | Resultado esperado |
|---|---|---|
| AC-01 | Camino feliz | Pedido `CONFIRMADO`; stock descontado, cobro registrado, envío creado, 1 notificación. |
| AC-02 | Stock insuficiente | Pedido `CANCELADO`; no hay cobro ni envío; sin compensaciones. |
| AC-03 | Pago rechazado | Stock liberado; pedido `CANCELADO`. |
| AC-04 | Sin transportista | Cobro y reembolso registrados; stock liberado; pedido `CANCELADO`. |
| AC-05 | Caída de Pedidos tras crear el pedido | Al reiniciar se publica `PedidoCreado` desde el outbox; la saga arranca una sola vez. |
| AC-06 | `PedidoCreado` duplicado | El orquestador crea una única saga. |
| AC-07 | Comando duplicado a Pagos | Un solo cobro. |
| AC-08 | Caída del orquestador a mitad de saga | Al reiniciar (o en otra réplica) la saga continúa desde su último estado. |
| AC-09 | Caída de 1 broker | Producción y consumo continúan; elección de nuevo líder automática; sin pérdida de mensajes confirmados. |
| AC-10 | Compensación que falla repetidamente | Reintentos con *backoff*; luego DLQ y alerta. |
| AC-11 | Reinicio de un consumidor | Rebalanceo; continúa desde el último offset confirmado, sin pérdidas. |

## 7. Trazabilidad
Los RF se implementan en los servicios de la sección 2.2; las reglas RA-01…RA-07 se materializan en `02-arquitectura.md` (§4–§7) y las tablas `outbox`, `processed_message` y `saga_state` en `03-modelo-de-datos.md`.
