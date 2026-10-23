-- Tablas comunes
CREATE TABLE outbox (
    seq            bigint      GENERATED ALWAYS AS IDENTITY,
    id             uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type text        NOT NULL,
    aggregate_id   text        NOT NULL,
    event_type     text        NOT NULL,
    payload        jsonb       NOT NULL,
    headers        jsonb       NOT NULL DEFAULT '{}',
    created_at     timestamptz NOT NULL DEFAULT now(),
    published_at   timestamptz
);
CREATE UNIQUE INDEX outbox_seq_idx ON outbox (seq);
CREATE INDEX outbox_pendientes_idx ON outbox (seq) WHERE published_at IS NULL;

CREATE TABLE processed_message (
    message_id   uuid        PRIMARY KEY,
    consumer     text        NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX processed_message_at_idx ON processed_message (processed_at);


CREATE TABLE saga_state (
    saga_id      uuid PRIMARY KEY,
    pedido_id    uuid        NOT NULL UNIQUE,
    estado       text        NOT NULL CHECK (estado IN (
                   'INICIADA','RESERVANDO_STOCK','COBRANDO','CREANDO_ENVIO',
                   'COMPLETADA','COMPENSANDO','CANCELADA','FALLIDA_MANUAL')),
    paso_actual  text,
    intentos     int         NOT NULL DEFAULT 0,
    deadline_at  timestamptz,
    contexto     jsonb       NOT NULL DEFAULT '{}',
    motivo_fallo text,
    version      int         NOT NULL DEFAULT 0,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX saga_vencidas_idx ON saga_state (deadline_at)
    WHERE estado NOT IN ('COMPLETADA','CANCELADA','FALLIDA_MANUAL');

CREATE TABLE saga_step (
    id        bigserial PRIMARY KEY,
    saga_id   uuid        NOT NULL REFERENCES saga_state(saga_id) ON DELETE CASCADE,
    paso      text        NOT NULL CHECK (paso IN ('RESERVAR_STOCK','COBRAR','CREAR_ENVIO')),
    tipo      text        NOT NULL CHECK (tipo IN ('ACCION','COMPENSACION')),
    resultado text        NOT NULL CHECK (resultado IN ('ENVIADO','OK','FALLO')),
    detalle   jsonb,
    fecha     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX saga_step_idx ON saga_step (saga_id, id);
