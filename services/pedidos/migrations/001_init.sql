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


CREATE TABLE pedido (
    id                 uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    cliente_id         uuid          NOT NULL,
    estado             text          NOT NULL DEFAULT 'PENDIENTE'
                       CHECK (estado IN ('PENDIENTE','CONFIRMADO','CANCELADO')),
    total              numeric(12,2) NOT NULL CHECK (total >= 0),
    moneda             char(3)       NOT NULL DEFAULT 'EUR',
    direccion_envio    jsonb         NOT NULL,
    motivo_cancelacion text,
    created_at         timestamptz   NOT NULL DEFAULT now(),
    updated_at         timestamptz   NOT NULL DEFAULT now(),
    version            int           NOT NULL DEFAULT 0
);
CREATE INDEX pedido_cliente_idx ON pedido (cliente_id, created_at DESC);
CREATE INDEX pedido_estado_idx  ON pedido (estado) WHERE estado = 'PENDIENTE';

CREATE TABLE pedido_linea (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    pedido_id       uuid          NOT NULL REFERENCES pedido(id) ON DELETE CASCADE,
    producto_id     uuid          NOT NULL,
    cantidad        int           NOT NULL CHECK (cantidad > 0),
    precio_unitario numeric(12,2) NOT NULL CHECK (precio_unitario >= 0),
    UNIQUE (pedido_id, producto_id)
);

CREATE TABLE pedido_historial (
    id              bigserial PRIMARY KEY,
    pedido_id       uuid        NOT NULL REFERENCES pedido(id) ON DELETE CASCADE,
    estado_anterior text,
    estado_nuevo    text        NOT NULL,
    fecha           timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX pedido_historial_idx ON pedido_historial (pedido_id, fecha);

CREATE TABLE idempotency_key (
    cliente_id  uuid        NOT NULL,
    key         text        NOT NULL,
    pedido_id   uuid        NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (cliente_id, key)
);
