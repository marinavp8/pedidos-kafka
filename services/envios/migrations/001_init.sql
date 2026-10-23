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


CREATE TABLE transportista (
    id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    nombre           text    NOT NULL,
    activo           boolean NOT NULL DEFAULT true,
    capacidad_diaria int     NOT NULL CHECK (capacidad_diaria >= 0),
    paises           text[]  NOT NULL DEFAULT '{ES}'
);

CREATE TABLE envio (
    id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    saga_id          uuid        NOT NULL UNIQUE,
    pedido_id        uuid        NOT NULL,
    transportista_id uuid REFERENCES transportista(id),
    estado           text        NOT NULL CHECK (estado IN ('CREADO','CANCELADO','SIN_TRANSPORTISTA')),
    direccion        jsonb       NOT NULL,
    tracking         text,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX envio_pedido_idx ON envio (pedido_id);
