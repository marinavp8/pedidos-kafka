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


CREATE TABLE producto_stock (
    producto_id uuid PRIMARY KEY,
    nombre      text        NOT NULL DEFAULT '',
    disponible  int         NOT NULL CHECK (disponible >= 0),
    reservado   int         NOT NULL DEFAULT 0 CHECK (reservado >= 0),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE reserva (
    id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    saga_id    uuid        NOT NULL UNIQUE,
    pedido_id  uuid        NOT NULL,
    estado     text        NOT NULL CHECK (estado IN ('RESERVADA','LIBERADA','RECHAZADA')),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE reserva_linea (
    reserva_id  uuid NOT NULL REFERENCES reserva(id) ON DELETE CASCADE,
    producto_id uuid NOT NULL REFERENCES producto_stock(producto_id),
    cantidad    int  NOT NULL CHECK (cantidad > 0),
    PRIMARY KEY (reserva_id, producto_id)
);
