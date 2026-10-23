CREATE TABLE processed_message (
    message_id   uuid        PRIMARY KEY,
    consumer     text        NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX processed_message_at_idx ON processed_message (processed_at);

CREATE TABLE notificacion (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    pedido_id   uuid        NOT NULL,
    cliente_id  uuid        NOT NULL,
    tipo        text        NOT NULL CHECK (tipo IN ('PEDIDO_CONFIRMADO','PEDIDO_CANCELADO')),
    canal       text        NOT NULL CHECK (canal IN ('EMAIL','PUSH')),
    estado      text        NOT NULL CHECK (estado IN ('PENDIENTE','ENVIADA','FALLIDA')),
    intentos    int         NOT NULL DEFAULT 0,
    created_at  timestamptz NOT NULL DEFAULT now(),
    enviada_at  timestamptz,
    UNIQUE (pedido_id, tipo, canal)
);
