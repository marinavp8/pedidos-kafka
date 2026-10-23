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


CREATE TABLE cobro (
    id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    saga_id             uuid          NOT NULL UNIQUE,
    pedido_id           uuid          NOT NULL,
    importe             numeric(12,2) NOT NULL CHECK (importe > 0),
    moneda              char(3)       NOT NULL,
    estado              text          NOT NULL CHECK (estado IN ('PENDIENTE','COMPLETADO','RECHAZADO','REEMBOLSADO')),
    referencia_pasarela text,
    motivo_rechazo      text,
    created_at          timestamptz   NOT NULL DEFAULT now(),
    updated_at          timestamptz   NOT NULL DEFAULT now()
);

CREATE TABLE reembolso (
    id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    cobro_id            uuid          NOT NULL UNIQUE REFERENCES cobro(id),
    importe             numeric(12,2) NOT NULL CHECK (importe > 0),
    estado              text          NOT NULL CHECK (estado IN ('PENDIENTE','COMPLETADO','FALLIDO')),
    referencia_pasarela text,
    created_at          timestamptz   NOT NULL DEFAULT now()
);
