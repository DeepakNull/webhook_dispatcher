-- Source of truth for delivery state. RabbitMQ is only the transport.

CREATE TYPE delivery_status AS ENUM ('PENDING', 'DELIVERED', 'RETRY', 'FAILED');


-- what a webhook looks like or what to send 
CREATE TABLE webhooks (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    subscriber_url TEXT        NOT NULL,
    payload        JSONB       NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- how the sending is going 
CREATE TABLE deliveries (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    webhook_id      UUID            NOT NULL REFERENCES webhooks(id),
    status          delivery_status NOT NULL DEFAULT 'PENDING',
    attempt_count   INT             NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT now()
);

-- One row per HTTP attempt: powers the "attempt history" in GET /events/{id}
-- this is the logbook
CREATE TABLE delivery_attempts (
    id             BIGSERIAL PRIMARY KEY,
    delivery_id    UUID        NOT NULL REFERENCES deliveries(id),
    attempt_number INT         NOT NULL,
    status_code    INT,                 -- NULL on timeout / connection error
    error          TEXT,
    duration_ms    INT,
    attempted_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_deliveries_status ON deliveries(status);
CREATE INDEX idx_attempts_delivery ON delivery_attempts(delivery_id);
