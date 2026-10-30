CREATE TABLE orders (
    id               UUID           PRIMARY KEY,
    customer_id      UUID           NOT NULL,
    client_reference TEXT           NOT NULL UNIQUE,
    total            NUMERIC(19, 2) NOT NULL,
    status           TEXT           NOT NULL,
    version          BIGINT         NOT NULL,
    created_at       TIMESTAMPTZ    NOT NULL DEFAULT now()
);
