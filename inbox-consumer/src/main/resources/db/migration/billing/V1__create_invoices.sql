CREATE TABLE invoices (
    id           UUID           PRIMARY KEY,
    order_id     UUID           NOT NULL UNIQUE,
    customer_id  UUID           NOT NULL,
    amount       NUMERIC(19, 2) NOT NULL,
    status       TEXT           NOT NULL,
    issued_at    TIMESTAMPTZ    NOT NULL DEFAULT now(),
    cancelled_at TIMESTAMPTZ
);
