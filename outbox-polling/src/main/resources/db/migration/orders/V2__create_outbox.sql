CREATE TABLE outbox (
    id             BIGSERIAL   PRIMARY KEY,
    event_id       UUID        NOT NULL UNIQUE,
    aggregate_type TEXT        NOT NULL,
    aggregate_id   TEXT        NOT NULL,
    event_type     TEXT        NOT NULL,
    payload        JSONB       NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at   TIMESTAMPTZ,
    attempts       INT         NOT NULL DEFAULT 0
);

-- Only pending rows are indexed; published rows drop out and the index stays small.
CREATE INDEX outbox_pending ON outbox (id) WHERE published_at IS NULL;
