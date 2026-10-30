ALTER TABLE outbox ADD COLUMN failed_at TIMESTAMPTZ;

DROP INDEX outbox_pending;
CREATE INDEX outbox_pending ON outbox (id) WHERE published_at IS NULL AND failed_at IS NULL;
