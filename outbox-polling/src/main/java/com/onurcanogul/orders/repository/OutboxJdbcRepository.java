package com.onurcanogul.orders.repository;

import com.onurcanogul.orders.relay.PendingEvent;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Raw SQL the relay needs. Lives apart from the JPA repository because the
 * relay must control locking and never load rows into the persistence context.
 */
@Component
public class OutboxJdbcRepository {

    private final JdbcClient jdbcClient;

    public OutboxJdbcRepository(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    /**
     * Locks the oldest pending rows for the current transaction. Rows locked by
     * another relay instance are skipped rather than waited for.
     */
    public List<PendingEvent> lockPendingBatch(int limit) {
        return jdbcClient.sql("""
                        SELECT id, event_id, aggregate_type, aggregate_id, event_type,
                               payload::text AS payload, attempts
                        FROM outbox
                        WHERE published_at IS NULL
                        ORDER BY id
                        LIMIT :limit
                        FOR UPDATE SKIP LOCKED
                        """)
                .param("limit", limit)
                .query(PendingEvent.class)
                .list();
    }

    public void markPublished(List<Long> ids) {
        jdbcClient.sql("UPDATE outbox SET published_at = now() WHERE id IN (:ids)")
                .param("ids", ids)
                .update();
    }
}
