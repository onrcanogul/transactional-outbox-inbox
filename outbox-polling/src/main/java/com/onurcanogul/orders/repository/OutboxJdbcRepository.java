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
                        WHERE published_at IS NULL AND failed_at IS NULL
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

    /**
     * Counts one more failed attempt and parks the row as failed once the
     * limit is reached so a poison event stops blocking the relay.
     */
    public void recordFailedAttempt(List<Long> ids, int maxAttempts) {
        jdbcClient.sql("""
                        UPDATE outbox
                        SET attempts = attempts + 1,
                            failed_at = CASE WHEN attempts + 1 >= :maxAttempts THEN now() END
                        WHERE id IN (:ids)
                        """)
                .param("ids", ids)
                .param("maxAttempts", maxAttempts)
                .update();
    }

    public OutboxStats stats() {
        return jdbcClient.sql("""
                        SELECT count(*) FILTER (WHERE published_at IS NULL AND failed_at IS NULL)        AS pending,
                               count(*) FILTER (WHERE failed_at IS NOT NULL)                              AS failed,
                               coalesce(extract(epoch FROM now() - min(created_at)
                                   FILTER (WHERE published_at IS NULL AND failed_at IS NULL)), 0)::bigint AS lag_seconds
                        FROM outbox
                        """)
                .query(OutboxStats.class)
                .single();
    }

    public record OutboxStats(long pending, long failed, long lagSeconds) {
    }
}
