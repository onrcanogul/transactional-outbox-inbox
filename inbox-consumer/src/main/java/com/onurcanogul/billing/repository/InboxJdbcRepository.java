package com.onurcanogul.billing.repository;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public class InboxJdbcRepository {

    private final JdbcClient jdbcClient;

    public InboxJdbcRepository(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    /**
     * @return {@code true} when this event has not been seen before; the row is
     * written in the caller's transaction, so a rollback forgets it again
     */
    public boolean markProcessed(UUID eventId) {
        int inserted = jdbcClient.sql("INSERT INTO inbox (event_id) VALUES (:eventId) ON CONFLICT (event_id) DO NOTHING")
                .param("eventId", eventId)
                .update();
        return inserted == 1;
    }

    public boolean contains(UUID eventId) {
        return jdbcClient.sql("SELECT count(*) FROM inbox WHERE event_id = :eventId")
                .param("eventId", eventId)
                .query(Long.class)
                .single() > 0;
    }
}
