package com.onurcanogul.billing.repository;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public class OrderVersionJdbcRepository {

    private final JdbcClient jdbcClient;

    public OrderVersionJdbcRepository(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    /**
     * Records the highest aggregate version seen for an order.
     *
     * @return {@code false} when an equal or newer version was already applied,
     * meaning the incoming event is stale and must be ignored
     */
    public boolean advanceTo(UUID orderId, long version) {
        int updated = jdbcClient.sql("""
                        INSERT INTO order_versions (order_id, version) VALUES (:orderId, :version)
                        ON CONFLICT (order_id) DO UPDATE SET version = EXCLUDED.version
                        WHERE order_versions.version < EXCLUDED.version
                        """)
                .param("orderId", orderId)
                .param("version", version)
                .update();
        return updated == 1;
    }

    public Long currentVersion(UUID orderId) {
        return jdbcClient.sql("SELECT version FROM order_versions WHERE order_id = :orderId")
                .param("orderId", orderId)
                .query(Long.class)
                .optional()
                .orElse(null);
    }
}
