package com.onurcanogul.contracts.orders;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Fact emitted once an order has been accepted. Carries only what consumers need,
 * never the entity itself, so the producer's schema can evolve independently.
 */
public record OrderPlacedEvent(
        UUID orderId,
        UUID customerId,
        BigDecimal total,
        long aggregateVersion,
        int schemaVersion) {

    public static final String TYPE = "OrderPlacedEvent";
    public static final int CURRENT_SCHEMA_VERSION = 1;

    public static OrderPlacedEvent of(UUID orderId, UUID customerId, BigDecimal total, long aggregateVersion) {
        return new OrderPlacedEvent(orderId, customerId, total, aggregateVersion, CURRENT_SCHEMA_VERSION);
    }
}
