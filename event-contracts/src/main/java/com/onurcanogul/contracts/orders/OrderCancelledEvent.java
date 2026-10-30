package com.onurcanogul.contracts.orders;

import java.util.UUID;

public record OrderCancelledEvent(
        UUID orderId,
        String reason,
        long aggregateVersion,
        int schemaVersion) {

    public static final String TYPE = "OrderCancelledEvent";
    public static final int CURRENT_SCHEMA_VERSION = 1;

    public static OrderCancelledEvent of(UUID orderId, String reason, long aggregateVersion) {
        return new OrderCancelledEvent(orderId, reason, aggregateVersion, CURRENT_SCHEMA_VERSION);
    }
}
