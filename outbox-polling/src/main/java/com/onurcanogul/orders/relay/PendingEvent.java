package com.onurcanogul.orders.relay;

import java.util.UUID;

public record PendingEvent(
        long id,
        UUID eventId,
        String aggregateType,
        String aggregateId,
        String eventType,
        String payload,
        int attempts) {
}
