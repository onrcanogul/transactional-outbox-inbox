package com.onurcanogul.orders.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "outbox.cleanup")
public record OutboxCleanupProperties(boolean enabled, Duration retention, int batchSize) {

    public OutboxCleanupProperties {
        if (retention == null) {
            retention = Duration.ofDays(7);
        }
        if (batchSize <= 0) {
            batchSize = 1000;
        }
    }
}
