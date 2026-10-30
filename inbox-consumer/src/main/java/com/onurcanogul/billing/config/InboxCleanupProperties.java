package com.onurcanogul.billing.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Retention must outlast the longest window in which a record can still be
 * redelivered: the topic's own retention plus any replay you might run.
 */
@ConfigurationProperties(prefix = "billing.inbox.cleanup")
public record InboxCleanupProperties(boolean enabled, Duration retention, int batchSize) {

    public InboxCleanupProperties {
        if (retention == null) {
            retention = Duration.ofDays(14);
        }
        if (batchSize <= 0) {
            batchSize = 1000;
        }
    }
}
