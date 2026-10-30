package com.onurcanogul.orders.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.Map;

@ConfigurationProperties(prefix = "outbox.relay")
public record OutboxRelayProperties(
        boolean enabled,
        Duration pollInterval,
        int batchSize,
        Duration sendTimeout,
        long leaderLockKey,
        Map<String, String> topics) {

    public OutboxRelayProperties {
        if (pollInterval == null) {
            pollInterval = Duration.ofMillis(200);
        }
        if (batchSize <= 0) {
            batchSize = 100;
        }
        if (sendTimeout == null) {
            sendTimeout = Duration.ofSeconds(10);
        }
        if (leaderLockKey == 0) {
            leaderLockKey = 7_431_001L;
        }
        if (topics == null) {
            topics = Map.of();
        }
    }

    public String topicFor(String aggregateType) {
        String topic = topics.get(aggregateType);
        if (topic == null) {
            throw new IllegalStateException("No topic configured for aggregate type " + aggregateType);
        }
        return topic;
    }
}
