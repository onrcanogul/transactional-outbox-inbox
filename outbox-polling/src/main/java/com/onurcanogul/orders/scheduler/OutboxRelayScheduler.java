package com.onurcanogul.orders.scheduler;

import com.onurcanogul.orders.relay.OutboxRelay;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "outbox.relay", name = "enabled", havingValue = "true", matchIfMissing = true)
public class OutboxRelayScheduler {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelayScheduler.class);

    private final OutboxRelay outboxRelay;

    public OutboxRelayScheduler(OutboxRelay outboxRelay) {
        this.outboxRelay = outboxRelay;
    }

    @Scheduled(fixedDelayString = "${outbox.relay.poll-interval:200ms}")
    public void tick() {
        try {
            outboxRelay.relayBatch();
        } catch (RuntimeException e) {
            log.warn("Outbox relay run failed, batch will be retried", e);
        }
    }
}
