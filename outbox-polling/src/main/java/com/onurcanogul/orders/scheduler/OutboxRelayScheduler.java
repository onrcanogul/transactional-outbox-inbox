package com.onurcanogul.orders.scheduler;

import com.onurcanogul.orders.relay.OutboxRelay;
import com.onurcanogul.orders.relay.RelayLeaderElection;
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
    private final RelayLeaderElection relayLeaderElection;

    public OutboxRelayScheduler(OutboxRelay outboxRelay, RelayLeaderElection relayLeaderElection) {
        this.outboxRelay = outboxRelay;
        this.relayLeaderElection = relayLeaderElection;
    }

    @Scheduled(fixedDelayString = "${outbox.relay.poll-interval:200ms}")
    public void tick() {
        if (!relayLeaderElection.isLeader()) {
            return;
        }
        try {
            outboxRelay.relayBatch();
        } catch (RuntimeException e) {
            log.warn("Outbox relay run failed, batch will be retried", e);
        }
    }
}
