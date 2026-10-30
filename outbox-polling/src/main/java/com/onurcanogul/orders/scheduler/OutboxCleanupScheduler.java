package com.onurcanogul.orders.scheduler;

import com.onurcanogul.orders.service.OutboxCleanupService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "outbox.cleanup", name = "enabled", havingValue = "true", matchIfMissing = true)
public class OutboxCleanupScheduler {

    private final OutboxCleanupService outboxCleanupService;

    public OutboxCleanupScheduler(OutboxCleanupService outboxCleanupService) {
        this.outboxCleanupService = outboxCleanupService;
    }

    @Scheduled(fixedDelayString = "${outbox.cleanup.interval:1h}", initialDelayString = "${outbox.cleanup.initial-delay:5m}")
    public void run() {
        outboxCleanupService.purgePublishedEvents();
    }
}
