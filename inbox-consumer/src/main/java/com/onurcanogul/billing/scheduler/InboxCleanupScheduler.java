package com.onurcanogul.billing.scheduler;

import com.onurcanogul.billing.service.InboxCleanupService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "billing.inbox.cleanup", name = "enabled", havingValue = "true", matchIfMissing = true)
public class InboxCleanupScheduler {

    private final InboxCleanupService inboxCleanupService;

    public InboxCleanupScheduler(InboxCleanupService inboxCleanupService) {
        this.inboxCleanupService = inboxCleanupService;
    }

    @Scheduled(fixedDelayString = "${billing.inbox.cleanup.interval:1h}", initialDelayString = "${billing.inbox.cleanup.initial-delay:5m}")
    public void run() {
        inboxCleanupService.purgeProcessedEvents();
    }
}
