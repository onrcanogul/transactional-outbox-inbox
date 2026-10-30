package com.onurcanogul.billing.service;

import com.onurcanogul.billing.config.InboxCleanupProperties;
import com.onurcanogul.billing.repository.InboxJdbcRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Service
public class InboxCleanupService {

    private static final Logger log = LoggerFactory.getLogger(InboxCleanupService.class);

    private final InboxJdbcRepository inboxJdbcRepository;
    private final InboxCleanupProperties properties;

    public InboxCleanupService(InboxJdbcRepository inboxJdbcRepository, InboxCleanupProperties properties) {
        this.inboxJdbcRepository = inboxJdbcRepository;
        this.properties = properties;
    }

    /**
     * @return number of inbox rows removed
     */
    public int purgeProcessedEvents() {
        Instant cutoff = Instant.now().minus(properties.retention());
        int total = 0;
        int deleted;
        do {
            deleted = inboxJdbcRepository.deleteProcessedBefore(cutoff, properties.batchSize());
            total += deleted;
        } while (deleted == properties.batchSize());
        if (total > 0) {
            log.info("Removed {} inbox entries older than {}", total, cutoff);
        }
        return total;
    }
}
