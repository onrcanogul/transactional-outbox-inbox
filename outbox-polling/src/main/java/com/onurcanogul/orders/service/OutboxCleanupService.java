package com.onurcanogul.orders.service;

import com.onurcanogul.orders.config.OutboxCleanupProperties;
import com.onurcanogul.orders.repository.OutboxJdbcRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Service
public class OutboxCleanupService {

    private static final Logger log = LoggerFactory.getLogger(OutboxCleanupService.class);

    private final OutboxJdbcRepository outboxJdbcRepository;
    private final OutboxCleanupProperties properties;

    public OutboxCleanupService(OutboxJdbcRepository outboxJdbcRepository, OutboxCleanupProperties properties) {
        this.outboxJdbcRepository = outboxJdbcRepository;
        this.properties = properties;
    }

    /**
     * @return number of published rows removed
     */
    public int purgePublishedEvents() {
        Instant cutoff = Instant.now().minus(properties.retention());
        int total = 0;
        int deleted;
        do {
            deleted = outboxJdbcRepository.deletePublishedBefore(cutoff, properties.batchSize());
            total += deleted;
        } while (deleted == properties.batchSize());
        if (total > 0) {
            log.info("Removed {} published outbox events older than {}", total, cutoff);
        }
        return total;
    }
}
