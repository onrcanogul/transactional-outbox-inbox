package com.onurcanogul.orders.relay;

import com.onurcanogul.contracts.orders.EventHeaders;
import com.onurcanogul.orders.config.OutboxRelayProperties;
import com.onurcanogul.orders.repository.OutboxJdbcRepository;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

/**
 * Moves pending outbox rows to Kafka. A batch is locked, sent, acknowledged by
 * the broker and only then marked as published, all inside one transaction.
 * Rows the broker did not acknowledge stay pending with one more attempt
 * counted; a crash before the update leaves them untouched. Either way the
 * same rows are picked up again on the next run, which is what makes delivery
 * at-least-once.
 */
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxJdbcRepository outboxJdbcRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final TransactionTemplate transactionTemplate;
    private final OutboxRelayProperties properties;

    public OutboxRelay(OutboxJdbcRepository outboxJdbcRepository,
                       KafkaTemplate<String, String> kafkaTemplate,
                       TransactionTemplate transactionTemplate,
                       OutboxRelayProperties properties) {
        this.outboxJdbcRepository = outboxJdbcRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.transactionTemplate = transactionTemplate;
        this.properties = properties;
    }

    /**
     * @return number of events published in this run
     */
    public int relayBatch() {
        Integer published = transactionTemplate.execute(status -> {
            List<PendingEvent> batch = outboxJdbcRepository.lockPendingBatch(properties.batchSize());
            if (batch.isEmpty()) {
                return 0;
            }

            Map<Long, CompletableFuture<?>> sends = new LinkedHashMap<>();
            for (PendingEvent event : batch) {
                sends.put(event.id(), send(event));
            }
            awaitAcks(sends.values());

            List<Long> acknowledged = new ArrayList<>();
            List<Long> unacknowledged = new ArrayList<>();
            sends.forEach((id, send) -> (isAcknowledged(send) ? acknowledged : unacknowledged).add(id));

            if (!unacknowledged.isEmpty()) {
                outboxJdbcRepository.recordFailedAttempt(unacknowledged, properties.maxAttempts());
                log.warn("Broker did not acknowledge {} outbox events, they will be retried", unacknowledged.size());
            }
            if (!acknowledged.isEmpty()) {
                outboxJdbcRepository.markPublished(acknowledged);
                log.debug("Published {} outbox events", acknowledged.size());
            }
            return acknowledged.size();
        });
        return published == null ? 0 : published;
    }

    private CompletableFuture<?> send(PendingEvent event) {
        try {
            return kafkaTemplate.send(toRecord(event));
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private void awaitAcks(Collection<CompletableFuture<?>> sends) {
        try {
            CompletableFuture.allOf(sends.toArray(CompletableFuture[]::new))
                    .orTimeout(properties.sendTimeout().toMillis(), TimeUnit.MILLISECONDS)
                    .join();
        } catch (CompletionException e) {
            log.debug("At least one send in the batch failed", e);
        }
    }

    private static boolean isAcknowledged(CompletableFuture<?> send) {
        return send.isDone() && !send.isCompletedExceptionally();
    }

    private ProducerRecord<String, String> toRecord(PendingEvent event) {
        var record = new ProducerRecord<>(
                properties.topicFor(event.aggregateType()),
                event.aggregateId(),
                event.payload());
        record.headers()
                .add(new RecordHeader(EventHeaders.EVENT_ID, bytes(event.eventId().toString())))
                .add(new RecordHeader(EventHeaders.EVENT_TYPE, bytes(event.eventType())));
        return record;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
