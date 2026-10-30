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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Moves pending outbox rows to Kafka. A batch is locked, sent, acknowledged by
 * the broker and only then marked as published, all inside one transaction.
 * Any failure rolls the transaction back and the same rows are picked up again
 * on the next run, which is what makes delivery at-least-once.
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

            CompletableFuture<?>[] sends = batch.stream()
                    .map(event -> kafkaTemplate.send(toRecord(event)))
                    .toArray(CompletableFuture[]::new);

            CompletableFuture.allOf(sends)
                    .orTimeout(properties.sendTimeout().toMillis(), TimeUnit.MILLISECONDS)
                    .join();

            outboxJdbcRepository.markPublished(batch.stream().map(PendingEvent::id).toList());
            log.debug("Published {} outbox events", batch.size());
            return batch.size();
        });
        return published == null ? 0 : published;
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
