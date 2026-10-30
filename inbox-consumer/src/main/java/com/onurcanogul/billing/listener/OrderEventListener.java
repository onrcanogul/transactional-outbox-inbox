package com.onurcanogul.billing.listener;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.onurcanogul.billing.repository.InboxJdbcRepository;
import com.onurcanogul.billing.service.BillingService;
import com.onurcanogul.contracts.orders.EventHeaders;
import com.onurcanogul.contracts.orders.OrderCancelledEvent;
import com.onurcanogul.contracts.orders.OrderPlacedEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * The inbox row and the business change commit together. If anything fails the
 * row is rolled back as well, the record is redelivered and processed from
 * scratch; if the record is redelivered after a commit, the inbox says so and
 * it is skipped.
 */
@Component
public class OrderEventListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventListener.class);

    private final InboxJdbcRepository inboxJdbcRepository;
    private final BillingService billingService;
    private final ObjectMapper objectMapper;

    public OrderEventListener(InboxJdbcRepository inboxJdbcRepository,
                              BillingService billingService,
                              ObjectMapper objectMapper) {
        this.inboxJdbcRepository = inboxJdbcRepository;
        this.billingService = billingService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "${billing.orders-topic}", groupId = "${spring.kafka.consumer.group-id}")
    @Transactional
    public void on(ConsumerRecord<String, String> record) {
        UUID eventId = UUID.fromString(header(record, EventHeaders.EVENT_ID));
        if (!inboxJdbcRepository.markProcessed(eventId)) {
            log.info("Skipping already processed event {}", eventId);
            return;
        }

        String eventType = header(record, EventHeaders.EVENT_TYPE);
        switch (eventType) {
            case OrderPlacedEvent.TYPE -> billingService.createInvoice(parse(record.value(), OrderPlacedEvent.class));
            case OrderCancelledEvent.TYPE -> billingService.cancelInvoice(parse(record.value(), OrderCancelledEvent.class));
            default -> log.warn("Ignoring unknown event type {} ({})", eventType, eventId);
        }
    }

    private <T> T parse(String json, Class<T> type) {
        try {
            return objectMapper.readValue(json, type);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Malformed " + type.getSimpleName() + " payload", e);
        }
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        if (header == null) {
            throw new IllegalArgumentException("Record is missing the " + name + " header");
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
