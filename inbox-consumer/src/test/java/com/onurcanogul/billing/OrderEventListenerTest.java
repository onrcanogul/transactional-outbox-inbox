package com.onurcanogul.billing;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.onurcanogul.billing.entity.Invoice;
import com.onurcanogul.billing.entity.InvoiceStatus;
import com.onurcanogul.billing.repository.InboxJdbcRepository;
import com.onurcanogul.billing.repository.InvoiceRepository;
import com.onurcanogul.billing.repository.OrderVersionJdbcRepository;
import com.onurcanogul.contracts.orders.EventHeaders;
import com.onurcanogul.contracts.orders.OrderCancelledEvent;
import com.onurcanogul.contracts.orders.OrderPlacedEvent;
import com.onurcanogul.contracts.orders.OrderTopics;
import com.onurcanogul.testsupport.KafkaTestProducer;
import com.onurcanogul.testsupport.PostgresKafkaTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
class OrderEventListenerTest extends PostgresKafkaTest {

    static KafkaTestProducer producer;

    @Autowired
    InvoiceRepository invoiceRepository;

    @Autowired
    InboxJdbcRepository inboxJdbcRepository;

    @Autowired
    OrderVersionJdbcRepository orderVersionJdbcRepository;

    @Autowired
    ObjectMapper objectMapper;

    @BeforeAll
    static void startProducer() {
        producer = new KafkaTestProducer(KAFKA.getBootstrapServers());
    }

    @AfterAll
    static void stopProducer() {
        producer.close();
    }

    @Test
    void issuesAnInvoiceWhenAnOrderIsPlaced() {
        UUID orderId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();

        publish(eventId, OrderPlacedEvent.of(orderId, UUID.randomUUID(), new BigDecimal("42.00"), 1));

        Invoice invoice = awaitInvoice(orderId);
        assertThat(invoice.getAmount()).isEqualByComparingTo("42.00");
        assertThat(invoice.getStatus()).isEqualTo(InvoiceStatus.ISSUED);
        assertThat(inboxJdbcRepository.contains(eventId)).isTrue();
    }

    @Test
    void cancelsTheInvoiceWhenTheOrderIsCancelled() {
        UUID orderId = UUID.randomUUID();

        publish(UUID.randomUUID(), OrderPlacedEvent.of(orderId, UUID.randomUUID(), new BigDecimal("10.00"), 1));
        publish(UUID.randomUUID(), OrderCancelledEvent.of(orderId, "changed mind", 2));

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(invoiceRepository.findByOrderId(orderId))
                        .get()
                        .extracting(Invoice::getStatus)
                        .isEqualTo(InvoiceStatus.CANCELLED));
    }

    @Test
    void sameEventDeliveredTwiceProducesOneInvoice() {
        UUID orderId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        var event = OrderPlacedEvent.of(orderId, UUID.randomUUID(), new BigDecimal("99.00"), 1);

        publish(eventId, event);
        publish(eventId, event);

        awaitInvoice(orderId);
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(invoiceRepository.findAll())
                        .filteredOn(invoice -> invoice.getOrderId().equals(orderId))
                        .hasSize(1));
    }

    @Test
    void placedEventArrivingAfterTheCancellationIsIgnored() {
        UUID orderId = UUID.randomUUID();

        publish(UUID.randomUUID(), OrderCancelledEvent.of(orderId, "cancelled before billing saw it", 2));
        publish(UUID.randomUUID(), OrderPlacedEvent.of(orderId, UUID.randomUUID(), new BigDecimal("15.00"), 1));

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(orderVersionJdbcRepository.currentVersion(orderId)).isEqualTo(2));
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(invoiceRepository.findByOrderId(orderId)).isEmpty());
    }

    private Invoice awaitInvoice(UUID orderId) {
        return await().atMost(Duration.ofSeconds(10))
                .until(() -> invoiceRepository.findByOrderId(orderId), Optional::isPresent)
                .orElseThrow();
    }

    private void publish(UUID eventId, OrderPlacedEvent event) {
        publish(eventId, OrderPlacedEvent.TYPE, event.orderId(), event);
    }

    private void publish(UUID eventId, OrderCancelledEvent event) {
        publish(eventId, OrderCancelledEvent.TYPE, event.orderId(), event);
    }

    private void publish(UUID eventId, String eventType, UUID orderId, Object payload) {
        try {
            producer.send(OrderTopics.ORDERS, orderId.toString(), objectMapper.writeValueAsString(payload),
                    Map.of(EventHeaders.EVENT_ID, eventId.toString(), EventHeaders.EVENT_TYPE, eventType));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }
}
