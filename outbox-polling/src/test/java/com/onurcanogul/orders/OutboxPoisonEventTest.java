package com.onurcanogul.orders;

import com.onurcanogul.contracts.orders.OrderTopics;
import com.onurcanogul.orders.dto.PlaceOrderCommand;
import com.onurcanogul.orders.entity.Order;
import com.onurcanogul.orders.entity.OutboxEvent;
import com.onurcanogul.orders.relay.OutboxRelay;
import com.onurcanogul.orders.repository.OutboxRepository;
import com.onurcanogul.orders.service.OrderService;
import com.onurcanogul.testsupport.KafkaTestConsumer;
import com.onurcanogul.testsupport.PostgresKafkaTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "outbox.relay.enabled=false",
        "outbox.relay.max-attempts=3",
        "spring.kafka.producer.properties.max.request.size=4096"
})
class OutboxPoisonEventTest extends PostgresKafkaTest {

    @Autowired
    OrderService orderService;

    @Autowired
    OutboxRelay outboxRelay;

    @Autowired
    OutboxRepository outboxRepository;

    @Test
    void eventTheBrokerKeepsRejectingIsParkedAfterMaxAttemptsWithoutBlockingTheRest() {
        drainOutbox();
        UUID customerId = UUID.randomUUID();
        Order order = orderService.placeOrder(new PlaceOrderCommand(customerId, "ref-" + customerId, new BigDecimal("10.00")));
        orderService.cancelOrder(order.getId(), "x".repeat(8_000));

        assertThat(outboxRelay.relayBatch()).isEqualTo(1);
        assertThat(outboxRelay.relayBatch()).isZero();
        assertThat(outboxRelay.relayBatch()).isZero();

        List<OutboxEvent> events = outboxRepository.findByAggregateIdOrderByIdAsc(order.getId().toString());
        OutboxEvent placed = events.get(0);
        OutboxEvent cancelled = events.get(1);
        assertThat(placed.getPublishedAt()).isNotNull();
        assertThat(placed.getAttempts()).isZero();
        assertThat(cancelled.getPublishedAt()).isNull();
        assertThat(cancelled.getAttempts()).isEqualTo(3);
        assertThat(cancelled.getFailedAt()).isNotNull();

        assertThat(outboxRelay.relayBatch()).isZero();
        assertThat(outboxRepository.findById(cancelled.getId()).orElseThrow().getAttempts()).isEqualTo(3);

        try (var consumer = new KafkaTestConsumer(KAFKA.getBootstrapServers(), OrderTopics.ORDERS)) {
            assertThat(consumer.drain(Duration.ofSeconds(2)))
                    .filteredOn(record -> record.key().equals(order.getId().toString()))
                    .hasSize(1);
        }
    }

    private void drainOutbox() {
        int published;
        do {
            published = outboxRelay.relayBatch();
        } while (published > 0);
    }
}
