package com.onurcanogul.orders;

import com.onurcanogul.contracts.orders.EventHeaders;
import com.onurcanogul.contracts.orders.OrderCancelledEvent;
import com.onurcanogul.contracts.orders.OrderPlacedEvent;
import com.onurcanogul.contracts.orders.OrderTopics;
import com.onurcanogul.orders.dto.PlaceOrderCommand;
import com.onurcanogul.orders.entity.Order;
import com.onurcanogul.orders.entity.OutboxEvent;
import com.onurcanogul.orders.relay.OutboxRelay;
import com.onurcanogul.orders.repository.OutboxJdbcRepository;
import com.onurcanogul.orders.repository.OutboxRepository;
import com.onurcanogul.orders.service.OrderService;
import com.onurcanogul.testsupport.KafkaTestConsumer;
import com.onurcanogul.testsupport.PostgresKafkaTest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.DockerClientFactory;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "outbox.relay.enabled=false",
        "outbox.relay.send-timeout=2s",
        "spring.kafka.producer.properties.max.block.ms=1000",
        "spring.kafka.producer.properties.request.timeout.ms=1000",
        "spring.kafka.producer.properties.delivery.timeout.ms=2000"
})
class OutboxRelayTest extends PostgresKafkaTest {

    @Autowired
    OrderService orderService;

    @Autowired
    OutboxRelay outboxRelay;

    @Autowired
    OutboxJdbcRepository outboxJdbcRepository;

    @Autowired
    OutboxRepository outboxRepository;

    @Autowired
    TransactionTemplate transactionTemplate;

    @Test
    void publishesPendingEventsKeyedByAggregateAndMarksThemPublished() {
        Order order = orderService.placeOrder(command());
        orderService.cancelOrder(order.getId(), "out of stock");

        drainOutbox();

        List<OutboxEvent> events = outboxRepository.findByAggregateIdOrderByIdAsc(order.getId().toString());
        assertThat(events).allSatisfy(event -> assertThat(event.getPublishedAt()).isNotNull());

        List<ConsumerRecord<String, String>> records = recordsFor(order);
        assertThat(records).hasSize(2);
        assertThat(records).extracting(ConsumerRecord::key).containsOnly(order.getId().toString());
        assertThat(records).extracting(record -> header(record, EventHeaders.EVENT_TYPE))
                .containsExactly(OrderPlacedEvent.TYPE, OrderCancelledEvent.TYPE);
        assertThat(records).extracting(record -> header(record, EventHeaders.EVENT_ID))
                .containsExactly(events.get(0).getEventId().toString(), events.get(1).getEventId().toString());
    }

    @Test
    void doesNotMarkAnythingPublishedWhenTheBrokerNeverAcknowledges() {
        Order order = orderService.placeOrder(command());

        var docker = DockerClientFactory.instance().client();
        docker.pauseContainerCmd(KAFKA.getContainerId()).exec();
        try {
            assertThat(outboxRelay.relayBatch()).isZero();
        } finally {
            docker.unpauseContainerCmd(KAFKA.getContainerId()).exec();
        }

        assertThat(pendingFor(order)).hasSize(1);
        assertThat(pendingFor(order).getFirst().getAttempts()).isEqualTo(1);

        drainOutbox();

        assertThat(pendingFor(order)).isEmpty();
        assertThat(recordsFor(order)).hasSize(1);
    }

    @Test
    void rowsLockedByAnotherRelayAreSkippedInsteadOfPublishedTwice() throws Exception {
        Order order = orderService.placeOrder(command());

        var locked = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var executor = Executors.newSingleThreadExecutor();
        try {
            executor.submit(() -> transactionTemplate.executeWithoutResult(status -> {
                outboxJdbcRepository.lockPendingBatch(100);
                locked.countDown();
                await(release);
            }));
            locked.await(5, TimeUnit.SECONDS);

            assertThat(outboxRelay.relayBatch()).isZero();
            assertThat(pendingFor(order)).hasSize(1);
        } finally {
            release.countDown();
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }

        drainOutbox();
        assertThat(recordsFor(order)).hasSize(1);
    }

    private void drainOutbox() {
        int published;
        do {
            published = outboxRelay.relayBatch();
        } while (published > 0);
    }

    private List<OutboxEvent> pendingFor(Order order) {
        return outboxRepository.findByAggregateIdOrderByIdAsc(order.getId().toString()).stream()
                .filter(event -> event.getPublishedAt() == null)
                .toList();
    }

    private List<ConsumerRecord<String, String>> recordsFor(Order order) {
        try (var consumer = new KafkaTestConsumer(KAFKA.getBootstrapServers(), OrderTopics.ORDERS)) {
            return consumer.drain(Duration.ofSeconds(2)).stream()
                    .filter(record -> record.key().equals(order.getId().toString()))
                    .toList();
        }
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        return new String(record.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static PlaceOrderCommand command() {
        UUID customerId = UUID.randomUUID();
        return new PlaceOrderCommand(customerId, "ref-" + customerId, new BigDecimal("75.50"));
    }
}
