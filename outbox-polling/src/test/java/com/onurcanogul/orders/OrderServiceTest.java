package com.onurcanogul.orders;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.onurcanogul.contracts.orders.OrderCancelledEvent;
import com.onurcanogul.contracts.orders.OrderPlacedEvent;
import com.onurcanogul.orders.dto.PlaceOrderCommand;
import com.onurcanogul.orders.entity.OutboxEvent;
import com.onurcanogul.orders.repository.OrderRepository;
import com.onurcanogul.orders.repository.OutboxRepository;
import com.onurcanogul.orders.service.OrderService;
import com.onurcanogul.orders.service.OutboxService;
import com.onurcanogul.testsupport.PostgresKafkaTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataAccessException;
import org.springframework.transaction.IllegalTransactionStateException;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(properties = "outbox.relay.enabled=false")
class OrderServiceTest extends PostgresKafkaTest {

    @Autowired
    OrderService orderService;

    @Autowired
    OutboxService outboxService;

    @Autowired
    OrderRepository orderRepository;

    @Autowired
    OutboxRepository outboxRepository;

    @Autowired
    ObjectMapper objectMapper;

    @Test
    void placingAnOrderWritesTheEventToTheOutboxInTheSameTransaction() throws Exception {
        var order = orderService.placeOrder(command());

        List<OutboxEvent> events = outboxRepository.findByAggregateIdOrderByIdAsc(order.getId().toString());
        assertThat(events).hasSize(1);
        OutboxEvent event = events.getFirst();
        assertThat(event.getEventType()).isEqualTo(OrderPlacedEvent.TYPE);
        assertThat(event.getAggregateType()).isEqualTo("Order");
        assertThat(event.getPublishedAt()).isNull();
        assertThat(aggregateVersion(event)).isEqualTo(1);
    }

    @Test
    void cancellingAnOrderAppendsASecondEventWithTheNextVersion() throws Exception {
        var order = orderService.placeOrder(command());

        orderService.cancelOrder(order.getId(), "customer changed their mind");

        List<OutboxEvent> events = outboxRepository.findByAggregateIdOrderByIdAsc(order.getId().toString());
        assertThat(events).extracting(OutboxEvent::getEventType)
                .containsExactly(OrderPlacedEvent.TYPE, OrderCancelledEvent.TYPE);
        assertThat(aggregateVersion(events.get(1))).isEqualTo(2);
        assertThat(orderRepository.findById(order.getId()).orElseThrow().getVersion()).isEqualTo(2);
    }

    @Test
    void failedCommitLeavesNoEventBehind() {
        var command = command();
        orderService.placeOrder(command);

        assertThatThrownBy(() -> orderService.placeOrder(command)).isInstanceOf(DataAccessException.class);

        assertThat(outboxRepository.findAll())
                .filteredOn(event -> event.getPayload().contains(command.customerId().toString()))
                .hasSize(1);
    }

    @Test
    void outboxRefusesToRunOutsideACallerTransaction() {
        assertThatThrownBy(() -> outboxService.add("Order", "x", OrderPlacedEvent.TYPE, "{}"))
                .isInstanceOf(IllegalTransactionStateException.class);
    }

    private long aggregateVersion(OutboxEvent event) throws Exception {
        return objectMapper.readTree(event.getPayload()).get("aggregateVersion").asLong();
    }

    private static PlaceOrderCommand command() {
        UUID customerId = UUID.randomUUID();
        return new PlaceOrderCommand(customerId, "ref-" + customerId, new BigDecimal("120.00"));
    }
}
