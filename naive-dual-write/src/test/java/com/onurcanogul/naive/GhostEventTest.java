package com.onurcanogul.naive;

import com.onurcanogul.contracts.orders.OrderTopics;
import com.onurcanogul.naive.dto.PlaceOrderCommand;
import com.onurcanogul.naive.repository.OrderRepository;
import com.onurcanogul.naive.service.SendInTransactionOrderService;
import com.onurcanogul.testsupport.KafkaTestConsumer;
import com.onurcanogul.testsupport.PostgresKafkaTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataAccessException;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest
class GhostEventTest extends PostgresKafkaTest {

    @Autowired
    SendInTransactionOrderService orderService;

    @Autowired
    OrderRepository orderRepository;

    @Test
    void commitFailureAfterSendLeavesAnEventForAnOrderThatDoesNotExist() {
        UUID customerId = UUID.randomUUID();
        var command = new PlaceOrderCommand(customerId, "ref-" + customerId, new BigDecimal("99.90"));

        orderService.placeOrder(command);

        // Same client reference: the INSERT is deferred to commit, so the unique
        // violation surfaces only after the event has already been sent.
        assertThatThrownBy(() -> orderService.placeOrder(command))
                .isInstanceOf(DataAccessException.class);

        assertThat(orderRepository.findAll())
                .filteredOn(order -> order.getCustomerId().equals(customerId))
                .hasSize(1);

        try (var consumer = new KafkaTestConsumer(KAFKA.getBootstrapServers(), OrderTopics.ORDERS)) {
            var records = consumer.drain(Duration.ofSeconds(3));
            assertThat(records)
                    .filteredOn(record -> record.value().contains(customerId.toString()))
                    .as("Kafka holds two OrderPlaced events although only one order was committed")
                    .hasSize(2);
        }
    }
}
