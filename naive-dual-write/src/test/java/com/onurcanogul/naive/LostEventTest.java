package com.onurcanogul.naive;

import com.onurcanogul.contracts.orders.OrderTopics;
import com.onurcanogul.naive.dto.PlaceOrderCommand;
import com.onurcanogul.naive.repository.OrderRepository;
import com.onurcanogul.naive.service.SendAfterCommitOrderService;
import com.onurcanogul.testsupport.KafkaTestConsumer;
import com.onurcanogul.testsupport.PostgresKafkaTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.testcontainers.DockerClientFactory;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "spring.kafka.producer.properties.max.block.ms=1000",
        "spring.kafka.producer.properties.request.timeout.ms=1000",
        "spring.kafka.producer.properties.delivery.timeout.ms=2000"
})
class LostEventTest extends PostgresKafkaTest {

    @Autowired
    SendAfterCommitOrderService orderService;

    @Autowired
    OrderRepository orderRepository;

    @Test
    void brokerOutageRightAfterCommitLosesTheEventForGood() {
        UUID customerId = UUID.randomUUID();
        var command = new PlaceOrderCommand(customerId, "ref-" + customerId, new BigDecimal("49.00"));

        var docker = DockerClientFactory.instance().client();
        docker.pauseContainerCmd(KAFKA.getContainerId()).exec();
        try {
            orderService.placeOrder(command);
        } finally {
            docker.unpauseContainerCmd(KAFKA.getContainerId()).exec();
        }

        assertThat(orderRepository.findAll())
                .filteredOn(order -> order.getCustomerId().equals(customerId))
                .as("the order was committed")
                .hasSize(1);

        try (var consumer = new KafkaTestConsumer(KAFKA.getBootstrapServers(), OrderTopics.ORDERS)) {
            var records = consumer.drain(Duration.ofSeconds(3));
            assertThat(records)
                    .filteredOn(record -> record.value().contains(customerId.toString()))
                    .as("nobody will ever retry the send, the event is gone")
                    .isEmpty();
        }
    }
}
