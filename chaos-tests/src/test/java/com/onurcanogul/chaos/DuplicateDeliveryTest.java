package com.onurcanogul.chaos;

import com.onurcanogul.billing.entity.InvoiceStatus;
import com.onurcanogul.billing.repository.InvoiceRepository;
import com.onurcanogul.contracts.orders.OrderTopics;
import com.onurcanogul.orders.OrderServiceApplication;
import com.onurcanogul.orders.dto.PlaceOrderCommand;
import com.onurcanogul.orders.entity.Order;
import com.onurcanogul.orders.service.OrderService;
import com.onurcanogul.testsupport.KafkaTestConsumer;
import com.onurcanogul.testsupport.KafkaTestProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Every record the relay published is replayed once more, exactly as the
 * broker would redeliver it. Billing must end up with one invoice per order
 * in the state the last event left it.
 */
@SpringBootTest(classes = OrderServiceApplication.class)
class DuplicateDeliveryTest extends ChaosTest {

    static ConfigurableApplicationContext billingService;

    @Autowired
    OrderService orderService;

    @DynamicPropertySource
    static void orderServiceProperties(DynamicPropertyRegistry registry) {
        serviceProperties(ORDERS_DB).forEach((name, value) -> registry.add(name, () -> value));
    }

    @BeforeAll
    static void startBilling() {
        billingService = startBillingService("billing-duplicate-delivery");
    }

    @AfterAll
    static void stopBilling() {
        billingService.close();
    }

    @Test
    void replayingEveryRecordChangesNothingOnTheBillingSide() {
        UUID customerId = UUID.randomUUID();
        Order order = orderService.placeOrder(new PlaceOrderCommand(customerId, "ref-" + customerId, new BigDecimal("80.00")));
        orderService.cancelOrder(order.getId(), "duplicate delivery drill");

        InvoiceRepository invoiceRepository = billingService.getBean(InvoiceRepository.class);
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(invoiceRepository.findByOrderId(order.getId()))
                        .get()
                        .extracting(invoice -> invoice.getStatus())
                        .isEqualTo(InvoiceStatus.CANCELLED));

        List<ConsumerRecord<String, String>> published;
        try (var consumer = new KafkaTestConsumer(KAFKA.getBootstrapServers(), OrderTopics.ORDERS)) {
            published = consumer.drain(Duration.ofSeconds(2)).stream()
                    .filter(record -> record.key().equals(order.getId().toString()))
                    .toList();
        }
        assertThat(published).hasSize(2);

        try (var producer = new KafkaTestProducer(KAFKA.getBootstrapServers())) {
            for (ConsumerRecord<String, String> record : published) {
                producer.send(record.topic(), record.key(), record.value(), headersOf(record));
            }
        }

        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(8)).untilAsserted(() ->
                assertThat(invoiceRepository.findAll())
                        .filteredOn(invoice -> invoice.getOrderId().equals(order.getId()))
                        .singleElement()
                        .extracting(invoice -> invoice.getStatus())
                        .isEqualTo(InvoiceStatus.CANCELLED));
    }

    private static Map<String, String> headersOf(ConsumerRecord<String, String> record) {
        Map<String, String> headers = new HashMap<>();
        record.headers().forEach(header -> headers.put(header.key(), new String(header.value(), StandardCharsets.UTF_8)));
        return headers;
    }
}
