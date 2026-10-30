package com.onurcanogul.chaos;

import com.onurcanogul.billing.repository.InvoiceRepository;
import com.onurcanogul.contracts.orders.EventHeaders;
import com.onurcanogul.contracts.orders.OrderTopics;
import com.onurcanogul.orders.OrderServiceApplication;
import com.onurcanogul.orders.dto.PlaceOrderCommand;
import com.onurcanogul.orders.entity.Order;
import com.onurcanogul.orders.repository.OutboxJdbcRepository;
import com.onurcanogul.orders.service.OrderService;
import com.onurcanogul.testsupport.KafkaTestConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;

/**
 * The relay dies after the broker acknowledged the send but before the row was
 * marked as published. The next run sends the same event again, so Kafka ends
 * up with a duplicate; the inbox on the billing side swallows it.
 */
@SpringBootTest(classes = OrderServiceApplication.class)
class RelayCrashBetweenSendAndUpdateTest extends ChaosTest {

    static ConfigurableApplicationContext billingService;

    @Autowired
    OrderService orderService;

    @MockitoSpyBean
    OutboxJdbcRepository outboxJdbcRepository;

    @DynamicPropertySource
    static void orderServiceProperties(DynamicPropertyRegistry registry) {
        serviceProperties(ORDERS_DB).forEach((name, value) -> registry.add(name, () -> value));
    }

    @BeforeAll
    static void startBilling() {
        billingService = startBillingService("billing-relay-crash");
    }

    @AfterAll
    static void stopBilling() {
        billingService.close();
    }

    @Test
    void duplicateFromARelayCrashReachesKafkaButNotTheInvoices() {
        AtomicBoolean crashed = new AtomicBoolean();
        doAnswer(invocation -> {
            if (crashed.compareAndSet(false, true)) {
                throw new IllegalStateException("relay process killed after send, before update");
            }
            return invocation.callRealMethod();
        }).when(outboxJdbcRepository).markPublished(anyList());

        UUID customerId = UUID.randomUUID();
        Order order = orderService.placeOrder(new PlaceOrderCommand(customerId, "ref-" + customerId, new BigDecimal("30.00")));

        try (var consumer = new KafkaTestConsumer(KAFKA.getBootstrapServers(), OrderTopics.ORDERS)) {
            await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
                var records = consumer.drain(Duration.ofSeconds(1)).stream()
                        .filter(record -> record.key().equals(order.getId().toString()))
                        .toList();
                assertThat(records).as("the same event was published twice").hasSize(2);
                assertThat(records).extracting(RelayCrashBetweenSendAndUpdateTest::eventId)
                        .as("both records carry the same event id")
                        .containsOnly(eventId(records.getFirst()));
            });
        }

        InvoiceRepository invoiceRepository = billingService.getBean(InvoiceRepository.class);
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(invoiceRepository.findByOrderId(order.getId())).isPresent());
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(invoiceRepository.findAll())
                        .filteredOn(invoice -> invoice.getOrderId().equals(order.getId()))
                        .hasSize(1));
    }

    private static String eventId(ConsumerRecord<String, String> record) {
        return new String(record.headers().lastHeader(EventHeaders.EVENT_ID).value(), StandardCharsets.UTF_8);
    }
}
