package com.onurcanogul.chaos;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.onurcanogul.billing.BillingServiceApplication;
import com.onurcanogul.billing.repository.InboxJdbcRepository;
import com.onurcanogul.billing.repository.InvoiceRepository;
import com.onurcanogul.billing.service.BillingService;
import com.onurcanogul.contracts.orders.EventHeaders;
import com.onurcanogul.contracts.orders.OrderPlacedEvent;
import com.onurcanogul.contracts.orders.OrderTopics;
import com.onurcanogul.testsupport.KafkaTestProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.util.AopTestUtils;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * The consumer fails after the inbox row was written but before the
 * transaction committed. Both the inbox row and the invoice roll back, the
 * record is redelivered and processed from scratch, exactly once.
 * The spy sits behind the transactional proxy, so it is stubbed on the target.
 */
@SpringBootTest(classes = BillingServiceApplication.class)
class ConsumerCrashMidHandlerTest extends ChaosTest {

    @Autowired
    InvoiceRepository invoiceRepository;

    @Autowired
    InboxJdbcRepository inboxJdbcRepository;

    @Autowired
    ObjectMapper objectMapper;

    @MockitoSpyBean
    BillingService billingService;

    @DynamicPropertySource
    static void billingServiceProperties(DynamicPropertyRegistry registry) {
        serviceProperties(BILLING_DB).forEach((name, value) -> registry.add(name, () -> value));
        registry.add("spring.kafka.consumer.group-id", () -> "billing-consumer-crash");
    }

    @Test
    void crashAfterInboxInsertRollsBackAndTheRetryProcessesTheEventOnce() throws Exception {
        BillingService spy = AopTestUtils.getUltimateTargetObject(billingService);
        AtomicBoolean crashed = new AtomicBoolean();
        doAnswer(invocation -> {
            if (crashed.compareAndSet(false, true)) {
                throw new IllegalStateException("consumer process killed before commit");
            }
            return invocation.callRealMethod();
        }).when(spy).createInvoice(any());

        UUID orderId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        var event = OrderPlacedEvent.of(orderId, UUID.randomUUID(), new BigDecimal("12.50"), 1);
        try (var producer = new KafkaTestProducer(KAFKA.getBootstrapServers())) {
            producer.send(OrderTopics.ORDERS, orderId.toString(), objectMapper.writeValueAsString(event),
                    Map.of(EventHeaders.EVENT_ID, eventId.toString(), EventHeaders.EVENT_TYPE, OrderPlacedEvent.TYPE));
        }

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(invoiceRepository.findByOrderId(orderId)).isPresent());

        verify(spy, times(2)).createInvoice(any());
        assertThat(inboxJdbcRepository.contains(eventId)).isTrue();
        assertThat(invoiceRepository.findAll())
                .filteredOn(invoice -> invoice.getOrderId().equals(orderId))
                .hasSize(1);
    }
}
