package com.onurcanogul.orders;

import com.onurcanogul.orders.dto.PlaceOrderCommand;
import com.onurcanogul.orders.metrics.OutboxMetrics;
import com.onurcanogul.orders.relay.OutboxRelay;
import com.onurcanogul.orders.service.OrderService;
import com.onurcanogul.testsupport.PostgresKafkaTest;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.math.BigDecimal;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {"outbox.relay.enabled=false", "outbox.metrics.refresh-interval=1h"})
class OutboxMetricsTest extends PostgresKafkaTest {

    @Autowired
    OrderService orderService;

    @Autowired
    OutboxRelay outboxRelay;

    @Autowired
    OutboxMetrics outboxMetrics;

    @Autowired
    MeterRegistry meterRegistry;

    @Test
    void reportsPendingRowsAndLagUntilTheRelayCatchesUp() {
        UUID customerId = UUID.randomUUID();
        orderService.placeOrder(new PlaceOrderCommand(customerId, "ref-" + customerId, new BigDecimal("5.00")));

        outboxMetrics.refresh();
        assertThat(outboxMetrics.pending()).isPositive();
        assertThat(meterRegistry.get("outbox.events.pending").gauge().value()).isPositive();
        assertThat(meterRegistry.get("outbox.lag.seconds").gauge().value()).isGreaterThanOrEqualTo(0);

        drainOutbox();
        outboxMetrics.refresh();
        assertThat(outboxMetrics.pending()).isZero();
        assertThat(outboxMetrics.lagSeconds()).isZero();
    }

    private void drainOutbox() {
        int published;
        do {
            published = outboxRelay.relayBatch();
        } while (published > 0);
    }
}
