package com.onurcanogul.orders.metrics;

import com.onurcanogul.orders.repository.OutboxJdbcRepository;
import com.onurcanogul.orders.repository.OutboxJdbcRepository.OutboxStats;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

/**
 * The number that matters is the age of the oldest unpublished event. Pending
 * and failed counts sit next to it so an alert on lag has context.
 */
@Component
public class OutboxMetrics {

    private final OutboxJdbcRepository outboxJdbcRepository;
    private final AtomicLong pending = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final AtomicLong lagSeconds = new AtomicLong();

    public OutboxMetrics(OutboxJdbcRepository outboxJdbcRepository, MeterRegistry meterRegistry) {
        this.outboxJdbcRepository = outboxJdbcRepository;
        Gauge.builder("outbox.events.pending", pending, AtomicLong::get)
                .description("Outbox rows waiting to be published")
                .register(meterRegistry);
        Gauge.builder("outbox.events.failed", failed, AtomicLong::get)
                .description("Outbox rows parked after exhausting their attempts")
                .register(meterRegistry);
        Gauge.builder("outbox.lag.seconds", lagSeconds, AtomicLong::get)
                .description("Age of the oldest unpublished outbox row")
                .register(meterRegistry);
    }

    @Scheduled(fixedDelayString = "${outbox.metrics.refresh-interval:5s}")
    public void refresh() {
        OutboxStats stats = outboxJdbcRepository.stats();
        pending.set(stats.pending());
        failed.set(stats.failed());
        lagSeconds.set(stats.lagSeconds());
    }

    public long pending() {
        return pending.get();
    }

    public long failed() {
        return failed.get();
    }

    public long lagSeconds() {
        return lagSeconds.get();
    }
}
