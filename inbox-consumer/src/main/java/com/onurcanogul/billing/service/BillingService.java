package com.onurcanogul.billing.service;

import com.onurcanogul.billing.entity.Invoice;
import com.onurcanogul.billing.repository.InvoiceRepository;
import com.onurcanogul.billing.repository.OrderVersionJdbcRepository;
import com.onurcanogul.contracts.orders.OrderCancelledEvent;
import com.onurcanogul.contracts.orders.OrderPlacedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;
import java.util.UUID;

/**
 * Kafka keeps order within a partition, but the relay can still hand events
 * of one order over out of sequence. Every handler therefore checks the
 * aggregate version first and drops anything older than what was already
 * applied.
 */
@Service
public class BillingService {

    private static final Logger log = LoggerFactory.getLogger(BillingService.class);

    private final InvoiceRepository invoiceRepository;
    private final OrderVersionJdbcRepository orderVersionJdbcRepository;

    public BillingService(InvoiceRepository invoiceRepository, OrderVersionJdbcRepository orderVersionJdbcRepository) {
        this.invoiceRepository = invoiceRepository;
        this.orderVersionJdbcRepository = orderVersionJdbcRepository;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public Optional<Invoice> createInvoice(OrderPlacedEvent event) {
        if (isStale(event.orderId(), event.aggregateVersion())) {
            return Optional.empty();
        }
        return Optional.of(invoiceRepository.save(Invoice.issue(event.orderId(), event.customerId(), event.total())));
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void cancelInvoice(OrderCancelledEvent event) {
        if (isStale(event.orderId(), event.aggregateVersion())) {
            return;
        }
        invoiceRepository.findByOrderId(event.orderId()).ifPresentOrElse(
                Invoice::cancel,
                () -> log.warn("No invoice to cancel for order {}", event.orderId()));
    }

    private boolean isStale(UUID orderId, long version) {
        if (orderVersionJdbcRepository.advanceTo(orderId, version)) {
            return false;
        }
        log.info("Ignoring stale event for order {} at version {}", orderId, version);
        return true;
    }
}
