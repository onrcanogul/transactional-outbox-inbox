package com.onurcanogul.billing.service;

import com.onurcanogul.billing.entity.Invoice;
import com.onurcanogul.billing.repository.InvoiceRepository;
import com.onurcanogul.contracts.orders.OrderCancelledEvent;
import com.onurcanogul.contracts.orders.OrderPlacedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service
public class BillingService {

    private static final Logger log = LoggerFactory.getLogger(BillingService.class);

    private final InvoiceRepository invoiceRepository;

    public BillingService(InvoiceRepository invoiceRepository) {
        this.invoiceRepository = invoiceRepository;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public Invoice createInvoice(OrderPlacedEvent event) {
        return invoiceRepository.save(Invoice.issue(event.orderId(), event.customerId(), event.total()));
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void cancelInvoice(OrderCancelledEvent event) {
        invoiceRepository.findByOrderId(event.orderId()).ifPresentOrElse(
                Invoice::cancel,
                () -> log.warn("No invoice to cancel for order {}", event.orderId()));
    }
}
