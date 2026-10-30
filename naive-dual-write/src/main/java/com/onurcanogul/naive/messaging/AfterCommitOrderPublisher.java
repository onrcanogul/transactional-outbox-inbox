package com.onurcanogul.naive.messaging;

import com.onurcanogul.naive.event.OrderPersistedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

@Component
public class AfterCommitOrderPublisher {

    private static final Logger log = LoggerFactory.getLogger(AfterCommitOrderPublisher.class);

    private final OrderEventPublisher orderEventPublisher;

    public AfterCommitOrderPublisher(OrderEventPublisher orderEventPublisher) {
        this.orderEventPublisher = orderEventPublisher;
    }

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void on(OrderPersistedEvent event) {
        try {
            orderEventPublisher.publishOrderPlaced(event.order());
        } catch (RuntimeException e) {
            // The transaction is already committed, so there is nothing left to roll back.
            log.error("Lost OrderPlaced for order {}", event.order().getId(), e);
        }
    }
}
