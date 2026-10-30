package com.onurcanogul.naive.service;

import com.onurcanogul.naive.dto.PlaceOrderCommand;
import com.onurcanogul.naive.entity.Order;
import com.onurcanogul.naive.messaging.OrderEventPublisher;
import com.onurcanogul.naive.repository.OrderRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Scenario A: the event is sent while the database transaction is still open.
 * If the commit fails afterwards, Kafka already has an event for an order that
 * never existed.
 */
@Service
public class SendInTransactionOrderService {

    private final OrderRepository orderRepository;
    private final OrderEventPublisher orderEventPublisher;

    public SendInTransactionOrderService(OrderRepository orderRepository, OrderEventPublisher orderEventPublisher) {
        this.orderRepository = orderRepository;
        this.orderEventPublisher = orderEventPublisher;
    }

    @Transactional
    public Order placeOrder(PlaceOrderCommand command) {
        Order order = orderRepository.save(Order.from(command));
        orderEventPublisher.publishOrderPlaced(order);
        return order;
    }
}
