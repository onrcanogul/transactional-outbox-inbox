package com.onurcanogul.orders.service;

import com.onurcanogul.contracts.orders.OrderCancelledEvent;
import com.onurcanogul.contracts.orders.OrderPlacedEvent;
import com.onurcanogul.contracts.orders.OrderTopics;
import com.onurcanogul.orders.dto.PlaceOrderCommand;
import com.onurcanogul.orders.entity.Order;
import com.onurcanogul.orders.exception.OrderNotFoundException;
import com.onurcanogul.orders.repository.OrderRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/**
 * Writes the order and its event in the same transaction. Nothing here knows
 * about Kafka; the relay picks the event up from the outbox table later.
 */
@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final OutboxService outboxService;

    public OrderService(OrderRepository orderRepository, OutboxService outboxService) {
        this.orderRepository = orderRepository;
        this.outboxService = outboxService;
    }

    @Transactional
    public Order placeOrder(PlaceOrderCommand command) {
        Order order = orderRepository.save(Order.place(command));
        outboxService.add(
                OrderTopics.AGGREGATE_TYPE,
                order.getId().toString(),
                OrderPlacedEvent.TYPE,
                OrderPlacedEvent.of(order.getId(), order.getCustomerId(), order.getTotal(), order.getVersion()));
        return order;
    }

    @Transactional
    public Order cancelOrder(UUID orderId, String reason) {
        Order order = orderRepository.findById(orderId).orElseThrow(() -> new OrderNotFoundException(orderId));
        order.cancel();
        outboxService.add(
                OrderTopics.AGGREGATE_TYPE,
                order.getId().toString(),
                OrderCancelledEvent.TYPE,
                OrderCancelledEvent.of(order.getId(), reason, order.getVersion()));
        return order;
    }
}
