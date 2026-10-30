package com.onurcanogul.naive.service;

import com.onurcanogul.naive.dto.PlaceOrderCommand;
import com.onurcanogul.naive.entity.Order;
import com.onurcanogul.naive.event.OrderPersistedEvent;
import com.onurcanogul.naive.repository.OrderRepository;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Scenario B: the event is sent only after the commit succeeded. The order is
 * durable, but the event lives in memory until the broker acknowledges it.
 * A crash or an unreachable broker in that window loses the event for good.
 */
@Service
public class SendAfterCommitOrderService {

    private final OrderRepository orderRepository;
    private final ApplicationEventPublisher applicationEventPublisher;

    public SendAfterCommitOrderService(OrderRepository orderRepository, ApplicationEventPublisher applicationEventPublisher) {
        this.orderRepository = orderRepository;
        this.applicationEventPublisher = applicationEventPublisher;
    }

    @Transactional
    public Order placeOrder(PlaceOrderCommand command) {
        Order order = orderRepository.save(Order.from(command));
        applicationEventPublisher.publishEvent(new OrderPersistedEvent(order));
        return order;
    }
}
