package com.onurcanogul.orders.dto.response;

import com.onurcanogul.orders.entity.Order;
import com.onurcanogul.orders.entity.OrderStatus;

import java.math.BigDecimal;
import java.util.UUID;

public record OrderResponse(UUID id, UUID customerId, BigDecimal total, OrderStatus status) {

    public static OrderResponse from(Order order) {
        return new OrderResponse(order.getId(), order.getCustomerId(), order.getTotal(), order.getStatus());
    }
}
