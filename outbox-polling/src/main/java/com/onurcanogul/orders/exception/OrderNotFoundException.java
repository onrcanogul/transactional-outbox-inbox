package com.onurcanogul.orders.exception;

import java.util.UUID;

public class OrderNotFoundException extends RuntimeException {

    public OrderNotFoundException(UUID orderId) {
        super("Order " + orderId + " not found");
    }
}
