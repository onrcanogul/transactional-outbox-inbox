package com.onurcanogul.orders.dto;

import java.math.BigDecimal;
import java.util.UUID;

public record PlaceOrderCommand(UUID customerId, String clientReference, BigDecimal total) {
}
