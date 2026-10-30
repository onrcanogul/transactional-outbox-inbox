package com.onurcanogul.orders.dto.request;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;
import java.util.UUID;

public record PlaceOrderRequest(
        @NotNull UUID customerId,
        @NotBlank String clientReference,
        @NotNull @DecimalMin("0.01") BigDecimal total) {
}
