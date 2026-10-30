package com.onurcanogul.orders.dto.request;

import jakarta.validation.constraints.NotBlank;

public record CancelOrderRequest(@NotBlank String reason) {
}
