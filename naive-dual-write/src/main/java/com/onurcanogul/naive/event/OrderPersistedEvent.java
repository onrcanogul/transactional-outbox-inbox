package com.onurcanogul.naive.event;

import com.onurcanogul.naive.entity.Order;

public record OrderPersistedEvent(Order order) {
}
