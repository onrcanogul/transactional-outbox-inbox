package com.onurcanogul.orders.controller;

import com.onurcanogul.orders.dto.PlaceOrderCommand;
import com.onurcanogul.orders.dto.request.CancelOrderRequest;
import com.onurcanogul.orders.dto.request.PlaceOrderRequest;
import com.onurcanogul.orders.dto.response.OrderResponse;
import com.onurcanogul.orders.entity.Order;
import com.onurcanogul.orders.service.OrderService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public OrderResponse place(@Valid @RequestBody PlaceOrderRequest request) {
        Order order = orderService.placeOrder(
                new PlaceOrderCommand(request.customerId(), request.clientReference(), request.total()));
        return OrderResponse.from(order);
    }

    @PostMapping("/{orderId}/cancel")
    public OrderResponse cancel(@PathVariable UUID orderId, @Valid @RequestBody CancelOrderRequest request) {
        return OrderResponse.from(orderService.cancelOrder(orderId, request.reason()));
    }
}
