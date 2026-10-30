package com.onurcanogul.naive.entity;

import com.onurcanogul.naive.dto.PlaceOrderCommand;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "orders")
public class Order {

    @Id
    @GeneratedValue
    private UUID id;

    @Column(name = "customer_id", nullable = false)
    private UUID customerId;

    @Column(name = "client_reference", nullable = false, unique = true)
    private String clientReference;

    @Column(nullable = false)
    private BigDecimal total;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected Order() {
    }

    private Order(UUID customerId, String clientReference, BigDecimal total) {
        this.customerId = customerId;
        this.clientReference = clientReference;
        this.total = total;
        this.createdAt = Instant.now();
    }

    public static Order from(PlaceOrderCommand command) {
        return new Order(command.customerId(), command.clientReference(), command.total());
    }

    public UUID getId() {
        return id;
    }

    public UUID getCustomerId() {
        return customerId;
    }

    public String getClientReference() {
        return clientReference;
    }

    public BigDecimal getTotal() {
        return total;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
