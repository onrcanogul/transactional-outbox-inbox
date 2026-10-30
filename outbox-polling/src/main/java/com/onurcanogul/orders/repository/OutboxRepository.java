package com.onurcanogul.orders.repository;

import com.onurcanogul.orders.entity.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface OutboxRepository extends JpaRepository<OutboxEvent, Long> {

    List<OutboxEvent> findByAggregateIdOrderByIdAsc(String aggregateId);
}
