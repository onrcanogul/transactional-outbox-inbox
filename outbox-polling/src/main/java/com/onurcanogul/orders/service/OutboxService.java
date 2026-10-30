package com.onurcanogul.orders.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.onurcanogul.orders.entity.OutboxEvent;
import com.onurcanogul.orders.repository.OutboxRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Appends an event to the outbox inside the caller's transaction.
 * {@link Propagation#MANDATORY} makes a call without a transaction fail fast
 * instead of silently committing the event on its own.
 */
@Service
public class OutboxService {

    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    public OutboxService(OutboxRepository outboxRepository, ObjectMapper objectMapper) {
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public OutboxEvent add(String aggregateType, String aggregateId, String eventType, Object payload) {
        try {
            String json = objectMapper.writeValueAsString(payload);
            return outboxRepository.save(OutboxEvent.of(aggregateType, aggregateId, eventType, json));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Could not serialize " + eventType + " payload", e);
        }
    }
}
