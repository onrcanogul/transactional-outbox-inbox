package com.onurcanogul.naive.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.onurcanogul.contracts.orders.OrderPlacedEvent;
import com.onurcanogul.contracts.orders.OrderTopics;
import com.onurcanogul.naive.entity.Order;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
public class OrderEventPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public OrderEventPublisher(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public void publishOrderPlaced(Order order) {
        OrderPlacedEvent event = OrderPlacedEvent.of(order.getId(), order.getCustomerId(), order.getTotal(), 1);
        try {
            kafkaTemplate.send(OrderTopics.ORDERS, order.getId().toString(), objectMapper.writeValueAsString(event))
                    .get(5, TimeUnit.SECONDS);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Could not serialize " + OrderPlacedEvent.TYPE, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while publishing " + OrderPlacedEvent.TYPE, e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("Broker did not acknowledge " + OrderPlacedEvent.TYPE, e);
        }
    }
}
