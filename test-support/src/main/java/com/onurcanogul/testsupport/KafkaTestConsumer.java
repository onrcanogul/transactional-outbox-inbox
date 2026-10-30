package com.onurcanogul.testsupport;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Reads a topic from the beginning with a throwaway consumer group so tests can
 * assert exactly which records reached the broker.
 */
public final class KafkaTestConsumer implements AutoCloseable {

    private final KafkaConsumer<String, String> consumer;

    public KafkaTestConsumer(String bootstrapServers, String topic) {
        this.consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class));
        consumer.subscribe(List.of(topic));
    }

    public List<ConsumerRecord<String, String>> drain(Duration quietPeriod) {
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        Duration pollTimeout = Duration.ofMillis(200);
        long deadline = System.nanoTime() + quietPeriod.toNanos();
        while (System.nanoTime() < deadline) {
            var polled = consumer.poll(pollTimeout);
            if (!polled.isEmpty()) {
                polled.forEach(records::add);
                deadline = System.nanoTime() + quietPeriod.toNanos();
            }
        }
        return records;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
