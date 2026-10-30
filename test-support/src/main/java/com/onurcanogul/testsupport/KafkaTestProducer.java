package com.onurcanogul.testsupport;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public final class KafkaTestProducer implements AutoCloseable {

    private final KafkaProducer<String, String> producer;

    public KafkaTestProducer(String bootstrapServers) {
        this.producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class));
    }

    public void send(String topic, String key, String value, Map<String, String> headers) {
        var record = new ProducerRecord<>(topic, key, value);
        headers.forEach((name, headerValue) ->
                record.headers().add(new RecordHeader(name, headerValue.getBytes(StandardCharsets.UTF_8))));
        try {
            producer.send(record).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while sending test record", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("Test record was not acknowledged", e.getCause());
        }
    }

    @Override
    public void close() {
        producer.close();
    }
}
