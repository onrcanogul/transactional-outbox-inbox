package com.onurcanogul.testsupport;

import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Starts one PostgreSQL and one Kafka container per JVM and shares them across
 * every test class that extends this. Spring Boot wires the datasource and the
 * Kafka bootstrap servers through {@link ServiceConnection}.
 */
public abstract class PostgresKafkaTest {

    @ServiceConnection
    protected static final PostgreSQLContainer<?> POSTGRES = Containers.postgres();

    @ServiceConnection
    protected static final KafkaContainer KAFKA = Containers.kafka();
}
