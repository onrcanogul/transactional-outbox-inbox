package com.onurcanogul.testsupport;

import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

public final class Containers {

    private static final DockerImageName POSTGRES_IMAGE = DockerImageName.parse("postgres:17-alpine");
    private static final DockerImageName KAFKA_IMAGE = DockerImageName.parse("apache/kafka-native:3.9.1");

    private static PostgreSQLContainer<?> postgres;
    private static KafkaContainer kafka;

    private Containers() {
    }

    public static synchronized PostgreSQLContainer<?> postgres() {
        if (postgres == null) {
            postgres = new PostgreSQLContainer<>(POSTGRES_IMAGE);
            postgres.start();
        }
        return postgres;
    }

    public static synchronized KafkaContainer kafka() {
        if (kafka == null) {
            kafka = new KafkaContainer(KAFKA_IMAGE);
            kafka.start();
        }
        return kafka;
    }
}
