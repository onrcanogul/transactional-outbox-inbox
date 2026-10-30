package com.onurcanogul.chaos;

import com.onurcanogul.billing.BillingServiceApplication;
import com.onurcanogul.orders.OrderServiceApplication;
import com.onurcanogul.testsupport.Containers;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.annotation.DirtiesContext;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * Both services run in this JVM against one PostgreSQL (two databases) and one
 * Kafka. The service under test is booted by Spring Boot Test so its beans can
 * be spied on; the other side is started by hand. Every context is closed after
 * its test class: a cached order-service context would keep its relay running
 * and hold the leader lock, so the relay under test would never get a turn.
 */
@DirtiesContext
abstract class ChaosTest {

    static final PostgreSQLContainer<?> POSTGRES = Containers.postgres();
    static final KafkaContainer KAFKA = Containers.kafka();

    static final String ORDERS_DB = "orders";
    static final String BILLING_DB = "billing";

    static {
        createDatabase(ORDERS_DB);
        createDatabase(BILLING_DB);
    }

    static String jdbcUrl(String database) {
        return "jdbc:postgresql://" + POSTGRES.getHost() + ":" + POSTGRES.getFirstMappedPort() + "/" + database;
    }

    /**
     * The test module's own application.yml shadows both services' files, so
     * everything that differs per service is passed here.
     */
    static Map<String, Object> serviceProperties(String database) {
        Map<String, Object> properties = new HashMap<>();
        properties.put("spring.datasource.url", jdbcUrl(database));
        properties.put("spring.datasource.username", POSTGRES.getUsername());
        properties.put("spring.datasource.password", POSTGRES.getPassword());
        properties.put("spring.flyway.locations", "classpath:db/migration/" + database);
        properties.put("spring.kafka.bootstrap-servers", KAFKA.getBootstrapServers());
        return properties;
    }

    static ConfigurableApplicationContext startBillingService(String consumerGroup) {
        Map<String, Object> properties = serviceProperties(BILLING_DB);
        properties.put("spring.kafka.consumer.group-id", consumerGroup);
        return new SpringApplicationBuilder(BillingServiceApplication.class)
                .web(WebApplicationType.NONE)
                .properties(properties)
                .run();
    }

    static ConfigurableApplicationContext startOrderService() {
        return new SpringApplicationBuilder(OrderServiceApplication.class)
                .web(WebApplicationType.NONE)
                .properties(serviceProperties(ORDERS_DB))
                .run();
    }

    private static void createDatabase(String name) {
        try (Connection connection = DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE " + name);
        } catch (SQLException e) {
            if (!"42P04".equals(e.getSQLState())) {
                throw new IllegalStateException("Could not create database " + name, e);
            }
        }
    }
}
