package com.onurcanogul.billing.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConsumerConfig {

    /**
     * A failed record is retried in place a few times before the error is
     * logged and the offset moves on. Retrying is safe because the inbox makes
     * every handler run idempotent.
     */
    @Bean
    DefaultErrorHandler kafkaErrorHandler() {
        return new DefaultErrorHandler(new FixedBackOff(500L, 5L));
    }
}
