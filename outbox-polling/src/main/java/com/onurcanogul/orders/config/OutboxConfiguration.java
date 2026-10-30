package com.onurcanogul.orders.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(OutboxRelayProperties.class)
class OutboxConfiguration {
}
