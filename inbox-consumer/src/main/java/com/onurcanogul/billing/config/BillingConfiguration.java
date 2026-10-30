package com.onurcanogul.billing.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(InboxCleanupProperties.class)
class BillingConfiguration {
}
