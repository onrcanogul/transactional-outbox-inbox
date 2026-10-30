package com.onurcanogul.billing;

import com.onurcanogul.billing.service.InboxCleanupService;
import com.onurcanogul.testsupport.PostgresKafkaTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "billing.inbox.cleanup.enabled=false",
        "billing.inbox.cleanup.retention=7d",
        "billing.inbox.cleanup.batch-size=2"
})
class InboxCleanupServiceTest extends PostgresKafkaTest {

    @Autowired
    InboxCleanupService inboxCleanupService;

    @Autowired
    JdbcClient jdbcClient;

    @Test
    void removesEntriesOlderThanTheRetentionAndKeepsRecentOnes() {
        UUID recent = UUID.randomUUID();
        for (int i = 0; i < 5; i++) {
            insert(UUID.randomUUID(), "now() - interval '10 days'");
        }
        insert(recent, "now() - interval '1 day'");

        assertThat(inboxCleanupService.purgeProcessedEvents()).isEqualTo(5);

        Long remaining = jdbcClient.sql("SELECT count(*) FROM inbox WHERE processed_at < now() - interval '7 days'")
                .query(Long.class)
                .single();
        assertThat(remaining).isZero();
        assertThat(jdbcClient.sql("SELECT count(*) FROM inbox WHERE event_id = :id").param("id", recent).query(Long.class).single())
                .isEqualTo(1);
    }

    private void insert(UUID eventId, String processedAtExpression) {
        jdbcClient.sql("INSERT INTO inbox (event_id, processed_at) VALUES (:eventId, %s)".formatted(processedAtExpression))
                .param("eventId", eventId)
                .update();
    }
}
