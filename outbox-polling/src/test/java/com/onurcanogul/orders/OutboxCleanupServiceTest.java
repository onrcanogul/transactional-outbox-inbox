package com.onurcanogul.orders;

import com.onurcanogul.orders.service.OutboxCleanupService;
import com.onurcanogul.testsupport.PostgresKafkaTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "outbox.relay.enabled=false",
        "outbox.cleanup.enabled=false",
        "outbox.cleanup.retention=1d",
        "outbox.cleanup.batch-size=2"
})
class OutboxCleanupServiceTest extends PostgresKafkaTest {

    @Autowired
    OutboxCleanupService outboxCleanupService;

    @Autowired
    JdbcClient jdbcClient;

    @Test
    void removesOnlyPublishedRowsOlderThanTheRetentionInBatches() {
        String aggregateId = UUID.randomUUID().toString();
        for (int i = 0; i < 5; i++) {
            insert(aggregateId, "now() - interval '3 days'");
        }
        insert(aggregateId, "now()");
        insert(aggregateId, "NULL");

        assertThat(outboxCleanupService.purgePublishedEvents()).isEqualTo(5);

        Integer remaining = jdbcClient.sql("SELECT count(*) FROM outbox WHERE aggregate_id = :id")
                .param("id", aggregateId)
                .query(Integer.class)
                .single();
        assertThat(remaining).isEqualTo(2);
    }

    private void insert(String aggregateId, String publishedAtExpression) {
        jdbcClient.sql("""
                        INSERT INTO outbox (event_id, aggregate_type, aggregate_id, event_type, payload, created_at, published_at)
                        VALUES (:eventId, 'Order', :aggregateId, 'OrderPlacedEvent', '{}'::jsonb, now() - interval '3 days', %s)
                        """.formatted(publishedAtExpression))
                .param("eventId", UUID.randomUUID())
                .param("aggregateId", aggregateId)
                .update();
    }
}
