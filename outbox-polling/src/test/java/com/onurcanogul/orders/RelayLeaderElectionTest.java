package com.onurcanogul.orders;

import com.onurcanogul.orders.config.OutboxRelayProperties;
import com.onurcanogul.orders.relay.RelayLeaderElection;
import com.onurcanogul.testsupport.PostgresKafkaTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.jdbc.JdbcConnectionDetails;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = "outbox.relay.enabled=false")
class RelayLeaderElectionTest extends PostgresKafkaTest {

    @Autowired
    JdbcConnectionDetails jdbcConnectionDetails;

    @Autowired
    OutboxRelayProperties outboxRelayProperties;

    @Test
    void onlyOneInstanceHoldsTheLockUntilItGoesAway() {
        var first = new RelayLeaderElection(jdbcConnectionDetails, outboxRelayProperties);
        var second = new RelayLeaderElection(jdbcConnectionDetails, outboxRelayProperties);
        try {
            assertThat(first.isLeader()).isTrue();
            assertThat(second.isLeader()).isFalse();
            assertThat(second.isLeader()).isFalse();

            first.close();

            assertThat(second.isLeader()).isTrue();
            assertThat(first.isLeader()).isFalse();
        } finally {
            first.close();
            second.close();
        }
    }
}
