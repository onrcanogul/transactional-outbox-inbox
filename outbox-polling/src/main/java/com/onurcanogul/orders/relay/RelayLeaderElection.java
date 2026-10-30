package com.onurcanogul.orders.relay;

import com.onurcanogul.orders.config.OutboxRelayProperties;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.jdbc.JdbcConnectionDetails;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Keeps a single relay active across instances with a PostgreSQL session-level
 * advisory lock. The lock lives on a dedicated connection outside the pool: it
 * is released when the connection closes, so a dead leader frees the slot on
 * its own and a standby takes over on its next check.
 */
@Component
public class RelayLeaderElection implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RelayLeaderElection.class);

    private final JdbcConnectionDetails connectionDetails;
    private final long lockKey;

    private Connection connection;
    private boolean leader;

    public RelayLeaderElection(JdbcConnectionDetails connectionDetails, OutboxRelayProperties properties) {
        this.connectionDetails = connectionDetails;
        this.lockKey = properties.leaderLockKey();
    }

    public synchronized boolean isLeader() {
        try {
            ensureConnection();
            if (!leader) {
                leader = tryLock();
                if (leader) {
                    log.info("This instance is now the active outbox relay");
                }
            }
            return leader;
        } catch (SQLException e) {
            log.warn("Could not check relay leadership, treating this instance as standby", e);
            closeQuietly();
            return false;
        }
    }

    private void ensureConnection() throws SQLException {
        if (connection != null && connection.isValid(1)) {
            return;
        }
        closeQuietly();
        connection = DriverManager.getConnection(
                connectionDetails.getJdbcUrl(),
                connectionDetails.getUsername(),
                connectionDetails.getPassword());
        connection.setAutoCommit(true);
    }

    private boolean tryLock() throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("SELECT pg_try_advisory_lock(?)")) {
            statement.setLong(1, lockKey);
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next() && resultSet.getBoolean(1);
            }
        }
    }

    @PreDestroy
    @Override
    public synchronized void close() {
        if (connection == null) {
            return;
        }
        try (PreparedStatement statement = connection.prepareStatement("SELECT pg_advisory_unlock(?)")) {
            statement.setLong(1, lockKey);
            statement.execute();
        } catch (SQLException e) {
            log.debug("Advisory lock release failed, the connection close will release it", e);
        }
        closeQuietly();
    }

    private void closeQuietly() {
        leader = false;
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException ignored) {
                // nothing to recover here, the lock dies with the session
            }
            connection = null;
        }
    }
}
