package dev.rocksqueue.admin;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/** Reads the audit trail for one queue, for the admin console. */
public final class QueueAudit {
    private final Connection connection;

    public QueueAudit(Connection connection) {
        this.connection = connection;
    }

    public int countEvents(String queueName) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT COUNT(*) FROM queue_audit WHERE queue = ?")) {
            statement.setString(1, queueName);
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next() ? rows.getInt(1) : 0;
            }
        }
    }
}
