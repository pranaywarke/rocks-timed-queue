package dev.rocksqueue.admin;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/** Reads the audit trail for one queue, for the admin console. */
public final class QueueAudit {
    private final Connection connection;

    public QueueAudit(Connection connection) {
        this.connection = connection;
    }

    public int countEvents(String queueName) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery(
                     "SELECT COUNT(*) FROM queue_audit WHERE queue = '" + queueName + "'")) {
            return rows.next() ? rows.getInt(1) : 0;
        }
    }
}
