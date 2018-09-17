package net.andreaskluth.elefantenstark.maintenance;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class QueueMetrics {

  public static final String COUNT = "SELECT COUNT(*) FROM queue";
  public static final String COUNT_AVAILABLE = "SELECT COUNT(*) FROM queue WHERE available";
  public static final String COUNT_NOT_AVAILABLE = "SELECT COUNT(*) FROM queue WHERE NOT available";
  public static final String COUNT_QUEUE_ADVISORY_LOCK = "SELECT COUNT(*) FROM pg_locks WHERE locktype = 'advisory' AND classid = 'queue'::regclass::int;";

  public QueueMetrics() {}

  public static QueueMetrics metrics() {
    return new QueueMetrics();
  }

  public long size(Connection connection) {
    return sizeFor(connection, COUNT);
  }

  public long sizeAvailable(Connection connection) {
    return sizeFor(connection, COUNT_AVAILABLE);
  }

  public long sizeNotAvailable(Connection connection) {
    return sizeFor(connection, COUNT_NOT_AVAILABLE);
  }

  public long currentLocks(Connection connection) {
    return sizeFor(connection, COUNT_QUEUE_ADVISORY_LOCK);
  }

  protected long sizeFor(Connection connection, String query) {
    try (PreparedStatement preparedStatement = connection.prepareStatement(query);
        ResultSet resultSet = preparedStatement.executeQuery()) {
      if (resultSet.next()) {
        return resultSet.getInt(1);
      }
      return 0;
    } catch (SQLException e) {
      throw new QueueMetricsException(e);
    }
  }

  public static class QueueMetricsException extends RuntimeException {
    private static final long serialVersionUID = 1683162842548587430L;

    public QueueMetricsException(SQLException cause) {
      super(cause);
    }
  }
}
