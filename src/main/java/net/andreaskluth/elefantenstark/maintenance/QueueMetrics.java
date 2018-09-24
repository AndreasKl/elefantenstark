package net.andreaskluth.elefantenstark.maintenance;

import static java.util.Objects.requireNonNull;
import static net.andreaskluth.elefantenstark.maintenance.QueueMetricsQueries.COUNT;
import static net.andreaskluth.elefantenstark.maintenance.QueueMetricsQueries.COUNT_NOT_PROCESSED;
import static net.andreaskluth.elefantenstark.maintenance.QueueMetricsQueries.COUNT_PROCESSED;
import static net.andreaskluth.elefantenstark.maintenance.QueueMetricsQueries.COUNT_QUEUE_ADVISORY_LOCK;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/** Various metrics used for monitoring the state of the queue and taken locks. */
public class QueueMetrics {

  protected QueueMetrics() {}

  public static QueueMetrics metrics() {
    return new QueueMetrics();
  }

  public long size(Connection connection) {
    return sizeFor(connection, COUNT);
  }

  public long sizeNotProcessed(Connection connection) {
    return sizeFor(connection, COUNT_NOT_PROCESSED);
  }

  public long sizeProcessed(Connection connection) {
    return sizeFor(connection, COUNT_PROCESSED);
  }

  public long currentLocks(Connection connection) {
    return sizeFor(connection, COUNT_QUEUE_ADVISORY_LOCK);
  }

  protected long sizeFor(Connection connection, String query) {
    requireNonNull(connection);

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
}
