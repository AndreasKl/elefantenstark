package net.andreaskluth.elefantenstark.consumer;

import static net.andreaskluth.elefantenstark.consumer.ConsumerQueries.UNLOCK_WORK_QUERY_SESSION_SCOPED;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import net.andreaskluth.elefantenstark.WorkItem;

class SessionScopedConsumer extends Consumer {

  SessionScopedConsumer(String obtainWorkQuery) {
    super(obtainWorkQuery);
  }

  @Override
  public java.util.function.Consumer<Connection> next(
      java.util.function.Consumer<WorkItem> worker) {

    return connection -> {
      try {
        WorkItemContext workItemContext = fetchWorkAndLock(connection);
        try {
          updateRetryCount(connection, workItemContext);
          worker.accept(workItemContext.workItem());
        } finally {
          unlock(connection, workItemContext.workItem());
        }
      } catch (SQLException e) {
        throw new WorkConsumerException(e);
      }
    };
  }

  private void updateRetryCount(Connection connection, WorkItemContext workItemContext)
      throws SQLException {
    try (PreparedStatement preparedStatement =
        connection.prepareStatement(
            "UPDATE queue SET retry_count = (SELECT retry_count FROM queue WHERE id = ?) + 1 WHERE id = ?")) {
      preparedStatement.setInt(1, workItemContext.id());
      preparedStatement.setInt(2, workItemContext.id());
      preparedStatement.execute();
    }
  }

  private void unlock(Connection connection, WorkItem workItem) throws SQLException {
    try (PreparedStatement preparedStatement =
        connection.prepareStatement(UNLOCK_WORK_QUERY_SESSION_SCOPED)) {
      preparedStatement.setString(1, workItem.key());
      preparedStatement.execute();
    }
  }
}
