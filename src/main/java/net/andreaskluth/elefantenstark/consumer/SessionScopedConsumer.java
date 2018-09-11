package net.andreaskluth.elefantenstark.consumer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Objects;
import net.andreaskluth.elefantenstark.WorkItem;

class SessionScopedConsumer extends Consumer {

  SessionScopedConsumer(String obtainWorkQuery) {
    super(obtainWorkQuery);
  }

  @Override
  public void next(Connection connection, java.util.function.Consumer<WorkItem> worker) {
    Objects.requireNonNull(connection);
    Objects.requireNonNull(worker);

    fetchWorkAndLock(connection)
        .ifPresent(
            wic -> {
              try {
                worker.accept(wic.workItem());
                markAsProcessed(connection, wic);
              } finally {
                unlock(connection, wic.workItem());
              }
            });
  }

  private void markAsProcessed(Connection connection, WorkItemContext workItemContext) {
    try (PreparedStatement statement =
        connection.prepareStatement("UPDATE queue SET available = false WHERE id = ?")) {
      statement.setInt(1, workItemContext.id());
      statement.execute();
    } catch (SQLException e) {
      throw new ConsumerException(e);
    }
  }

  private void unlock(Connection connection, WorkItem workItem) {
    try (PreparedStatement preparedStatement =
        connection.prepareStatement(
            "SELECT pg_advisory_unlock('queue'::regclass::int, ('x'||substr(md5(?),1,8))::bit(32)::int);")) {
      preparedStatement.setString(1, workItem.key());
      preparedStatement.execute();
    } catch (SQLException e) {
      throw new ConsumerException(e);
    }
  }
}
