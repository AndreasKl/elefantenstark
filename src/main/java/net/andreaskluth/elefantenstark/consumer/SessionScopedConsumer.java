package net.andreaskluth.elefantenstark.consumer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;
import net.andreaskluth.elefantenstark.WorkItem;

class SessionScopedConsumer extends Consumer {

  private static final String MARK_AS_NOT_AVAILABLE =
      "UPDATE queue SET available = false WHERE id = ?";
  private static final String UNLOCK_ADVISoRY_LOCK =
      "SELECT pg_advisory_unlock('queue'::regclass::int, ('x'||substr(md5(?),1,8))::bit(32)::int);";

  SessionScopedConsumer(String obtainWorkQuery) {
    super(obtainWorkQuery);
  }

  @Override
  public <T> Optional<T> next(
      Connection connection, java.util.function.Function<WorkItem, T> worker) {
    Objects.requireNonNull(connection);
    Objects.requireNonNull(worker);

    return fetchWorkAndLock(connection)
        .map(
            wic -> {
              try {
                T result = worker.apply(wic.workItem());
                markAsProcessed(connection, wic);
                return result;
              } finally {
                unlock(connection, wic.workItem());
              }
            });
  }

  private void markAsProcessed(Connection connection, WorkItemContext workItemContext) {
    try (PreparedStatement statement = connection.prepareStatement(MARK_AS_NOT_AVAILABLE)) {
      statement.setInt(1, workItemContext.id());
      statement.execute();
    } catch (SQLException e) {
      throw new ConsumerException(e);
    }
  }

  private void unlock(Connection connection, WorkItem workItem) {
    try (PreparedStatement preparedStatement = connection.prepareStatement(UNLOCK_ADVISoRY_LOCK)) {
      preparedStatement.setString(1, workItem.key());
      preparedStatement.execute();
    } catch (SQLException e) {
      throw new ConsumerException(e);
    }
  }
}
