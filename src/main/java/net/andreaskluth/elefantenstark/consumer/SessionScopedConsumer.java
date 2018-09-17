package net.andreaskluth.elefantenstark.consumer;

import static java.util.Objects.requireNonNull;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Optional;
import net.andreaskluth.elefantenstark.work.WorkItem;

class SessionScopedConsumer extends Consumer {

  private static final String MARK_AS_NOT_AVAILABLE =
      "UPDATE queue SET available = false WHERE id = ?";
  private static final String UNLOCK_ADVISORY_LOCK =
      "SELECT pg_advisory_unlock('queue'::regclass::int, ?);";

  SessionScopedConsumer(String obtainWorkQuery) {
    super(obtainWorkQuery);
  }

  @Override
  public <T> Optional<T> next(
      Connection connection, java.util.function.Function<WorkItemContext, T> worker) {
    requireNonNull(connection);
    requireNonNull(worker);

    return fetchWorkAndLock(connection)
        .map(
            wic -> {
              try {
                T result = worker.apply(wic);
                markAsProcessed(connection, wic);
                return result;
              } finally {
                unlock(connection, wic.workItem());
              }
            });
  }

  @Override
  public boolean supportsStatefulProcessing() {
    return true;
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
    try (PreparedStatement preparedStatement = connection.prepareStatement(UNLOCK_ADVISORY_LOCK)) {
      preparedStatement.setInt(1, workItem.group());
      preparedStatement.execute();
    } catch (SQLException e) {
      throw new ConsumerException(e);
    }
  }
}
