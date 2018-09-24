package net.andreaskluth.elefantenstark.consumer;

import static java.util.Objects.requireNonNull;
import static net.andreaskluth.elefantenstark.consumer.ConsumerQueries.SESSION_SCOPED_MARK_AS_PROCESSED;
import static net.andreaskluth.elefantenstark.consumer.ConsumerQueries.SESSION_SCOPED_UNLOCK_ADVISORY_LOCK;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Optional;
import net.andreaskluth.elefantenstark.work.WorkItem;
import net.andreaskluth.elefantenstark.work.WorkItemContext;

class SessionScopedConsumer extends Consumer {

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

  private void markAsProcessed(Connection connection, WorkItemContext workItemContext) {
    try (PreparedStatement statement =
        connection.prepareStatement(SESSION_SCOPED_MARK_AS_PROCESSED)) {
      statement.setInt(1, workItemContext.id());
      statement.execute();
    } catch (SQLException e) {
      throw new ConsumerException(e);
    }
  }

  private void unlock(Connection connection, WorkItem workItem) {
    try (PreparedStatement preparedStatement =
        connection.prepareStatement(SESSION_SCOPED_UNLOCK_ADVISORY_LOCK)) {
      preparedStatement.setInt(1, workItem.hash());
      preparedStatement.execute();
    } catch (SQLException e) {
      throw new ConsumerException(e);
    }
  }

  @Override
  public boolean supportsStatefulProcessing() {
    return true;
  }
}
