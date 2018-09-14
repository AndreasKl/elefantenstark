package net.andreaskluth.elefantenstark.consumer;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;
import net.andreaskluth.elefantenstark.WorkItem;

class TransactionScopedConsumer extends Consumer {

  TransactionScopedConsumer(String obtainWorkQuery) {
    super(obtainWorkQuery);
  }

  @Override
  public <T> Optional<T> next(
      Connection connection, java.util.function.Function<WorkItem, T> worker) {
    Objects.requireNonNull(connection);
    Objects.requireNonNull(worker);

    try {
      try {
        connection.setAutoCommit(false);
        Optional<T> result =
            fetchWorkAndLock(connection).map(WorkItemContext::workItem).map(worker);
        connection.commit();
        return result;
      } catch (Exception ex) {
        connection.rollback();
        throw ex;
      } finally {
        connection.setAutoCommit(true);
      }
    } catch (SQLException e) {
      throw new ConsumerException(e);
    }
  }
}
