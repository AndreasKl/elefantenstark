package net.andreaskluth.elefantenstark.consumer;

import static java.util.Objects.requireNonNull;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;

class TransactionScopedConsumer extends Consumer {

  TransactionScopedConsumer(String obtainWorkQuery) {
    super(obtainWorkQuery);
  }

  @Override
  public <T> Optional<T> next(
      Connection connection, java.util.function.Function<WorkItemContext, T> worker) {
    requireNonNull(connection);
    requireNonNull(worker);

    try {
      try {
        connection.setAutoCommit(false);
        Optional<T> result = fetchWorkAndLock(connection).map(worker);
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

  @Override
  public boolean supportsStatefulProcessing() {
    return false;
  }
}
