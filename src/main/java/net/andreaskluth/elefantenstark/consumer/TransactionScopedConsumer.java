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
  public void next(Connection connection, java.util.function.Consumer<WorkItem> worker) {
    Objects.requireNonNull(connection);
    Objects.requireNonNull(worker);

    try {
      try {
        connection.setAutoCommit(false);
        Optional<WorkItemContext> workItemContext = fetchWorkAndLock(connection);
        workItemContext.ifPresent(wic -> worker.accept(wic.workItem()));
        connection.commit();
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
