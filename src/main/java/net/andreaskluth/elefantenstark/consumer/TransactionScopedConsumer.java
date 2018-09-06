package net.andreaskluth.elefantenstark.consumer;

import java.sql.Connection;
import java.sql.SQLException;
import net.andreaskluth.elefantenstark.WorkItem;

class TransactionScopedConsumer extends Consumer {

  TransactionScopedConsumer(String obtainWorkQuery) {
    super(obtainWorkQuery);
  }

  /**
   * Obtains the next work item from the queue and provides the data to the @worker. If the @worker
   * fails with an {@link Exception} the work is returned and available for the next @worker.
   *
   * @param worker the worker consuming the @{@link WorkItem} to work on.
   * @return a {@link java.util.function.Consumer} of {@link Connection}.
   */
  public java.util.function.Consumer<Connection> next(
      java.util.function.Consumer<WorkItem> worker) {

    return connection -> {
      try {
        try {
          connection.setAutoCommit(false);
          WorkItemContext workItemContext = fetchWorkAndLock(connection);
          if (workItemContext != null) {
            worker.accept(workItemContext.workItem());
          }
          connection.commit();
        } catch (Exception ex) {
          connection.rollback();
          throw ex;
        } finally {
          connection.setAutoCommit(true);
        }
      } catch (SQLException e) {
        throw new WorkConsumerException(e);
      }
    };
  }
}
