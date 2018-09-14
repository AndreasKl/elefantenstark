package net.andreaskluth.elefantenstark.consumer;

import static java.util.Objects.requireNonNull;
import static net.andreaskluth.elefantenstark.consumer.ConsumerQueries.OBTAIN_WORK_QUERY_SESSION_SCOPED;
import static net.andreaskluth.elefantenstark.consumer.ConsumerQueries.OBTAIN_WORK_QUERY_TRANSACTION_SCOPED;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import net.andreaskluth.elefantenstark.WorkItem;

public abstract class Consumer {

  private final String obtainWorkQuery;

  protected Consumer(String obtainWorkQuery) {
    requireNonNull(obtainWorkQuery, "obtainWorkQuery must not be null");
    this.obtainWorkQuery = obtainWorkQuery;
  }

  /**
   * Creates a {@link Consumer} using a <code>pg_try_advisory_xact_lock</code> query locking on the
   * key to obtain work form the work queue. If the {@link WorkItem} is processed the entry is
   * updated as not 'available'.
   */
  public static Consumer transactionScoped() {
    return new TransactionScopedConsumer(OBTAIN_WORK_QUERY_TRANSACTION_SCOPED);
  }

  /**
   * Creates a {@link Consumer} using a <code>pg_try_advisory_lock</code> query locking on the key
   * to obtain work form the work queue. If the {@link WorkItem} is processed the entry is updated
   * as not 'available'. If the JVM crashes e.g. with a SEG_FAULT while the database connection is
   * still in use and the lock was not released. The lock will not be freed until the connection is
   * closed.
   */
  public static Consumer sessionScoped() {
    return new SessionScopedConsumer(OBTAIN_WORK_QUERY_SESSION_SCOPED);
  }

  /**
   * Obtains the next work item from the queue and provides the data to the @worker. If the @worker
   * fails with an {@link Exception} the work is returned and available for the next @worker.
   *
   * @param connection the connection the work is retrieved from.
   * @param worker the worker consuming the @{@link WorkItem} to work on.
   */
  public abstract <T> Optional<T> next(Connection connection, java.util.function.Function<WorkItem, T> worker);

  protected Optional<WorkItemContext> fetchWorkAndLock(Connection connection) {
    try (Statement statement = connection.createStatement();
        ResultSet rawWorkEntry = statement.executeQuery(obtainWorkQuery())) {
      if (!rawWorkEntry.next()) {
        return Optional.empty();
      }
      return Optional.of(mapWorkEntryFrom(rawWorkEntry));
    } catch (SQLException e) {
      throw new ConsumerException(e);
    }
  }

  protected WorkItemContext mapWorkEntryFrom(ResultSet rawWorkEntry) throws SQLException {

    return new WorkItemContext(
        rawWorkEntry.getInt("id"),
        new WorkItem(
            rawWorkEntry.getString("key"),
            rawWorkEntry.getString("value"),
            rawWorkEntry.getLong("version")));
  }

  protected String obtainWorkQuery() {
    return obtainWorkQuery;
  }

  public static class WorkItemContext {

    private final int id;
    private final WorkItem workItem;

    protected WorkItemContext(int id, WorkItem workItem) {
      this.id = id;
      this.workItem = requireNonNull(workItem);
    }

    int id() {
      return id;
    }

    WorkItem workItem() {
      return workItem;
    }
  }

  public static class ConsumerException extends RuntimeException {

    protected ConsumerException(Throwable cause) {
      super(cause);
    }
  }
}
