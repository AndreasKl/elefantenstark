package net.andreaskluth.elefantenstark.consumer;

import static java.util.Objects.requireNonNull;
import static net.andreaskluth.elefantenstark.consumer.ConsumerQueries.SESSION_SCOPED_OBTAIN_WORK_QUERY;
import static net.andreaskluth.elefantenstark.consumer.ConsumerQueries.TRANSACTION_SCOPED_OBTAIN_WORK_QUERY;
import static net.andreaskluth.elefantenstark.work.WorkItemDataMapDeserializer.deserialize;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import java.util.function.Function;
import net.andreaskluth.elefantenstark.work.WorkItem;

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
   *
   * @return the postgres transaction scoped {@link Consumer}
   */
  public static Consumer transactionScoped() {
    return new TransactionScopedConsumer(TRANSACTION_SCOPED_OBTAIN_WORK_QUERY);
  }

  /**
   * Creates a {@link Consumer} using a <code>pg_try_advisory_lock</code> query locking on the key
   * to obtain work form the work queue. If the {@link WorkItem} is processed the entry is updated
   * as not 'available'. If the JVM crashes e.g. with a SEG_FAULT while the database connection is
   * still in use and the lock was not released. The lock will not be freed until the connection is
   * closed.
   *
   * @return the connection/session scoped {@link Consumer}
   */
  public static Consumer sessionScoped() {
    return new SessionScopedConsumer(SESSION_SCOPED_OBTAIN_WORK_QUERY);
  }

  /**
   * Obtains the next work item from the queue and provides the data to the @worker. If the @worker
   * fails with an {@link Exception} the work is returned and available for the next @worker.
   *
   * @param connection the connection the work is retrieved from.
   * @param worker the worker consuming the @{@link WorkItemContext} containing the {@link WorkItem}
   *     to work on.
   * @param <T> the kind of object the {@link Function} would return.
   * @return the {@link Optional} value that the worker passed to {@link Consumer#next(Connection,
   *     Function)} returns, or {@link Optional#empty()} if there was nothing to process.
   */
  public abstract <T> Optional<T> next(Connection connection, Function<WorkItemContext, T> worker);

  /**
   * @return whether the {@link Consumer} implementation can update the database state while holding
   *     a lock, that is still persisted when the held lock is rolled back.
   */
  public abstract boolean supportsStatefulProcessing();

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
        rawWorkEntry.getInt("times_processed"),
        new WorkItem(
            rawWorkEntry.getString("key"),
            rawWorkEntry.getString("value"),
            rawWorkEntry.getInt("hash"),
            rawWorkEntry.getLong("version"),
            deserialize(rawWorkEntry.getBytes("data_map"))));
  }

  protected String obtainWorkQuery() {
    return obtainWorkQuery;
  }

  public static class WorkItemContext {

    private final int id;
    private final int timesProcessed;
    private final WorkItem workItem;

    protected WorkItemContext(int id, int timesProcessed, WorkItem workItem) {
      this.id = id;
      this.timesProcessed = timesProcessed;
      this.workItem = requireNonNull(workItem);
    }

    public int id() {
      return id;
    }

    public int timesProcessed() {
      return timesProcessed;
    }

    public WorkItem workItem() {
      return workItem;
    }
  }

  public static class ConsumerException extends RuntimeException {
    private static final long serialVersionUID = 6127755713170973126L;

    public ConsumerException(Throwable cause) {
      super(cause);
    }
  }
}
