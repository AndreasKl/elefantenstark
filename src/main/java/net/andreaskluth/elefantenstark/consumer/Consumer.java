package net.andreaskluth.elefantenstark.consumer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import net.andreaskluth.elefantenstark.WorkItem;

import static java.util.Objects.requireNonNull;

public class Consumer {

  public static String OBTAIN_WORK_QUERY =
      "DELETE FROM queue "
          + "WHERE id = ( "
          + "  SELECT id "
          + "  FROM queue "
          + "  ORDER BY id"
          + "  FOR UPDATE SKIP LOCKED "
          + "  LIMIT 1 "
          + ")"
          + "RETURNING *;";

  private final String query;

  /**
   * Creates a {@link Consumer} using a <code>FOR UPDATE SKIP LOCKED</code> query to obtain work
   * form the work queue. If the {@link WorkItem} is processed the entry is deleted from the
   * database.
   */
  public Consumer() {
    this.query = OBTAIN_WORK_QUERY;
  }

  public Consumer(String obtainWorkQuery) {
    requireNonNull(obtainWorkQuery, "obtainWorkQuery must not be null");
    this.query = obtainWorkQuery;
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
          fetchWork(connection, worker);
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

  protected void fetchWork(Connection connection, java.util.function.Consumer<WorkItem> worker)
      throws SQLException {

    try (Statement statement = connection.createStatement();
        ResultSet rawWorkEntry = statement.executeQuery(query)) {;
      if (!rawWorkEntry.next()) {
        return;
      }
      worker.accept(mapWorkEntryFrom(rawWorkEntry));
    }
  }

  protected WorkItem mapWorkEntryFrom(ResultSet rawWorkEntry) throws SQLException {

    return new WorkItem(
        rawWorkEntry.getString("key"),
        rawWorkEntry.getString("value"),
        rawWorkEntry.getLong("version"));
  }

  public static class WorkConsumerException extends RuntimeException {
    WorkConsumerException(Throwable cause) {
      super(cause);
    }
  }
}
