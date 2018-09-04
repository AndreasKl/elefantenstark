package net.andreaskluth.elefantenstark.consumer;

import static java.util.Objects.requireNonNull;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import net.andreaskluth.elefantenstark.WorkItem;

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

  public static String OBTAIN_WORK_QUERY_ADVANCED =
      "UPDATE"
          + " queue"
          + " SET available = FALSE"
          + " WHERE id ="
          + "    ("
          + "        SELECT id FROM "
          + "        ("
          + "            SELECT"
          + "                id, \"key\""
          + "            FROM queue"
          + "            WHERE available"
          + "            ORDER BY id"
          + "            LIMIT 16"
          + "        ) ss"
          + "        WHERE pg_try_advisory_xact_lock('queue'::regclass::int, ('x'||substr(md5(\"key\"),1,8))::bit(32)::int)"
          + "        ORDER BY id"
          + "        LIMIT 1"
          + "    )"
          + " AND available"
          + " RETURNING *;";

  private final String query;

  public Consumer(String obtainWorkQuery) {
    requireNonNull(obtainWorkQuery, "obtainWorkQuery must not be null");
    this.query = obtainWorkQuery;
  }

  /**
   * Creates a {@link Consumer} using a <code>FOR UPDATE SKIP LOCKED</code> query to obtain work
   * form the work queue. If the {@link WorkItem} is processed the entry is deleted from the
   * database.
   */
  public static Consumer simple() {
    return new Consumer(OBTAIN_WORK_QUERY);
  }

  /**
   * Creates a {@link Consumer} using a <code>pg_try_advisory_xact_lock</code> query locking on the
   * key to obtain work form the work queue. If the {@link WorkItem} is processed the entry is
   * updated as not 'available'.
   */
  public static Consumer advanced() {
    return new Consumer(OBTAIN_WORK_QUERY_ADVANCED);
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
